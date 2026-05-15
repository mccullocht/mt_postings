mod reader;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use fjall::{KeyspaceCreateOptions, SingleWriterTxDatabase};
use indicatif::{ProgressBar, ProgressStyle};
use mt_postings::{TokenFieldFeatures, TokenFieldIndex, write_tokens};
use reader::BinDocReader;
use tantivy::tokenizer::{SimpleTokenizer, TokenStream, Tokenizer};

#[derive(Parser)]
#[command(name = "mp", about = "mt_postings CLI")]
struct Cli {
    /// Path to the fjall db directory.
    #[arg(long)]
    db: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Ingest {
        /// Path to the prepared binary doc file containing the index data.
        #[arg(long)]
        bin_file: PathBuf,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Drop all keyspaces for the ingested fields.
    Drop,
    /// Print estimated byte size of each section of each field.
    Stat,
    /// Flush memtables and run major compaction on all keyspaces.
    Compact,
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1_024;
    const MB: u64 = 1_024 * KB;
    const GB: u64 = 1_024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn tokenize(text: &str) -> Vec<(u32, String)> {
    let mut tokenizer = SimpleTokenizer::default();
    let mut stream = tokenizer.token_stream(text);
    let mut tokens = Vec::new();
    while let Some(token) = stream.next() {
        tokens.push((token.position as u32, token.text.clone()));
    }
    tokens
}

fn ingest(db: &PathBuf, bin_file: &PathBuf, limit: Option<usize>) {
    let database = SingleWriterTxDatabase::builder(db)
        .max_journaling_size(64 * 1_024 * 1_024)
        .open()
        .expect("failed to open fjall database");

    let ks_opts = KeyspaceCreateOptions::default().max_memtable_size(8 * 1_024 * 1_024);
    let title_field = TokenFieldIndex::new(
        &database,
        "enwiki",
        "title",
        TokenFieldFeatures::WithPositions,
        Some(ks_opts.clone()),
    )
    .expect("failed to create title field index");
    let body_field = TokenFieldIndex::new(
        &database,
        "enwiki",
        "body",
        TokenFieldFeatures::WithPositions,
        Some(ks_opts.clone()),
    )
    .expect("failed to create body field index");
    let random_label_field = TokenFieldIndex::new(
        &database,
        "enwiki",
        "random_label",
        TokenFieldFeatures::WithPositions,
        Some(ks_opts),
    )
    .expect("failed to create random_label field index");

    let pb = match limit {
        Some(n) => {
            let pb = ProgressBar::new(n as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{pos}/{len} docs [{wide_bar}] {per_sec} eta {eta_precise} elapsed {elapsed_precise}")
                    .unwrap(),
            );
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner} {pos} docs {per_sec} {elapsed_precise}")
                    .unwrap(),
            );
            pb
        }
    };

    let reader = BinDocReader::open(bin_file).expect("failed to open bin file");
    for (docid, record) in reader.take(limit.unwrap_or(usize::MAX)).enumerate() {
        let record = record.expect("failed to read record");
        let mut tx = database.write_tx();
        write_tokens(
            docid as u64,
            &title_field,
            tokenize(&record.title).into_iter(),
            &mut tx,
        )
        .expect("failed to write title tokens");
        write_tokens(
            docid as u64,
            &body_field,
            tokenize(&record.body).into_iter(),
            &mut tx,
        )
        .expect("failed to write body tokens");
        write_tokens(
            docid as u64,
            &random_label_field,
            tokenize(&record.random_label).into_iter(),
            &mut tx,
        )
        .expect("failed to write random_label tokens");
        tx.commit().expect("failed to commit transaction");
        pb.inc(1);
    }
    database
        .persist(fjall::PersistMode::SyncAll)
        .expect("persist");
    pb.finish_with_message("done");
}

fn stat(db: &PathBuf) {
    let database = SingleWriterTxDatabase::builder(db)
        .open()
        .expect("failed to open fjall database");

    for field in ["title", "body", "random_label"] {
        println!("{field}:");
        for section in ["stats", "docpl", "pospl"] {
            let name = format!("enwiki.{field}.{section}");
            if database.keyspace_exists(&name) {
                let ks = database
                    .keyspace(&name, KeyspaceCreateOptions::default)
                    .expect("failed to open keyspace");
                println!("  {section}: {}", format_bytes(ks.inner().disk_space()));
            } else {
                println!("  {section}: (not found)");
            }
        }
    }
}

fn compact(db: &PathBuf) {
    let database = SingleWriterTxDatabase::builder(db)
        .open()
        .expect("failed to open fjall database");

    let start = std::time::Instant::now();
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg} [{elapsed_precise}]")
            .unwrap(),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    for field in ["title", "body", "random_label"] {
        for section in ["stats", "docpl", "pospl"] {
            let name = format!("enwiki.{field}.{section}");
            if database.keyspace_exists(&name) {
                pb.set_message(format!("compacting {name}"));
                let ks = database
                    .keyspace(&name, KeyspaceCreateOptions::default)
                    .expect("failed to open keyspace");
                ks.inner()
                    .rotate_memtable_and_wait()
                    .expect("failed to flush memtable");
                ks.inner().major_compact().expect("failed to compact");
            }
        }
    }

    pb.finish_with_message(format!("done in {:.1}s", start.elapsed().as_secs_f64()));
}

fn drop(db: &PathBuf) {
    let database = SingleWriterTxDatabase::builder(db)
        .open()
        .expect("failed to open fjall database");

    for field in ["title", "body", "random_label"] {
        for section in ["stats", "docpl", "pospl"] {
            let name = format!("enwiki.{field}.{section}");
            if database.keyspace_exists(&name) {
                let ks = database
                    .keyspace(&name, KeyspaceCreateOptions::default)
                    .expect("failed to open keyspace");
                database
                    .inner()
                    .delete_keyspace(ks.inner().clone())
                    .expect("failed to delete keyspace");
                println!("dropped {name}");
            } else {
                println!("skipped {name} (does not exist)");
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Ingest { bin_file, limit } => ingest(&cli.db, &bin_file, limit),
        Command::Stat => stat(&cli.db),
        Command::Compact => compact(&cli.db),
        Command::Drop => drop(&cli.db),
    }
}
