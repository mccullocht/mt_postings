mod reader;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use fjall::{Database, KeyspaceCreateOptions};
use indicatif::{ProgressBar, ProgressStyle};
use mt_postings::{TokenFieldFeatures, TokenFieldIndex, TokenIndexBuffer};
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
        /// Number of documents to buffer before flushing and committing.
        #[arg(long, default_value_t = 1)]
        batch_size: usize,
        /// Maximum journal size in megabytes before a flush is triggered.
        #[arg(long, default_value_t = 64)]
        journaling_size_mb: u64,
        /// Maximum memtable size in megabytes per keyspace before a flush is triggered.
        #[arg(long, default_value_t = 8)]
        memtable_size_mb: u64,
        /// Block cache size in megabytes.
        #[arg(long, default_value_t = 64)]
        cache_size_mb: u64,
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

fn ingest(
    db: &PathBuf,
    bin_file: &PathBuf,
    limit: Option<usize>,
    batch_size: usize,
    journaling_size_mb: u64,
    memtable_size_mb: u64,
    cache_size_mb: u64,
) {
    let database = Database::builder(db)
        .max_journaling_size(journaling_size_mb * 1_024 * 1_024)
        .cache_size(cache_size_mb * 1_024 * 1_024)
        .open()
        .expect("failed to open fjall database");

    let ks_opts =
        KeyspaceCreateOptions::default().max_memtable_size(memtable_size_mb * 1_024 * 1_024);
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

    let mut next_docid: u64 = 0;
    let mut title_buf: TokenIndexBuffer<String> =
        TokenIndexBuffer::new(TokenFieldFeatures::WithPositions);
    let mut body_buf: TokenIndexBuffer<String> =
        TokenIndexBuffer::new(TokenFieldFeatures::WithPositions);
    let mut random_label_buf: TokenIndexBuffer<String> =
        TokenIndexBuffer::new(TokenFieldFeatures::WithPositions);
    let mut batch_count = 0usize;

    let flush = |title_buf: TokenIndexBuffer<String>,
                 body_buf: TokenIndexBuffer<String>,
                 random_label_buf: TokenIndexBuffer<String>,
                 start_docid: u64|
     -> u64 {
        let snap = database.snapshot();
        let mut batch = database.batch();
        let next = title_buf
            .apply_append(&title_field, start_docid, &snap, &mut batch)
            .expect("failed to apply title buffer");
        body_buf
            .apply_append(&body_field, start_docid, &snap, &mut batch)
            .expect("failed to apply body buffer");
        random_label_buf
            .apply_append(&random_label_field, start_docid, &snap, &mut batch)
            .expect("failed to apply random_label buffer");
        batch.commit().expect("failed to commit batch");
        next
    };

    let mut total_bytes = 0u64;
    let reader = BinDocReader::open(bin_file).expect("failed to open bin file");
    for record in reader.take(limit.unwrap_or(usize::MAX)) {
        let record = record.expect("failed to read record");
        total_bytes += (record.title.len() + record.body.len() + record.random_label.len()) as u64;
        title_buf.add_doc(tokenize(&record.title).into_iter());
        body_buf.add_doc(tokenize(&record.body).into_iter());
        random_label_buf.add_doc(tokenize(&record.random_label).into_iter());
        batch_count += 1;
        pb.inc(1);

        if batch_count >= batch_size {
            next_docid = flush(title_buf, body_buf, random_label_buf, next_docid);
            title_buf = TokenIndexBuffer::new(TokenFieldFeatures::WithPositions);
            body_buf = TokenIndexBuffer::new(TokenFieldFeatures::WithPositions);
            random_label_buf = TokenIndexBuffer::new(TokenFieldFeatures::WithPositions);
            batch_count = 0;
        }
    }

    if batch_count > 0 {
        flush(title_buf, body_buf, random_label_buf, next_docid);
    }

    database
        .persist(fjall::PersistMode::SyncAll)
        .expect("persist");
    let elapsed_secs = pb.elapsed().as_secs_f64();
    let gb_per_hour = (total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) / (elapsed_secs / 3600.0);
    pb.finish();
    println!("{gb_per_hour:.2} GB/hour");
}

fn stat(db: &PathBuf) {
    let database = Database::builder(db)
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
                println!("  {section}: {}", format_bytes(ks.disk_space()));
            } else {
                println!("  {section}: (not found)");
            }
        }
    }
}

fn compact(db: &PathBuf) {
    let database = Database::builder(db)
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
                ks.rotate_memtable_and_wait()
                    .expect("failed to flush memtable");
                ks.major_compact().expect("failed to compact");
            }
        }
    }

    pb.finish_with_message(format!("done in {:.1}s", start.elapsed().as_secs_f64()));
}

fn drop(db: &PathBuf) {
    let database = Database::builder(db)
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
                    .delete_keyspace(ks)
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
        Command::Ingest {
            bin_file,
            limit,
            batch_size,
            journaling_size_mb,
            memtable_size_mb,
            cache_size_mb,
        } => ingest(
            &cli.db,
            &bin_file,
            limit,
            batch_size,
            journaling_size_mb,
            memtable_size_mb,
            cache_size_mb,
        ),
        Command::Stat => stat(&cli.db),
        Command::Compact => compact(&cli.db),
        Command::Drop => drop(&cli.db),
    }
}
