use std::{collections::HashMap, hash::Hash, io};

use fjall::{
    Guard, KeyspaceCreateOptions, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
    SingleWriterWriteTx, Slice, Snapshot,
};

/// Type for document identifiers.
pub type DocId = u64;
/// Type for in-document position identifiers.
pub type PosId = u32;

/// Statistics entry for a single token in the index.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TokenStats {
    /// Entry is actually a single hit entry in the index.
    /// In this case there isn't a document posting list associated with this term.
    SingleHit {
        /// DocId matching this token.
        docid: u64,
        /// Number of occurrences within the document.
        term_frequency: u64,
    },
    /// Entry maps to hits across multiple documents.
    MultiHit {
        /// Number of documents containing this term.
        doc_frequency: u64,
        /// Number of occurrences across all documents.
        term_frequency: u64,
    },
}

impl TokenStats {
    /// Decode from a raw byte array. Returns `None` if the input is too short.
    pub fn decode(encoded: impl AsRef<[u8]>) -> Option<Self> {
        let mut cursor = io::Cursor::new(encoded.as_ref());
        let doc = leb128::read::unsigned(&mut cursor).ok()?;
        let term = leb128::read::unsigned(&mut cursor).ok()?;
        if doc & 1 == 1 {
            Some(Self::SingleHit {
                docid: doc >> 1,
                term_frequency: term,
            })
        } else {
            Some(Self::MultiHit {
                doc_frequency: doc >> 1,
                term_frequency: term,
            })
        }
    }

    /// Encode this token stats into a byte array to be written
    pub fn encode(&self) -> EncodedTokenStats {
        let mut stats = EncodedTokenStats::default();
        let mut buf: &mut [u8] = &mut stats.0;
        match self {
            Self::SingleHit {
                docid,
                term_frequency,
            } => {
                leb128::write::unsigned(&mut buf, *docid << 1 | 1).unwrap();
                leb128::write::unsigned(&mut buf, *term_frequency).unwrap();
            }
            Self::MultiHit {
                doc_frequency,
                term_frequency,
            } => {
                leb128::write::unsigned(&mut buf, *doc_frequency << 1).unwrap();
                leb128::write::unsigned(&mut buf, *term_frequency).unwrap();
            }
        };
        let rem = buf.len();
        stats.1 = stats.0.len() - rem;
        stats
    }

    /// Return a the table key for `term` in `field`.
    pub fn key(field: &str, term: impl AsRef<[u8]>) -> Vec<u8> {
        let mut key = Vec::with_capacity(field.len() + "::stats".len() + term.as_ref().len());
        key.extend_from_slice(field.as_bytes());
        key.extend_from_slice(b":stats:");
        key.extend_from_slice(term.as_ref());
        key
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct EncodedTokenStats([u8; 24], usize);

impl AsRef<[u8]> for EncodedTokenStats {
    fn as_ref(&self) -> &[u8] {
        &self.0[..self.1]
    }
}

#[derive(Debug, Clone, Default)]
struct DocPostingBlock {
    docids: Vec<u64>,
    term_frequencies: Vec<u32>,
}

impl DocPostingBlock {
    pub fn first_docid(&self) -> Option<DocId> {
        self.docids.first().copied()
    }

    /// Iterator over all the docids in this block.
    pub fn doc_iter(&self) -> impl ExactSizeIterator<Item = DocId> + '_ {
        self.docids.iter().copied()
    }

    /// Returns the number of documents in the block.
    pub fn len(&self) -> usize {
        self.docids.len()
    }

    /// Returns true if there are no documents in the block.
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.docids.is_empty()
    }

    /// Insert docid in this block. If already present, update term_freq.
    /// Returns the rank (index) of the entry, which callers can use to keep a
    /// paired `PosPostingBlock` in sync.
    pub fn insert(&mut self, docid: u64, term_freq: u32) -> usize {
        match self.docids.binary_search(&docid) {
            Ok(i) => {
                self.term_frequencies[i] = term_freq;
                i
            }
            Err(i) => {
                self.docids.insert(i, docid);
                self.term_frequencies.insert(i, term_freq);
                i
            }
        }
    }

    /// Moves half of the entries in this block into a new block.
    pub fn split(&mut self) -> DocPostingBlock {
        let half = self.docids.len() / 2;
        DocPostingBlock {
            docids: self.docids.drain(half..).collect(),
            term_frequencies: self.term_frequencies.drain(half..).collect(),
        }
    }

    /// Decode a block from the key (contains first docid) and value.
    pub fn decode(
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        features: TokenFieldFeatures,
    ) -> Option<Self> {
        let first_docid = TokenFieldIndex::pl_key_extract_docid(&key)?;

        let mut cursor = io::Cursor::new(value.as_ref());
        let n = leb128::read::unsigned(&mut cursor).ok()? as usize;

        let mut docids = Vec::with_capacity(MAX_PL_BLOCK_SIZE + 1);
        docids.push(first_docid);
        for _ in 1..n {
            let delta = leb128::read::unsigned(&mut cursor).ok()?;
            docids.push(docids.last().unwrap() + delta);
        }

        let mut term_frequencies = Vec::with_capacity(MAX_PL_BLOCK_SIZE + 1);
        if features.has_term_frequency() {
            for _ in 0..n {
                term_frequencies.push(leb128::read::unsigned(&mut cursor).ok()? as u32);
            }
        } else {
            term_frequencies.resize(n, 1);
        }

        Some(Self {
            docids,
            term_frequencies,
        })
    }

    /// Load from a single (docid, tf) frequence provided in term stats.
    pub fn from_singleton(docid: DocId, tf: u64) -> Self {
        Self {
            docids: vec![docid],
            term_frequencies: vec![tf as u32],
        }
    }

    /// Encode a block. The caller is responsible for recording first_docid() as the starting point
    /// for all of the subsequent deltas.
    // TODO: include field type to avoid encoding term frequencies when unnecessary.
    pub fn encode(&self, features: TokenFieldFeatures) -> Vec<u8> {
        assert!(!self.docids.is_empty());
        let mut value = vec![];
        leb128::write::unsigned(&mut value, self.docids.len() as u64).unwrap();
        for w in self.docids.windows(2) {
            leb128::write::unsigned(&mut value, w[1] - w[0]).unwrap();
        }
        if features.has_term_frequency() {
            for f in self.term_frequencies.iter() {
                leb128::write::unsigned(&mut value, *f as u64).unwrap();
            }
        }
        value
    }
}

/// Positions posting block: parallel to `DocPostingBlock`, storing per-document
/// position lists. The block key uses the same `first_docid` anchor as the
/// corresponding doc posting block; callers derive which block to read from the
/// doc posting list.
#[derive(Debug, Clone, Default)]
struct PosPostingBlock {
    /// `positions[i]` is the sorted position list for the i-th document in the
    /// paired `DocPostingBlock`.
    positions: Vec<Vec<u32>>,
}

impl PosPostingBlock {
    /// Insert a position list at `rank` for a new document. `rank` is the
    /// index returned by `DocPostingBlock::insert` when inserting the same
    /// document.
    pub fn insert(&mut self, rank: usize, positions: Vec<u32>) {
        self.positions.insert(rank, positions);
    }

    /// Moves the upper half of entries into a new block, mirroring
    /// `DocPostingBlock::split`.
    pub fn split(&mut self) -> PosPostingBlock {
        let half = self.positions.len() / 2;
        PosPostingBlock {
            positions: self.positions.drain(half..).collect(),
        }
    }

    /// Decode a block from its value, using `freqs` to determine how many
    /// positions belong to each document (via term frequency).
    pub fn decode(
        value: impl AsRef<[u8]>,
        freqs: impl ExactSizeIterator<Item = u32>,
    ) -> Option<Self> {
        let mut cursor = io::Cursor::new(value.as_ref());
        let mut positions = Vec::with_capacity(freqs.len());
        for tf in freqs {
            let mut doc_positions = Vec::with_capacity(tf as usize);
            let mut prev = 0u32;
            for _ in 0..tf {
                let delta = leb128::read::unsigned(&mut cursor).ok()?;
                prev += delta as u32;
                doc_positions.push(prev);
            }
            positions.push(doc_positions);
        }
        Some(Self { positions })
    }

    /// Encode the block. Position counts are not stored; they are recovered from the paired
    /// `DocPostingBlock` on decode.
    pub fn encode(&self) -> Vec<u8> {
        assert!(!self.positions.is_empty());
        let mut value = vec![];
        for doc_positions in &self.positions {
            let mut prev = 0u32;
            for &pos in doc_positions {
                leb128::write::unsigned(&mut value, (pos - prev) as u64).unwrap();
                prev = pos;
            }
        }
        value
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub enum TokenFieldFeatures {
    Doc,
    WithTermFrequency,
    #[default]
    WithPositions,
}

impl TokenFieldFeatures {
    pub fn has_term_frequency(&self) -> bool {
        *self != TokenFieldFeatures::Doc
    }

    pub fn has_positions(&self) -> bool {
        *self == TokenFieldFeatures::WithPositions
    }
}

pub struct TokenFieldIndex {
    name: String,
    features: TokenFieldFeatures,
    stats_keyspace: SingleWriterTxKeyspace,
    docpl_keyspace: SingleWriterTxKeyspace,
    pospl_keyspace: Option<SingleWriterTxKeyspace>,
}

impl TokenFieldIndex {
    pub fn new(
        db: &SingleWriterTxDatabase,
        index: &str,
        name: &str,
        features: TokenFieldFeatures,
        keyspace_create_options: Option<KeyspaceCreateOptions>,
    ) -> fjall::Result<Self> {
        let stats_keyspace = db.keyspace(&format!("{index}.{name}.stats"), || {
            keyspace_create_options.clone().unwrap_or_default()
        })?;
        let docpl_keyspace = db.keyspace(&format!("{index}.{name}.docpl"), || {
            keyspace_create_options.clone().unwrap_or_default()
        })?;
        let pospl_keyspace = if features == TokenFieldFeatures::WithPositions {
            Some(db.keyspace(&format!("{index}.{name}.pospl"), || {
                keyspace_create_options.clone().unwrap_or_default()
            })?)
        } else {
            None
        };
        Ok(Self {
            name: name.to_string(),
            features,
            stats_keyspace,
            docpl_keyspace,
            pospl_keyspace,
        })
    }

    fn stats_key(&self, token: impl AsRef<[u8]>) -> Vec<u8> {
        let token_ref = token.as_ref();
        let mut key = Vec::with_capacity(self.name.len() + 1 + token_ref.len());
        key.extend_from_slice(self.name.as_bytes());
        key.push(b':');
        key.extend_from_slice(token_ref);
        key
    }

    fn pl_key(&self, token: impl AsRef<[u8]>, first_docid: DocId) -> Vec<u8> {
        let token_ref = token.as_ref();
        let mut key = Vec::with_capacity(self.name.len() + 1 + token_ref.len() + 9);
        key.extend_from_slice(self.name.as_bytes());
        key.push(b':');
        key.extend_from_slice(token_ref);
        key.push(b':');
        key.extend_from_slice(&first_docid.to_be_bytes());
        key
    }

    fn pl_key_extract_docid(key: impl AsRef<[u8]>) -> Option<DocId> {
        let key_ref = key.as_ref();
        // Field name and token text must be at least, plus separators and the docid: 2+2+8=12
        if key_ref.len() < 12 {
            return None;
        }

        // The byte before the start of the docid must be ':'.
        let docid_start = key_ref.len() - 8;
        if key_ref[docid_start - 1] == b':' {
            Some(u64::from_be_bytes(
                key_ref[docid_start..].try_into().unwrap(),
            ))
        } else {
            None
        }
    }
}

/// Maximum number of values in a single doc block.
const MAX_PL_BLOCK_SIZE: usize = 256;

/// Write a sequence of tokens into `field` at `docid`.
///
/// `tokens` is a stream of (position, term) so that callers may place more than one index term at
/// a single position.
///
/// Results are written to `tx` so that multiple fields and/or documents can be processed at once.
///
/// Returns an error if reads from the underlying transaction fail.
pub fn write_tokens<T: AsRef<[u8]> + Eq + Hash>(
    docid: DocId,
    field: &TokenFieldIndex,
    tokens: impl Iterator<Item = (u32, T)>,
    tx: &mut SingleWriterWriteTx<'_>,
) -> fjall::Result<()> {
    let mut index: HashMap<T, Vec<u32>> = HashMap::new();
    for (pos, token) in tokens {
        index.entry(token).or_default().push(pos);
    }

    for (token, positions) in index {
        let stats_key = field.stats_key(&token);
        let stats = tx
            .get(&field.stats_keyspace, &stats_key)?
            .and_then(TokenStats::decode);
        let tf = if field.features.has_term_frequency() {
            positions.len() as u64
        } else {
            1
        };

        let new_stats = match stats {
            None => {
                if let Some(pospl_keyspace) = field.pospl_keyspace.as_ref() {
                    let key = field.pl_key(&token, docid);
                    let mut pos_block = PosPostingBlock::default();
                    pos_block.insert(0, positions);
                    tx.insert(pospl_keyspace, &key, pos_block.encode());
                }
                TokenStats::SingleHit {
                    docid,
                    term_frequency: tf,
                }
            }
            Some(TokenStats::SingleHit {
                docid: hit_docid,
                term_frequency,
            }) => {
                // The single hit rep does not have an actual posting list. Insert one before
                // writing the posting entry.
                let mut doc_block = DocPostingBlock::default();
                doc_block.insert(hit_docid, term_frequency as u32);
                tx.insert(
                    &field.docpl_keyspace,
                    field.pl_key(&token, hit_docid),
                    doc_block.encode(field.features),
                );
                write_posting_entry(token.as_ref(), docid, positions, field, tx)?;
                TokenStats::MultiHit {
                    doc_frequency: 2,
                    term_frequency: term_frequency + tf,
                }
            }
            Some(TokenStats::MultiHit {
                doc_frequency,
                term_frequency,
            }) => {
                write_posting_entry(token.as_ref(), docid, positions, field, tx)?;
                TokenStats::MultiHit {
                    doc_frequency: doc_frequency + 1,
                    term_frequency: term_frequency + tf,
                }
            }
        };
        tx.insert(
            &field.stats_keyspace,
            &stats_key,
            new_stats.encode().as_ref(),
        );
    }

    Ok(())
}

fn write_posting_entry(
    token: &[u8],
    docid: DocId,
    positions: Vec<u32>,
    field: &TokenFieldIndex,
    tx: &mut SingleWriterWriteTx<'_>,
) -> fjall::Result<()> {
    let pl_start_key = field.pl_key(token, 0);
    let pl_end_key = field.pl_key(token, docid);
    let mut doc_pl_iter = tx.range(&field.docpl_keyspace, pl_start_key..=pl_end_key);
    let (mut pl_key, doc_pl_value) = doc_pl_iter
        .next_back()
        .or_else(|| doc_pl_iter.next())
        .expect("multi-hit pl must have at least one block")
        .into_inner()?;

    let mut doc_pl_block =
        DocPostingBlock::decode(&pl_key, doc_pl_value, field.features).expect("doc pl decode");

    let mut pos_pl_state = field
        .pospl_keyspace
        .as_ref()
        .map(|keyspace| {
            let pos_pl_value = tx.get(keyspace, &pl_key)?.expect("parallel pos pl block");
            Ok::<_, fjall::Error>((
                keyspace,
                PosPostingBlock::decode(
                    pos_pl_value,
                    doc_pl_block.term_frequencies.iter().copied(),
                )
                .expect("pos pl decode"),
            ))
        })
        .transpose()?;

    // Insert into both blocks at the same rank. Note that we are writing term frequency regardless
    // of settings, the data will be dropped at encoding time if it is not used.
    let insert_rank = doc_pl_block.insert(docid, positions.len() as u32);
    if let Some((_, pos_block)) = pos_pl_state.as_mut() {
        pos_block.insert(insert_rank, positions);
    }

    if doc_pl_block.len() > MAX_PL_BLOCK_SIZE {
        let doc_block_tail = doc_pl_block.split();
        let docid_tail = doc_block_tail.doc_iter().next().unwrap();
        let key = field.pl_key(token, docid_tail);
        tx.insert(
            &field.docpl_keyspace,
            &key,
            doc_block_tail.encode(field.features),
        );
        if let Some((keyspace, pos_block)) = pos_pl_state.as_mut() {
            let pos_block_tail = pos_block.split();
            tx.insert(keyspace, &key, pos_block_tail.encode());
        }
    }

    let old_pl_key_docid = doc_pl_block
        .doc_iter()
        .next()
        .expect("non-empty doc pl block");
    let new_pl_key_docid = doc_pl_block.doc_iter().next().unwrap();
    if old_pl_key_docid != new_pl_key_docid {
        tx.remove(&field.docpl_keyspace, pl_key.clone());
        if let Some((keyspace, _)) = pos_pl_state.as_ref() {
            tx.remove(keyspace, pl_key);
        }
        pl_key = Slice::from(field.pl_key(token, new_pl_key_docid));
    }

    tx.insert(
        &field.docpl_keyspace,
        pl_key.clone(),
        doc_pl_block.encode(field.features),
    );
    if let Some((keyspace, pos_block)) = pos_pl_state {
        tx.insert(keyspace, pl_key, pos_block.encode());
    }
    Ok(())
}

/// Value yielded by methods returning a DocId if there are no more results.
const DOC_ID_DONE: u64 = u64::MAX;
/// Value yielded by `doc()` when the iterator is not positioned.
const DOC_ID_UNPOSITIONED: DocId = u64::MAX - 1;

/// Trait for iterating over an ordered set of DocIds.
///
/// On creation iterator should be in an unpositioned state.
pub trait DocIdSetIterator: Send {
    /// Return the current doc.
    ///
    /// May return `DONE` if there are no more docs or `UNPOSITIONED` if this iterator has not
    /// been advanced at all.
    fn doc(&self) -> DocId;

    /// Advance to the next doc and return its id.
    ///
    /// This operation positions the iterator.
    fn next(&mut self) -> DocId;

    /// Advance to the next document in the list _at or after_ `target`.
    ///
    /// This operation positions the iterator.
    ///
    /// *Panics* if `target < doc()` on a positioned iterator.
    fn advance_to(&mut self, target: DocId) -> DocId;

    /// Return an estimate of the total number of doc hits this iterator will yield.
    fn size_hint(&self) -> u64;
}

/// Trait for iterating over posting data for a document, including positions.
pub trait PostingIterator: DocIdSetIterator {
    /// Value yielded by `next_pos()` if there are no more positions.
    const POS_DONE: PosId = u32::MAX;

    /// Return the features available on this posting.
    fn features(&self) -> TokenFieldFeatures;

    /// Return the number of matching positions on this doc.
    ///
    /// Will return 1 if `!features().has_term_frequency()`.
    ///
    /// *Panics* if called on an unpositioned iterator.
    fn term_frequency(&mut self) -> u32;

    /// Append the position of all hits within the document.
    ///
    /// This method will resize and fill `out`. If `!features().has_positions()` the output vector
    /// will be empty.
    fn append_positions(&mut self, out: &mut Vec<u32>);
}

struct DocBlockState {
    block: DocPostingBlock,
    index: usize,
}

struct PosState {
    keyspace: SingleWriterTxKeyspace,
    /// Current position block, loaded in parallel with `doc_block`. `None` for
    /// single-doc terms (their pos block is fetched lazily on demand).
    block: Option<PosPostingBlock>,
}

// TODO: generics -- Readable instead of Snapshot; AsRef<Keyspace> instead of SingleWriterTxKeyspace.
pub struct TokenPostingIterator {
    features: TokenFieldFeatures,
    size_hint: u64,

    snapshot: Snapshot,
    scratch_key: Vec<u8>,
    end_key: Vec<u8>,

    keyspace: SingleWriterTxKeyspace,
    /// `Some((docid, term_frequency))` when the stats entry is `SingleHit` and
    /// there is no doc posting list in the keyspace.
    single_doc: Option<(DocId, u64)>,
    doc_state: Option<DocBlockState>,
    pos_state: Option<PosState>,
    doc: DocId,
}

impl TokenPostingIterator {
    pub fn new(
        snapshot: Snapshot,
        field: &TokenFieldIndex,
        token: impl AsRef<[u8]>,
    ) -> fjall::Result<Option<Self>> {
        let stats_value = snapshot.get(&field.stats_keyspace, field.stats_key(&token))?;
        if stats_value.is_none() {
            return Ok(None);
        }

        let end_key = field.pl_key(&token, DOC_ID_DONE);
        let (size_hint, single_doc) =
            match TokenStats::decode(stats_value.unwrap()).expect("decode token stats") {
                TokenStats::SingleHit {
                    docid,
                    term_frequency,
                } => (1, Some((docid, term_frequency))),
                TokenStats::MultiHit {
                    doc_frequency,
                    term_frequency: _,
                } => (doc_frequency, None),
            };
        let pos_state = field.pospl_keyspace.as_ref().map(|ks| PosState {
            keyspace: ks.clone(),
            block: None,
        });
        Ok(Some(TokenPostingIterator {
            features: field.features,
            size_hint,
            snapshot,
            scratch_key: end_key.clone(),
            end_key,
            keyspace: field.docpl_keyspace.clone(),
            single_doc,
            doc_state: None,
            pos_state,
            doc: DOC_ID_UNPOSITIONED,
        }))
    }

    fn update_scratch_key(&mut self, target: DocId) {
        self.scratch_key[(self.end_key.len() - 8)..].copy_from_slice(&target.to_be_bytes());
    }

    /// Load the block that contains target or the first doc after target.
    /// Updates doc and doc_block, returning doc.
    fn advance_to_block(&mut self, target: DocId) -> DocId {
        if let Some((docid, tf)) = self.single_doc {
            if target <= docid {
                self.doc_state = Some(DocBlockState {
                    block: DocPostingBlock::from_singleton(docid, tf),
                    index: 0,
                });
                self.doc = docid;
            } else {
                self.doc_state = None;
                self.doc = DOC_ID_DONE;
            }
            self.doc
        } else {
            // TODO: consider keying blocks by the last docid (or a docid after the last docid)
            // instead of the first docid. It would fix the double table seek below since we could
            // seek to max(target, last_block_docid + 1).
            //
            // This has to be a double seek as there's no single range that represents what we want,
            // which is the first block after the last docid in this block but at or just before the
            // target docid.
            self.update_scratch_key(target);
            // Find the last block whose first docid <= target. If there is no such block or that block
            // is the same as the current block, find the block that starts after `target`.
            let current_block_docid = self.doc_state.as_ref().and_then(|s| s.block.first_docid());
            let block_entry = self
                .snapshot
                .range(&self.keyspace, ..=self.scratch_key.as_slice())
                .next_back()
                .map(Guard::into_inner)
                .filter(|g| {
                    g.as_ref().is_ok_and(|(k, _)| {
                        current_block_docid
                            .zip(TokenFieldIndex::pl_key_extract_docid(k))
                            .is_none_or(|(cd, td)| cd != td)
                    })
                })
                .or_else(|| {
                    self.snapshot
                        .range(
                            &self.keyspace,
                            self.scratch_key.as_slice()..self.end_key.as_slice(),
                        )
                        .next()
                        .map(Guard::into_inner)
                });
            self.update_doc_block(block_entry.transpose())
        }
    }

    fn next_block(&mut self) -> DocId {
        if let Some((docid, tf)) = self.single_doc {
            if self.doc_state.take().is_some() {
                self.doc = DOC_ID_DONE;
            } else {
                self.doc_state = Some(DocBlockState {
                    block: DocPostingBlock::from_singleton(docid, tf),
                    index: 0,
                });
                self.doc = docid;
            }
            self.doc
        } else {
            let target_block_docid = self
                .doc_state
                .as_ref()
                .map(|s| *s.block.docids.first().unwrap() + 1)
                .unwrap_or(0);
            self.update_scratch_key(target_block_docid);
            self.update_doc_block(
                self.snapshot
                    .range(
                        &self.keyspace,
                        self.scratch_key.as_slice()..self.end_key.as_slice(),
                    )
                    .next()
                    .map(Guard::into_inner)
                    .transpose(),
            )
        }
    }

    fn update_doc_block(&mut self, block_entry: fjall::Result<Option<(Slice, Slice)>>) -> DocId {
        // Invalidate cached pos block whenever the doc block changes.
        if let Some(ps) = self.pos_state.as_mut() {
            ps.block = None;
        }
        if let Some((key, value)) = block_entry.expect("no read error") {
            let block =
                DocPostingBlock::decode(&key, &value, self.features).expect("doc pl decode");
            self.doc = block.first_docid().unwrap();
            self.doc_state = Some(DocBlockState { block, index: 0 });
        } else {
            self.doc_state = None;
            self.doc = DOC_ID_DONE;
        }
        self.doc
    }
}

impl DocIdSetIterator for TokenPostingIterator {
    fn doc(&self) -> DocId {
        self.doc
    }

    fn next(&mut self) -> DocId {
        if self.doc == DOC_ID_UNPOSITIONED {
            return self.next_block();
        }

        if self.doc_state.is_none() {
            self.doc = DOC_ID_DONE;
            return self.doc;
        }

        let state = self.doc_state.as_mut().unwrap();
        state.index += 1;
        if state.index < state.block.len() {
            self.doc = state.block.docids[state.index];
        } else {
            self.next_block();
        }
        self.doc
    }

    fn advance_to(&mut self, target: DocId) -> DocId {
        if self.doc == DOC_ID_UNPOSITIONED {
            if let Some((d, _)) = self.single_doc {
                if target <= d {
                    self.doc = target;
                } else {
                    self.doc = DOC_ID_DONE;
                }
            } else {
                self.doc = 0;
            }
        }
        assert!(self.doc <= target, "may not advance_to() backwards");

        if self.doc == DOC_ID_DONE {
            return DOC_ID_DONE;
        }

        // If the current block cannot contain target, then load the needed block. If no block
        // contains the target then exit.
        if self
            .doc_state
            .as_ref()
            .is_none_or(|s| *s.block.docids.last().unwrap() < target)
            && self.advance_to_block(target) == DOC_ID_DONE
        {
            return self.doc;
        }

        let state = self.doc_state.as_mut().expect("advance_to loaded block");
        let last_block_docid = *state.block.docids.last().unwrap();
        if target <= last_block_docid {
            state.index += match state.block.docids[state.index..].binary_search(&target) {
                Ok(i) | Err(i) => i,
            };
            self.doc = state.block.docids[state.index];
        } else {
            self.advance_to_block(last_block_docid + 1);
        }
        self.doc
    }

    fn size_hint(&self) -> u64 {
        self.size_hint
    }
}

impl PostingIterator for TokenPostingIterator {
    fn features(&self) -> TokenFieldFeatures {
        self.features
    }

    fn term_frequency(&mut self) -> u32 {
        if !self.features.has_term_frequency() {
            return 1;
        }
        if let Some(doc_state) = self.doc_state.as_ref() {
            doc_state.block.term_frequencies[doc_state.index]
        } else {
            self.single_doc.map_or(1, |(_, tf)| tf as u32)
        }
    }

    fn append_positions(&mut self, out: &mut Vec<u32>) {
        assert_ne!(self.doc, DOC_ID_UNPOSITIONED);
        out.clear();
        if !self.features.has_positions() || self.doc == DOC_ID_DONE {
            return;
        }

        assert!(self.doc_state.is_some());
        if self.pos_state.as_ref().unwrap().block.is_none() {
            let block_docid = self
                .doc_state
                .as_ref()
                .unwrap()
                .block
                .first_docid()
                .unwrap();
            self.update_scratch_key(block_docid);
            let value = self
                .snapshot
                .get(
                    &self.pos_state.as_ref().unwrap().keyspace,
                    &self.scratch_key,
                )
                .expect("no io error")
                .expect("parallel pos block exists");
            let pos_block = if let Some((_, tf)) = self.single_doc {
                PosPostingBlock::decode(value, [tf as u32].into_iter())
            } else {
                PosPostingBlock::decode(
                    value,
                    self.doc_state
                        .as_ref()
                        .unwrap()
                        .block
                        .term_frequencies
                        .iter()
                        .copied(),
                )
            }
            .expect("decode pos block");
            self.pos_state.as_mut().unwrap().block = Some(pos_block);
        }

        let doc_state = self.doc_state.as_ref().unwrap();
        let pos_block = self.pos_state.as_ref().unwrap().block.as_ref().unwrap();
        out.extend_from_slice(&pos_block.positions[doc_state.index]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::SingleWriterTxDatabase;

    // Fields are dropped in declaration order: db before tmpdir, ensuring the fjall
    // database is closed before the temporary directory is removed.
    struct Fixture {
        db: SingleWriterTxDatabase,
        _tmpdir: tempfile::TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let tmpdir = tempfile::tempdir().unwrap();
            let db = SingleWriterTxDatabase::builder(tmpdir.path())
                .open()
                .unwrap();
            Self {
                db,
                _tmpdir: tmpdir,
            }
        }

        fn field(&self, name: &str, features: TokenFieldFeatures) -> TokenFieldIndex {
            TokenFieldIndex::new(&self.db, name, name, features, None).unwrap()
        }

        fn write(&self, field: &TokenFieldIndex, docid: u64, tokens: &[(u32, &[u8])]) {
            let mut tx = self.db.write_tx();
            write_tokens(docid, field, tokens.iter().map(|&(p, t)| (p, t)), &mut tx).unwrap();
            tx.commit().unwrap();
        }

        fn iter(&self, field: &TokenFieldIndex, token: &[u8]) -> Option<TokenPostingIterator> {
            let snap = self.db.read_tx();
            TokenPostingIterator::new(snap, field, token).unwrap()
        }
    }

    fn collect_docs(mut it: TokenPostingIterator) -> Vec<DocId> {
        let mut docs = vec![];
        loop {
            let d = it.next();
            if d == DOC_ID_DONE {
                break;
            }
            docs.push(d);
        }
        docs
    }

    /// Drain the iterator into `(docid, term_frequency, positions)` triples.
    fn collect_postings(mut it: TokenPostingIterator) -> Vec<(DocId, u32, Vec<u32>)> {
        let mut out = vec![];
        let mut pos = vec![];
        loop {
            let d = it.next();
            if d == DOC_ID_DONE {
                break;
            }
            let tf = it.term_frequency();
            it.append_positions(&mut pos);
            out.push((d, tf, pos.clone()));
        }
        out
    }

    // --- TokenFieldFeatures::Doc ---

    #[test]
    fn doc_single_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        fix.write(&field, 5, &[(0, b"hello")]);

        let mut it = fix.iter(&field, b"hello").unwrap();
        assert_eq!(it.next(), 5);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    #[test]
    fn doc_multi_hit_next() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        for docid in [1u64, 3, 7, 9] {
            fix.write(&field, docid, &[(0, b"word")]);
        }
        assert_eq!(
            collect_docs(fix.iter(&field, b"word").unwrap()),
            [1, 3, 7, 9]
        );
    }

    #[test]
    fn doc_advance_to_exact() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        for docid in [2u64, 4, 6, 8] {
            fix.write(&field, docid, &[(0, b"token")]);
        }

        let mut it = fix.iter(&field, b"token").unwrap();
        assert_eq!(it.advance_to(4), 4);
        assert_eq!(it.advance_to(6), 6);
        assert_eq!(it.next(), 8);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    #[test]
    fn doc_advance_to_gap() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        for docid in [10u64, 20, 30] {
            fix.write(&field, docid, &[(0, b"gap")]);
        }

        let mut it = fix.iter(&field, b"gap").unwrap();
        // advance_to a value between 10 and 20 should land on 20
        assert_eq!(it.advance_to(15), 20);
        assert_eq!(it.advance_to(25), 30);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    #[test]
    fn doc_missing_token_returns_none() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        assert!(fix.iter(&field, b"ghost").is_none());
    }

    // --- TokenFieldFeatures::WithTermFrequency ---

    #[test]
    fn with_tf_single_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithTermFrequency);
        fix.write(&field, 1, &[(0, b"hi"), (3, b"hi"), (7, b"hi")]);

        let mut it = fix.iter(&field, b"hi").unwrap();
        assert_eq!(it.next(), 1);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    #[test]
    fn with_tf_multi_hit_next() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithTermFrequency);
        fix.write(&field, 1, &[(0, b"a"), (1, b"a")]);
        fix.write(&field, 5, &[(0, b"a")]);
        fix.write(&field, 9, &[(0, b"a"), (1, b"a"), (2, b"a")]);

        assert_eq!(collect_docs(fix.iter(&field, b"a").unwrap()), [1, 5, 9]);
    }

    #[test]
    fn with_tf_advance_to() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithTermFrequency);
        for docid in [1u64, 2, 3, 4, 5] {
            fix.write(&field, docid, &[(0, b"b")]);
        }

        let mut it = fix.iter(&field, b"b").unwrap();
        assert_eq!(it.advance_to(3), 3);
        assert_eq!(it.next(), 4);
        assert_eq!(it.advance_to(5), 5);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    // --- TokenFieldFeatures::WithPositions ---

    #[test]
    fn with_pos_single_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 42, &[(5, b"pos"), (10, b"pos")]);

        let mut it = fix.iter(&field, b"pos").unwrap();
        assert_eq!(it.next(), 42);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    #[test]
    fn with_pos_multi_hit_next() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 1, &[(0, b"p"), (2, b"p")]);
        fix.write(&field, 3, &[(1, b"p")]);
        fix.write(&field, 6, &[(0, b"p"), (4, b"p"), (8, b"p")]);

        assert_eq!(collect_docs(fix.iter(&field, b"p").unwrap()), [1, 3, 6]);
    }

    #[test]
    fn with_pos_advance_to_gap() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        for docid in [10u64, 20, 30, 40] {
            fix.write(&field, docid, &[(0, b"q")]);
        }

        let mut it = fix.iter(&field, b"q").unwrap();
        assert_eq!(it.advance_to(11), 20);
        assert_eq!(it.advance_to(30), 30);
        assert_eq!(it.next(), 40);
        assert_eq!(it.next(), DOC_ID_DONE);
    }

    // --- PostingIterator: features() ---

    #[test]
    fn posting_features() {
        let fix = Fixture::new();
        for (features, expected) in [
            (TokenFieldFeatures::Doc, TokenFieldFeatures::Doc),
            (
                TokenFieldFeatures::WithTermFrequency,
                TokenFieldFeatures::WithTermFrequency,
            ),
            (
                TokenFieldFeatures::WithPositions,
                TokenFieldFeatures::WithPositions,
            ),
        ] {
            let field = fix.field(&format!("f_{features:?}"), features);
            fix.write(&field, 1, &[(0, b"x")]);
            let mut it = fix.iter(&field, b"x").unwrap();
            assert_eq!(it.features(), expected);
            it.next();
            assert_eq!(it.features(), expected);
        }
    }

    // --- PostingIterator: term_frequency() ---

    #[test]
    fn tf_doc_always_one() {
        // Doc mode does not store real frequencies; must always return 1.
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        fix.write(&field, 1, &[(0, b"w"), (1, b"w"), (2, b"w")]);
        fix.write(&field, 2, &[(0, b"w")]);

        let postings = collect_postings(fix.iter(&field, b"w").unwrap());
        assert_eq!(postings, [(1, 1, vec![]), (2, 1, vec![])]);
    }

    #[test]
    fn tf_with_tf_single_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithTermFrequency);
        fix.write(&field, 7, &[(0, b"hi"), (3, b"hi"), (9, b"hi")]);

        let postings = collect_postings(fix.iter(&field, b"hi").unwrap());
        assert_eq!(postings, [(7, 3, vec![])]);
    }

    #[test]
    fn tf_with_tf_multi_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithTermFrequency);
        fix.write(&field, 1, &[(0, b"a"), (1, b"a")]);
        fix.write(&field, 5, &[(0, b"a")]);
        fix.write(&field, 9, &[(0, b"a"), (1, b"a"), (2, b"a")]);

        let postings = collect_postings(fix.iter(&field, b"a").unwrap());
        assert_eq!(postings, [(1, 2, vec![]), (5, 1, vec![]), (9, 3, vec![])]);
    }

    #[test]
    fn tf_with_pos_single_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 42, &[(5, b"p"), (10, b"p")]);

        let mut it = fix.iter(&field, b"p").unwrap();
        assert_eq!(it.next(), 42);
        assert_eq!(it.term_frequency(), 2);
    }

    #[test]
    fn tf_with_pos_multi_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 1, &[(0, b"p"), (2, b"p")]);
        fix.write(&field, 3, &[(1, b"p")]);
        fix.write(&field, 6, &[(0, b"p"), (4, b"p"), (8, b"p")]);

        let mut it = fix.iter(&field, b"p").unwrap();
        assert_eq!(it.next(), 1);
        assert_eq!(it.term_frequency(), 2);
        assert_eq!(it.next(), 3);
        assert_eq!(it.term_frequency(), 1);
        assert_eq!(it.next(), 6);
        assert_eq!(it.term_frequency(), 3);
    }

    // --- PostingIterator: append_positions() ---

    #[test]
    fn positions_doc_always_empty() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        fix.write(&field, 1, &[(0, b"x"), (1, b"x")]);
        fix.write(&field, 2, &[(0, b"x")]);

        let postings = collect_postings(fix.iter(&field, b"x").unwrap());
        assert!(postings.iter().all(|(_, _, pos)| pos.is_empty()));
    }

    #[test]
    fn positions_with_tf_always_empty() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithTermFrequency);
        fix.write(&field, 1, &[(0, b"x"), (5, b"x")]);
        fix.write(&field, 2, &[(0, b"x")]);

        let postings = collect_postings(fix.iter(&field, b"x").unwrap());
        assert!(postings.iter().all(|(_, _, pos)| pos.is_empty()));
    }

    #[test]
    fn positions_with_pos_single_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 42, &[(5, b"pos"), (10, b"pos")]);

        let postings = collect_postings(fix.iter(&field, b"pos").unwrap());
        assert_eq!(postings, [(42, 2, vec![5, 10])]);
    }

    #[test]
    fn positions_with_pos_multi_hit() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 1, &[(0, b"p"), (2, b"p")]);
        fix.write(&field, 3, &[(1, b"p")]);
        fix.write(&field, 6, &[(0, b"p"), (4, b"p"), (8, b"p")]);

        let postings = collect_postings(fix.iter(&field, b"p").unwrap());
        assert_eq!(
            postings,
            [(1, 2, vec![0, 2]), (3, 1, vec![1]), (6, 3, vec![0, 4, 8]),]
        );
    }

    #[test]
    fn positions_after_advance_to() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 10, &[(1, b"q"), (5, b"q")]);
        fix.write(&field, 20, &[(3, b"q")]);
        fix.write(&field, 30, &[(0, b"q"), (7, b"q")]);

        let mut it = fix.iter(&field, b"q").unwrap();
        let mut pos = vec![];

        assert_eq!(it.advance_to(20), 20);
        it.append_positions(&mut pos);
        assert_eq!(pos, [3]);

        assert_eq!(it.next(), 30);
        it.append_positions(&mut pos);
        assert_eq!(pos, [0, 7]);
    }

    #[test]
    fn positions_append_idempotent() {
        // Calling append_positions twice on the same doc replaces rather than appends.
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        fix.write(&field, 1, &[(3, b"r"), (6, b"r")]);
        fix.write(&field, 2, &[(0, b"r")]);

        let mut it = fix.iter(&field, b"r").unwrap();
        let mut pos = vec![];

        it.next();
        it.append_positions(&mut pos);
        it.append_positions(&mut pos);
        assert_eq!(pos, [3, 6]);
    }

    #[test]
    fn positions_across_block_boundary() {
        // Positions must be correct for docs in both the first and second block, and
        // the cached pos block must be invalidated when crossing the boundary.
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::WithPositions);
        let n = MAX_PL_BLOCK_SIZE as u64 + 3;
        for docid in 0..n {
            // Give each doc a unique position equal to its docid, plus a second at docid+1000.
            fix.write(
                &field,
                docid,
                &[(docid as u32, b"x"), (docid as u32 + 1000, b"x")],
            );
        }

        let mut it = fix.iter(&field, b"x").unwrap();
        let mut pos = vec![];

        // Sample from the first block.
        assert_eq!(it.next(), 0);
        it.append_positions(&mut pos);
        assert_eq!(pos, [0, 1000]);

        // Jump into the second block.
        let second_block_start = MAX_PL_BLOCK_SIZE as u64;
        assert_eq!(it.advance_to(second_block_start), second_block_start);
        it.append_positions(&mut pos);
        assert_eq!(
            pos,
            [second_block_start as u32, second_block_start as u32 + 1000]
        );
    }

    // --- multi-block iteration ---

    #[test]
    fn multi_block_next() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        let n = (MAX_PL_BLOCK_SIZE + MAX_PL_BLOCK_SIZE / 2) as u64;
        for docid in 0..n {
            fix.write(&field, docid, &[(0, b"big")]);
        }

        let docs = collect_docs(fix.iter(&field, b"big").unwrap());
        assert_eq!(docs.len(), n as usize);
        assert_eq!(docs, (0..n).collect::<Vec<_>>());
    }

    #[test]
    fn multi_block_advance_to_second_block() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        let n = (MAX_PL_BLOCK_SIZE + 10) as u64;
        for docid in 0..n {
            fix.write(&field, docid, &[(0, b"blk")]);
        }

        let target = MAX_PL_BLOCK_SIZE as u64 + 5;
        let mut it = fix.iter(&field, b"blk").unwrap();
        assert_eq!(it.advance_to(target), target);
        assert_eq!(it.next(), target + 1);
    }

    // --- advance_to from unpositioned ---

    #[test]
    fn advance_to_from_unpositioned() {
        let fix = Fixture::new();
        let field = fix.field("f", TokenFieldFeatures::Doc);
        for docid in [5u64, 10, 15] {
            fix.write(&field, docid, &[(0, b"z")]);
        }

        let mut it = fix.iter(&field, b"z").unwrap();
        assert_eq!(it.advance_to(10), 10);
        assert_eq!(it.next(), 15);
        assert_eq!(it.next(), DOC_ID_DONE);
    }
}
