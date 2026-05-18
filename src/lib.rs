use std::{collections::HashMap, hash::Hash, io};

use fjall::{
    Database, Guard, KeyspaceCreateOptions, Keyspace, OwnedWriteBatch, Readable, Slice, Snapshot,
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

    fn frequencies(&self) -> (u64, u64) {
        match self {
            Self::SingleHit {
                docid: _,
                term_frequency,
            } => (1, *term_frequency),
            Self::MultiHit {
                doc_frequency,
                term_frequency,
            } => (*doc_frequency, *term_frequency),
        }
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

    /// Returns the number of documents in the block.
    pub fn len(&self) -> usize {
        self.docids.len()
    }

    /// Returns true if there are no documents in the block.
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.docids.is_empty()
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

/// Encode a single document's sorted position list as delta-compressed LEB128.
fn encode_positions(positions: &[u32]) -> Vec<u8> {
    let mut value = vec![];
    let mut prev = 0u32;
    for &pos in positions {
        leb128::write::unsigned(&mut value, (pos - prev) as u64).unwrap();
        prev = pos;
    }
    value
}

/// Decode a single document's position list produced by `encode_positions` into `out`.
fn decode_positions(value: impl AsRef<[u8]>, out: &mut Vec<PosId>) {
    let mut cursor = io::Cursor::new(value.as_ref());
    let mut prev = 0u32;
    while let Ok(delta) = leb128::read::unsigned(&mut cursor) {
        prev += delta as u32;
        out.push(prev);
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
    stats_keyspace: Keyspace,
    docpl_keyspace: Keyspace,
    pospl_keyspace: Option<Keyspace>,
}

impl TokenFieldIndex {
    pub fn new(
        db: &Database,
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

    fn pospl_key(&self, docid: DocId, token: impl AsRef<[u8]>) -> Vec<u8> {
        let token_ref = token.as_ref();
        let mut key = Vec::with_capacity(8 + 1 + self.name.len() + 1 + token_ref.len());
        key.extend_from_slice(&docid.to_be_bytes());
        key.push(b':');
        key.extend_from_slice(self.name.as_bytes());
        key.push(b':');
        key.extend_from_slice(token_ref);
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

enum TokenIndexBufferRep<T> {
    Doc(HashMap<T, Vec<u32>>),
    WithTermFrequency(HashMap<T, Vec<(u32, u32)>>),
    WithPositions(HashMap<T, Vec<(u32, Vec<u32>)>>),
}

/// Buffer for TokenFieldIndex mutations that can span multiple documents.
/// These changes can be applied to an index as a block of documents, amortizing costs related to
/// patching posting data across many docs.
///
/// This is likely to be helpful in the average case but may not help in cases where documents have
/// no overlap in their token data. This is unlikely for natural language input.
pub struct TokenIndexBuffer<T> {
    rep: TokenIndexBufferRep<T>,
    next_docid: u32,
}

impl<T: AsRef<[u8]> + Eq + Hash> TokenIndexBuffer<T> {
    /// Create a new buffer.
    ///
    /// `features` is used to minimize the size of the representation.
    // XXX maybe fields should generate these objects?
    pub fn new(features: TokenFieldFeatures) -> Self {
        let rep = match features {
            TokenFieldFeatures::Doc => TokenIndexBufferRep::Doc(HashMap::new()),
            TokenFieldFeatures::WithTermFrequency => {
                TokenIndexBufferRep::WithTermFrequency(HashMap::new())
            }
            TokenFieldFeatures::WithPositions => TokenIndexBufferRep::WithPositions(HashMap::new()),
        };
        Self { rep, next_docid: 0 }
    }

    /// Add a doc as a stream of (pos, token).
    pub fn add_doc(&mut self, tokens: impl IntoIterator<Item = (u32, T)>) {
        let docid = self.next_docid;
        self.next_docid += 1;

        match &mut self.rep {
            TokenIndexBufferRep::Doc(m) => {
                for (_, token) in tokens {
                    // Flatten repeated occurrences.
                    let pl = m.entry(token).or_default();
                    if pl.last().is_none_or(|d| *d != docid) {
                        pl.push(docid);
                    }
                }
            }
            TokenIndexBufferRep::WithTermFrequency(m) => {
                for (_, token) in tokens {
                    // Increment on repeated occurrences.
                    let pl = m.entry(token).or_default();
                    if let Some((d, tf)) = pl.last_mut()
                        && *d == docid
                    {
                        *tf += 1;
                    } else {
                        pl.push((docid, 1))
                    }
                }
            }
            TokenIndexBufferRep::WithPositions(m) => {
                for (pos, token) in tokens {
                    let pl = m.entry(token).or_default();
                    if let Some((d, pos_pl)) = pl.last_mut()
                        && *d == docid
                    {
                        pos_pl.push(pos);
                    } else {
                        pl.push((docid, vec![pos]))
                    }
                }
            }
        }
    }

    /// Apply this batch as an append to `field` starting at `start_docid`.
    ///
    /// Reads committed state from `snapshot`; writes are queued into `batch`.
    ///
    /// Returns the next free docid.
    ///
    /// _This method assumes that start_docid and all docids after are unused._
    pub fn apply_append(
        self,
        field: &TokenFieldIndex,
        start_docid: DocId,
        snapshot: &Snapshot,
        batch: &mut OwnedWriteBatch,
    ) -> fjall::Result<DocId> {
        match self.rep {
            TokenIndexBufferRep::Doc(m) => {
                for (token, pl) in m {
                    let doc_block = Self::update_stats_and_generate_doc_pl(
                        field,
                        token.as_ref(),
                        start_docid,
                        pl.iter().map(|&d| (d, 1)),
                        snapshot,
                        batch,
                    )?;
                    Self::update_doc_pl(field, token.as_ref(), doc_block, snapshot, batch)?;
                }
            }
            TokenIndexBufferRep::WithTermFrequency(m) => {
                for (token, pl) in m {
                    let doc_block = Self::update_stats_and_generate_doc_pl(
                        field,
                        token.as_ref(),
                        start_docid,
                        pl.iter().copied(),
                        snapshot,
                        batch,
                    )?;
                    Self::update_doc_pl(field, token.as_ref(), doc_block, snapshot, batch)?;
                }
            }
            TokenIndexBufferRep::WithPositions(m) => {
                for (token, pl) in m {
                    let doc_block = Self::update_stats_and_generate_doc_pl(
                        field,
                        token.as_ref(),
                        start_docid,
                        pl.iter().map(|(doc, pospl)| (*doc, pospl.len() as u32)),
                        snapshot,
                        batch,
                    )?;
                    Self::update_doc_pl(field, token.as_ref(), doc_block, snapshot, batch)?;
                    for (doc, positions) in pl {
                        let key = field.pospl_key(start_docid + doc as u64, token.as_ref());
                        batch.insert(
                            field.pospl_keyspace.as_ref().unwrap(),
                            key,
                            encode_positions(&positions),
                        );
                    }
                }
            }
        }
        Ok(start_docid + self.next_docid as DocId)
    }

    fn update_stats_and_generate_doc_pl(
        field: &TokenFieldIndex,
        token: &[u8],
        start_docid: DocId,
        mut postings: impl ExactSizeIterator<Item = (u32, u32)>,
        snapshot: &Snapshot,
        batch: &mut OwnedWriteBatch,
    ) -> fjall::Result<DocPostingBlock> {
        let mut block = DocPostingBlock::default();

        let stats_key = field.stats_key(token);
        let old_stats = snapshot
            .get(&field.stats_keyspace, &stats_key)?
            .and_then(TokenStats::decode);
        if let Some(TokenStats::SingleHit {
            docid,
            term_frequency,
        }) = old_stats.as_ref()
        {
            block.docids.push(*docid);
            block.term_frequencies.push(*term_frequency as u32);
        }

        let (stats, block) = if postings.len() == 1 && old_stats.is_none() {
            let (ld, tf) = postings.next().unwrap();
            let docid = start_docid + ld as u64;
            // SingleHit: leave block empty — no PL block is written for a single-doc term.
            (
                TokenStats::SingleHit {
                    docid,
                    term_frequency: tf as u64,
                },
                block,
            )
        } else {
            let (mut total_df, mut total_tf) = old_stats.map(|s| s.frequencies()).unwrap_or((0, 0));
            total_df += postings.len() as u64;
            for (ld, tf) in postings {
                block.docids.push(start_docid + ld as u64);
                block.term_frequencies.push(tf);
                total_tf += tf as u64;
            }
            (
                TokenStats::MultiHit {
                    doc_frequency: total_df,
                    term_frequency: total_tf,
                },
                block,
            )
        };

        batch.insert(
            &field.stats_keyspace,
            stats_key,
            stats.encode().as_ref(),
        );

        Ok(block)
    }

    fn update_doc_pl(
        field: &TokenFieldIndex,
        token: &[u8],
        block: DocPostingBlock,
        snapshot: &Snapshot,
        batch: &mut OwnedWriteBatch,
    ) -> fjall::Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        let mut block_it = block.docids.iter().zip(block.term_frequencies.iter());
        let start_pl_key = field.pl_key(token, 0);
        let end_pl_key = field.pl_key(token, block.first_docid().unwrap());
        if let Some(g) = snapshot
            .range(
                &field.docpl_keyspace,
                start_pl_key.as_slice()..=end_pl_key.as_slice(),
            )
            .next_back()
        {
            let (key, value) = g.into_inner().expect("no read error");
            let mut last_block =
                DocPostingBlock::decode(&key, &value, field.features).expect("doc decode");
            while last_block.len() < MAX_PL_BLOCK_SIZE {
                if let Some((doc, tf)) = block_it.next() {
                    last_block.docids.push(*doc);
                    last_block.term_frequencies.push(*tf);
                } else {
                    break;
                }
            }
            batch.insert(
                &field.docpl_keyspace,
                key,
                last_block.encode(field.features),
            );
        }

        while block_it.len() > 0 {
            let mut doc_block = DocPostingBlock::default();
            while doc_block.len() < MAX_PL_BLOCK_SIZE {
                if let Some((doc, tf)) = block_it.next() {
                    doc_block.docids.push(*doc);
                    doc_block.term_frequencies.push(*tf);
                } else {
                    break;
                }
            }

            let key = field.pl_key(token, doc_block.first_docid().unwrap());
            batch.insert(
                &field.docpl_keyspace,
                key,
                doc_block.encode(field.features),
            );
        }

        Ok(())
    }
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
    keyspace: Keyspace,
    /// Full pospl key with a placeholder docid in the first 8 bytes, updated before each lookup.
    key: Vec<u8>,
}

pub struct TokenPostingIterator {
    features: TokenFieldFeatures,
    size_hint: u64,

    snapshot: Snapshot,
    scratch_key: Vec<u8>,
    end_key: Vec<u8>,

    keyspace: Keyspace,
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
            key: field.pospl_key(0, &token),
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
        let pos_state = self.pos_state.as_mut().unwrap();
        pos_state.key[..8].copy_from_slice(&self.doc.to_be_bytes());
        let value = self
            .snapshot
            .get(&pos_state.keyspace, &pos_state.key)
            .expect("no io error")
            .expect("pos entry exists");
        decode_positions(value, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Fields are dropped in declaration order: db before tmpdir, ensuring the fjall
    // database is closed before the temporary directory is removed.
    struct Fixture {
        db: Database,
        _tmpdir: tempfile::TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let tmpdir = tempfile::tempdir().unwrap();
            let db = Database::builder(tmpdir.path()).open().unwrap();
            Self {
                db,
                _tmpdir: tmpdir,
            }
        }

        fn field(&self, name: &str, features: TokenFieldFeatures) -> TokenFieldIndex {
            TokenFieldIndex::new(&self.db, name, name, features, None).unwrap()
        }

        fn write(&self, field: &TokenFieldIndex, docid: u64, tokens: &[(u32, &[u8])]) {
            let mut buf = TokenIndexBuffer::new(field.features);
            buf.add_doc(tokens.iter().map(|&(p, t)| (p, t)));
            let snap = self.db.snapshot();
            let mut batch = self.db.batch();
            buf.apply_append(field, docid, &snap, &mut batch).unwrap();
            batch.commit().unwrap();
        }

        fn iter(&self, field: &TokenFieldIndex, token: &[u8]) -> Option<TokenPostingIterator> {
            let snap = self.db.snapshot();
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
