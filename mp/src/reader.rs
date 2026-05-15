use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

/// A record parsed from the binary line-docs file.
#[derive(Debug)]
pub struct IndexRecord {
    pub title: String,
    pub body: String,
    pub random_label: String,
    /// Hour×3600 + min×60 + sec for the document timestamp.
    #[allow(unused)]
    pub time_of_day_secs: i32,
    /// Milliseconds since the Unix epoch.
    #[allow(unused)]
    pub timestamp_ms: i64,
}

/// Iterator that parses `IndexRecord`s from a binary line-docs file produced by
/// `buildBinaryLineDocs.py`.
///
/// File layout:
/// ```text
/// repeat {
///   [4 bytes i]  number of docs in this chunk
///   [4 bytes i]  byte length of the chunk payload
///   [N bytes]    chunk payload (concatenated doc records)
/// }
/// ```
/// Each doc record in the payload:
/// ```text
/// [4 bytes i]  title length (UTF-8)
/// [4 bytes i]  body length (UTF-8)
/// [4 bytes i]  randomLabel length (UTF-8)
/// [4 bytes i]  time-of-day in seconds
/// [8 bytes l]  milliseconds since epoch
/// [N bytes]    title (UTF-8)
/// [M bytes]    body (UTF-8)
/// [K bytes]    randomLabel (UTF-8)
/// ```
/// All integers are native byte order, matching Python's unadorned struct format.
pub struct BinDocReader<R> {
    reader: R,
    chunk_remaining: u32,
    chunk_buf: Vec<u8>,
    chunk_pos: usize,
}

impl BinDocReader<BufReader<File>> {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self::new(BufReader::new(File::open(path)?)))
    }
}

impl<R: Read> BinDocReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            chunk_remaining: 0,
            chunk_buf: Vec::new(),
            chunk_pos: 0,
        }
    }

    fn read_next_chunk(&mut self) -> io::Result<bool> {
        let mut header = [0u8; 8];
        match self.reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        let num_docs = i32::from_ne_bytes(header[0..4].try_into().unwrap()) as u32;
        let payload_len = i32::from_ne_bytes(header[4..8].try_into().unwrap()) as usize;
        self.chunk_buf.resize(payload_len, 0);
        self.reader.read_exact(&mut self.chunk_buf)?;
        self.chunk_remaining = num_docs;
        self.chunk_pos = 0;
        Ok(true)
    }

    fn parse_record(&mut self) -> io::Result<IndexRecord> {
        const HEADER_LEN: usize = 24; // 4×i + 1×l
        let buf = &self.chunk_buf[self.chunk_pos..];
        if buf.len() < HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated record header",
            ));
        }
        let title_len = i32::from_ne_bytes(buf[0..4].try_into().unwrap()) as usize;
        let body_len = i32::from_ne_bytes(buf[4..8].try_into().unwrap()) as usize;
        let label_len = i32::from_ne_bytes(buf[8..12].try_into().unwrap()) as usize;
        let time_of_day_secs = i32::from_ne_bytes(buf[12..16].try_into().unwrap());
        let timestamp_ms = i64::from_ne_bytes(buf[16..24].try_into().unwrap());

        let title_start = HEADER_LEN;
        let body_start = title_start + title_len;
        let label_start = body_start + body_len;
        let record_end = label_start + label_len;

        if buf.len() < record_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated record payload",
            ));
        }

        let to_string = |bytes: &[u8]| {
            String::from_utf8(bytes.to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        };
        let title = to_string(&buf[title_start..body_start])?;
        let body = to_string(&buf[body_start..label_start])?;
        let random_label = to_string(&buf[label_start..record_end])?;

        self.chunk_pos += record_end;
        self.chunk_remaining -= 1;

        Ok(IndexRecord {
            title,
            body,
            random_label,
            time_of_day_secs,
            timestamp_ms,
        })
    }
}

impl<R: Read> Iterator for BinDocReader<R> {
    type Item = io::Result<IndexRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.chunk_remaining > 0 {
                return Some(self.parse_record());
            }
            match self.read_next_chunk() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
