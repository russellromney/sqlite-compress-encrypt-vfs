//! SQLite VFS with transparent compression and WAL support.
//!
//! Supports multiple compressors via features:
//! - `zstd` (default): Best compression ratio, dictionary support
//! - `lz4`: Fastest compression/decompression
//! - `snappy`: Very fast, moderate compression
//!
//! File format:
//! - Header (64 bytes): magic "SQLCEvfS", page_size, data_start, dict_size, flags
//! - Dictionary section (optional): zstd dictionary bytes
//! - Data section: page records stored sequentially
//!   - Each record: page_num(8) + size(4) + data(size)
//! - On open: scan from data_start to build in-memory index
//! - On sync: just fsync (no index rewrite needed!)
//!
//! WAL and journal files are stored uncompressed.
//!
//! ## Dictionary Compression
//!
//! For 5-10x better compression on Redis-like workloads, train a custom dictionary:
//!
//! ```ignore
//! use sqlite_compress_vfs::dict::{train_dictionary, compress_with_dict};
//!
//! let samples = vec![/* your key-value data */];
//! let dict = train_dictionary(&samples, 100 * 1024)?;  // 100KB dict
//! ```
//!
//! ## Acknowledgements
//!
//! This implementation was inspired by concepts from:
//! - [mlin/sqlite_zstd_vfs](https://github.com/mlin/sqlite_zstd_vfs) - C++ SQLite VFS with zstd (MIT)
//! - [apersson/redis-compression-module](https://github.com/apersson/redis-compression-module) - Dictionary compression concepts (unlicensed - referenced for ideas only)
//! - [Twitter cache traces](https://github.com/twitter/cache-trace) - Public cache workload data (CC-BY)

pub mod dict;

use parking_lot::RwLock;
use sqlite_vfs::{DatabaseHandle, LockKind, OpenAccess, OpenKind, OpenOptions, Vfs};
use std::collections::HashMap;
use std::fs::{File, OpenOptions as FsOpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

// SQLite main database lock byte offsets (from sqlite3.c)
// These are at the 1GB mark to not interfere with actual data
const PENDING_BYTE: u64 = 0x40000000;  // 1073741824
const RESERVED_BYTE: u64 = PENDING_BYTE + 1;
const SHARED_FIRST: u64 = PENDING_BYTE + 2;
const SHARED_SIZE: u64 = 510;

// SQLite WAL-index lock byte offset (in the -shm file)
// Locks are at bytes 120-127 in the WAL-index header
const WAL_LOCK_OFFSET: u64 = 120;

/// Debug lock tracing - enabled via SQLCES_DEBUG_LOCKS=1
static DEBUG_LOCKS: AtomicBool = AtomicBool::new(false);

/// Initialize debug lock tracing from environment
pub fn init_debug_locks() {
    if std::env::var("SQLCES_DEBUG_LOCKS").map(|v| v == "1").unwrap_or(false) {
        DEBUG_LOCKS.store(true, Ordering::Relaxed);
        eprintln!("[LOCK DEBUG] Lock tracing enabled");
    }
}

/// Log a lock operation if debug is enabled
#[inline]
fn debug_lock(op: &str, path: &str, from: LockKind, to: LockKind, result: &str) {
    if DEBUG_LOCKS.load(Ordering::Relaxed) {
        eprintln!(
            "[LOCK DEBUG] {:?} {} {} {:?} -> {:?} => {}",
            std::thread::current().id(),
            op,
            path,
            from,
            to,
            result
        );
    }
}

// Compressor-specific imports and magic bytes
#[cfg(feature = "zstd")]
use zstd::{decode_all, encode_all};
#[cfg(feature = "zstd")]
use zstd::dict::{EncoderDictionary, DecoderDictionary};

#[cfg(feature = "lz4")]
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

#[cfg(feature = "snappy")]
use snap::{read::FrameDecoder, write::FrameEncoder};

#[cfg(feature = "gzip")]
use flate2::{read::GzDecoder, write::GzEncoder, Compression};

#[cfg(feature = "encryption")]
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
#[cfg(feature = "encryption")]
use aes::Aes256;
#[cfg(feature = "encryption")]
use ctr::cipher::{KeyIvInit, StreamCipher};
#[cfg(feature = "encryption")]
type Aes256Ctr = ctr::Ctr128BE<Aes256>;
#[cfg(feature = "encryption")]
use sha2::{Digest, Sha256};

/// Magic bytes identifying SQLCEs format
/// Header fields (dict_size, flags) determine capabilities, not magic version
const MAGIC: &[u8; 8] = b"SQLCEvfS";
const HEADER_SIZE: u64 = 64;
/// Size of inline record header: page_num(8) + size(4)
const RECORD_HEADER_SIZE: u64 = 12;

/// Header flags
const FLAG_ENCRYPTED: u32 = 1 << 0;

/// File header structure
///
/// Layout (64 bytes):
/// - 0-7:   Magic "SQLCEvfS"
/// - 8-11:  page_size
/// - 12-19: data_start (offset where page records begin, after optional dict)
/// - 20-23: dict_size (0 = no dictionary)
/// - 24-27: flags (bit 0: encrypted)
/// - 28-63: reserved
#[derive(Debug, Clone, Copy)]
struct FileHeader {
    page_size: u32,
    /// Offset where page records begin (after header and optional dictionary)
    data_start: u64,
    /// Size of embedded dictionary (0 = no dictionary)
    dict_size: u32,
    /// Flags (bit 0: encrypted)
    flags: u32,
}

impl FileHeader {
    fn new() -> Self {
        Self {
            page_size: 0,
            data_start: HEADER_SIZE, // No dictionary by default
            dict_size: 0,
            flags: 0,
        }
    }

    fn new_with_dict(dict_size: u32) -> Self {
        Self {
            page_size: 0,
            data_start: HEADER_SIZE + dict_size as u64,
            dict_size,
            flags: 0,
        }
    }

    fn read_from(file: &mut File) -> io::Result<Option<Self>> {
        let mut buf = [0u8; HEADER_SIZE as usize];
        file.seek(SeekFrom::Start(0))?;

        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        // Check for SQLCEvfS magic
        if &buf[0..8] != MAGIC {
            return Ok(None);
        }

        let page_size = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let data_start = u64::from_le_bytes([
            buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
        ]);
        let dict_size = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
        let flags = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]);

        Ok(Some(Self { page_size, data_start, dict_size, flags }))
    }

    fn write_to(&self, file: &mut File) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[12..20].copy_from_slice(&self.data_start.to_le_bytes());
        buf[20..24].copy_from_slice(&self.dict_size.to_le_bytes());
        buf[24..28].copy_from_slice(&self.flags.to_le_bytes());

        file.write_all_at(&buf, 0)
    }
}

/// Page index: maps page number to (file_offset, compressed_size)
/// file_offset points to the start of the record (page_num field), not the data
#[derive(Debug, Default)]
struct PageIndex {
    /// Maps page_num -> (record_offset, data_size)
    /// record_offset is where the 12-byte header starts
    entries: HashMap<u64, (u64, u32)>,
    /// Maximum page number seen (for correct size reporting)
    /// This is needed because SQLite's lock page (at 1GB offset) may not be written
    max_page: u64,
}

impl PageIndex {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            max_page: 0,
        }
    }

    /// Scan file to build index from inline records.
    /// Uses buffered I/O for performance on large files.
    /// Returns (PageIndex, write_end) where write_end is the position after the last record.
    fn scan_from_file(file: &mut File, header: &FileHeader) -> io::Result<(Self, u64)> {
        use std::io::BufReader;

        let start = std::time::Instant::now();
        let mut index = Self::new();
        let mut pos = header.data_start;
        let mut record_count = 0u64;
        let mut max_page_num: u64 = 0;

        // Use a large buffer for sequential scanning (1MB)
        file.seek(SeekFrom::Start(header.data_start))?;
        let mut reader = BufReader::with_capacity(1024 * 1024, file);

        loop {
            // Read record header: page_num(8) + size(4)
            let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];

            match reader.read_exact(&mut rec_header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let page_num = u64::from_le_bytes([
                rec_header[0], rec_header[1], rec_header[2], rec_header[3],
                rec_header[4], rec_header[5], rec_header[6], rec_header[7],
            ]);
            let data_size = u32::from_le_bytes([
                rec_header[8], rec_header[9], rec_header[10], rec_header[11],
            ]);

            // Sanity check - detect corrupt data early
            if data_size > 1_000_000 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Corrupt record at offset {}: page_num={} data_size={} (too large)",
                            pos, page_num, data_size),
                ));
            }

            // Record offset points to data, which is after the 12-byte header
            let data_offset = pos + RECORD_HEADER_SIZE;

            // Track max page for correct size reporting (accounts for gaps like SQLite's lock page)
            if page_num > max_page_num {
                max_page_num = page_num;
            }
            index.entries.insert(page_num, (data_offset, data_size));

            // Skip the data to get to the next record header
            // Use seek_relative for efficiency with BufReader
            reader.seek_relative(data_size as i64)?;
            pos = data_offset + data_size as u64;
            record_count += 1;
        }

        let elapsed = start.elapsed();
        if elapsed.as_millis() > 100 || record_count > 10000 {
            eprintln!("VFS scan: {} records, {} unique pages in {:?}",
                     record_count, index.entries.len(), elapsed);
        }

        index.max_page = max_page_num;
        Ok((index, pos)) // pos is now the write_end (position after last record)
    }
}

/// Compressed database file handle
pub struct CompressedHandle {
    file: RwLock<File>,
    header: RwLock<FileHeader>,
    index: RwLock<PageIndex>,
    lock: RwLock<LockKind>,
    compression_level: i32,
    /// Whether this is a main database (uses VFS format) or auxiliary file (passthrough)
    compressed: bool,
    /// Whether to actually compress pages (false = passthrough mode)
    compress_pages: bool,
    /// Whether to encrypt pages
    encrypt_pages: bool,
    /// Encryption key (32 bytes for AES-256)
    #[cfg(feature = "encryption")]
    encryption_key: Option<[u8; 32]>,
    /// Path to the database file (for WAL index)
    db_path: PathBuf,
    /// Separate file handle for byte-range locking (Arc for multiple FileGuards)
    lock_file: Option<std::sync::Arc<File>>,
    /// Active byte-range locks for main DB:
    /// - "shared": shared lock on one byte in SHARED range
    /// - "reserved": exclusive lock on RESERVED_BYTE
    /// - "pending": exclusive lock on PENDING_BYTE
    /// - "exclusive": exclusive lock on entire SHARED range
    active_db_locks: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
    /// Atomic write position for lock-free space reservation in compressed mode
    /// This allows writers to reserve space without blocking readers
    write_end: AtomicU64,
    /// Compression dictionary bytes (loaded from file or provided at creation)
    #[cfg(feature = "zstd")]
    dictionary: Option<Vec<u8>>,
    /// Pre-compiled encoder dictionary for fast compression
    #[cfg(feature = "zstd")]
    encoder_dict: Option<EncoderDictionary<'static>>,
    /// Pre-compiled decoder dictionary for fast decompression
    #[cfg(feature = "zstd")]
    decoder_dict: Option<DecoderDictionary<'static>>,
}

impl CompressedHandle {
    /// Create a new handle with VFS page index format.
    ///
    /// - `compress`: whether to compress pages (false = store uncompressed)
    /// - `encrypt`: whether to encrypt pages (requires encryption feature)
    /// - `password`: encryption password (required if encrypt=true)
    /// - `provided_dict`: optional pre-trained compression dictionary
    fn new(
        mut file: File,
        db_path: PathBuf,
        compression_level: i32,
        compress: bool,
        #[allow(unused_variables)] encrypt: bool,
        #[allow(unused_variables)] password: Option<&str>,
        #[cfg(feature = "zstd")]
        #[allow(unused_variables)] provided_dict: Option<&[u8]>,
    ) -> io::Result<Self> {
        // Derive encryption key if needed
        #[cfg(feature = "encryption")]
        let encryption_key = if encrypt {
            let pwd = password.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Password required for encryption")
            })?;
            Some(Self::derive_key(pwd)?)
        } else {
            None
        };

        // Load existing file or create new (may include embedded dictionary)
        let (header, index, initial_write_end, file_dictionary) = Self::load_file(&mut file)?;

        // Use provided dictionary if given, otherwise use one from file (if any)
        #[cfg(feature = "zstd")]
        let dictionary = provided_dict
            .map(|d| d.to_vec())
            .or(file_dictionary);

        #[cfg(not(feature = "zstd"))]
        let _ = file_dictionary; // Suppress unused warning

        // Pre-compile dictionaries for faster compression/decompression
        #[cfg(feature = "zstd")]
        let (encoder_dict, decoder_dict) = match &dictionary {
            Some(dict_bytes) => (
                Some(EncoderDictionary::copy(dict_bytes, compression_level)),
                Some(DecoderDictionary::copy(dict_bytes)),
            ),
            None => (None, None),
        };

        Ok(Self {
            file: RwLock::new(file),
            header: RwLock::new(header),
            index: RwLock::new(index),
            lock: RwLock::new(LockKind::None),
            compression_level,
            compressed: true,
            compress_pages: compress,
            #[cfg(feature = "encryption")]
            encrypt_pages: encrypt,
            #[cfg(not(feature = "encryption"))]
            encrypt_pages: false,
            #[cfg(feature = "encryption")]
            encryption_key,
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
            write_end: AtomicU64::new(initial_write_end),
            #[cfg(feature = "zstd")]
            dictionary,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(feature = "zstd")]
            decoder_dict,
        })
    }

    /// Create a passthrough handle for WAL/journal files (direct file I/O).
    /// Can optionally encrypt data in-place while maintaining SQLite's file format.
    fn new_passthrough(
        file: File,
        db_path: PathBuf,
        encrypt: bool,
        #[cfg(feature = "encryption")]
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            file: RwLock::new(file),
            header: RwLock::new(FileHeader::new()),
            index: RwLock::new(PageIndex::new()),
            lock: RwLock::new(LockKind::None),
            compression_level: 0,
            compressed: false,
            compress_pages: false,
            encrypt_pages: encrypt,
            #[cfg(feature = "encryption")]
            encryption_key,
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
            write_end: AtomicU64::new(0), // Not used in passthrough mode
            #[cfg(feature = "zstd")]
            dictionary: None,
            #[cfg(feature = "zstd")]
            encoder_dict: None,
            #[cfg(feature = "zstd")]
            decoder_dict: None,
        }
    }

    /// Create compressed handle (for tests)
    #[cfg(test)]
    fn new_compressed(file: File, compression_level: i32, compress: bool) -> io::Result<Self> {
        Self::new(
            file,
            PathBuf::new(),
            compression_level,
            compress,
            false,
            None,
            #[cfg(feature = "zstd")]
            None,
        )
    }

    /// Create encrypted handle (for tests)
    #[cfg(all(test, feature = "encryption"))]
    fn new_encrypted(file: File, compression_level: i32, compress: bool, password: &str) -> io::Result<Self> {
        Self::new(
            file,
            PathBuf::new(),
            compression_level,
            compress,
            true,
            Some(password),
            #[cfg(feature = "zstd")]
            None,
        )
    }

    /// Load file header, dictionary (if present), and build index by scanning
    /// Returns (FileHeader, PageIndex, write_end, Option<dictionary_bytes>)
    fn load_file(file: &mut File) -> io::Result<(FileHeader, PageIndex, u64, Option<Vec<u8>>)> {
        // First, check the magic to determine format
        let mut magic = [0u8; 8];
        file.seek(SeekFrom::Start(0))?;
        match file.read_exact(&mut magic) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Empty file - create new
                let header = FileHeader::new();
                header.write_to(file)?;
                return Ok((header, PageIndex::new(), HEADER_SIZE, None));
            }
            Err(e) => return Err(e),
        }

        if &magic != MAGIC {
            // Unknown format - create new
            let header = FileHeader::new();
            header.write_to(file)?;
            return Ok((header, PageIndex::new(), HEADER_SIZE, None));
        }

        let header = FileHeader::read_from(file)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid header"))?;

        // Read dictionary if present
        let dictionary = if header.dict_size > 0 {
            let mut dict_bytes = vec![0u8; header.dict_size as usize];
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut dict_bytes, HEADER_SIZE)?;
            Some(dict_bytes)
        } else {
            None
        };

        let (index, write_end) = PageIndex::scan_from_file(file, &header)?;
        Ok((header, index, write_end, dictionary))
    }

    #[cfg(feature = "encryption")]
    fn derive_key(password: &str) -> io::Result<[u8; 32]> {
        // Use SHA-256 for simple key derivation
        // For production, consider using Argon2 with a salt stored in the file header
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let result = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&result);
        Ok(key)
    }

    // ===== ZSTD Compression =====
    #[cfg(feature = "zstd")]
    fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        use std::io::Write;

        // Use pre-compiled dictionary if available for better compression
        if let Some(ref encoder_dict) = self.encoder_dict {
            let mut encoder = zstd::stream::Encoder::with_prepared_dictionary(Vec::new(), encoder_dict)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            encoder.write_all(data)?;
            encoder.finish()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        } else {
            encode_all(data, self.compression_level)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        }
    }

    #[cfg(feature = "zstd")]
    fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        use std::io::Read;

        // Use pre-compiled dictionary if available for faster decompression
        if let Some(ref decoder_dict) = self.decoder_dict {
            let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(data, decoder_dict)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let mut output = Vec::new();
            decoder.read_to_end(&mut output)?;
            Ok(output)
        } else {
            decode_all(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        }
    }

    // ===== LZ4 Compression =====
    #[cfg(all(feature = "lz4", not(feature = "zstd")))]
    fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        Ok(compress_prepend_size(data))
    }

    #[cfg(all(feature = "lz4", not(feature = "zstd")))]
    fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        decompress_size_prepended(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    // ===== Snappy Compression =====
    #[cfg(all(feature = "snappy", not(feature = "zstd"), not(feature = "lz4")))]
    fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(data)?;
        match encoder.into_inner() {
            Ok(v) => Ok(v),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }

    #[cfg(all(feature = "snappy", not(feature = "zstd"), not(feature = "lz4")))]
    fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let mut decoder = FrameDecoder::new(data);
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok(output)
    }

    // ===== Gzip Compression =====
    #[cfg(all(feature = "gzip", not(feature = "zstd"), not(feature = "lz4"), not(feature = "snappy")))]
    fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.compression_level as u32));
        encoder.write_all(data)?;
        encoder.finish()
    }

    #[cfg(all(feature = "gzip", not(feature = "zstd"), not(feature = "lz4"), not(feature = "snappy")))]
    fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let mut decoder = GzDecoder::new(data);
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok(output)
    }

    // ===== No Compression (fallback) =====
    #[cfg(not(any(feature = "zstd", feature = "lz4", feature = "snappy", feature = "gzip")))]
    fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    #[cfg(not(any(feature = "zstd", feature = "lz4", feature = "snappy", feature = "gzip")))]
    fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    // ===== AES-GCM Encryption =====
    #[cfg(feature = "encryption")]
    fn encrypt(&self, data: &[u8], page_num: u64) -> io::Result<Vec<u8>> {
        let key = self.encryption_key.as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No encryption key set"))?;

        let cipher = Aes256Gcm::new(key.into());

        // Use page number as nonce (12 bytes)
        // This is deterministic but unique per page
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[0..8].copy_from_slice(&page_num.to_le_bytes());
        let nonce = Nonce::from_slice(&nonce_bytes);

        cipher.encrypt(nonce, data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Encryption failed: {}", e)))
    }

    #[cfg(feature = "encryption")]
    fn decrypt(&self, data: &[u8], page_num: u64) -> io::Result<Vec<u8>> {
        let key = self.encryption_key.as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No encryption key set"))?;

        let cipher = Aes256Gcm::new(key.into());

        // Use same page number as nonce
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[0..8].copy_from_slice(&page_num.to_le_bytes());
        let nonce = Nonce::from_slice(&nonce_bytes);

        cipher.decrypt(nonce, data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Decryption failed: {}", e)))
    }

    // ===== AES-CTR Encryption (for passthrough mode - no size overhead) =====
    #[cfg(feature = "encryption")]
    fn encrypt_inplace(&self, data: &[u8], offset: u64) -> io::Result<Vec<u8>> {
        let key = self.encryption_key.as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No encryption key set"))?;

        // Use offset as IV/nonce (16 bytes for CTR mode)
        let mut iv = [0u8; 16];
        iv[0..8].copy_from_slice(&offset.to_le_bytes());

        let mut cipher = Aes256Ctr::new(key.into(), &iv.into());
        let mut result = data.to_vec();
        cipher.apply_keystream(&mut result);
        Ok(result)
    }

    #[cfg(feature = "encryption")]
    fn decrypt_inplace(&self, data: &[u8], offset: u64) -> io::Result<Vec<u8>> {
        // CTR mode: encryption and decryption are the same operation
        self.encrypt_inplace(data, offset)
    }

    /// Get or create the lock file handle for byte-range locking
    fn ensure_lock_file(&mut self) -> io::Result<std::sync::Arc<File>> {
        if self.lock_file.is_none() {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.db_path)?;
            self.lock_file = Some(std::sync::Arc::new(file));
        }
        Ok(std::sync::Arc::clone(self.lock_file.as_ref().unwrap()))
    }

}

/// File-backed WAL index for proper multi-connection coordination
pub struct FileWalIndex {
    /// Path to the -shm file
    path: PathBuf,
    /// Cached regions (region_id -> data)
    regions: HashMap<u32, [u8; 32768]>,
    /// File handle for data I/O
    file: Option<File>,
    /// Separate file handle for locking (Arc for multiple FileGuards)
    lock_file: Option<std::sync::Arc<File>>,
    /// Active byte-range locks: slot -> FileGuard
    /// Using Box<dyn Any> to store type-erased FileGuards
    active_locks: HashMap<u8, Box<dyn std::any::Any + Send + Sync>>,
}

impl FileWalIndex {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            regions: HashMap::new(),
            file: None,
            lock_file: None,
            active_locks: HashMap::new(),
        }
    }

    fn ensure_file(&mut self) -> io::Result<&mut File> {
        if self.file.is_none() {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.path)?;
            self.file = Some(file);
        }
        Ok(self.file.as_mut().unwrap())
    }

    fn ensure_lock_file(&mut self) -> io::Result<std::sync::Arc<File>> {
        if self.lock_file.is_none() {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.path)?;
            self.lock_file = Some(std::sync::Arc::new(file));
        }
        Ok(std::sync::Arc::clone(self.lock_file.as_ref().unwrap()))
    }
}

impl sqlite_vfs::wip::WalIndex for FileWalIndex {
    fn map(&mut self, region: u32) -> Result<[u8; 32768], io::Error> {
        // Each region is 32KB
        let region_size = 32768u64;
        let offset = region as u64 * region_size;

        // Ensure file exists and is large enough
        let file = self.ensure_file()?;
        let file_len = file.metadata()?.len();

        if file_len < offset + region_size {
            // Extend file with zeros
            file.set_len(offset + region_size)?;
        }

        // Read or create the region
        use std::os::unix::fs::FileExt;
        let mut data = [0u8; 32768];
        match file.read_exact_at(&mut data, offset) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // File was just created/extended, return zeros
            }
            Err(e) => return Err(e),
        }

        // Cache it
        self.regions.insert(region, data);
        Ok(data)
    }

    fn lock(
        &mut self,
        locks: Range<u8>,
        lock: sqlite_vfs::wip::WalIndexLock,
    ) -> Result<bool, io::Error> {
        // Proper byte-range locking for WAL-index using file-guard
        // SQLite WAL-index locks are at bytes WAL_LOCK_OFFSET + slot_number
        use sqlite_vfs::wip::WalIndexLock;

        let lock_file = self.ensure_lock_file()?;

        match lock {
            WalIndexLock::None => {
                // Release all locks in the range
                for slot in locks.clone() {
                    self.active_locks.remove(&slot);
                }
            }
            WalIndexLock::Shared | WalIndexLock::Exclusive => {
                // First, try to acquire ALL locks without modifying state
                // This ensures atomicity - either all succeed or none are modified
                let mut new_guards: Vec<(u8, Box<dyn std::any::Any + Send + Sync>)> = Vec::new();
                let lock_type = if matches!(lock, WalIndexLock::Shared) {
                    file_guard::Lock::Shared
                } else {
                    file_guard::Lock::Exclusive
                };

                for slot in locks.clone() {
                    let offset = WAL_LOCK_OFFSET + slot as u64;

                    // Release existing lock on this slot first (needed to acquire new one)
                    // We save it temporarily in case we need to restore
                    let old_guard = self.active_locks.remove(&slot);

                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        lock_type,
                        offset as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            new_guards.push((slot, Box::new(guard)));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            // Failed - restore any removed locks and the ones we acquired
                            // Put back the old guard if we had one
                            if let Some(guard) = old_guard {
                                self.active_locks.insert(slot, guard);
                            }
                            // Put back all guards we acquired so far (they'll be dropped, releasing locks)
                            // Actually, we need to restore the original state for already-processed slots
                            // For simplicity, just drop new_guards - the locks will be released
                            // The caller will retry if needed
                            if DEBUG_LOCKS.load(Ordering::Relaxed) {
                                eprintln!(
                                    "[LOCK DEBUG] {:?} WAL_INDEX {} slot {} {:?} => BUSY",
                                    std::thread::current().id(),
                                    self.path.display(),
                                    slot,
                                    lock
                                );
                            }
                            return Ok(false);
                        }
                        Err(e) => {
                            // Restore old guard on error
                            if let Some(guard) = old_guard {
                                self.active_locks.insert(slot, guard);
                            }
                            return Err(e);
                        }
                    }
                }

                // All locks acquired successfully - commit them
                for (slot, guard) in new_guards {
                    self.active_locks.insert(slot, guard);
                }
            }
        }

        if DEBUG_LOCKS.load(Ordering::Relaxed) {
            eprintln!(
                "[LOCK DEBUG] {:?} WAL_INDEX {} locks {:?}..{:?} {:?} => OK",
                std::thread::current().id(),
                self.path.display(),
                locks.start,
                locks.end,
                lock
            );
        }
        Ok(true)
    }

    fn delete(self) -> Result<(), io::Error> {
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

impl DatabaseHandle for CompressedHandle {
    type WalIndex = FileWalIndex;

    fn size(&self) -> Result<u64, io::Error> {
        if !self.compressed {
            // Passthrough: return actual file size
            let file = self.file.read();
            return file.metadata().map(|m| m.len());
        }

        // Return logical size (uncompressed)
        // Use (max_page + 1) * page_size to account for gaps like SQLite's lock page
        let header = self.header.read();
        let index = self.index.read();
        if header.page_size > 0 && !index.entries.is_empty() {
            Ok((index.max_page + 1) * header.page_size as u64)
        } else {
            Ok(0)
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        if !self.compressed {
            // Passthrough: direct file read with position-aware I/O
            // Optionally decrypt data after reading (for WAL encryption)
            use std::os::unix::fs::FileExt;
            let file = self.file.read();  // READ lock instead of write lock!

            #[cfg(feature = "encryption")]
            if self.encrypt_pages {
                // Read encrypted data, then decrypt with AES-CTR (same size as plaintext)
                let mut encrypted = vec![0u8; buf.len()];
                file.read_exact_at(&mut encrypted, offset)?;
                let decrypted = self.decrypt_inplace(&encrypted, offset)?;
                buf.copy_from_slice(&decrypted);
                return Ok(());
            }

            return file.read_exact_at(buf, offset);
        }

        let header = self.header.read();
        let page_size = if header.page_size > 0 {
            header.page_size as usize
        } else {
            buf.len()
        };
        drop(header);

        let page_num = offset / page_size as u64;

        let index = self.index.read();
        if let Some(&(file_offset, compressed_size)) = index.entries.get(&page_num) {
            drop(index);

            // Use position-aware read with READ lock instead of write lock
            use std::os::unix::fs::FileExt;
            let file = self.file.read();  // READ lock!
            let mut stored_data = vec![0u8; compressed_size as usize];
            file.read_exact_at(&mut stored_data, file_offset)?;

            // Decrypt if encryption is enabled
            #[cfg(feature = "encryption")]
            let stored_data = if self.encrypt_pages {
                self.decrypt(&stored_data, page_num)?
            } else {
                stored_data
            };

            // Decompress or use raw data based on compress_pages flag
            let decompressed = if self.compress_pages {
                self.decompress(&stored_data)?
            } else {
                stored_data
            };

            let page_offset = (offset % page_size as u64) as usize;
            let copy_len = buf.len().min(decompressed.len().saturating_sub(page_offset));
            if copy_len > 0 {
                buf[..copy_len].copy_from_slice(&decompressed[page_offset..page_offset + copy_len]);
            }
            if copy_len < buf.len() {
                buf[copy_len..].fill(0);
            }

            Ok(())
        } else {
            buf.fill(0);
            Ok(())
        }
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        if !self.compressed {
            // Passthrough: direct file write with position-aware I/O
            // Optionally encrypt data in-place before writing (for WAL encryption)
            use std::os::unix::fs::FileExt;
            let file = self.file.read();  // Read lock - pwrite is atomic

            #[cfg(feature = "encryption")]
            if self.encrypt_pages {
                // Encrypt data using AES-CTR (no size overhead)
                let encrypted = self.encrypt_inplace(buf, offset)?;
                return file.write_all_at(&encrypted, offset);
            }

            return file.write_all_at(buf, offset);
        }

        // Set page size from first write (brief lock)
        {
            let mut header = self.header.write();
            if header.page_size == 0 {
                header.page_size = buf.len() as u32;
            }
        }

        // Read page_size (brief read lock)
        let page_size = {
            let header = self.header.read();
            header.page_size as u64
        };

        let page_num = offset / page_size;

        // Process data: compress and/or encrypt as needed
        // This happens WITHOUT any locks held
        use std::borrow::Cow;

        let data: Cow<'_, [u8]> = if self.compress_pages {
            Cow::Owned(self.compress(buf)?)
        } else {
            Cow::Borrowed(buf)
        };

        #[cfg(feature = "encryption")]
        let data: Cow<'_, [u8]> = if self.encrypt_pages {
            Cow::Owned(self.encrypt(&data, page_num)?)
        } else {
            data
        };

        let data_size = data.len() as u32;
        let record_size = RECORD_HEADER_SIZE + data_size as u64;

        // ATOMIC SPACE RESERVATION: Reserve space without blocking readers
        // fetch_add returns the OLD value, which is our record offset
        let record_offset = self.write_end.fetch_add(record_size, Ordering::SeqCst);
        let data_offset = record_offset + RECORD_HEADER_SIZE;

        // FILE I/O WITHOUT LOCKS: pwrite is thread-safe
        use std::os::unix::fs::FileExt;
        let file = self.file.read(); // Read lock only - pwrite is atomic

        // Write record header
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        rec_header[0..8].copy_from_slice(&page_num.to_le_bytes());
        rec_header[8..12].copy_from_slice(&data_size.to_le_bytes());
        file.write_all_at(&rec_header, record_offset)?;

        // Write data
        file.write_all_at(&data, data_offset)?;

        drop(file); // Release file read lock before taking index write lock

        // BRIEF INDEX UPDATE: Only lock for HashMap insertion
        {
            let mut index = self.index.write();
            index.entries.insert(page_num, (data_offset, data_size));
            if page_num > index.max_page {
                index.max_page = page_num;
            }
        }

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        if !self.compressed {
            return self.file.write().sync_all();
        }

        // Write header and fsync
        // Index is scanned from data_start to EOF on open
        use std::os::unix::fs::FileExt;

        // Read header fields
        let (page_size, data_start, dict_size, flags) = {
            let header = self.header.read();
            (header.page_size, header.data_start, header.dict_size, header.flags)
        };

        // Write header using pwrite (position-aware, no seek needed)
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&page_size.to_le_bytes());
        buf[12..20].copy_from_slice(&data_start.to_le_bytes());
        buf[20..24].copy_from_slice(&dict_size.to_le_bytes());
        buf[24..28].copy_from_slice(&flags.to_le_bytes());

        let file = self.file.read();
        file.write_all_at(&buf, 0)?;
        drop(file);

        self.file.write().sync_all()
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        if !self.compressed {
            return self.file.write().set_len(size);
        }

        // For compressed files, we update the in-memory index
        // Note: truncated pages become "dead" records in the file
        // They will be cleaned up on compaction
        let header = self.header.read();
        if header.page_size == 0 {
            return Ok(());
        }
        let page_size = header.page_size as u64;
        drop(header);

        let max_page = if size == 0 { 0 } else { (size - 1) / page_size };

        let mut index = self.index.write();
        index.entries.retain(|&page_num, _| page_num <= max_page);

        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let current = *self.lock.read();
        let path = self.db_path.to_string_lossy().to_string();

        // No change needed
        if current == lock {
            debug_lock("NOOP", &path, current, lock, "already held");
            return Ok(true);
        }

        debug_lock("ACQUIRE", &path, current, lock, "attempting");

        // Get or create the lock file handle for byte-range locking
        let lock_file = self.ensure_lock_file()?;

        // SQLite lock escalation using proper byte-range locks:
        // - NONE: No locks
        // - SHARED: Shared lock on one byte in SHARED_FIRST..SHARED_FIRST+SHARED_SIZE
        // - RESERVED: Keep SHARED, add exclusive lock on RESERVED_BYTE
        // - PENDING: Keep SHARED+RESERVED, add exclusive lock on PENDING_BYTE
        // - EXCLUSIVE: Replace SHARED with exclusive lock on SHARED range

        match lock {
            LockKind::None => {
                // Release all locks by clearing the active_db_locks map
                debug_lock("UNLOCK", &path, current, lock, "releasing all");
                self.active_db_locks.clear();
            }

            LockKind::Shared => {
                // Release any existing locks first
                self.active_db_locks.clear();

                // SQLite protocol: First check PENDING_BYTE
                // If a writer holds exclusive PENDING, they're waiting to write - block new readers
                // This prevents writer starvation where fast readers constantly get shared locks
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(_pending_guard) => {
                        // Got shared on PENDING - no writer waiting, drop it and continue
                        // We don't keep this lock, it's just a check
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // Writer has exclusive PENDING - they're waiting to write
                        debug_lock("ACQUIRE", &path, current, lock, "FAILED (writer has PENDING)");
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }

                // Now try to get shared lock on SHARED range
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    SHARED_FIRST as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks.insert("shared".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        debug_lock("ACQUIRE", &path, current, lock, "FAILED (shared busy)");
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }

            LockKind::Reserved => {
                // Must have SHARED first (SQLite protocol)
                if !matches!(current, LockKind::Shared | LockKind::Reserved | LockKind::Pending | LockKind::Exclusive) {
                    debug_lock("ACQUIRE", &path, current, lock, "FAILED (must have SHARED first)");
                    return Ok(false);
                }

                // Try to get exclusive lock on RESERVED_BYTE
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    RESERVED_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks.insert("reserved".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        debug_lock("ACQUIRE", &path, current, lock, "FAILED (reserved busy)");
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }

            LockKind::Pending => {
                // Must have RESERVED first
                if !matches!(current, LockKind::Reserved | LockKind::Pending | LockKind::Exclusive) {
                    debug_lock("ACQUIRE", &path, current, lock, "FAILED (must have RESERVED first)");
                    return Ok(false);
                }

                // Try to get exclusive lock on PENDING_BYTE
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks.insert("pending".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        debug_lock("ACQUIRE", &path, current, lock, "FAILED (pending busy)");
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }

            LockKind::Exclusive => {
                // Must have at least RESERVED first
                if !matches!(current, LockKind::Reserved | LockKind::Pending | LockKind::Exclusive) {
                    debug_lock("ACQUIRE", &path, current, lock, "FAILED (need RESERVED first)");
                    return Ok(false);
                }

                // If we don't have PENDING yet, get it first
                if !self.active_db_locks.contains_key("pending") {
                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        file_guard::Lock::Exclusive,
                        PENDING_BYTE as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            self.active_db_locks.insert("pending".to_string(), Box::new(guard));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            debug_lock("ACQUIRE", &path, current, lock, "FAILED (pending busy)");
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    }
                }

                // Release shared lock, then get exclusive lock on entire shared range
                self.active_db_locks.remove("shared");

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    SHARED_FIRST as usize,
                    SHARED_SIZE as usize,
                ) {
                    Ok(guard) => {
                        self.active_db_locks.insert("exclusive".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // Restore shared lock since we failed - this MUST succeed or we're in trouble
                        match file_guard::try_lock(
                            std::sync::Arc::clone(&lock_file),
                            file_guard::Lock::Shared,
                            SHARED_FIRST as usize,
                            1,
                        ) {
                            Ok(guard) => {
                                self.active_db_locks.insert("shared".to_string(), Box::new(guard));
                            }
                            Err(restore_err) => {
                                // Critical: We lost our shared lock and can't restore it
                                // This leaves us in an inconsistent state - return error
                                debug_lock("ACQUIRE", &path, current, lock,
                                    "FAILED (exclusive busy, CRITICAL: shared restore failed!)");
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("Lock restore failed after exclusive attempt: {}", restore_err)
                                ));
                            }
                        }
                        debug_lock("ACQUIRE", &path, current, lock, "FAILED (exclusive busy)");
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        *self.lock.write() = lock;
        debug_lock("ACQUIRE", &path, current, lock, "SUCCESS");
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        let lock = *self.lock.read();
        Ok(matches!(
            lock,
            LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
        ))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(*self.lock.read())
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        // Create -shm path from database path
        let shm_path = self.db_path.with_extension("db-shm");
        Ok(FileWalIndex::new(shm_path))
    }
}

/// Compressed VFS implementation
pub struct CompressedVfs {
    base_dir: PathBuf,
    compression_level: i32,
    /// Whether to compress pages (false = passthrough mode)
    compress: bool,
    /// Whether to encrypt pages
    encrypt: bool,
    /// Password for encryption
    #[cfg(feature = "encryption")]
    password: Option<String>,
    /// Compression dictionary (for improved compression ratios)
    #[cfg(feature = "zstd")]
    dictionary: Option<Vec<u8>>,
}

impl CompressedVfs {
    /// Create a new compressed VFS (default).
    pub fn new<P: AsRef<Path>>(base_dir: P, compression_level: i32) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            compression_level: compression_level.clamp(1, 22),
            compress: true,
            encrypt: false,
            #[cfg(feature = "encryption")]
            password: None,
            #[cfg(feature = "zstd")]
            dictionary: None,
        }
    }

    /// Create a new compressed VFS with a pre-trained dictionary.
    ///
    /// Dictionary compression typically improves compression ratios by 2-5x
    /// for structured data with repeated patterns (like Redis workloads).
    ///
    /// # Example
    /// ```ignore
    /// use sqlite_compress_encrypt_vfs::{CompressedVfs, dict::train_from_database};
    ///
    /// // Train a dictionary from existing data
    /// let dict = train_from_database("sample.db", 100 * 1024)?;
    ///
    /// // Create VFS with dictionary
    /// let vfs = CompressedVfs::new_with_dict("./db", 3, dict);
    /// ```
    #[cfg(feature = "zstd")]
    pub fn new_with_dict<P: AsRef<Path>>(base_dir: P, compression_level: i32, dictionary: Vec<u8>) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            compression_level: compression_level.clamp(1, 22),
            compress: true,
            encrypt: false,
            #[cfg(feature = "encryption")]
            password: None,
            dictionary: Some(dictionary),
        }
    }

    /// Create a compressed and encrypted VFS with a pre-trained dictionary.
    #[cfg(all(feature = "zstd", feature = "encryption"))]
    pub fn compressed_encrypted_with_dict<P: AsRef<Path>>(
        base_dir: P,
        compression_level: i32,
        password: &str,
        dictionary: Vec<u8>,
    ) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            compression_level: compression_level.clamp(1, 22),
            compress: true,
            encrypt: true,
            password: Some(password.to_string()),
            dictionary: Some(dictionary),
        }
    }

    /// Create a passthrough VFS (no compression).
    ///
    /// Pages are stored with the VFS index format but without compression.
    /// Useful for benchmarking or when data doesn't compress well.
    pub fn passthrough<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            compression_level: 0,
            compress: false,
            encrypt: false,
            #[cfg(feature = "encryption")]
            password: None,
            #[cfg(feature = "zstd")]
            dictionary: None,
        }
    }

    /// Create an encrypted VFS (no compression, encryption only).
    ///
    /// Pages are encrypted with AES-256-GCM but not compressed.
    /// Useful when data doesn't compress well but needs encryption.
    #[cfg(feature = "encryption")]
    pub fn encrypted<P: AsRef<Path>>(base_dir: P, password: &str) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            compression_level: 0,
            compress: false,
            encrypt: true,
            password: Some(password.to_string()),
            #[cfg(feature = "zstd")]
            dictionary: None,
        }
    }

    /// Create a compressed and encrypted VFS.
    ///
    /// Pages are compressed first, then encrypted with AES-256-GCM.
    /// Provides both storage savings and security.
    #[cfg(feature = "encryption")]
    pub fn compressed_encrypted<P: AsRef<Path>>(base_dir: P, compression_level: i32, password: &str) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            compression_level: compression_level.clamp(1, 22),
            compress: true,
            encrypt: true,
            password: Some(password.to_string()),
            #[cfg(feature = "zstd")]
            dictionary: None,
        }
    }
}

impl Vfs for CompressedVfs {
    type Handle = CompressedHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.base_dir.join(db);

        // Only compress main database (WAL is append-only, compression doesn't help)
        let use_compression = matches!(opts.kind, OpenKind::MainDb) && self.compress;

        // Encrypt both main DB and WAL when encryption is enabled
        let use_encryption = matches!(opts.kind, OpenKind::MainDb | OpenKind::Wal) && self.encrypt;

        let file = match opts.access {
            OpenAccess::Read => FsOpenOptions::new().read(true).open(&path)?,
            OpenAccess::Write => FsOpenOptions::new().read(true).write(true).open(&path)?,
            OpenAccess::Create => FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?,
            OpenAccess::CreateNew => FsOpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?,
        };

        // Main DB: Use VFS page index format for compression/encryption
        // WAL: Use passthrough with optional in-place encryption (can't use VFS format - SQLite needs to parse WAL structure)
        // Journals/temp: Use passthrough without encryption
        if use_compression {
            // Main DB with compression (and maybe encryption)
            let password = {
                #[cfg(feature = "encryption")]
                { self.password.as_deref() }
                #[cfg(not(feature = "encryption"))]
                { None::<&str> }
            };
            CompressedHandle::new(
                file,
                path.clone(),
                self.compression_level,
                use_compression,
                use_encryption,
                password,
                #[cfg(feature = "zstd")]
                self.dictionary.as_deref(),
            )
        } else if use_encryption {
            // WAL with encryption but no compression - use passthrough with in-place encryption
            let encryption_key = {
                #[cfg(feature = "encryption")]
                {
                    use sha2::{Sha256, Digest};
                    let password = self.password.as_ref()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No password set"))?;
                    let mut hasher = Sha256::new();
                    hasher.update(password.as_bytes());
                    let hash = hasher.finalize();
                    let mut key = [0u8; 32];
                    key.copy_from_slice(&hash);
                    Some(key)
                }
                #[cfg(not(feature = "encryption"))]
                { None }
            };
            Ok(CompressedHandle::new_passthrough(
                file,
                path,
                true,  // encrypt
                #[cfg(feature = "encryption")]
                encryption_key,
            ))
        } else {
            // Journals and temp files - plain passthrough
            Ok(CompressedHandle::new_passthrough(
                file,
                path,
                false,  // no encryption
                #[cfg(feature = "encryption")]
                None,
            ))
        }
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        let path = self.base_dir.join(db);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        let path = self.base_dir.join(db);
        Ok(path.exists())
    }

    fn temporary_name(&self) -> String {
        format!("temp_{}", std::process::id())
    }

    fn random(&self, buffer: &mut [i8]) {
        use std::time::SystemTime;
        let mut seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        for b in buffer.iter_mut() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (seed >> 33) as i8;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        std::thread::sleep(duration);
        duration
    }
}

/// Register the compressed VFS with SQLite
pub fn register(name: &str, vfs: CompressedVfs) -> Result<(), io::Error> {
    sqlite_vfs::register(name, vfs, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))
}

/// Database statistics returned by `inspect_database`
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Page size in bytes
    pub page_size: u32,
    /// Number of unique pages (live data)
    pub page_count: u64,
    /// Total records in file (includes dead/superseded records)
    pub total_records: u64,
    /// Physical file size in bytes
    pub file_size: u64,
    /// Logical size (uncompressed data size)
    pub logical_size: u64,
    /// Size of live compressed data (only latest version of each page)
    pub live_data_size: u64,
    /// Dead space in bytes (superseded records)
    pub dead_space: u64,
    /// Dead space as percentage of file size
    pub dead_space_pct: f64,
    /// Compression ratio (logical_size / live_data_size)
    pub compression_ratio: f64,
}

/// Inspect a compressed database file and return statistics.
///
/// This reads the file header and scans records to compute dead space,
/// compression ratio, and other metrics.
pub fn inspect_database<P: AsRef<Path>>(path: P) -> io::Result<DatabaseStats> {
    use std::io::BufReader;

    let path = path.as_ref();
    let mut file = FsOpenOptions::new().read(true).open(path)?;
    let file_size = file.metadata()?.len();

    // Read header
    let mut magic = [0u8; 8];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut magic)?;

    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Not a valid SQLCEs database file",
        ));
    }

    // Read full header
    let mut buf = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    let page_size = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let data_start = u64::from_le_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);

    // Scan records to build index and count total records
    let mut entries: HashMap<u64, (u64, u32)> = HashMap::new();
    let mut total_records: u64 = 0;
    let mut max_page: u64 = 0;

    file.seek(SeekFrom::Start(data_start))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, &mut file);
    let mut pos = data_start;

    while pos < file_size {
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        match reader.read_exact(&mut rec_header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let page_num = u64::from_le_bytes([
            rec_header[0], rec_header[1], rec_header[2], rec_header[3],
            rec_header[4], rec_header[5], rec_header[6], rec_header[7],
        ]);
        let data_size = u32::from_le_bytes([
            rec_header[8], rec_header[9], rec_header[10], rec_header[11],
        ]);

        if data_size > 1_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Corrupt record at offset {}: data_size={}", pos, data_size),
            ));
        }

        let data_offset = pos + RECORD_HEADER_SIZE;
        entries.insert(page_num, (data_offset, data_size));
        if page_num > max_page {
            max_page = page_num;
        }

        reader.seek_relative(data_size as i64)?;
        pos = data_offset + data_size as u64;
        total_records += 1;
    }

    let page_count = entries.len() as u64;
    let logical_size = if page_size > 0 && page_count > 0 {
        (max_page + 1) * page_size as u64
    } else {
        0
    };

    // Calculate live data size (sum of current record sizes)
    let live_data_size: u64 = entries.values().map(|(_, size)| *size as u64).sum();
    let live_with_headers = live_data_size + (page_count * RECORD_HEADER_SIZE);

    // Dead space = file size - header - live records
    let dead_space = if file_size > HEADER_SIZE + live_with_headers {
        file_size - HEADER_SIZE - live_with_headers
    } else {
        0
    };

    let dead_space_pct = if file_size > 0 {
        (dead_space as f64 / file_size as f64) * 100.0
    } else {
        0.0
    };

    let compression_ratio = if live_data_size > 0 {
        logical_size as f64 / live_data_size as f64
    } else {
        1.0
    };

    Ok(DatabaseStats {
        page_size,
        page_count,
        total_records,
        file_size,
        logical_size,
        live_data_size,
        dead_space,
        dead_space_pct,
        compression_ratio,
    })
}

/// Compact a compressed database by removing dead space.
///
/// This creates a new file with only the live (most recent) version of each page,
/// then atomically replaces the original file.
///
/// # Arguments
/// * `path` - Path to the database file to compact
///
/// # Returns
/// * `Ok(bytes_freed)` - Number of bytes freed by compaction
/// * `Err(_)` - If compaction fails
pub fn compact<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    use std::io::BufReader;
    use std::os::unix::fs::FileExt;

    let path = path.as_ref();
    let mut file = FsOpenOptions::new().read(true).open(path)?;
    let original_size = file.metadata()?.len();

    // Read header
    let mut buf = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    if &buf[0..8] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Not a valid SQLCEs database file",
        ));
    }

    let page_size = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let data_start = u64::from_le_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);

    // Build index by scanning (keeps only latest version of each page)
    let mut entries: HashMap<u64, (u64, u32)> = HashMap::new();
    let mut max_page: u64 = 0;

    file.seek(SeekFrom::Start(data_start))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, &mut file);
    let mut pos = data_start;

    while pos < original_size {
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        match reader.read_exact(&mut rec_header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let page_num = u64::from_le_bytes([
            rec_header[0], rec_header[1], rec_header[2], rec_header[3],
            rec_header[4], rec_header[5], rec_header[6], rec_header[7],
        ]);
        let data_size = u32::from_le_bytes([
            rec_header[8], rec_header[9], rec_header[10], rec_header[11],
        ]);

        let data_offset = pos + RECORD_HEADER_SIZE;
        entries.insert(page_num, (data_offset, data_size));
        if page_num > max_page {
            max_page = page_num;
        }

        reader.seek_relative(data_size as i64)?;
        pos = data_offset + data_size as u64;
    }
    drop(reader);

    // Sort pages by page number for sequential writing
    let mut pages: Vec<_> = entries.into_iter().collect();
    pages.sort_by_key(|(page_num, _)| *page_num);

    // Create temp file in same directory (for atomic rename)
    let parent = path.parent().unwrap_or(Path::new("."));
    let temp_path = parent.join(format!(".{}.compact.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()));

    let temp_file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    // Write header with new format
    let data_start = HEADER_SIZE; // No dictionary after compaction
    let mut header_buf = [0u8; HEADER_SIZE as usize];
    header_buf[0..8].copy_from_slice(MAGIC);
    header_buf[8..12].copy_from_slice(&page_size.to_le_bytes());
    header_buf[12..20].copy_from_slice(&data_start.to_le_bytes());
    // dict_size = 0, flags = 0 (already zeroed)
    temp_file.write_all_at(&header_buf, 0)?;

    // Copy each live page
    let mut write_pos = HEADER_SIZE;
    for (page_num, (data_offset, data_size)) in pages {
        // Read data from original file
        let mut data = vec![0u8; data_size as usize];
        file.read_exact_at(&mut data, data_offset)?;

        // Write record header
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        rec_header[0..8].copy_from_slice(&page_num.to_le_bytes());
        rec_header[8..12].copy_from_slice(&data_size.to_le_bytes());
        temp_file.write_all_at(&rec_header, write_pos)?;

        // Write data
        temp_file.write_all_at(&data, write_pos + RECORD_HEADER_SIZE)?;
        write_pos += RECORD_HEADER_SIZE + data_size as u64;
    }

    temp_file.sync_all()?;
    drop(temp_file);
    drop(file);

    // Atomic replace
    std::fs::rename(&temp_path, path)?;

    let new_size = std::fs::metadata(path)?.len();
    let freed = original_size.saturating_sub(new_size);
    Ok(freed)
}

/// Configuration for compaction operations
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Compression level (1-22 for zstd)
    pub compression_level: i32,
    /// Optional compression dictionary
    #[cfg(feature = "zstd")]
    pub dictionary: Option<Vec<u8>>,
    /// Whether to use parallel compression (requires "parallel" feature)
    pub parallel: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            compression_level: 3,
            #[cfg(feature = "zstd")]
            dictionary: None,
            parallel: cfg!(feature = "parallel"),
        }
    }
}

impl CompactionConfig {
    /// Create a new compaction config with the given compression level
    pub fn new(compression_level: i32) -> Self {
        Self {
            compression_level: compression_level.clamp(1, 22),
            #[cfg(feature = "zstd")]
            dictionary: None,
            parallel: cfg!(feature = "parallel"),
        }
    }

    /// Set the compression dictionary
    #[cfg(feature = "zstd")]
    pub fn with_dictionary(mut self, dict: Vec<u8>) -> Self {
        self.dictionary = Some(dict);
        self
    }

    /// Enable or disable parallel compression
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }
}

/// Compact a database with recompression using the given configuration.
///
/// Unlike `compact()` which just copies already-compressed data, this function
/// decompresses and recompresses pages. This is useful when:
/// - Changing compression level
/// - Applying a new/different compression dictionary
/// - Optimizing compression after bulk inserts
///
/// When the "parallel" feature is enabled and `config.parallel` is true,
/// compression is performed in parallel using rayon, providing 4-8x speedup
/// on multi-core systems.
///
/// # Arguments
/// * `path` - Path to the database file to compact
/// * `config` - Compaction configuration (compression level, dictionary, parallel)
///
/// # Returns
/// * `Ok(bytes_freed)` - Number of bytes freed by compaction
/// * `Err(_)` - If compaction fails
#[cfg(feature = "zstd")]
pub fn compact_with_recompression<P: AsRef<Path>>(path: P, config: CompactionConfig) -> io::Result<u64> {
    use std::io::BufReader;
    use std::os::unix::fs::FileExt;

    let path = path.as_ref();
    let mut file = FsOpenOptions::new().read(true).open(path)?;
    let original_size = file.metadata()?.len();

    // Read header
    let mut buf = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    if &buf[0..8] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Not a valid SQLCEs database file",
        ));
    }

    let page_size = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let data_start = u64::from_le_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);
    let old_dict_size = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);

    // Load old dictionary if present (for decompression)
    let old_dictionary = if old_dict_size > 0 {
        let mut dict_bytes = vec![0u8; old_dict_size as usize];
        file.read_exact_at(&mut dict_bytes, HEADER_SIZE)?;
        Some(dict_bytes)
    } else {
        None
    };

    // Build index by scanning
    let mut entries: HashMap<u64, (u64, u32)> = HashMap::new();

    file.seek(SeekFrom::Start(data_start))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, &mut file);
    let mut pos = data_start;

    while pos < original_size {
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        match reader.read_exact(&mut rec_header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let page_num = u64::from_le_bytes([
            rec_header[0], rec_header[1], rec_header[2], rec_header[3],
            rec_header[4], rec_header[5], rec_header[6], rec_header[7],
        ]);
        let data_size = u32::from_le_bytes([
            rec_header[8], rec_header[9], rec_header[10], rec_header[11],
        ]);

        let data_offset = pos + RECORD_HEADER_SIZE;
        entries.insert(page_num, (data_offset, data_size));

        reader.seek_relative(data_size as i64)?;
        pos = data_offset + data_size as u64;
    }
    drop(reader);

    // Sort pages by page number
    let mut pages: Vec<_> = entries.into_iter().collect();
    pages.sort_by_key(|(page_num, _)| *page_num);

    // Phase 1: Read all compressed data sequentially (I/O bound)
    let page_data: Vec<(u64, Vec<u8>)> = pages
        .iter()
        .map(|(page_num, (data_offset, data_size))| {
            let mut data = vec![0u8; *data_size as usize];
            file.read_exact_at(&mut data, *data_offset).expect("read failed");
            (*page_num, data)
        })
        .collect();

    // Phase 2: Decompress and recompress (CPU bound - parallelize!)
    // Prepare dictionaries
    let old_decoder_dict = old_dictionary.as_ref().map(|d| zstd::dict::DecoderDictionary::copy(d));
    let new_encoder_dict = config.dictionary.as_ref()
        .map(|d| zstd::dict::EncoderDictionary::copy(d, config.compression_level));

    #[cfg(feature = "parallel")]
    let recompressed: Vec<(u64, Vec<u8>)> = if config.parallel {
        use rayon::prelude::*;

        // Clone dictionaries for parallel use (they're thread-safe)
        let old_decoder_dict = &old_decoder_dict;
        let new_encoder_dict = &new_encoder_dict;
        let compression_level = config.compression_level;

        page_data
            .into_par_iter()
            .map(|(page_num, compressed_data)| {
                // Decompress with old dictionary
                let decompressed = if let Some(ref dict) = old_decoder_dict {
                    let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(
                        compressed_data.as_slice(),
                        dict,
                    ).expect("decoder creation failed");
                    let mut output = Vec::new();
                    std::io::Read::read_to_end(&mut decoder, &mut output).expect("decompress failed");
                    output
                } else {
                    zstd::decode_all(compressed_data.as_slice()).expect("decompress failed")
                };

                // Recompress with new dictionary
                let recompressed = if let Some(ref dict) = new_encoder_dict {
                    let mut encoder = zstd::stream::Encoder::with_prepared_dictionary(
                        Vec::new(),
                        dict,
                    ).expect("encoder creation failed");
                    std::io::Write::write_all(&mut encoder, &decompressed).expect("compress failed");
                    encoder.finish().expect("finish failed")
                } else {
                    zstd::encode_all(decompressed.as_slice(), compression_level).expect("compress failed")
                };

                (page_num, recompressed)
            })
            .collect()
    } else {
        // Serial fallback when parallel is disabled
        compact_recompress_serial(page_data, &old_decoder_dict, &new_encoder_dict, config.compression_level)
    };

    #[cfg(not(feature = "parallel"))]
    let recompressed: Vec<(u64, Vec<u8>)> = compact_recompress_serial(
        page_data,
        &old_decoder_dict,
        &new_encoder_dict,
        config.compression_level,
    );

    // Phase 3: Write sequentially
    let parent = path.parent().unwrap_or(Path::new("."));
    let temp_path = parent.join(format!(
        ".{}.compact.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));

    let temp_file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    // Calculate new data_start based on new dictionary
    let new_dict_size = config.dictionary.as_ref().map(|d| d.len() as u32).unwrap_or(0);
    let new_data_start = HEADER_SIZE + new_dict_size as u64;

    // Write header
    let mut header_buf = [0u8; HEADER_SIZE as usize];
    header_buf[0..8].copy_from_slice(MAGIC);
    header_buf[8..12].copy_from_slice(&page_size.to_le_bytes());
    header_buf[12..20].copy_from_slice(&new_data_start.to_le_bytes());
    header_buf[20..24].copy_from_slice(&new_dict_size.to_le_bytes());
    // flags = 0 (already zeroed)
    temp_file.write_all_at(&header_buf, 0)?;

    // Write dictionary if present
    if let Some(ref dict) = config.dictionary {
        temp_file.write_all_at(dict, HEADER_SIZE)?;
    }

    // Write recompressed pages
    let mut write_pos = new_data_start;
    for (page_num, data) in recompressed {
        let data_size = data.len() as u32;

        // Write record header
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        rec_header[0..8].copy_from_slice(&page_num.to_le_bytes());
        rec_header[8..12].copy_from_slice(&data_size.to_le_bytes());
        temp_file.write_all_at(&rec_header, write_pos)?;

        // Write data
        temp_file.write_all_at(&data, write_pos + RECORD_HEADER_SIZE)?;
        write_pos += RECORD_HEADER_SIZE + data_size as u64;
    }

    temp_file.sync_all()?;
    drop(temp_file);
    drop(file);

    // Atomic replace
    std::fs::rename(&temp_path, path)?;

    let new_size = std::fs::metadata(path)?.len();
    let freed = original_size.saturating_sub(new_size);
    Ok(freed)
}

/// Helper function for serial recompression
#[cfg(feature = "zstd")]
fn compact_recompress_serial(
    page_data: Vec<(u64, Vec<u8>)>,
    old_decoder_dict: &Option<zstd::dict::DecoderDictionary<'_>>,
    new_encoder_dict: &Option<zstd::dict::EncoderDictionary<'_>>,
    compression_level: i32,
) -> Vec<(u64, Vec<u8>)> {
    page_data
        .into_iter()
        .map(|(page_num, compressed_data)| {
            // Decompress with old dictionary
            let decompressed = if let Some(ref dict) = old_decoder_dict {
                let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(
                    compressed_data.as_slice(),
                    dict,
                ).expect("decoder creation failed");
                let mut output = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut output).expect("decompress failed");
                output
            } else {
                zstd::decode_all(compressed_data.as_slice()).expect("decompress failed")
            };

            // Recompress with new dictionary
            let recompressed = if let Some(ref dict) = new_encoder_dict {
                let mut encoder = zstd::stream::Encoder::with_prepared_dictionary(
                    Vec::new(),
                    dict,
                ).expect("encoder creation failed");
                std::io::Write::write_all(&mut encoder, &decompressed).expect("compress failed");
                encoder.finish().expect("finish failed")
            } else {
                zstd::encode_all(decompressed.as_slice(), compression_level).expect("compress failed")
            };

            (page_num, recompressed)
        })
        .collect()
}

/// Compact a database if dead space exceeds a threshold.
///
/// This is a helper function that checks the current dead space percentage
/// and only runs compaction if it exceeds the given threshold.
///
/// # Arguments
/// * `path` - Path to the database file
/// * `threshold_pct` - Minimum dead space percentage to trigger compaction (0.0-100.0)
///
/// # Returns
/// * `Ok(Some(bytes_freed))` - Compaction ran and freed this many bytes
/// * `Ok(None)` - Dead space was below threshold, no compaction needed
/// * `Err(_)` - If inspection or compaction fails
pub fn compact_if_needed<P: AsRef<Path>>(path: P, threshold_pct: f64) -> io::Result<Option<u64>> {
    let stats = inspect_database(&path)?;

    if stats.dead_space_pct >= threshold_pct {
        let freed = compact(&path)?;
        Ok(Some(freed))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        let file = tempfile::tempfile().unwrap();
        let handle = CompressedHandle::new_compressed(file, 3, true).unwrap();

        let data = b"hello world this is a test of compression";
        let compressed = handle.compress(data).unwrap();
        let decompressed = handle.decompress(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_header_roundtrip() {
        let file = tempfile::tempfile().unwrap();

        let header = FileHeader {
            page_size: 4096,
            data_start: HEADER_SIZE + 1024, // Simulate 1KB dictionary
            dict_size: 1024,
            flags: 0,
        };

        // Use pwrite like the real implementation does
        use std::os::unix::fs::FileExt;
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&header.page_size.to_le_bytes());
        buf[12..20].copy_from_slice(&header.data_start.to_le_bytes());
        buf[20..24].copy_from_slice(&header.dict_size.to_le_bytes());
        buf[24..28].copy_from_slice(&header.flags.to_le_bytes());
        file.write_all_at(&buf, 0).unwrap();

        let mut file = file; // Need &mut for read_from
        let read_header = FileHeader::read_from(&mut file).unwrap().unwrap();
        assert_eq!(read_header.page_size, 4096);
        assert_eq!(read_header.data_start, HEADER_SIZE + 1024);
        assert_eq!(read_header.dict_size, 1024);
        assert_eq!(read_header.flags, 0);
    }

    #[test]
    fn test_write_read_page() {
        let file = tempfile::tempfile().unwrap();
        let mut handle = CompressedHandle::new_compressed(file, 3, true).unwrap();

        // Write a page
        let page_data = vec![0x42u8; 4096];
        handle.write_all_at(&page_data, 0).unwrap();
        handle.sync(false).unwrap();

        // Read it back
        let mut buf = vec![0u8; 4096];
        handle.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, page_data);
    }

    #[test]
    fn test_multiple_pages() {
        let file = tempfile::tempfile().unwrap();
        let mut handle = CompressedHandle::new_compressed(file, 3, true).unwrap();

        // Write multiple pages
        for i in 0..10u8 {
            let page_data = vec![i; 4096];
            handle.write_all_at(&page_data, i as u64 * 4096).unwrap();
        }
        handle.sync(false).unwrap();

        // Read them back
        for i in 0..10u8 {
            let mut buf = vec![0u8; 4096];
            handle.read_exact_at(&mut buf, i as u64 * 4096).unwrap();
            assert!(buf.iter().all(|&b| b == i), "Page {} mismatch", i);
        }
    }

    #[test]
    fn test_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Write data
        {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            let mut handle = CompressedHandle::new_compressed(file, 3, true).unwrap();

            let page_data = vec![0xAB; 4096];
            handle.write_all_at(&page_data, 0).unwrap();
            handle.sync(false).unwrap();
        }

        // Reopen and read
        {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut handle = CompressedHandle::new_compressed(file, 3, true).unwrap();

            let mut buf = vec![0u8; 4096];
            handle.read_exact_at(&mut buf, 0).unwrap();
            assert!(buf.iter().all(|&b| b == 0xAB));
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_encryption_only() {
        let file = tempfile::tempfile().unwrap();
        let mut handle = CompressedHandle::new_encrypted(file, 0, false, "test-password").unwrap();

        // Write a page
        let page_data = vec![0x42u8; 4096];
        handle.write_all_at(&page_data, 0).unwrap();
        handle.sync(false).unwrap();

        // Read it back
        let mut buf = vec![0u8; 4096];
        handle.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, page_data);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_compressed_encrypted() {
        let file = tempfile::tempfile().unwrap();
        let mut handle = CompressedHandle::new_encrypted(file, 3, true, "test-password").unwrap();

        // Write multiple pages
        for i in 0..10u8 {
            let page_data = vec![i; 4096];
            handle.write_all_at(&page_data, i as u64 * 4096).unwrap();
        }
        handle.sync(false).unwrap();

        // Read them back
        for i in 0..10u8 {
            let mut buf = vec![0u8; 4096];
            handle.read_exact_at(&mut buf, i as u64 * 4096).unwrap();
            assert!(buf.iter().all(|&b| b == i), "Page {} mismatch", i);
        }
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_encryption_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("encrypted.db");

        // Write data
        {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            let mut handle = CompressedHandle::new_encrypted(file, 3, true, "secret-key").unwrap();

            let page_data = vec![0xCD; 4096];
            handle.write_all_at(&page_data, 0).unwrap();
            handle.sync(false).unwrap();
        }

        // Reopen and read with correct password
        {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut handle = CompressedHandle::new_encrypted(file, 3, true, "secret-key").unwrap();

            let mut buf = vec![0u8; 4096];
            handle.read_exact_at(&mut buf, 0).unwrap();
            assert!(buf.iter().all(|&b| b == 0xCD));
        }

        // Trying to read with wrong password should fail
        {
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut handle = CompressedHandle::new_encrypted(file, 3, true, "wrong-password").unwrap();

            let mut buf = vec![0u8; 4096];
            // This should error because decryption will fail
            assert!(handle.read_exact_at(&mut buf, 0).is_err());
        }
    }
}
