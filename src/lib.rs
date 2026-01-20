//! SQLite VFS with transparent compression and WAL support.
//!
//! Supports multiple compressors via features:
//! - `zstd` (default): Best compression ratio, dictionary support
//! - `lz4`: Fastest compression/decompression
//! - `snappy`: Very fast, moderate compression
//!
//! File format (v2 - inline index):
//! - Header (64 bytes): magic, version, page_size, write_end
//! - Data section: page records stored sequentially, each with inline metadata
//!   - Each record: page_num(8) + size(4) + data(size)
//! - On open: scan to build in-memory index (fast sequential read)
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
use std::time::Duration;

// Compressor-specific imports and magic bytes
#[cfg(feature = "zstd")]
use zstd::{decode_all, encode_all};

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
use sha2::{Digest, Sha256};

/// Magic bytes for v2 format (inline index)
const MAGIC_V2: &[u8; 8] = b"SQLCEvf2";
/// Magic bytes for v1 format (legacy, separate index)
const MAGIC_V1: &[u8; 8] = b"SQLCEvfS";
const VERSION: u32 = 2;
const HEADER_SIZE: u64 = 64;
/// Size of inline record header: page_num(8) + size(4)
const RECORD_HEADER_SIZE: u64 = 12;

/// File header structure (v2)
#[derive(Debug, Clone, Copy)]
struct FileHeader {
    page_size: u32,
    /// End of data section (where next write should go)
    write_end: u64,
}

impl FileHeader {
    fn new() -> Self {
        Self {
            page_size: 0,
            write_end: HEADER_SIZE,
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

        // Check for v2 magic
        if &buf[0..8] == MAGIC_V2 {
            let version = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
            if version != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unsupported version: {}", version),
                ));
            }

            let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
            let write_end = u64::from_le_bytes([
                buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
            ]);

            return Ok(Some(Self { page_size, write_end }));
        }

        // Check for v1 magic (legacy format) - convert on open
        if &buf[0..8] == MAGIC_V1 {
            let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
            // For v1, we need to scan to find write_end during index loading
            // Just return the page_size, PageIndex::read_from_v1 will handle the rest
            return Ok(Some(Self {
                page_size,
                write_end: HEADER_SIZE, // Will be updated during scan
            }));
        }

        Ok(None)
    }

    fn write_to(&self, file: &mut File) -> io::Result<()> {
        use std::os::unix::fs::FileExt;

        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC_V2);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.write_end.to_le_bytes());

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

    /// Scan file to build index from inline records (v2 format)
    /// Uses buffered I/O for performance on large files
    fn scan_from_file(file: &mut File, header: &FileHeader) -> io::Result<Self> {
        use std::io::BufReader;

        let start = std::time::Instant::now();
        let mut index = Self::new();
        let mut pos = HEADER_SIZE;
        let mut record_count = 0u64;
        let mut max_page_num: u64 = 0;

        // Use a large buffer for sequential scanning (1MB)
        file.seek(SeekFrom::Start(HEADER_SIZE))?;
        let mut reader = BufReader::with_capacity(1024 * 1024, file);

        while pos < header.write_end {
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
        Ok(index)
    }

    /// Read legacy v1 format (separate index section)
    fn read_from_v1(file: &mut File, page_count: u32, index_offset: u64) -> io::Result<(Self, u64)> {
        let mut index = Self::new();
        let mut max_end: u64 = HEADER_SIZE;

        if page_count == 0 {
            return Ok((index, max_end));
        }

        file.seek(SeekFrom::Start(index_offset))?;

        for _ in 0..page_count {
            let mut entry = [0u8; 20]; // page_num(8) + offset(8) + size(4)
            file.read_exact(&mut entry)?;

            let page_num = u64::from_le_bytes([
                entry[0], entry[1], entry[2], entry[3],
                entry[4], entry[5], entry[6], entry[7],
            ]);
            let offset = u64::from_le_bytes([
                entry[8], entry[9], entry[10], entry[11],
                entry[12], entry[13], entry[14], entry[15],
            ]);
            let size = u32::from_le_bytes([entry[16], entry[17], entry[18], entry[19]]);

            index.entries.insert(page_num, (offset, size));
            if page_num > index.max_page {
                index.max_page = page_num;
            }
            let end = offset + size as u64;
            if end > max_end {
                max_end = end;
            }
        }

        Ok((index, max_end))
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
}

impl CompressedHandle {
    fn new_compressed(mut file: File, compression_level: i32, compress_pages: bool) -> io::Result<Self> {
        // Try to read existing header and build index by scanning
        let (header, index) = Self::load_file(&mut file)?;

        Ok(Self {
            file: RwLock::new(file),
            header: RwLock::new(header),
            index: RwLock::new(index),
            lock: RwLock::new(LockKind::None),
            compression_level,
            compressed: true,
            compress_pages,
            encrypt_pages: false,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        })
    }

    fn new_passthrough(file: File) -> Self {
        Self {
            file: RwLock::new(file),
            header: RwLock::new(FileHeader::new()),
            index: RwLock::new(PageIndex::new()),
            lock: RwLock::new(LockKind::None),
            compression_level: 0,
            compressed: false,
            compress_pages: false,
            encrypt_pages: false,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        }
    }

    #[cfg(feature = "encryption")]
    fn new_encrypted(mut file: File, compression_level: i32, compress_pages: bool, password: &str) -> io::Result<Self> {
        // Derive encryption key from password
        let key = Self::derive_key(password)?;

        // Try to read existing header and build index by scanning
        let (header, index) = Self::load_file(&mut file)?;

        Ok(Self {
            file: RwLock::new(file),
            header: RwLock::new(header),
            index: RwLock::new(index),
            lock: RwLock::new(LockKind::None),
            compression_level,
            compressed: true,
            compress_pages,
            encrypt_pages: true,
            encryption_key: Some(key),
        })
    }

    /// Load file header and build index by scanning (v2) or reading index section (v1 legacy)
    fn load_file(file: &mut File) -> io::Result<(FileHeader, PageIndex)> {
        // First, check the magic to determine format version
        let mut magic = [0u8; 8];
        file.seek(SeekFrom::Start(0))?;
        match file.read_exact(&mut magic) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Empty file - create new
                let header = FileHeader::new();
                header.write_to(file)?;
                return Ok((header, PageIndex::new()));
            }
            Err(e) => return Err(e),
        }

        if &magic == MAGIC_V2 {
            // V2 format: scan to build index
            let header = FileHeader::read_from(file)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid header"))?;
            let index = PageIndex::scan_from_file(file, &header)?;
            Ok((header, index))
        } else if &magic == MAGIC_V1 {
            // V1 legacy format: read index section, then migrate to v2 on next write
            let mut buf = [0u8; HEADER_SIZE as usize];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buf)?;

            let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
            let page_count = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
            let index_offset = u64::from_le_bytes([
                buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
            ]);

            let (index, write_end) = PageIndex::read_from_v1(file, page_count, index_offset)?;
            let header = FileHeader { page_size, write_end };

            Ok((header, index))
        } else {
            // Unknown format or empty - create new
            let header = FileHeader::new();
            header.write_to(file)?;
            Ok((header, PageIndex::new()))
        }
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
        encode_all(data, self.compression_level)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    #[cfg(feature = "zstd")]
    fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        decode_all(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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

}

/// Stub WAL index (uses default file-based WAL)
pub struct StubWalIndex;

impl sqlite_vfs::wip::WalIndex for StubWalIndex {
    fn map(&mut self, _region: u32) -> Result<[u8; 32768], io::Error> {
        Ok([0u8; 32768])
    }

    fn lock(
        &mut self,
        _locks: Range<u8>,
        _lock: sqlite_vfs::wip::WalIndexLock,
    ) -> Result<bool, io::Error> {
        Ok(true)
    }

    fn delete(self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl DatabaseHandle for CompressedHandle {
    type WalIndex = StubWalIndex;

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
            use std::os::unix::fs::FileExt;
            let file = self.file.read();  // READ lock instead of write lock!
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
            use std::os::unix::fs::FileExt;
            let file = self.file.write();  // WRITE lock needed for writes!
            return file.write_all_at(buf, offset);
        }

        // Set page size from first write
        {
            let mut header = self.header.write();
            if header.page_size == 0 {
                header.page_size = buf.len() as u32;
            }
        }

        let header = self.header.read();
        let page_size = header.page_size as u64;
        drop(header);

        let page_num = offset / page_size;

        // Compress or passthrough based on compress_pages flag
        let data = if self.compress_pages {
            self.compress(buf)?
        } else {
            buf.to_vec()
        };

        // Encrypt if encryption is enabled
        #[cfg(feature = "encryption")]
        let data = if self.encrypt_pages {
            self.encrypt(&data, page_num)?
        } else {
            data
        };

        let data_size = data.len() as u32;

        let mut index = self.index.write();
        let mut header = self.header.write();
        let file = self.file.read(); // Read lock is enough with pwrite

        // Always append new records (simpler and more reliable)
        // In-place update is tricky with variable-size compressed data
        // Old records become dead space and can be compacted later
        let record_offset = header.write_end;
        let data_offset = record_offset + RECORD_HEADER_SIZE;
        header.write_end = data_offset + data_size as u64;

        // Write inline record: page_num(8) + size(4) + data
        use std::os::unix::fs::FileExt;

        // Write record header
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        rec_header[0..8].copy_from_slice(&page_num.to_le_bytes());
        rec_header[8..12].copy_from_slice(&data_size.to_le_bytes());
        file.write_all_at(&rec_header, record_offset)?;

        // Write data
        file.write_all_at(&data, data_offset)?;

        // Update in-memory index (data_offset points to data, not record header)
        index.entries.insert(page_num, (data_offset, data_size));
        if page_num > index.max_page {
            index.max_page = page_num;
        }

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        if !self.compressed {
            return self.file.write().sync_all();
        }

        // V2 format: just update header with write_end and fsync
        // No index rewrite needed - index entries are inline with the data!
        use std::os::unix::fs::FileExt;

        let header = self.header.read();

        // Write header using pwrite (position-aware, no seek needed)
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC_V2);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..16].copy_from_slice(&header.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&header.write_end.to_le_bytes());

        let file = self.file.read();
        file.write_all_at(&buf, 0)?;
        drop(header);
        drop(file);

        self.file.write().sync_all()
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        if !self.compressed {
            return self.file.write().set_len(size);
        }

        // For compressed files, we update the in-memory index
        // Note: In v2 format, truncated pages become "dead" records in the file
        // They will be cleaned up on compaction (future feature)
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
        *self.lock.write() = lock;
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
        Ok(StubWalIndex)
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
        }
    }
}

impl Vfs for CompressedVfs {
    type Handle = CompressedHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.base_dir.join(db);

        eprintln!("VFS open: db={:?}, base_dir={:?}, path={:?}, kind={:?}, access={:?}",
                  db, self.base_dir, path, opts.kind, opts.access);

        // WAL and journal files are stored uncompressed
        let use_compression = matches!(opts.kind, OpenKind::MainDb);

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

        // Use passthrough for WAL/journal files, OR when VFS is in passthrough mode
        let use_vfs_format = use_compression && (self.compress || self.encrypt);

        if use_vfs_format {
            #[cfg(feature = "encryption")]
            {
                if self.encrypt {
                    let password = self.password.as_ref()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No password set for encryption"))?;
                    CompressedHandle::new_encrypted(file, self.compression_level, self.compress, password)
                } else {
                    CompressedHandle::new_compressed(file, self.compression_level, self.compress)
                }
            }
            #[cfg(not(feature = "encryption"))]
            {
                CompressedHandle::new_compressed(file, self.compression_level, self.compress)
            }
        } else {
            Ok(CompressedHandle::new_passthrough(file))
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
    /// Format version (1 or 2)
    pub version: u32,
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

    let version = if &magic == MAGIC_V2 {
        2
    } else if &magic == MAGIC_V1 {
        1
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Not a valid SQLCEs database file",
        ));
    };

    // Read full header
    let mut buf = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let write_end = if version == 2 {
        u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ])
    } else {
        // V1: index section is at the end, compute from page_count
        let page_count = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
        let index_offset = u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]);
        // For v1, write_end is approximate (index comes after data)
        index_offset.min(file_size - (page_count as u64 * 20))
    };

    // Scan records to build index and count total records
    let mut entries: HashMap<u64, (u64, u32)> = HashMap::new();
    let mut total_records: u64 = 0;
    let mut max_page: u64 = 0;

    file.seek(SeekFrom::Start(HEADER_SIZE))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, &mut file);
    let mut pos = HEADER_SIZE;

    while pos < write_end {
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
        version,
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

    if &buf[0..8] != MAGIC_V2 && &buf[0..8] != MAGIC_V1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Not a valid SQLCEs database file",
        ));
    }

    let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let write_end = u64::from_le_bytes([
        buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
    ]);

    // Build index by scanning (keeps only latest version of each page)
    let mut entries: HashMap<u64, (u64, u32)> = HashMap::new();
    let mut max_page: u64 = 0;

    file.seek(SeekFrom::Start(HEADER_SIZE))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, &mut file);
    let mut pos = HEADER_SIZE;

    while pos < write_end {
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

    // Write header
    let mut new_write_end = HEADER_SIZE;
    let mut header_buf = [0u8; HEADER_SIZE as usize];
    header_buf[0..8].copy_from_slice(MAGIC_V2);
    header_buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header_buf[12..16].copy_from_slice(&page_size.to_le_bytes());
    // write_end will be updated after writing all records
    temp_file.write_all_at(&header_buf, 0)?;

    // Copy each live page
    for (page_num, (data_offset, data_size)) in pages {
        // Read data from original file
        let mut data = vec![0u8; data_size as usize];
        file.read_exact_at(&mut data, data_offset)?;

        // Write record header
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        rec_header[0..8].copy_from_slice(&page_num.to_le_bytes());
        rec_header[8..12].copy_from_slice(&data_size.to_le_bytes());
        temp_file.write_all_at(&rec_header, new_write_end)?;

        // Write data
        temp_file.write_all_at(&data, new_write_end + RECORD_HEADER_SIZE)?;
        new_write_end += RECORD_HEADER_SIZE + data_size as u64;
    }

    // Update header with final write_end
    header_buf[16..24].copy_from_slice(&new_write_end.to_le_bytes());
    temp_file.write_all_at(&header_buf, 0)?;
    temp_file.sync_all()?;
    drop(temp_file);
    drop(file);

    // Atomic replace
    std::fs::rename(&temp_path, path)?;

    let new_size = std::fs::metadata(path)?.len();
    let freed = original_size.saturating_sub(new_size);
    Ok(freed)
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
            write_end: 12345,
        };

        // Use pwrite like the real implementation does
        use std::os::unix::fs::FileExt;
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC_V2);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..16].copy_from_slice(&header.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&header.write_end.to_le_bytes());
        file.write_all_at(&buf, 0).unwrap();

        let mut file = file; // Need &mut for read_from
        let read_header = FileHeader::read_from(&mut file).unwrap().unwrap();
        assert_eq!(read_header.page_size, 4096);
        assert_eq!(read_header.write_end, 12345);
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
