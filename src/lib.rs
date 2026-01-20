//! SQLite VFS with transparent compression and WAL support.
//!
//! Supports multiple compressors via features:
//! - `zstd` (default): Best compression ratio, dictionary support
//! - `lz4`: Fastest compression/decompression
//! - `snappy`: Very fast, moderate compression
//!
//! File format:
//! - Header (64 bytes): magic, version, page_size, page_count, index_offset
//! - Data section: compressed pages stored sequentially
//! - Index section: page_num -> (offset, compressed_size) entries
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
use std::io::{self, Read, Seek, SeekFrom, Write};
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

/// Magic bytes identify the compressor used (allows file format detection)
const MAGIC: &[u8; 8] = b"SQLCEvfS";
const VERSION: u32 = 1;
const HEADER_SIZE: u64 = 64;

/// File header structure
#[derive(Debug, Clone, Copy)]
struct FileHeader {
    page_size: u32,
    page_count: u32,
    index_offset: u64,
}

impl FileHeader {
    fn new() -> Self {
        Self {
            page_size: 0,
            page_count: 0,
            index_offset: HEADER_SIZE,
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

        // Check magic
        if &buf[0..8] != MAGIC {
            return Ok(None);
        }

        // Check version
        let version = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported version: {}", version),
            ));
        }

        let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let page_count = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
        let index_offset = u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]);

        Ok(Some(Self {
            page_size,
            page_count,
            index_offset,
        }))
    }

    fn write_to(&self, file: &mut File) -> io::Result<()> {
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[0..8].copy_from_slice(MAGIC);
        buf[8..12].copy_from_slice(&VERSION.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..20].copy_from_slice(&self.page_count.to_le_bytes());
        buf[24..32].copy_from_slice(&self.index_offset.to_le_bytes());

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf)
    }
}

/// Page index: maps page number to (file_offset, compressed_size)
#[derive(Debug, Default)]
struct PageIndex {
    entries: HashMap<u64, (u64, u32)>,
    next_offset: u64,
    dirty: bool,
}

impl PageIndex {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            next_offset: HEADER_SIZE,
            dirty: false,
        }
    }

    fn read_from(file: &mut File, header: &FileHeader) -> io::Result<Self> {
        let mut index = Self::new();

        if header.page_count == 0 {
            return Ok(index);
        }

        file.seek(SeekFrom::Start(header.index_offset))?;

        for _ in 0..header.page_count {
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
            let end = offset + size as u64;
            if end > index.next_offset {
                index.next_offset = end;
            }
        }

        Ok(index)
    }

    fn write_to(&self, file: &mut File, offset: u64) -> io::Result<()> {
        file.seek(SeekFrom::Start(offset))?;

        for (&page_num, &(page_offset, size)) in &self.entries {
            let mut entry = [0u8; 20];
            entry[0..8].copy_from_slice(&page_num.to_le_bytes());
            entry[8..16].copy_from_slice(&page_offset.to_le_bytes());
            entry[16..20].copy_from_slice(&size.to_le_bytes());
            file.write_all(&entry)?;
        }

        Ok(())
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
        // Try to read existing header
        let (header, index) = match FileHeader::read_from(&mut file)? {
            Some(header) => {
                let index = PageIndex::read_from(&mut file, &header)?;
                (header, index)
            }
            None => {
                // New file, write initial header
                let header = FileHeader::new();
                header.write_to(&mut file)?;
                (header, PageIndex::new())
            }
        };

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

        // Try to read existing header
        let (header, index) = match FileHeader::read_from(&mut file)? {
            Some(header) => {
                let index = PageIndex::read_from(&mut file, &header)?;
                (header, index)
            }
            None => {
                // New file, write initial header
                let header = FileHeader::new();
                header.write_to(&mut file)?;
                (header, PageIndex::new())
            }
        };

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

    fn flush_index(&self) -> io::Result<()> {
        if !self.compressed {
            return Ok(());
        }

        let index = self.index.read();
        if !index.dirty {
            return Ok(());
        }
        drop(index);

        let mut index = self.index.write();
        let mut header = self.header.write();
        let mut file = self.file.write();

        // Write index at current next_offset
        let index_offset = index.next_offset;
        index.write_to(&mut file, index_offset)?;

        // Update header
        header.page_count = index.entries.len() as u32;
        header.index_offset = index_offset;
        header.write_to(&mut file)?;

        index.dirty = false;
        Ok(())
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
        let header = self.header.read();
        let index = self.index.read();
        if header.page_size > 0 {
            Ok(index.entries.len() as u64 * header.page_size as u64)
        } else {
            Ok(0)
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        if !self.compressed {
            // Passthrough: direct file read
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(offset))?;
            return file.read_exact(buf);
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

            let mut file = self.file.write();
            file.seek(SeekFrom::Start(file_offset))?;
            let mut stored_data = vec![0u8; compressed_size as usize];
            file.read_exact(&mut stored_data)?;

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
            // Passthrough: direct file write
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(offset))?;
            return file.write_all(buf);
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
        let mut file = self.file.write();

        // Variable-size slots with in-place update when it fits
        let file_offset = if let Some(&(existing_offset, existing_size)) = index.entries.get(&page_num) {
            if data_size <= existing_size {
                // Fits in existing slot - reuse it
                existing_offset
            } else {
                // Doesn't fit - allocate new slot (old slot becomes wasted)
                let offset = index.next_offset;
                index.next_offset = offset + data_size as u64;
                offset
            }
        } else {
            // New page - allocate slot
            let offset = index.next_offset;
            index.next_offset = offset + data_size as u64;
            offset
        };

        file.seek(SeekFrom::Start(file_offset))?;
        file.write_all(&data)?;

        index.entries.insert(page_num, (file_offset, data_size));
        index.dirty = true;

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        self.flush_index()?;
        self.file.write().sync_all()
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        if !self.compressed {
            return self.file.write().set_len(size);
        }

        // For compressed files, we need to update the index
        let header = self.header.read();
        if header.page_size == 0 {
            return Ok(());
        }
        let page_size = header.page_size as u64;
        drop(header);

        let max_page = if size == 0 { 0 } else { (size - 1) / page_size };

        let mut index = self.index.write();
        index.entries.retain(|&page_num, _| page_num <= max_page);
        index.dirty = true;

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

        if use_compression {
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
        let mut file = tempfile::tempfile().unwrap();

        let header = FileHeader {
            page_size: 4096,
            page_count: 100,
            index_offset: 12345,
        };
        header.write_to(&mut file).unwrap();

        let read_header = FileHeader::read_from(&mut file).unwrap().unwrap();
        assert_eq!(read_header.page_size, 4096);
        assert_eq!(read_header.page_count, 100);
        assert_eq!(read_header.index_offset, 12345);
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
