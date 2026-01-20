# sqlite-compress-encrypt-vfs Reconstruction Guide

**Context:** This project was accidentally deleted. This document captures everything from memory/context to rebuild it.

---

## Project Overview

**SQLCEs** (pronounced "cinco seis") - SQLite VFS with compression and encryption support.

### Four VFS Modes

| Mode | Constructor | Features |
|------|-------------|----------|
| **Compressed** | `CompressedVfs::new(path, level)` | Compression only (zstd default) |
| **Passthrough** | `CompressedVfs::passthrough(path)` | No compression or encryption (VFS format with no processing) |
| **Encrypted** | `CompressedVfs::encrypted(path, password)` | AES-256-GCM encryption only, no compression |
| **Compressed+Encrypted** | `CompressedVfs::compressed_encrypted(path, level, password)` | Compress THEN encrypt |

---

## Architecture

### File Structure

```
sqlite-compress-encrypt-vfs/
├── Cargo.toml
├── README.md
├── ROADMAP.md (future features)
├── src/
│   ├── lib.rs          # Main VFS implementation
│   └── dict.rs         # Dictionary compression (stubbed for future)
├── examples/
│   └── quick_bench.rs  # Benchmark all 4 modes
├── tests/
│   └── integration.rs  # Integration tests for all modes
└── benches/
    └── vfs_benchmark.rs # Criterion benchmarks
```

### File Format (64-byte header)

```
Offset  Size  Field
0-7     8     magic: "SQLCEvfS"
8-11    4     version: 1
12-15   4     page_size
16-19   4     page_count
20-23   4     flags (bit 0: compressed, bit 1: encrypted, bit 2: dict_embedded)
24-31   8     index_offset
32-39   8     dict_hash (SHA-256, first 8 bytes)
40-43   4     encryption_key_version
44-47   4     compression_algorithm (0=none, 1=zstd, 2=lz4, 3=snappy, 4=gzip)
48-63   16    RESERVED

[Dictionary data - optional, dynamic size]
[Page index - maps page_num → (offset, size)]
[Page data - compressed and/or encrypted]
```

### Data Pipeline

**Write Path:**
```
SQLite Page → Compress (if enabled) → Encrypt (if enabled) → Disk
```

**Read Path:**
```
Disk → Decrypt (if enabled) → Decompress (if enabled) → SQLite Page
```

---

## Key Implementation Details

### CompressedHandle Structure

```rust
pub struct CompressedHandle {
    file: RwLock<File>,
    header: RwLock<FileHeader>,
    index: RwLock<PageIndex>,
    lock: RwLock<LockKind>,
    compression_level: i32,

    // Mode flags
    compressed: bool,           // Whether using VFS format vs passthrough
    compress_pages: bool,       // Whether to actually compress
    encrypt_pages: bool,        // Whether to encrypt

    // Encryption
    #[cfg(feature = "encryption")]
    encryption_key: Option<[u8; 32]>,
}
```

### CompressedVfs Structure

```rust
pub struct CompressedVfs {
    base_dir: PathBuf,
    compression_level: i32,
    compress: bool,
    encrypt: bool,

    #[cfg(feature = "encryption")]
    password: Option<String>,
}
```

### Constructors

```rust
impl CompressedHandle {
    // Standard compressed mode
    fn new_compressed(file: File, compression_level: i32, compress_pages: bool) -> io::Result<Self>

    // Passthrough (no compression/encryption)
    fn new_passthrough(file: File) -> Self

    // Encrypted mode (with or without compression)
    #[cfg(feature = "encryption")]
    fn new_encrypted(file: File, compression_level: i32, compress_pages: bool, password: &str) -> io::Result<Self>
}

impl CompressedVfs {
    pub fn new<P: AsRef<Path>>(base_dir: P, compression_level: i32) -> Self
    pub fn passthrough<P: AsRef<Path>>(base_dir: P) -> Self

    #[cfg(feature = "encryption")]
    pub fn encrypted<P: AsRef<Path>>(base_dir: P, password: &str) -> Self

    #[cfg(feature = "encryption")]
    pub fn compressed_encrypted<P: AsRef<Path>>(base_dir: P, compression_level: i32, password: &str) -> Self
}
```

### Encryption Implementation

**Key Derivation:**
```rust
fn derive_key(password: &str) -> io::Result<[u8; 32]> {
    // Simple SHA-256 for now (production: use Argon2 with salt)
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    Ok(key)
}
```

**Encryption (AES-256-GCM):**
```rust
#[cfg(feature = "encryption")]
fn encrypt(&self, data: &[u8], page_num: u64) -> io::Result<Vec<u8>> {
    let key = self.encryption_key.as_ref().ok_or(...)?;
    let cipher = Aes256Gcm::new(key.into());

    // Use page number as deterministic nonce (12 bytes)
    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[0..8].copy_from_slice(&page_num.to_le_bytes());
    let nonce = Nonce::from_slice(&nonce_bytes);

    cipher.encrypt(nonce, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Encryption failed: {}", e)))
}

#[cfg(feature = "encryption")]
fn decrypt(&self, data: &[u8], page_num: u64) -> io::Result<Vec<u8>> {
    let key = self.encryption_key.as_ref().ok_or(...)?;
    let cipher = Aes256Gcm::new(key.into());

    let mut nonce_bytes = [0u8; 12];
    nonce_bytes[0..8].copy_from_slice(&page_num.to_le_bytes());
    let nonce = Nonce::from_slice(&nonce_bytes);

    cipher.decrypt(nonce, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Decryption failed: {}", e)))
}
```

### Read/Write Implementation

**write_all_at:**
```rust
fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
    if !self.compressed {
        // Passthrough mode: direct write
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))?;
        return file.write_all(buf);
    }

    // Set page size on first write
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

    // Step 1: Compress if enabled
    let data = if self.compress_pages {
        self.compress(buf)?
    } else {
        buf.to_vec()
    };

    // Step 2: Encrypt if enabled
    #[cfg(feature = "encryption")]
    let data = if self.encrypt_pages {
        self.encrypt(&data, page_num)?
    } else {
        data
    };

    let data_size = data.len() as u32;

    // Write to file with variable-size slots
    let mut index = self.index.write();
    let mut file = self.file.write();

    let file_offset = if let Some(&(existing_offset, existing_size)) = index.entries.get(&page_num) {
        if data_size <= existing_size {
            existing_offset  // Reuse slot if it fits
        } else {
            let offset = index.next_offset;
            index.next_offset = offset + data_size as u64;
            offset
        }
    } else {
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
```

**read_exact_at:**
```rust
fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
    if !self.compressed {
        // Passthrough: direct read
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

        // Read from disk
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(file_offset))?;
        let mut stored_data = vec![0u8; compressed_size as usize];
        file.read_exact(&mut stored_data)?;

        // Step 1: Decrypt if enabled
        #[cfg(feature = "encryption")]
        let stored_data = if self.encrypt_pages {
            self.decrypt(&stored_data, page_num)?
        } else {
            stored_data
        };

        // Step 2: Decompress if enabled
        let decompressed = if self.compress_pages {
            self.decompress(&stored_data)?
        } else {
            stored_data
        };

        // Copy to buffer
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
```

---

## Cargo.toml

```toml
[package]
name = "sqlite-compress-encrypt-vfs"
version = "0.1.0"
edition = "2021"
description = "SQLCEs (cinco seis) - SQLite VFS with compression + encryption, custom compression dictionary support, WAL mode, full-text search"
license = "Apache-2.0"
repository = "https://github.com/russellromney/sqlite-compress-encrypt-vfs"
keywords = ["sqlite", "compression", "encryption", "vfs", "zstd"]
categories = ["database", "compression", "cryptography"]

[features]
default = ["zstd"]
zstd = ["dep:zstd"]
lz4 = ["dep:lz4_flex"]
snappy = ["dep:snap"]
gzip = ["dep:flate2"]
encryption = ["dep:aes-gcm", "dep:sha2", "dep:rand"]

[dependencies]
sqlite-vfs = "0.2"
zstd = { version = "0.13", optional = true }
lz4_flex = { version = "0.11", optional = true }
snap = { version = "1.1", optional = true }
flate2 = { version = "1.0", optional = true }
thiserror = "1"
parking_lot = "0.12"

# Encryption (optional)
aes-gcm = { version = "0.10", optional = true }
sha2 = { version = "0.10", optional = true }
rand = { version = "0.8", optional = true }

[dev-dependencies]
rusqlite = { version = "0.35", features = ["bundled"] }
tempfile = "3"
criterion = "0.5"

[[bench]]
name = "vfs_benchmark"
harness = false
```

---

## Tests

### Unit Tests (9 total)

Located in `src/lib.rs`:

1. `test_compress_decompress` - Basic compression roundtrip
2. `test_header_roundtrip` - File header serialization
3. `test_write_read_page` - Single page write/read
4. `test_multiple_pages` - Multiple page operations
5. `test_persistence` - Close and reopen database
6. `test_encryption_only` (encryption feature) - Encrypt without compression
7. `test_compressed_encrypted` (encryption feature) - Both compression and encryption
8. `test_encryption_persistence` (encryption feature) - Reopen with correct/wrong password
9. Dictionary tests in `dict.rs`

### Integration Tests (8 total)

Located in `tests/integration.rs`:

1. `test_basic_operations` - CREATE TABLE, INSERT, SELECT
2. `test_large_data` - 100KB blobs, 10 rows
3. `test_wal_mode` - WAL mode with checkpoints
4. `test_persistence_with_reopen` - Close and reopen
5. `test_passthrough_mode` - Passthrough VFS mode
6. `test_encrypted_mode` (encryption feature) - Encryption-only mode
7. `test_compressed_encrypted_mode` (encryption feature) - Both modes
8. `test_all_four_modes_comparison` (encryption feature) - All 4 modes with same data

---

## Migration & CLI Tool Plans

### Migration Strategy

**VACUUM INTO** is the ONLY supported migration method (safe, atomic, no corruption risk):

```sql
-- Migrate between any two modes
ATTACH DATABASE 'new.db' AS new USING new_vfs;
VACUUM main INTO new;
-- Then: mv new.db old.db
```

### sqlces CLI Tool

**Planned commands:**

```bash
# Inspect database metadata
sqlces inspect data.db
# Output: Mode, magic, compression, dict, encryption, page count, etc.

# Train dictionary FROM DATABASE (not just samples!)
sqlces dict train \
  --from-db data.db \
  --output app.dict \
  --size 100KB

# Train from samples (alternative)
sqlces dict train \
  --samples ./logs/*.json \
  --output app.dict

# Embed dictionary (creates new DB via VACUUM INTO)
sqlces dict embed \
  --source data.db \
  --dict app.dict \
  --output data-with-dict.db

# Extract embedded dictionary
sqlces dict extract \
  --source data.db \
  --output extracted.dict

# Migrate between modes (wraps VACUUM INTO with safety)
sqlces migrate \
  --from-mode compressed \
  --to-mode encrypted \
  --source old.db \
  --dest new.db \
  --password "secret"
# Should be INTERACTIVE with sensible defaults if missing flags

# Benchmark modes
sqlces bench \
  --modes compressed,encrypted,passthrough,both \
  --data ./samples/
```

### Dictionary Storage

**Dynamic dictionary area (no wasted space):**

```
[64-byte header]
[Dictionary data - variable size, if dict_embedded flag set]
[Page index]
[Page data]
```

- **Default:** Embed dictionary (self-contained, foolproof)
- **Opt-in:** Extract to separate file (smaller DB size)
- Dictionary hash stored in header for verification
- If dict_embedded=false and hash doesn't match external dict, fail-fast with clear error

---

## Security Considerations

- **Encryption:** AES-256-GCM (authenticated encryption, tamper detection)
- **Key Derivation:** Currently SHA-256(password) - TODO: Argon2 with salt
- **Nonce:** Deterministic based on page number (12 bytes)
- **Key Versioning:** Header stores `encryption_key_version` for rotation support
- **Wrong Password:** Decryption fails with clear error, no silent corruption

---

## Performance Notes

From `quick_bench.rs` testing (with real corpus data):

- **Compressed:** ~2-3x slower writes, ~1.5x slower reads, ~5-10x smaller files
- **Passthrough:** Nearly identical to plain SQLite
- **Encrypted:** ~1.2x slower (AES-NI hardware acceleration)
- **Compressed+Encrypted:** Combines both overheads

---

## Next Steps to Rebuild

1. **Phase 1:** Create basic structure
   - `Cargo.toml` with dependencies
   - `src/lib.rs` with magic, header, and basic structures
   - Commit + push

2. **Phase 2:** Compression support
   - Implement compress/decompress methods for all features (zstd, lz4, snappy, gzip)
   - `CompressedVfs::new()` and `new_compressed()`
   - Commit + push

3. **Phase 3:** Passthrough mode
   - `CompressedVfs::passthrough()`
   - `compress_pages: bool` flag
   - Commit + push

4. **Phase 4:** Encryption
   - Add AES-GCM encrypt/decrypt
   - `CompressedVfs::encrypted()` and `compressed_encrypted()`
   - Update read/write pipeline
   - Commit + push

5. **Phase 5:** Tests
   - Unit tests in `src/lib.rs`
   - Integration tests in `tests/integration.rs`
   - Commit + push

6. **Phase 6:** Documentation
   - README with usage examples
   - Migration guide
   - ROADMAP for `sqlces` CLI
   - Commit + push

---

## Important Design Decisions

1. **Magic: "SQLCEvfS"** - 8 bytes, identifies our format
2. **Header: 64 bytes** - Small, industry-standard size
3. **Dynamic dictionary** - No wasted space when not using dict
4. **Compression before encryption** - Better compression ratios
5. **VACUUM INTO only** - Safety over convenience
6. **Embedded dict by default** - Foolproof, portable
7. **Training from DB** - Self-optimizing compression

---

## Code Not Fully Reconstructed

The following still needs to be written:

- Complete `src/lib.rs` implementation (structure is here, need full code)
- `src/dict.rs` implementation (dictionary training from DB)
- `examples/quick_bench.rs` (benchmark harness)
- `tests/integration.rs` (full test suite)
- Full README with examples
- `sqlces` CLI tool (future)

But the ARCHITECTURE and LOGIC are all captured above!
