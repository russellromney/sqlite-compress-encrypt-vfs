# sqlite-compress-encrypt-vfs

**SQLCEs** (pronounced "cinco seis") - SQLite VFS with transparent compression and encryption.

[![Crates.io](https://img.shields.io/crates/v/sqlite-compress-encrypt-vfs.svg)](https://crates.io/crates/sqlite-compress-encrypt-vfs)
[![License](https://img.shields.io/crates/l/sqlite-compress-encrypt-vfs.svg)](LICENSE)

## Features

**Four VFS Modes:**

| Mode | Constructor | Use Case |
|------|-------------|----------|
| **Compressed** | `CompressedVfs::new(dir, level)` | Maximum storage savings |
| **Passthrough** | `CompressedVfs::passthrough(dir)` | Benchmarking, no overhead |
| **Encrypted** | `CompressedVfs::encrypted(dir, password)` | Security without compression |
| **Compressed+Encrypted** | `CompressedVfs::compressed_encrypted(dir, level, password)` | Both savings and security |

**Compression Algorithms:**
- `zstd` (default) - Best compression ratio, dictionary support
- `lz4` - Fastest compression/decompression
- `snappy` - Very fast, moderate compression
- `gzip` - Wide compatibility

**Encryption:**
- AES-256-GCM authenticated encryption
- Password-based key derivation (SHA-256)
- Deterministic nonces (page-based)
- Wrong password detection

**File Format:**
- Magic: `SQLCEvfS` (8 bytes)
- 64-byte header with metadata
- Variable-size page slots
- Optional embedded dictionary support

## Installation

```toml
[dependencies]
sqlite-compress-encrypt-vfs = "0.1"
```

With encryption:
```toml
[dependencies]
sqlite-compress-encrypt-vfs = { version = "0.1", features = ["encryption"] }
```

With parallel compaction (uses rayon):
```toml
[dependencies]
sqlite-compress-encrypt-vfs = { version = "0.1", features = ["parallel"] }
```

All features:
```toml
[dependencies]
sqlite-compress-encrypt-vfs = { version = "0.1", features = ["encryption", "parallel"] }
```

Alternative compressors:
```toml
# Use LZ4 instead of zstd
sqlite-compress-encrypt-vfs = { version = "0.1", default-features = false, features = ["lz4"] }
```

## Quick Start

### Compressed Mode

```rust
use sqlite_compress_encrypt_vfs::{register, CompressedVfs};
use rusqlite::Connection;

// Register VFS
let vfs = CompressedVfs::new("./data", 3); // compression level 1-22
register("compressed", vfs)?;

// Use with rusqlite
let conn = Connection::open_with_flags_and_vfs(
    "./data/app.db",
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    "compressed",
)?;

// Use normally - compression is transparent
conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", [])?;
conn.execute("INSERT INTO users VALUES (1, 'Alice')", [])?;
```

### Encrypted Mode

```rust
use sqlite_compress_encrypt_vfs::{register, CompressedVfs};

let vfs = CompressedVfs::encrypted("./data", "my-secret-password");
register("encrypted", vfs)?;

let conn = Connection::open_with_flags_and_vfs(
    "./data/secure.db",
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    "encrypted",
)?;

// Data is encrypted at rest
conn.execute("CREATE TABLE secrets (data TEXT)", [])?;
```

### Compressed + Encrypted Mode

```rust
let vfs = CompressedVfs::compressed_encrypted("./data", 3, "password");
register("both", vfs)?;

// Compress THEN encrypt for maximum security and savings
```

### Passthrough Mode

```rust
let vfs = CompressedVfs::passthrough("./data");
register("passthrough", vfs)?;

// No compression or encryption, useful for benchmarking
```

## Benchmarking

### Quick Benchmark

```bash
# Benchmark VFS modes (read/write throughput)
cargo run --example quick_bench --features encryption --release

# Benchmark parallel vs serial compaction
cargo run --release --bin sqlces-bench --features "encryption parallel" -- \
  bench-compact --rows 10000 --iterations 3
```

### sqlces-bench CLI

The `sqlces-bench` tool provides comprehensive benchmarking:

```bash
# Benchmark parallel compaction speedup
cargo run --release --bin sqlces-bench --features "encryption parallel" -- \
  bench-compact --rows 50000 --iterations 3 --compression-level 3

# Benchmark VFS modes with an existing database
cargo run --release --bin sqlces-bench --features encryption -- \
  --database path/to/db.db \
  --duration-secs 10 \
  --reader-threads 4 \
  --writer-threads 2
```

### Expected Results

**Compression Ratios** (by data type):
| Data Type | Typical Ratio | Notes |
|-----------|---------------|-------|
| JSON/XML | 5-10x | Great for structured data |
| Repeated text (logs) | 10-20x | Excellent compression |
| Structured data | 3-7x | Good for typical workloads |
| Binary/random | 1.0x | No benefit |

**Performance Overhead** (WAL mode):
| Mode | Write | Read | Notes |
|------|-------|------|-------|
| Passthrough | baseline | baseline | No overhead |
| Compressed | ~2-3x slower | ~1.5-2x slower | CPU-bound |
| Encrypted | ~1.2x slower | ~1.2x slower | AES-NI accelerated |
| Both | ~2.5-3x slower | ~2x slower | Compress then encrypt |

**Parallel Compaction Speedup** (M-series Mac, 8 cores):
| Rows | File Size | Serial | Parallel | Speedup |
|------|-----------|--------|----------|---------|
| 10k | 0.7 MB | 0.73s | 0.24s | **3.0x** |
| 50k | 3.6 MB | 3.82s | 1.31s | **2.9x** |
| 100k | 7.2 MB | 8.22s | 2.46s | **3.3x** |

Speedup scales with core count - expect 4-8x on servers with more cores.

## Migration Guide

### Between VFS Modes

**Use SQLite's `VACUUM INTO` for safe, atomic migration:**

```sql
-- Example: Migrate from compressed to encrypted
ATTACH DATABASE 'new_encrypted.db' AS new USING encrypted_vfs;
VACUUM main INTO new;
DETACH DATABASE new;

-- Then atomically rename:
-- mv new_encrypted.db production.db
```

This works for **all mode transitions:**
- Compressed → Encrypted
- Encrypted → Compressed+Encrypted
- Passthrough → Compressed
- Any → Any

**Why VACUUM INTO?**
- ✅ Atomic operation (no partial migration)
- ✅ Guaranteed data integrity
- ✅ Uses SQLite's battle-tested code
- ✅ Works across any VFS modes
- ❌ Requires 2x disk space temporarily

**⚠️ IMPORTANT: Always backup before migration!**

### Dictionary Compression (Future)

Train a custom compression dictionary from your data:

```rust
use sqlite_compress_vfs::dict::train_from_database;

// Train from existing database
let dict = train_from_database("prod.db", 100 * 1024)?; // 100KB dict
std::fs::write("prod.dict", &dict)?;

// TODO: API to use dictionary with VFS (coming in v0.2)
```

## Architecture

### File Format

```
[64-byte header]
├─ Magic: "SQLCEvfS"
├─ Version: 1
├─ Page size, page count
├─ Flags (compressed, encrypted, dict_embedded)
├─ Index offset
├─ Dict hash (SHA-256, first 8 bytes)
├─ Encryption key version
├─ Compression algorithm
└─ Reserved bytes

[Dictionary data - optional, dynamic size]

[Page index]
└─ Entries: page_num → (offset, size)

[Page data]
└─ Variable-size slots (compressed and/or encrypted)
```

### Data Pipeline

**Write:**
```
SQLite Page → Compress (if enabled) → Encrypt (if enabled) → Disk
```

**Read:**
```
Disk → Decrypt (if enabled) → Decompress (if enabled) → SQLite Page
```

### WAL Files

WAL and journal files are **always stored uncompressed** for compatibility and performance. Only the main database file uses the VFS format.

## Future: `sqlces` CLI Tool

Planned commands for managing databases:

```bash
# Inspect database metadata
sqlces inspect data.db
# Output: Mode, compression, encryption, dict info, page count

# Train dictionary from database (not just samples!)
sqlces dict train --from-db prod.db --output prod.dict --size 100KB

# Train from samples
sqlces dict train --samples ./logs/*.json --output app.dict

# Embed dictionary in database
sqlces dict embed --source data.db --dict app.dict --output data-with-dict.db

# Extract embedded dictionary
sqlces dict extract --source data.db --output extracted.dict

# Migrate between modes (wraps VACUUM INTO with safety checks)
sqlces migrate \
  --from-mode compressed \
  --to-mode encrypted \
  --source old.db \
  --dest new.db \
  --password "secret"
# Interactive with sensible defaults if flags missing

# Benchmark modes
sqlces bench --modes compressed,encrypted,passthrough,both --data ./samples/
```

**Design Principles:**
- Safety first (VACUUM INTO for all migrations, no in-place rekey)
- Interactive with sensible defaults
- Train dictionaries from databases, not just samples
- Embed dictionaries by default (foolproof, portable)
- Clear error messages

## Security Considerations

**Encryption:**
- AES-256-GCM (authenticated encryption with tamper detection)
- 12-byte deterministic nonces (page number based)
- Key derivation: Currently SHA-256(password) - TODO: Argon2 with salt
- Key versioning support in header for future rotation

**Limitations:**
- File-level metadata is NOT encrypted (page count, index structure)
- Database schema is NOT encrypted
- For maximum security, use full-disk encryption + this VFS

**Wrong Password Detection:**
Decryption fails immediately with clear error - no silent corruption.

## Performance Tips

1. **Use WAL mode:** Much faster than default rollback journal
   ```sql
   PRAGMA journal_mode=WAL;
   PRAGMA synchronous=NORMAL;
   ```

2. **Batch writes:** Use transactions for bulk inserts
   ```rust
   conn.execute("BEGIN", [])?;
   // ... many inserts
   conn.execute("COMMIT", [])?;
   ```

3. **Choose compression level wisely:**
   - Level 1-3: Fast, good for hot data
   - Level 10-15: Balanced
   - Level 20-22: Maximum compression, slow

4. **Compression works best on:**
   - JSON/XML data
   - Repeated text (logs, sessions)
   - Structured data with patterns

5. **Compression doesn't help:**
   - Already compressed data (images, videos)
   - Encrypted data
   - Random binary data

## Examples

See `examples/` directory:
- `quick_bench.rs` - Benchmark all 4 modes

## Testing

```bash
# Without encryption
cargo test

# With encryption
cargo test --features encryption

# All features
cargo test --all-features
```

## Roadmap

See [ROADMAP.md](ROADMAP.md) for detailed plans.

**v0.1 (Current):**
- [x] Four VFS modes (compressed, encrypted, both, passthrough)
- [x] Dictionary compression support
- [x] Parallel compaction with rayon (`parallel` feature)
- [x] `compact_if_needed()` helper
- [x] `sqlces-bench` CLI tool

**v0.2 (Planned):**
- [ ] Dictionary embedding in database files
- [ ] `sqlces` CLI tool (inspect, migrate, dict commands)
- [ ] Argon2 key derivation with salt
- [ ] Key rotation support

**v0.3 (Future):**
- [ ] Encryption key wrapping
- [ ] Page-level checksums (CRC32)
- [ ] Multi-version dictionary support

## Contributing

Contributions welcome! Please:
1. Add tests for new features
2. Update documentation
3. Run `cargo fmt` and `cargo clippy`
4. Ensure `cargo test --all-features` passes

## License

Apache-2.0

## Acknowledgements

This implementation was inspired by concepts from:
- [mlin/sqlite_zstd_vfs](https://github.com/mlin/sqlite_zstd_vfs) - C++ SQLite VFS with zstd (MIT)
- [apersson/redis-compression-module](https://github.com/apersson/redis-compression-module) - Dictionary compression concepts
- [Twitter cache traces](https://github.com/twitter/cache-trace) - Public cache workload data (CC-BY)

## FAQ

**Q: Can I change modes after creating a database?**
A: Use `VACUUM INTO` to migrate between any modes. See Migration Guide above.

**Q: Does encryption slow things down a lot?**
A: ~20% overhead on modern CPUs with AES-NI acceleration. Compression overhead is larger (~2-3x).

**Q: Can I use this in production?**
A: The code is tested but still v0.1. Recommended: thorough testing + backups.

**Q: How does this compare to SQLCipher?**
A: SQLCipher encrypts pages before SQLite sees them. This VFS encrypts after, allowing composition with compression. Different design, different tradeoffs.

**Q: Why "SQLCEs" / "cinco seis"?**
A: SQL Compression & Encryption → SQLCE → "C"(cinco) "E"(seis) → cinco seis 😄

---

**Built with ❤️ for the SQLite community**
