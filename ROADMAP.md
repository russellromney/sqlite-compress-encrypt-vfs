# SQLCEs Roadmap

## Current Status

File format with single magic "SQLCEvfS", inline page index, optional dictionary compression, and encryption support.

**Header (64 bytes):**
```
Offset  Size  Field
0       8     Magic "SQLCEvfS"
8       4     page_size
12      8     data_start (offset where page records begin)
20      4     dict_size (0 = no dictionary)
24      4     flags (bit 0: encrypted)
28      36    (reserved)

Dictionary Section (optional, if dict_size > 0):
Offset  Size       Field
64      dict_size  zstd dictionary bytes

Data Section:
Offset      Size     Field
data_start  varies   Page records (page_num:8 + size:4 + data)
```

---

## Phase 1: Dictionary Compression Integration ✅

### Completed
- [x] Pre-compiled EncoderDictionary/DecoderDictionary in CompressedHandle
- [x] compress/decompress methods use dictionaries when available
- [x] VFS constructors: `new_with_dict()`, `compressed_encrypted_with_dict()`
- [x] CLI commands: `embed-dict`, `extract-dict`
- [x] Integration test for dictionary compression

### Still TODO
- [ ] Add `--dict <path>` flag to sqlces-bench for benchmarking

---

## Phase 2: Parallel Compression in Compaction ✅

### Completed
- [x] Added `rayon` as optional dependency with `parallel` feature flag
- [x] Implemented `compact_with_recompression()` function:
  - Reads all pages sequentially (I/O)
  - Decompresses and recompresses in parallel with rayon (CPU-bound)
  - Writes sequentially
- [x] Added `CompactionConfig` struct for options:
  - `compression_level`: 1-22 for zstd
  - `dictionary`: Optional compression dictionary
  - `parallel`: Enable/disable parallel compression
- [x] Integration tests for parallel compaction
- [x] Benchmarks comparing parallel vs serial compaction

### Usage
```rust
use sqlite_compress_encrypt_vfs::{compact_with_recompression, CompactionConfig};

// Parallel compaction with default settings (enabled when parallel feature is on)
let config = CompactionConfig::new(3);
let freed = compact_with_recompression("database.db", config)?;

// Force serial mode
let config = CompactionConfig::new(3).with_parallel(false);

// With custom dictionary
let config = CompactionConfig::new(3).with_dictionary(dict_bytes);
```

### Notes
- Original `compact()` preserved for simple dead-space removal (no recompression)
- `compact_with_recompression()` enables changing compression level or dictionary
- Expected 4-8x speedup on multi-core systems for large databases

---

## Future Phases

### Phase 3: Page-level Checksums (Optional)
- CRC32 per page for corruption detection (~0.1% storage overhead, ~0.8µs per page)
- Optional verification on read
- Note: SQLite itself doesn't have checksums by default (cksumvfs is an extension)
- Note: AES-GCM already provides authentication for encrypted databases
- Decision: Implement only if users request it

### Phase 4: Client-Controlled Compaction Helpers ✅
- [x] `compact_if_needed(path, threshold_pct)` - compact if dead_space exceeds threshold
  - Returns `Ok(Some(bytes_freed))` if compaction ran
  - Returns `Ok(None)` if below threshold
- Document recommended patterns for embedded use:
  - On startup (check + compact if needed)
  - On graceful shutdown
  - Periodic (client's choice)
- Library provides tools, client decides policy

```rust
use sqlite_compress_encrypt_vfs::compact_if_needed;

// Compact if dead space exceeds 20%
if let Some(freed) = compact_if_needed("database.db", 20.0)? {
    println!("Freed {} bytes", freed);
}
```

---

## Completed

- [x] Single magic "SQLCEvfS" format (no versioned magic bytes)
- [x] Inline page index (scan on open)
- [x] Header with dict_size and flags fields
- [x] zstd compression (levels 1-22)
- [x] AES-256-GCM encryption
- [x] WAL mode support
- [x] Byte-range locking (SQLite protocol)
- [x] Atomic write_end for concurrent access
- [x] Debug lock tracing (SQLCES_DEBUG_LOCKS=1)
- [x] Dictionary compression integration (Phase 1)
- [x] Parallel compression in compaction (Phase 2)
- [x] compact_if_needed helper (Phase 4)
