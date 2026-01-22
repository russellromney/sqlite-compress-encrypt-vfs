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

## Phase 1: Dictionary Compression Integration

### Goal
Self-contained database files with embedded compression dictionaries for 2-5x better compression ratios and faster compression on small pages.

### Remaining Tasks

1. **Add dictionary fields to CompressedHandle** (pending)
   - `dictionary: Option<Vec<u8>>` - loaded from file or provided at creation
   - `encoder_dict: Option<zstd::dict::EncoderDictionary>` - pre-compiled for speed
   - `decoder_dict: Option<zstd::dict::DecoderDictionary>` - pre-compiled for speed

2. **Update compress/decompress methods** (pending)
   - Use `zstd::stream::Encoder::with_prepared_dictionary()` when dict available
   - Fall back to regular compression when no dict

3. **Add VFS constructors** (pending)
   - `CompressedVfs::new_with_dict(dir, level, dict_bytes)` - provide dict at creation
   - `CompressedVfs::open_with_embedded_dict(dir, level)` - read dict from file

4. **CLI support** (pending)
   - `sqlces embed-dict <db> <dict-file>` - embed existing dict into database
   - `sqlces extract-dict <db> <dict-file>` - extract dict from database

5. **Benchmark integration** (pending)
   - Add `--dict <path>` flag to sqlces-bench
   - Compare with/without dictionary performance

### Performance Expectations

- **Compression ratio**: 2-5x improvement on structured data
- **Compression speed**: Faster (dictionary pre-analysis helps encoder)
- **Decompression speed**: Faster (dictionary helps decoder)
- **Memory**: +100-500KB per connection for dictionary

---

## Future Phases

### Phase 2: Incremental Compaction
- Background thread to remove dead pages
- Write amplification reduction

### Phase 3: Page-level Checksums
- CRC32 per page for corruption detection
- Optional verification on read

### Phase 4: Parallel Compression
- Multi-threaded checkpoint compression
- Configurable thread pool

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
