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

## Phase 2: Parallel Compression in Compaction

### Goal
Speed up `compact()` by compressing pages in parallel using rayon. Currently compaction is CPU-bound (zstd ~300 MB/s vs NVMe ~3000 MB/s).

### Design
```rust
// Current (serial):
for (page_num, (offset, size)) in pages {
    let data = read_page(offset, size);
    let compressed = compress(data);  // CPU bottleneck
    write_record(page_num, compressed);
}

// Parallel (with rayon):
let work: Vec<_> = pages.iter()
    .map(|(page_num, (offset, size))| {
        let data = read_page(offset, size);  // I/O - can't parallelize
        (*page_num, data)
    })
    .collect();

let compressed: Vec<_> = work.par_iter()  // rayon parallel
    .map(|(page_num, data)| (*page_num, compress(data)))
    .collect();

for (page_num, data) in compressed {
    write_record(page_num, data);  // Sequential writes (fine)
}
```

### Tasks
1. Add `rayon` as optional dependency (`parallel` feature)
2. Modify `compact()` to use `par_iter()` for compression
3. Benchmark: measure speedup on multi-core systems
4. Consider chunk size tuning for memory efficiency

### Expected Speedup
- 4-8x on typical 4-8 core machines
- Memory: ~page_count * avg_page_size during compaction

### Locking Analysis
- No locking issues: compression is pure computation
- Reads are sequential (disk seek optimization)
- Writes are sequential (already the case)
- Only change is CPU parallelism during compression phase

---

## Future Phases

### Phase 3: Page-level Checksums (Optional)
- CRC32 per page for corruption detection (~0.1% storage overhead, ~0.8µs per page)
- Optional verification on read
- Note: SQLite itself doesn't have checksums by default (cksumvfs is an extension)
- Note: AES-GCM already provides authentication for encrypted databases
- Decision: Implement only if users request it

### Phase 4: Client-Controlled Compaction Helpers
- `compact_if_needed(path, threshold_pct)` - compact if dead_space exceeds threshold
- Document recommended patterns for embedded use:
  - On startup (check + compact if needed)
  - On graceful shutdown
  - Periodic (client's choice)
- Library provides tools, client decides policy

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
