# Benchmarking Guide

This directory contains benchmark infrastructure for testing sqlite-compress-encrypt-vfs performance across all four VFS modes.

## Quick Start

```bash
# Run the quick benchmark example (iteration-based)
cargo run --example quick_bench --features encryption --release

# Run concurrent load testing (time-based, recommended for production testing)
cargo run --bin sqlces-bench --features encryption -- bench-db \
  --database scripts/real_50mb.db \
  --reader-threads 4 \
  --writer-threads 2 \
  --duration-secs 10

# Run formal criterion benchmarks (future)
cargo bench --features encryption
```

## VFS Modes Tested

| Mode | Constructor | What's Tested |
|------|-------------|---------------|
| **Compressed** | `CompressedVfs::new(dir, 3)` | zstd compression overhead |
| **Passthrough** | `CompressedVfs::passthrough(dir)` | Baseline (no overhead) |
| **Encrypted** | `CompressedVfs::encrypted(dir, pwd)` | AES-256-GCM overhead |
| **Compressed+Encrypted** | `CompressedVfs::compressed_encrypted(dir, 3, pwd)` | Combined overhead |

## Benchmark Tools

### 1. sqlces-bench (Concurrent Load Testing)

Time-based concurrent benchmarking tool for production-like workloads.

**Features:**
- Multiple concurrent reader and writer threads running simultaneously
- Time-based testing (run for N seconds with concurrent load)
- Random access patterns for realistic cache pressure
- Configurable cache and mmap sizes
- Tests existing databases with all VFS modes

**Usage:**
```bash
cargo run --bin sqlces-bench --features encryption -- bench-db \
  --database <path> \
  --duration-secs 10 \
  --reader-threads 4 \
  --writer-threads 2 \
  --cache-percent 0.10 \
  --mmap-multiplier 4.0 \
  --output-format markdown \
  --output-file results.md
```

**Options:**
- `--duration-secs`: How long to run benchmark with concurrent readers+writers (default: 10)
- `--reader-threads`: Number of concurrent read threads (default: 4)
- `--writer-threads`: Number of concurrent write threads (default: 1)
- `--cache-percent`: Page cache as % of logical DB size (default: 0.10 = 10%)
- `--mmap-multiplier`: Memory-mapped I/O as multiplier of cache (default: 4.0)
- `--output-format`: `console`, `json`, or `markdown` (default: console)

**Example Output:**
```
=== SQLCEs Database Benchmark ===
Database: scripts/real_50mb.db
Duration: 10s with 4x readers + 2x writers running concurrently
Cache: 10% of logical size, mmap: 4.0x cache

Testing compressed mode...
  ✓ Complete - 32000 read ops/sec, 8500 write ops/sec, 20.59 MB
```

### 2. quick_bench (Iteration-based)

Example benchmark using realistic corpus data for detailed analysis.

**Features:**
- Fixed iteration counts
- Corpus data support (sessions, logs, API responses)
- Autocommit vs WAL mode comparison
- Per-operation timing

**Usage:**
```bash
cargo run --example quick_bench --features encryption --release
```

## Benchmark Workloads

### 1. Autocommit Writes (Worst Case)
Individual INSERT statements without transactions.
- **Why test**: Worst-case scenario, shows maximum overhead
- **Real-world**: Single writes (config updates, session tracking)
- **Journal mode**: SQLite default (rollback journal)

### 2. WAL Mode Writes (Recommended)
Individual autocommit transactions with Write-Ahead Logging.
- **Why test**: Realistic production workload with individual transactions
- **Real-world**: REST API writes, session updates, real-time data
- **Journal mode**: WAL with `PRAGMA synchronous=NORMAL` and auto-checkpoint

### 3. Concurrent Read/Write (Production)
Multiple reader and writer threads accessing database simultaneously.
- **Why test**: Realistic concurrent load, cache pressure, lock contention
- **Real-world**: Multi-user applications, web backends
- **Pattern**: Random access to simulate cache misses

### 4. Read Performance
Sequential and random reads after data is written.
- **Why test**: Read-heavy workloads (analytics, reporting)
- **Real-world**: Query-intensive applications

## Expected Results

### Compression Ratios
Based on data type:

| Data Type | Typical Ratio | Best Case | Worst Case |
|-----------|---------------|-----------|------------|
| JSON/XML | 5-10x | 15x | 3x |
| Repeated text (logs) | 10-20x | 50x | 5x |
| Structured data | 3-7x | 10x | 2x |
| Binary/random | 1.0x | 1.2x | 1.0x |

### Performance Overhead

**Write Operations (WAL mode, single thread):**
- Passthrough: ~10-15K ops/sec (baseline)
- Compressed: ~7-10K ops/sec (2-3x slower due to compression)
- Encrypted: ~8-12K ops/sec (1.2-1.5x slower)
- Compressed+Encrypted: ~5-8K ops/sec (2.5-3.5x slower)

**Read Operations (single thread):**
- Passthrough: ~150-200K ops/sec (baseline)
- Compressed: ~30-50K ops/sec (3-5x slower due to decompression)
- Encrypted: ~120-150K ops/sec (1.2-1.5x slower)
- Compressed+Encrypted: ~25-40K ops/sec (4-6x slower)

**Concurrent Operations (4 readers, 2 writers):**
- Read throughput: Scales ~3-4x with 4 threads
- Write throughput: Limited by WAL write serialization (~1.5-2x with 2 threads)
- Cache impact: 10% cache vs 50% cache shows ~2-3x improvement with random access
- Latency: P99 increases ~2-3x under concurrent load

**Autocommit Writes (worst case):**
- All modes: 3-5x slower than WAL mode
- **Always use WAL mode in production!**

### CPU and Hardware Factors

**AES-NI (Hardware AES):**
- Modern CPUs: ~1.2x overhead
- Without AES-NI: ~3-5x overhead
- Check with: `grep aes /proc/cpuinfo` (Linux) or `sysctl hw.optional.aes` (macOS)

**Compression Levels:**
| Level | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| 1-3 | Fast | Good | Hot data, latency-sensitive |
| 10-15 | Moderate | Better | Balanced (default: 3) |
| 20-22 | Slow | Best | Cold storage, archival |

## Using Corpus Data

The benchmark can use real-world data from corpus files for more accurate results.

### Supported Corpus Types

```
./corpus/
├── sessions/          # Session data (JSON)
│   ├── session_1.json
│   └── session_2.json
├── queries/           # SQL queries (text)
│   ├── query_1.sql
│   └── query_2.sql
└── events/            # Event logs (JSON)
    ├── event_1.json
    └── event_2.json
```

### Example Corpus Setup

```bash
# Create corpus directory
mkdir -p corpus/sessions corpus/queries corpus/events

# Generate sample session data
for i in {1..100}; do
  cat > corpus/sessions/session_$i.json <<EOF
{
  "user_id": "$i",
  "session_id": "$(uuidgen)",
  "timestamp": "2024-01-01T00:00:00Z",
  "events": [
    {"type": "login", "ip": "192.168.1.$i"},
    {"type": "page_view", "url": "/dashboard"},
    {"type": "click", "element": "button#export"}
  ]
}
EOF
done

# Run benchmark with corpus
cargo run --example quick_bench --features encryption --release
```

### Using Public Datasets

**Twitter Cache Traces** (CC-BY licensed):
```bash
# Download Twitter workload data
wget https://github.com/twitter/cache-trace/raw/main/data/cluster52.csv

# Convert to JSON for benchmark
python3 scripts/convert_trace.py cluster52.csv corpus/sessions/
```

## Interpreting Results

### Concurrent Benchmark Report

```markdown
## Performance Comparison

| Mode | Read (ops/s) | Write (ops/s) | Read P99 (µs) | File Size (MB) |
|------|-------------:|--------------:|--------------:|---------------:|
| passthrough | 180000 | 15000 | 50.0 | 64.60 |
| compressed | 32000 | 8000 | 180.0 | 20.59 |
| encrypted | 150000 | 12000 | 65.0 | 65.00 |
| both | 28000 | 7000 | 200.0 | 21.00 |
```

**What to look for:**
- **Throughput scaling**: 4 reader threads should give ~3-4x read throughput
- **Write serialization**: 2 writer threads typically give ~1.5-2x write throughput (WAL serializes writes)
- **Latency under load**: P99 latency shows tail behavior under concurrent load
- **Cache effectiveness**: Compare results with different `--cache-percent` values

### File Size Report
```
Mode             | File Size | Compression Ratio
-----------------+-----------+------------------
Passthrough      | 10.5 MB   | 1.0x (baseline)
Compressed       | 2.1 MB    | 5.0x
Encrypted        | 10.8 MB   | 0.97x (overhead)
Comp+Encrypted   | 2.3 MB    | 4.6x
```

**What to look for:**
- Compressed vs Passthrough: How compressible is your data?
- Encrypted vs Passthrough: Encryption adds ~3% file overhead (GCM tags)
- Comp+Encrypted vs Compressed: Should be similar (compress then encrypt)

### Throughput Report
```
Mode             | Writes/sec | Reads/sec
-----------------+------------+-----------
Passthrough      | 1000       | 5000
Compressed       | 350        | 2500
Encrypted        | 800        | 4200
Comp+Encrypted   | 320        | 2200
```

**What to look for:**
- WAL mode vs autocommit: WAL should be 3-10x faster for writes
- Read vs Write: Reads should be 2-5x faster than writes
- Encrypted overhead: Should be <30% on modern CPUs with AES-NI

## Performance Optimization

### 1. Always Use WAL Mode

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;  -- Safe for WAL mode
```

**Why:** 3-10x faster writes, better concurrency.

### 2. Batch Writes in Transactions

```rust
conn.execute("BEGIN", [])?;
for item in items {
    conn.execute("INSERT INTO ...", [item])?;
}
conn.execute("COMMIT", [])?;
```

**Why:** Amortizes compression/encryption overhead across many rows.

### 3. Choose Right Compression Level

```rust
// Hot data: level 1-3
let vfs = CompressedVfs::new("./data", 1);

// Balanced: level 10-15
let vfs = CompressedVfs::new("./data", 10);

// Cold storage: level 20-22
let vfs = CompressedVfs::new("./data", 20);
```

**Why:** Higher levels = better compression but slower writes.

### 4. Use Appropriate Page Size

```sql
-- For compressible data, larger pages = better compression
PRAGMA page_size=8192;  -- Default: 4096
VACUUM;  -- Apply page size change
```

**Why:** Larger pages give compression more context.

### 5. Profile Your Workload

```bash
# Test different cache sizes
for cache in 0.01 0.05 0.10 0.25 0.50; do
  cargo run --bin sqlces-bench --features encryption -- bench-db \
    --database scripts/real_50mb.db \
    --cache-percent $cache \
    --output-file "results/cache_${cache}.md"
done

# Test different thread counts
for readers in 1 2 4 8; do
  cargo run --bin sqlces-bench --features encryption -- bench-db \
    --database scripts/real_50mb.db \
    --reader-threads $readers \
    --writer-threads 1 \
    --output-file "results/readers_${readers}.md"
done

# Measure with different modes using quick_bench
for mode in passthrough compressed encrypted both; do
  cargo run --example quick_bench --features encryption --release | grep "Mode: $mode" -A 10
done
```

### 6. Production-Like Load Testing

```bash
# Simulate heavy read workload (8 readers, 1 writer)
cargo run --bin sqlces-bench --features encryption -- bench-db \
  --database scripts/real_100mb.db \
  --reader-threads 8 \
  --writer-threads 1 \
  --duration-secs 30

# Simulate balanced workload (4 readers, 4 writers)
cargo run --bin sqlces-bench --features encryption -- bench-db \
  --database scripts/real_100mb.db \
  --reader-threads 4 \
  --writer-threads 4 \
  --duration-secs 30

# Simulate write-heavy workload (2 readers, 8 writers)
cargo run --bin sqlces-bench --features encryption -- bench-db \
  --database scripts/real_100mb.db \
  --reader-threads 2 \
  --writer-threads 8 \
  --duration-secs 30
```

## Adding Custom Benchmarks

### Using Criterion (Future)

```rust
// benches/vfs_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sqlite_compress_encrypt_vfs::*;

fn bench_compressed_writes(c: &mut Criterion) {
    c.bench_function("compressed_writes", |b| {
        b.iter(|| {
            // Your benchmark code
        });
    });
}

criterion_group!(benches, bench_compressed_writes);
criterion_main!(benches);
```

Run with:
```bash
cargo bench --features encryption
```

### Custom Quick Bench

Copy `examples/quick_bench.rs` and modify:

```rust
// examples/my_workload.rs
fn main() {
    // Customize:
    // - Data patterns (JSON, binary, mixed)
    // - Write patterns (single, batch, transaction size)
    // - Read patterns (sequential, random, range scans)
    // - Compression levels to test
}
```

## Troubleshooting

### Slow Performance

**Problem:** Encrypted mode is 3-5x slower than expected.
**Solution:** Check for AES-NI support:
```bash
# Linux
grep aes /proc/cpuinfo

# macOS
sysctl hw.optional.aes

# Should output "1" or show "aes" flag
```

**Problem:** Compressed mode is barely faster than uncompressed.
**Solution:** Your data might not be compressible (already compressed, encrypted, or random). Try with text/JSON data.

### Poor Compression Ratios

**Problem:** Compression ratio < 1.5x for JSON data.
**Solution:**
1. Check compression level (should be ≥3 for JSON)
2. Ensure data has patterns (repeated keys, similar structure)
3. Consider dictionary compression (future feature)

### High Memory Usage

**Problem:** Benchmark crashes with OOM.
**Solution:**
1. Reduce batch size in quick_bench.rs
2. Reduce corpus size
3. Run with smaller page counts

## Benchmark Results Archive

Save your results for comparison:

```bash
# Save results
cargo run --example quick_bench --features encryption --release > results/$(date +%Y%m%d)_benchmark.txt

# Compare over time
diff results/20240101_benchmark.txt results/20240201_benchmark.txt
```

## Contributing Benchmark Data

If you have representative workload data you can share:

1. Anonymize/sanitize the data
2. Convert to corpus format (JSON/text)
3. Submit PR with corpus data and expected results
4. Help us improve real-world performance!

---

**Built with ❤️ for the SQLite community**
