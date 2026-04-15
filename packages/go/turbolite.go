// Package turbolite provides a Go interface to turbolite-backed SQLite databases.
//
// Usage:
//
//	db, err := turbolite.Open("my.db", nil)                       // local mode
//	db, err := turbolite.Open("my.db", &turbolite.Options{        // S3 mode
//	    Mode:     "s3",
//	    Bucket:   "my-bucket",
//	    Endpoint: "https://t3.storage.dev",
//	})
//	defer db.Close()
//
//	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
//	rows := db.Query("SELECT * FROM users")
package turbolite

/*
#cgo LDFLAGS: -lturbolite
#include <stdlib.h>

extern const char* turbolite_version();
extern const char* turbolite_last_error();
extern int turbolite_register_local(const char* name, const char* base_dir, int level);
extern int turbolite_register_s3(const char* name, const char* bucket, const char* prefix,
    const char* endpoint, const char* region, const char* cache_dir);
extern void* turbolite_open(const char* path, const char* vfs_name);
extern int turbolite_exec(void* db, const char* sql);
extern char* turbolite_query_json(void* db, const char* sql);
extern void turbolite_free_string(char* s);
extern void turbolite_close(void* db);
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"unsafe"
)

var vfsCounter uint64

// Options for opening a turbolite database.
type Options struct {
	// Storage mode: "local" (default) or "s3".
	Mode string
	// S3 bucket name (required for mode "s3").
	Bucket string
	// S3 endpoint URL (for Tigris, MinIO).
	Endpoint string
	// S3 key prefix (default "turbolite").
	Prefix string
	// AWS region.
	Region string
	// Local cache directory.
	CacheDir string
	// In-memory page cache size (default "64MB"). Set to "0" to disable.
	PageCache string
	// Zstd compression level 1-22 (local mode, default 3).
	Compression int
}

// DB is a turbolite-backed SQLite database connection.
type DB struct {
	ptr unsafe.Pointer
}

// Open a turbolite database. Pass nil for default options (local mode).
func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	if opts.Mode == "" {
		opts.Mode = "local"
	}
	if opts.PageCache == "" {
		opts.PageCache = "64MB"
	}

	// Set mem cache budget env var (read by turbolite C init)
	os.Setenv("TURBOLITE_MEM_CACHE_BUDGET", opts.PageCache)

	id := atomic.AddUint64(&vfsCounter, 1)
	vfsName := fmt.Sprintf("turbolite-go-%d", id)
	cVfsName := C.CString(vfsName)
	defer C.free(unsafe.Pointer(cVfsName))

	switch opts.Mode {
	case "s3":
		if opts.Bucket == "" {
			return nil, fmt.Errorf("turbolite: bucket is required for s3 mode")
		}
		cBucket := C.CString(opts.Bucket)
		defer C.free(unsafe.Pointer(cBucket))
		prefix := opts.Prefix
		if prefix == "" {
			prefix = "turbolite"
		}
		cPrefix := C.CString(prefix)
		defer C.free(unsafe.Pointer(cPrefix))
		cEndpoint := C.CString(opts.Endpoint)
		defer C.free(unsafe.Pointer(cEndpoint))
		cRegion := C.CString(opts.Region)
		defer C.free(unsafe.Pointer(cRegion))
		cacheDir := opts.CacheDir
		if cacheDir == "" {
			cacheDir = "/tmp/turbolite"
		}
		cCacheDir := C.CString(cacheDir)
		defer C.free(unsafe.Pointer(cCacheDir))

		rc := C.turbolite_register_s3(cVfsName, cBucket, cPrefix, cEndpoint, cRegion, cCacheDir)
		if rc != 0 {
			return nil, fmt.Errorf("turbolite: register s3 VFS failed: %s", C.GoString(C.turbolite_last_error()))
		}
	case "local":
		cacheDir := opts.CacheDir
		if cacheDir == "" {
			cacheDir = "/tmp/turbolite"
		}
		cDir := C.CString(cacheDir)
		defer C.free(unsafe.Pointer(cDir))
		level := opts.Compression
		if level == 0 {
			level = 3
		}
		rc := C.turbolite_register_local(cVfsName, cDir, C.int(level))
		if rc != 0 {
			return nil, fmt.Errorf("turbolite: register local VFS failed: %s", C.GoString(C.turbolite_last_error()))
		}
	default:
		return nil, fmt.Errorf("turbolite: mode must be 'local' or 's3', got %q", opts.Mode)
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// turbolite_open sets PRAGMA cache_size=0 internally (turbolite owns the page cache)
	ptr := C.turbolite_open(cPath, cVfsName)
	if ptr == nil {
		return nil, fmt.Errorf("turbolite: open failed: %s", C.GoString(C.turbolite_last_error()))
	}

	return &DB{ptr: ptr}, nil
}

// Exec executes SQL that returns no rows (DDL, INSERT, UPDATE, DELETE).
func (db *DB) Exec(sql string) error {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	if C.turbolite_exec(db.ptr, csql) != 0 {
		return fmt.Errorf("turbolite: %s", C.GoString(C.turbolite_last_error()))
	}
	return nil
}

// Query executes SQL and returns results as a slice of maps.
func (db *DB) Query(sql string) ([]map[string]interface{}, error) {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	ptr := C.turbolite_query_json(db.ptr, csql)
	if ptr == nil {
		return nil, fmt.Errorf("turbolite: %s", C.GoString(C.turbolite_last_error()))
	}
	defer C.turbolite_free_string(ptr)
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(C.GoString(ptr)), &rows); err != nil {
		return nil, fmt.Errorf("turbolite: parse result: %w", err)
	}
	return rows, nil
}

// Close the database connection.
func (db *DB) Close() {
	if db.ptr != nil {
		C.turbolite_close(db.ptr)
		db.ptr = nil
	}
}

// Version returns the turbolite library version string.
func Version() string {
	return C.GoString(C.turbolite_version())
}
