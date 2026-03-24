# turbolite build targets
#
# Shared library (.so / .dylib) for C FFI consumers.
# The library links against system SQLite (not bundled) so that it shares
# the same sqlite3 symbol table as the host application.

UNAME := $(shell uname -s)
ifeq ($(UNAME),Darwin)
  LIB_EXT := dylib
  LIB_PREFIX := lib
else
  LIB_EXT := so
  LIB_PREFIX := lib
endif

LIB_NAME := sqlite_compress_encrypt_vfs
LIB_FILE := $(LIB_PREFIX)$(LIB_NAME).$(LIB_EXT)
TARGET_DIR := target/release

# Features to include in the shared library build.
# Override with: make lib FEATURES="zstd,encryption,tiered"
FEATURES ?= zstd

# ── Shared library ─────────────────────────────────────────────────

.PHONY: lib
lib: ## Build shared library (.so / .dylib) linking system SQLite
	cargo build --release --lib --no-default-features --features $(FEATURES)
	@echo ""
	@echo "Built: $(TARGET_DIR)/$(LIB_FILE)"
	@ls -lh $(TARGET_DIR)/$(LIB_FILE)

.PHONY: lib-bundled
lib-bundled: ## Build shared library with bundled SQLite (self-contained)
	cargo build --release --lib --features $(FEATURES),bundled-sqlite
	@echo ""
	@echo "Built (bundled SQLite): $(TARGET_DIR)/$(LIB_FILE)"
	@ls -lh $(TARGET_DIR)/$(LIB_FILE)

# ── C header ───────────────────────────────────────────────────────

.PHONY: header
header: ## Generate turbolite.h C header via cbindgen
	cbindgen --config cbindgen.toml --crate sqlite-compress-encrypt-vfs --output turbolite.h
	@echo "Generated: turbolite.h"

# ── Install ────────────────────────────────────────────────────────

PREFIX ?= /usr/local

.PHONY: install
install: lib header ## Install shared library + header to PREFIX (default /usr/local)
	install -d $(PREFIX)/lib $(PREFIX)/include
	install -m 755 $(TARGET_DIR)/$(LIB_FILE) $(PREFIX)/lib/
	install -m 644 turbolite.h $(PREFIX)/include/
	@echo ""
	@echo "Installed to $(PREFIX)/lib/$(LIB_FILE) and $(PREFIX)/include/turbolite.h"

# ── CLI + binaries (bundled SQLite) ────────────────────────────────

.PHONY: build
build: ## Build all binaries (CLI, benchmarks) with bundled SQLite
	cargo build --release

# ── Tests ──────────────────────────────────────────────────────────

.PHONY: test
test: ## Run all tests
	cargo test --features zstd,bundled-sqlite

.PHONY: test-all
test-all: ## Run all tests including tiered/S3
	cargo test --features zstd,tiered,bundled-sqlite

# ── Cleanup ────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Clean build artifacts
	cargo clean
	rm -f turbolite.h

# ── Help ───────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
