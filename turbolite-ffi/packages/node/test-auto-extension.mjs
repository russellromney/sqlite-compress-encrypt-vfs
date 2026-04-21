/**
 * Node e2e for the loadable-ext + sqlite3_auto_extension path.
 *
 * Mirrors tests/test_auto_extension_python.py. Uses the turbolite Node
 * binding's `connect()` — same entry point users actually call — and
 * verifies:
 *   1. `turbolite_config_set` is auto-registered without any explicit
 *      install call.
 *   2. Two turbolite connections on the main event loop route each
 *      push to their own queue via pApp (the pre-h2 regression case).
 *   3. A plain better-sqlite3 connection opened in the same process
 *      coexists — doesn't have turbolite_config_set bound.
 */
import { connect } from './dist/index.js';
import Database from 'better-sqlite3';
import { strict as assert } from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'turbolite-autoext-node-'));
let testIdx = 0;
function testDir() {
  const dir = path.join(tmpRoot, `t${testIdx++}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

let passed = 0, failed = 0;
function run(name, fn) {
  try {
    fn();
    passed++;
    console.log(`  PASS  ${name}`);
  } catch (e) {
    failed++;
    console.log(`  FAIL  ${name}: ${e.message}`);
  }
}

run('single connection auto-installs turbolite_config_set', () => {
  const d = testDir();
  const db = connect(path.join(d, 'a.db'));
  try {
    const row = db.prepare(
      "SELECT turbolite_config_set('prefetch_search', '0.5,0.5') AS rc"
    ).get();
    assert.equal(row.rc, 0);
  } finally {
    db.close();
  }
});

run('two connections same thread route per-connection via pApp', () => {
  const a = connect(path.join(testDir(), 'a.db'));
  const b = connect(path.join(testDir(), 'b.db'));
  try {
    const rcA = a.prepare(
      "SELECT turbolite_config_set('prefetch_search', '0.11,0.22') AS rc"
    ).get().rc;
    const rcB = b.prepare(
      "SELECT turbolite_config_set('prefetch_search', '0.99,0.88') AS rc"
    ).get().rc;
    assert.equal(rcA, 0);
    assert.equal(rcB, 0);

    a.exec('CREATE TABLE IF NOT EXISTS t (x)');
    b.exec('CREATE TABLE IF NOT EXISTS t (x)');
    a.exec('INSERT INTO t VALUES (1), (2)');
    b.exec('INSERT INTO t VALUES (10)');
    assert.equal(a.prepare('SELECT count(*) AS n FROM t').get().n, 2);
    assert.equal(b.prepare('SELECT count(*) AS n FROM t').get().n, 1);
  } finally {
    a.close();
    b.close();
  }
});

run('unrelated memory connection coexists (auto-extension no-op)', () => {
  // Ensure turbolite is loaded first — open a turbolite connection so
  // the extension's init has fired.
  const d = testDir();
  const tlite = connect(path.join(d, 'warm.db'));

  try {
    // Plain in-memory better-sqlite3 in the same process. Our
    // auto-extension fires during sqlite3_open; the VFS guard should
    // see this isn't a turbolite VFS and bail. Open must succeed,
    // turbolite_config_set must NOT be bound on the plain conn.
    const plain = new Database(':memory:');
    try {
      plain.exec('CREATE TABLE t (x INTEGER)');
      plain.exec('INSERT INTO t VALUES (42)');
      assert.equal(plain.prepare('SELECT x FROM t').get().x, 42);

      let errMsg = null;
      try {
        plain.prepare(
          "SELECT turbolite_config_set('prefetch_search', '0.5,0.5')"
        ).get();
      } catch (e) {
        errMsg = e.message;
      }
      assert.ok(errMsg, 'expected turbolite_config_set to fail on plain connection');
      assert.match(errMsg, /no such function|turbolite_config_set/, `got: ${errMsg}`);
    } finally {
      plain.close();
    }
  } finally {
    tlite.close();
  }
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
