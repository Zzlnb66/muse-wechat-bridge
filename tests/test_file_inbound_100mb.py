"""Tests for the 2026-11-01 inbound file raise to 100 MiB (fix copy only).

Covers, against the fix-copy bridge.py:
  1. New constants: single plain 100 MiB, cipher = plain + 1024,
     message total 100 MiB, at most 5 files, 300 s shared budget,
     2 attempts sharing the budget, 1 MiB decrypt chunk.
  2. Chunked decrypt correctness through the real receive path:
     multi-chunk plaintext, exact chunk-boundary plaintext, and the
     streaming helper directly (bytes + hash equal whole-buffer
     decrypt).
  3. Streaming mid-stream abort: plaintext over the single cap or
     over the remaining message budget aborts before publish, with
     no final file, no receipt, and no tmp leftovers.
  4. Declared-length pre-reject: over-cap / over-budget declarations
     are refused with zero downloads; unparseable declarations fall
     through to the authoritative post-decrypt check; a declaration
     exactly equal to the cap is not over.
  5. Boundary logic at patched small scale (no real 100 MiB fixture
     is ever allocated, per the task constraint): a file exactly at
     the single cap passes, and a second file pushing the message
     total over is refused.

Run: python3 tests/test_file_inbound_100mb.py (exit 0 = all pass).
"""

import base64
import hashlib
import http.server
import json
import os
import socketserver
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


ROUTES = {}
HITS = {}


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        HITS[self.path] = HITS.get(self.path, 0) + 1
        route = ROUTES.get(self.path)
        if route is None:
            self.send_response(404)
            self.end_headers()
            return
        status, headers, body = route
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)


class FakeServer:
    def __enter__(self):
        self.httpd = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        bridge._TEST_MEDIA_ORIGINS.add(("http", "127.0.0.1", self.port))
        return self

    def __exit__(self, *a):
        bridge._TEST_MEDIA_ORIGINS.discard(("http", "127.0.0.1", self.port))
        self.httpd.shutdown()
        self.httpd.server_close()

    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"


def _key_item(url, key, name="file.bin", plain_len=None):
    item = {"media": {"full_url": url,
                      "aes_key": base64.b64encode(key).decode(),
                      "encrypt_type": 1},
            "file_name": name}
    if plain_len is not None:
        item["len"] = str(plain_len)
    return item


def _patch_dirs(tmp):
    patched = {}
    mapping = {
        "STATE": tmp,
        "INBOX": os.path.join(tmp, "inbox"),
        "DONE": os.path.join(tmp, "done"),
        "OUTBOX_PENDING": os.path.join(tmp, "outbox", "pending"),
        "OUTBOX_SENT": os.path.join(tmp, "outbox", "sent"),
        "OUTBOX_FAILED": os.path.join(tmp, "outbox", "failed"),
        "MEDIA_INBOUND": os.path.join(tmp, "media", "inbound"),
        "MEDIA_OUTBOUND": os.path.join(tmp, "media", "outbound"),
        "FAILURES_DIR": os.path.join(tmp, "failures"),
        "ANALYSIS_DIR": os.path.join(tmp, "analysis"),
        "LOCAL_VAULT_DIR": os.path.join(tmp, "local_vault"),
        "SESSION_FILE": os.path.join(tmp, "session.json"),
        "CURSOR_FILE": os.path.join(tmp, "cursor.txt"),
        "PROCESSED_FILE": os.path.join(tmp, "processed.json"),
        "PENDING_LOCAL_QUERIES_FILE": os.path.join(tmp, "pending.json"),
        "TTL_INDEX_FILE": os.path.join(tmp, "ttl.json"),
        "TOMBSTONE_FILE": os.path.join(tmp, "tombs.json"),
        "CHANNEL_STATUS_FILE": os.path.join(tmp, "channel.json"),
        "SAFE_VIEW_FILE": os.path.join(tmp, "safe.json"),
        "LOCAL_ONLY_MODE_FILE": os.path.join(tmp, "mode.json"),
        "NEEDS_RELOGIN_FILE": os.path.join(tmp, "NEEDS_RELOGIN"),
        "FILE_INBOUND_DIR": os.path.join(tmp, "media", "inbound", "files"),
        "FILE_RECEIPTS_FILE": os.path.join(tmp, "receipts.json"),
    }
    for name, path in mapping.items():
        patched[name] = getattr(bridge, name)
        setattr(bridge, name, path)
    return lambda: [setattr(bridge, k, v) for k, v in patched.items()]


def test_constants():
    check("caps: single plain 100 MiB",
          bridge.FILE_SINGLE_PLAIN_CAP == 100 * 1024 * 1024)
    check("caps: cipher = plain + 1024",
          bridge.FILE_SINGLE_CIPHER_CAP == bridge.FILE_SINGLE_PLAIN_CAP + 1024)
    check("caps: message total 100 MiB",
          bridge.FILE_MESSAGE_TOTAL_CAP == 100 * 1024 * 1024)
    check("caps: max count 5, attempts 2, budget 300 s",
          bridge.FILE_MESSAGE_MAX_COUNT == 5
          and bridge.FILE_DOWNLOAD_ATTEMPTS == 2
          and bridge.FILE_MESSAGE_BUDGET_SECONDS == 300)
    check("chunk: 1 MiB and a multiple of 16",
          bridge._FILE_DECRYPT_CHUNK == 1024 * 1024
          and bridge._FILE_DECRYPT_CHUNK % 16 == 0)


def test_chunked_correctness(server):
    tmp = tempfile.mkdtemp(prefix="fi100-chunk-")
    restore = _patch_dirs(tmp)
    key = os.urandom(16)
    try:
        # Multi-chunk: 3 MiB + 123 bytes spans 4 decrypt chunks.
        plain = os.urandom(3 * 1024 * 1024 + 123)
        cipher = bridge.aes_ecb_encrypt(plain, key)
        ROUTES["/big3.bin"] = (200, {"Content-Length": str(len(cipher))}, cipher)
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/big3.bin"), key, "big3.bin", len(plain)),
            "930001", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("chunked: 3 MiB + 123 B round-trips byte-identical",
              saved is not None and open(saved["path"], "rb").read() == plain
              and saved["sha256"] == hashlib.sha256(plain).hexdigest()
              and saved["size"] == len(plain))
        # Exact chunk boundary: 1 MiB plain -> 1 MiB cipher chunk
        # plus a 16-byte padding chunk.
        plain1 = os.urandom(1024 * 1024)
        cipher1 = bridge.aes_ecb_encrypt(plain1, key)
        ROUTES["/exact1.bin"] = (200, {"Content-Length": str(len(cipher1))}, cipher1)
        saved, _, _ = bridge.process_inbound_file(
            _key_item(server.url("/exact1.bin"), key, "exact1.bin", len(plain1)),
            "930002", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("chunked: exact 1 MiB boundary round-trips",
              saved is not None and open(saved["path"], "rb").read() == plain1)
        # Helper directly: bytes + hash equal whole-buffer decrypt.
        cpath = os.path.join(tmp, "c.bin")
        ppath = os.path.join(tmp, "p.tmp")
        with open(cpath, "wb") as f:
            f.write(cipher)
        size, sha, errc = bridge._stream_decrypt_file(
            cpath, ppath, key, bridge.FILE_SINGLE_PLAIN_CAP,
            bridge.FILE_MESSAGE_TOTAL_CAP)
        check("helper: streaming == whole-buffer decrypt",
              errc is None and size == len(plain)
              and sha == hashlib.sha256(plain).hexdigest()
              and open(ppath, "rb").read() == plain)
        # Helper failure removes its tmp (bad padding: nopad cipher
        # whose last byte is 0x00, so no valid PKCS#7 pad value).
        import subprocess
        r = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-K", key.hex(),
                            "-nosalt", "-nopad"], input=b"A" * 15 + b"\x00",
                           capture_output=True, timeout=30)
        with open(cpath, "wb") as f:
            f.write(r.stdout)
        size, sha, errc = bridge._stream_decrypt_file(
            cpath, ppath, key, bridge.FILE_SINGLE_PLAIN_CAP,
            bridge.FILE_MESSAGE_TOTAL_CAP)
        check("helper: bad padding -> DECRYPT_FAILED, tmp removed",
              errc == "DECRYPT_FAILED" and not os.path.exists(ppath))
    finally:
        restore()


def _dir_files(tmp, mid):
    d = os.path.join(tmp, "media", "inbound", "files", mid)
    if not os.path.isdir(d):
        return []
    return sorted(os.listdir(d))


def test_streaming_abort(server):
    tmp = tempfile.mkdtemp(prefix="fi100-abort-")
    restore = _patch_dirs(tmp)
    key = os.urandom(16)
    try:
        plain = os.urandom(3 * 1024 * 1024)
        cipher = bridge.aes_ecb_encrypt(plain, key)
        ROUTES["/abort.bin"] = (200, {"Content-Length": str(len(cipher))}, cipher)
        # Patched small single cap: abort must happen mid-stream.
        old_cap = bridge.FILE_SINGLE_PLAIN_CAP
        bridge.FILE_SINGLE_PLAIN_CAP = 2 * 1024 * 1024
        try:
            saved, err, diag = bridge.process_inbound_file(
                _key_item(server.url("/abort.bin"), key, "abort.bin"),
                "930011", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
            check("abort: over single cap mid-stream -> OVER_SINGLE_CAP",
                  saved is None and (diag or {}).get("error_code") == "OVER_SINGLE_CAP")
            check("abort: no final file, no tmp leftovers",
                  _dir_files(tmp, "930011") == [])
        finally:
            bridge.FILE_SINGLE_PLAIN_CAP = old_cap
        # Over remaining message budget (caps unpatched).
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/abort.bin"), key, "abort.bin"),
            "930012", 0, time.time() + 300, 1024 * 1024)
        check("abort: over message budget -> OVER_MESSAGE_TOTAL",
              saved is None and (diag or {}).get("error_code") == "OVER_MESSAGE_TOTAL")
        check("abort: budget abort leaves dir empty, no receipt",
              _dir_files(tmp, "930012") == []
              and not (os.path.exists(os.path.join(tmp, "receipts.json"))
                       and "930012" in open(os.path.join(tmp, "receipts.json"),
                                            encoding="utf-8").read()))
    finally:
        restore()


def test_declared_prereject(server):
    tmp = tempfile.mkdtemp(prefix="fi100-decl-")
    restore = _patch_dirs(tmp)
    key = os.urandom(16)
    try:
        plain = b"small declared test"
        cipher = bridge.aes_ecb_encrypt(plain, key)
        ROUTES["/decl.bin"] = (200, {"Content-Length": str(len(cipher))}, cipher)
        # Over single cap: zero downloads.
        HITS.clear()
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/decl.bin"), key, "decl.bin",
                      101 * 1024 * 1024),
            "930021", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("declared: over single cap -> DECLARED_OVER_SINGLE_CAP, 0 downloads",
              saved is None
              and (diag or {}).get("error_code") == "DECLARED_OVER_SINGLE_CAP"
              and HITS.get("/decl.bin", 0) == 0)
        # Over remaining budget: zero downloads.
        HITS.clear()
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/decl.bin"), key, "decl.bin", 50),
            "930022", 0, time.time() + 300, 10)
        check("declared: over budget -> DECLARED_OVER_MESSAGE_TOTAL, 0 downloads",
              saved is None
              and (diag or {}).get("error_code") == "DECLARED_OVER_MESSAGE_TOTAL"
              and HITS.get("/decl.bin", 0) == 0)
        # Unparseable declaration falls through to the real check.
        item = _key_item(server.url("/decl.bin"), key, "decl.bin")
        item["len"] = "not-a-number"
        saved, _, _ = bridge.process_inbound_file(
            item, "930023", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("declared: unparseable falls through and saves",
              saved is not None and saved["size"] == len(plain))
        # Exactly at the cap is not over.
        saved, _, _ = bridge.process_inbound_file(
            _key_item(server.url("/decl.bin"), key, "decl.bin",
                      100 * 1024 * 1024),
            "930024", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("declared: exactly at cap passes pre-reject and saves",
              saved is not None and saved["size"] == len(plain))
        # Integer (non-string) declaration over cap also pre-rejects.
        HITS.clear()
        item = _key_item(server.url("/decl.bin"), key, "decl.bin")
        item["len"] = 200 * 1024 * 1024
        saved, _, diag = bridge.process_inbound_file(
            item, "930025", 0, time.time() + 300, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("declared: int declaration over cap pre-rejects, 0 downloads",
              saved is None
              and (diag or {}).get("error_code") == "DECLARED_OVER_SINGLE_CAP"
              and HITS.get("/decl.bin", 0) == 0)
    finally:
        restore()


def test_boundary_patched_scale(server):
    tmp = tempfile.mkdtemp(prefix="fi100-bound-")
    restore = _patch_dirs(tmp)
    key = os.urandom(16)
    try:
        p100 = b"y" * 100
        c100 = bridge.aes_ecb_encrypt(p100, key)
        ROUTES["/b100.bin"] = (200, {"Content-Length": str(len(c100))}, c100)
        p1 = b"z"
        c1 = bridge.aes_ecb_encrypt(p1, key)
        ROUTES["/b1.bin"] = (200, {"Content-Length": str(len(c1))}, c1)
        old_single, old_total = bridge.FILE_SINGLE_PLAIN_CAP, bridge.FILE_MESSAGE_TOTAL_CAP
        bridge.FILE_SINGLE_PLAIN_CAP = 100
        bridge.FILE_MESSAGE_TOTAL_CAP = 100
        try:
            s1, _, _ = bridge.process_inbound_file(
                _key_item(server.url("/b100.bin"), key, "a.bin"),
                "930031", 0, time.time() + 300, 100)
            s2, _, diag2 = bridge.process_inbound_file(
                _key_item(server.url("/b1.bin"), key, "b.bin"),
                "930031", 1, time.time() + 300, 100 - (s1["size"] if s1 else 0))
            check("boundary: exactly at single cap saves",
                  s1 is not None and s1["size"] == 100)
            check("boundary: 1 byte over message total refused",
                  s2 is None
                  and (diag2 or {}).get("error_code") == "OVER_MESSAGE_TOTAL")
        finally:
            bridge.FILE_SINGLE_PLAIN_CAP = old_single
            bridge.FILE_MESSAGE_TOTAL_CAP = old_total
    finally:
        restore()


test_constants()
with FakeServer() as server:
    test_chunked_correctness(server)
    test_streaming_abort(server)
    test_declared_prereject(server)
    test_boundary_patched_scale(server)

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
