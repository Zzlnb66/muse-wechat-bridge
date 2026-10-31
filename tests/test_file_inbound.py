"""Offline tests for item 2 (ordinary file receive v1), 2026-10-31 plan.

Covers, against the fix-copy bridge.py:
  1. extract_file_items: original item_list indices are the stable keys.
  2. Display-name cleaning and the media-URL allowlist.
  3. process_inbound_file against a local HTTP server (curl path):
     happy path + receipt reuse without re-download, empty file,
     key-format error, missing key (no download), truncated cipher,
     over-cap download, past deadline, redirect to a disallowed host,
     invalid message_id, message-total cap.
  4. serve_loop-level integration (mocked getupdates only): a batch
     with a pure file, a text+file message, and a two-file message
     under a patched count cap reaches inbox records with files[] /
     file_errors[], processed ids, and the batch cursor; a bad-key
     pure file takes the fail-prompt path; a file under local-only
     mode takes the refuse path without any download.

Run: python3 tests/test_file_inbound.py (exit 0 = all pass).
"""

import base64
import hashlib
import http.server
import json
import os
import shutil
import socketserver
import subprocess
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


# ---------------------------------------------------------------- fake server

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
    """Redirect every bridge file/dir constant at tmp; returns restore fn."""
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


# ---------------------------------------------------------------- unit tests

def test_extract_and_clean():
    msg = {"item_list": [
        {"type": 2, "image_item": {"x": 1}},
        {"type": 4, "file_item": {"file_name": "a.txt"}},
        {"type": 5, "video_item": {}},
        {"type": 4, "file_item": {"file_name": "b.pdf"}},
        {"type": 4, "file_item": "not-a-dict"},
    ]}
    got = bridge.extract_file_items(msg)
    check("extract: original indices are the stable keys",
          got == [(1, {"file_name": "a.txt"}), (3, {"file_name": "b.pdf"})])
    check("clean: traversal stripped",
          "/" not in bridge._clean_file_display_name("../../etc/passwd", 0)
          and "\\" not in bridge._clean_file_display_name("a\\b", 0))
    check("clean: reserved and empty fall back",
          bridge._clean_file_display_name("CON", 2) == "文件_2"
          and bridge._clean_file_display_name("", 3) == "文件_3")
    check("clean: normal name kept, long name truncated",
          bridge._clean_file_display_name("报告.pdf", 0) == "报告.pdf"
          and len(bridge._clean_file_display_name("x" * 200, 0)) == 100)
    check("url allowlist: weixin https ok, http/evil refused",
          bridge._media_url_allowed("https://novac2c.cdn.weixin.qq.com/c2c/download?x=1")
          and not bridge._media_url_allowed("http://novac2c.cdn.weixin.qq.com/c2c")
          and not bridge._media_url_allowed("https://evil.example.com/f"))


def test_process_paths(server):
    tmp = tempfile.mkdtemp(prefix="fi-process-")
    restore = _patch_dirs(tmp)
    key = os.urandom(16)
    try:
        plain = b"hello file world\n"
        cipher = bridge.aes_ecb_encrypt(plain, key)
        ROUTES["/good.bin"] = (200, {"Content-Length": str(len(cipher))}, cipher)
        HITS.clear()
        item = _key_item(server.url("/good.bin"), key, "报告.pdf", len(plain))
        saved, err, diag = bridge.process_inbound_file(
            item, "910001", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: happy path saves identical bytes",
              saved is not None and open(saved["path"], "rb").read() == plain
              and saved["sha256"] == hashlib.sha256(plain).hexdigest()
              and saved["attachment_id"] == "910001:0" and saved["name"] == "报告.pdf")
        check("process: stored name is generated, never the display name",
              os.path.basename(saved["path"]).startswith("item_0_")
              and os.path.basename(saved["path"]).endswith(".bin"))
        hits_after_first = HITS.get("/good.bin", 0)
        saved2, _, _ = bridge.process_inbound_file(
            item, "910001", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: receipt reuse, no second download",
              saved2 is not None and saved2["path"] == saved["path"]
              and HITS.get("/good.bin", 0) == hits_after_first)
        # Empty file: 0-byte plain -> one PKCS#7 block of cipher.
        empty_cipher = bridge.aes_ecb_encrypt(b"", key)
        ROUTES["/empty.bin"] = (200, {"Content-Length": str(len(empty_cipher))}, empty_cipher)
        saved, _, _ = bridge.process_inbound_file(
            _key_item(server.url("/empty.bin"), key, "空.txt", 0),
            "910001", 1, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: empty file saves 0 bytes",
              saved is not None and saved["size"] == 0
              and os.path.getsize(saved["path"]) == 0)
        # Key format error: deterministic, before any download.
        HITS.clear()
        badkey = {"media": {"full_url": server.url("/good.bin"), "aes_key": "!!!not-base64!!!"},
                  "file_name": "bad.txt"}
        saved, err, diag = bridge.process_inbound_file(
            badkey, "910002", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: bad key format -> KEY_FORMAT_ERROR, no save",
              saved is None and (diag or {}).get("error_code") == "KEY_FORMAT_ERROR")
        # Missing key: no download at all.
        HITS.clear()
        nokey = {"media": {"full_url": server.url("/good.bin")}, "file_name": "nokey.txt"}
        saved, err, diag = bridge.process_inbound_file(
            nokey, "910003", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: missing key -> NO_KEY, zero downloads",
              saved is None and (diag or {}).get("error_code") == "NO_KEY"
              and HITS.get("/good.bin", 0) == 0)
        # Deterministically bad padding: 16-byte plain ending 0x00,
        # encrypted without padding, so the bridge's padding check fails.
        plain16 = b"A" * 15 + b"\x00"
        r = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-K", key.hex(),
                            "-nosalt", "-nopad"], input=plain16,
                           capture_output=True, timeout=30)
        assert r.returncode == 0
        ROUTES["/badpad.bin"] = (200, {"Content-Length": str(len(r.stdout))}, r.stdout)
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/badpad.bin"), key, "badpad.bin"),
            "910004", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: bad padding -> DECRYPT_FAILED, ciphertext not saved",
              saved is None and (diag or {}).get("error_code") == "DECRYPT_FAILED")
        # Truncated cipher (not a multiple of 16).
        ROUTES["/trunc.bin"] = (200, {"Content-Length": "17"}, b"x" * 17)
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/trunc.bin"), key, "trunc.bin"),
            "910005", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: truncated cipher -> BAD_CIPHER_SIZE",
              saved is None and (diag or {}).get("error_code") == "BAD_CIPHER_SIZE")
        # Over cipher cap: 11 MiB body.
        big = os.urandom(11 * 1024 * 1024)
        ROUTES["/big.bin"] = (200, {"Content-Length": str(len(big))}, big)
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/big.bin"), key, "big.bin"),
            "910006", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: over-cap download -> error, no save",
              saved is None and (diag or {}).get("error_code") in
              ("CURL_DOWNLOAD_FAILED", "OVER_CIPHER_CAP"))
        # Past deadline: no download.
        HITS.clear()
        saved, err, diag = bridge.process_inbound_file(
            item, "910007", 0, time.time() - 1, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: past deadline -> BUDGET_EXHAUSTED, zero downloads",
              saved is None and (diag or {}).get("error_code") == "BUDGET_EXHAUSTED"
              and HITS.get("/good.bin", 0) == 0)
        # Redirect to a disallowed host (localhost, not the 127.0.0.1 hook).
        ROUTES["/redir"] = (302, {"Location": server.url("/good.bin").replace("127.0.0.1", "localhost")}, b"")
        saved, err, diag = bridge.process_inbound_file(
            _key_item(server.url("/redir"), key, "redir.bin"),
            "910008", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: redirect to disallowed host refused",
              saved is None and (diag or {}).get("error_code") == "REDIRECT_HOST_NOT_ALLOWED")
        # Invalid message_id: no download.
        HITS.clear()
        saved, err, diag = bridge.process_inbound_file(
            item, "../evil", 0, time.time() + 60, bridge.FILE_MESSAGE_TOTAL_CAP)
        check("process: non-digit message_id refused, zero downloads",
              saved is None and (diag or {}).get("error_code") == "INVALID_MESSAGE_ID"
              and HITS.get("/good.bin", 0) == 0)
        # Message total cap via a patched small cap.
        old_cap = bridge.FILE_MESSAGE_TOTAL_CAP
        bridge.FILE_MESSAGE_TOTAL_CAP = 100
        try:
            p60 = b"z" * 60
            c60 = bridge.aes_ecb_encrypt(p60, key)
            ROUTES["/s60.bin"] = (200, {"Content-Length": str(len(c60))}, c60)
            s1, _, _ = bridge.process_inbound_file(
                _key_item(server.url("/s60.bin"), key, "a.bin"),
                "910009", 0, time.time() + 60, 100)
            s2, _, diag2 = bridge.process_inbound_file(
                _key_item(server.url("/s60.bin"), key, "b.bin"),
                "910009", 1, time.time() + 60, 100 - (s1["size"] if s1 else 0))
            check("process: second file over message total -> OVER_MESSAGE_TOTAL",
                  s1 is not None and s2 is None
                  and (diag2 or {}).get("error_code") == "OVER_MESSAGE_TOTAL")
        finally:
            bridge.FILE_MESSAGE_TOTAL_CAP = old_cap
    finally:
        restore()


def _run_serve_batch(tmp, messages, session_user="USER1", mode_on=False,
                     max_count=None):
    """Run serve_loop once over a mocked batch; returns (sent_calls, inbox_dir)."""
    restore = _patch_dirs(tmp)
    old_post = bridge.post_json
    old_send = bridge.send_direct_or_outbox
    old_ticket = bridge.get_typing_ticket
    old_count = bridge.FILE_MESSAGE_MAX_COUNT
    sent_calls = []
    try:
        if max_count is not None:
            bridge.FILE_MESSAGE_MAX_COUNT = max_count
        session = {"baseurl": "https://ilinkai.weixin.qq.com", "bot_token": "T",
                   "context_token": "CTX", "ilink_bot_id": "1",
                   "ilink_user_id": session_user}
        with open(os.path.join(tmp, "session.json"), "w", encoding="utf-8") as f:
            json.dump(session, f)
        if mode_on:
            bridge.set_local_only_mode(True)
        calls = {"n": 0}

        def fake_post(base, path, payload, session=None, authenticated=False, timeout=45):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"ret": 0, "msgs": messages, "get_updates_buf": "cursor-1"}
            raise bridge.TokenExpired("test stop")

        bridge.post_json = fake_post
        bridge.send_direct_or_outbox = lambda *a, **k: sent_calls.append(a) or True
        bridge.get_typing_ticket = lambda session: None
        rc = bridge.serve_loop()
        return rc, sent_calls, os.path.join(tmp, "inbox"), tmp
    finally:
        bridge.post_json = old_post
        bridge.send_direct_or_outbox = old_send
        bridge.get_typing_ticket = old_ticket
        bridge.FILE_MESSAGE_MAX_COUNT = old_count
        restore()


def _file_msg(mid, items, text=None, user="USER1"):
    item_list = [{"type": 4, "file_item": it} for it in items]
    msg = {"message_id": mid, "from_user_id": user, "context_token": "CTX",
           "create_time_ms": int(time.time() * 1000), "item_list": item_list}
    if text is not None:
        msg["item_list"].insert(0, {"type": 1, "text_item": {"text": text}})
    return msg


def test_serve_loop(server):
    tmp = tempfile.mkdtemp(prefix="fi-serve-")
    key = os.urandom(16)
    plain_a = b"alpha file content"
    plain_b = b"beta file content!!"
    cipher_a = bridge.aes_ecb_encrypt(plain_a, key)
    cipher_b = bridge.aes_ecb_encrypt(plain_b, key)
    ROUTES["/fa.bin"] = (200, {"Content-Length": str(len(cipher_a))}, cipher_a)
    ROUTES["/fb.bin"] = (200, {"Content-Length": str(len(cipher_b))}, cipher_b)
    item_a = _key_item(server.url("/fa.bin"), key, "a.txt", len(plain_a))
    item_b = _key_item(server.url("/fb.bin"), key, "b.txt", len(plain_b))
    msgs = [
        _file_msg("920001", [item_a]),
        _file_msg("920002", [item_b], text="请看这份文件"),
        _file_msg("920003", [item_a, item_b]),
    ]
    rc, sent_calls, inbox_dir, base = _run_serve_batch(tmp, msgs, max_count=1)
    check("serve: loop exits 14 on mocked token expiry", rc == 14)
    rec1 = json.load(open(os.path.join(inbox_dir, "920001.json"), encoding="utf-8"))
    check("serve: pure file reaches inbox with files[]",
          len(rec1.get("files", [])) == 1
          and open(rec1["files"][0]["path"], "rb").read() == plain_a
          and rec1["files"][0]["sha256"] == hashlib.sha256(plain_a).hexdigest()
          and rec1["files"][0]["attachment_id"] == "920001:0"
          and "context_token" not in rec1)
    rec2 = json.load(open(os.path.join(inbox_dir, "920002.json"), encoding="utf-8"))
    check("serve: text+file keeps both",
          rec2.get("text") == "请看这份文件" and len(rec2.get("files", [])) == 1)
    rec3 = json.load(open(os.path.join(inbox_dir, "920003.json"), encoding="utf-8"))
    check("serve: second file over count cap -> file_errors, first saved",
          len(rec3.get("files", [])) == 1
          and len(rec3.get("file_errors", [])) == 1
          and rec3["file_errors"][0].get("error_code") == "OVER_MESSAGE_COUNT"
          and rec3["file_errors"][0].get("index") == 1)
    proc = json.load(open(os.path.join(base, "processed.json"), encoding="utf-8"))
    check("serve: all three messages marked processed",
          all(m in proc for m in ("920001", "920002", "920003")))
    check("serve: batch cursor advanced",
          open(os.path.join(base, "cursor.txt"), encoding="utf-8").read().strip() == "cursor-1")

    # Bad-key pure file: fail-prompt path, no inbox record.
    tmp2 = tempfile.mkdtemp(prefix="fi-serve-bad-")
    bad = {"media": {"full_url": server.url("/fa.bin"), "aes_key": "!!!not-base64!!!"},
           "file_name": "bad.txt"}
    rc, sent_calls, inbox_dir2, _ = _run_serve_batch(tmp2, [_file_msg("920011", [bad])])
    prompts = [a[1] for a in sent_calls if len(a) > 1]
    check("serve: bad-key pure file -> fail prompt, no inbox",
          any("文件接收失败" in (p or "") for p in prompts)
          and not os.path.exists(os.path.join(inbox_dir2, "920011.json")))

    # Local-only mode on: refuse path, zero downloads.
    tmp3 = tempfile.mkdtemp(prefix="fi-serve-mode-")
    HITS.clear()
    rc, sent_calls, inbox_dir3, _ = _run_serve_batch(
        tmp3, [_file_msg("920021", [item_a])], mode_on=True)
    prompts = [a[1] for a in sent_calls if len(a) > 1]
    check("serve: local-only mode refuses file without downloading",
          any("仅本地模式" in (p or "") for p in prompts)
          and not os.path.exists(os.path.join(inbox_dir3, "920021.json"))
          and HITS.get("/fa.bin", 0) == 0)


test_extract_and_clean()
with FakeServer() as server:
    test_process_paths(server)
    test_serve_loop(server)

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
