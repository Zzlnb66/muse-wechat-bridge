"""Offline tests for Gate 3 item 2 wiring: auto-parse after file receipt.

Covers, against the fix-copy bridge.py:
  1. Helper on a real DOCX (renamed .bin): evidence lands next to the
     file as doc_evidence_<idx>.json with correct anchors, mode 0600;
     a second call skips without re-running.
  2. Helper on unknown bytes: clean failure evidence, no raise.
  3. Helper timeout (sleep hook + patched fuses): timeout evidence,
     no raise, receipt unaffected.
  4. Helper with pre-existing evidence: skipped, content untouched.
  5. Helper with parser missing: wiring failure evidence, no raise.
  6. Helper with no usable saved path: failed, no raise, no evidence.
  7. serve_loop integration: a mocked batch with one real DOCX reaches
     inbox AND leaves a completed doc_evidence_0.json next to the
     landed file; a local-only batch refuses without downloading and
     leaves no doc_evidence files anywhere.

Run: python3 tests/test_doc_parser_wiring.py (exit 0 = all pass).
"""

import base64
import http.server
import json
import os
import shutil
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


def _make_docx_bytes():
    from docx import Document
    path = os.path.join(tempfile.mkdtemp(prefix="dpw-docx-"), "s.docx")
    d = Document()
    d.add_paragraph("wiring paragraph text")
    t = d.add_table(rows=1, cols=2)
    t.cell(0, 0).text = "cell-one"
    t.cell(0, 1).text = "cell-two"
    d.save(path)
    with open(path, "rb") as f:
        return f.read()


def _landed_file(tmp, mid, idx, data):
    """Fake a landed .bin file exactly like process_inbound_file names it."""
    d = os.path.join(tmp, "media", "inbound", "files", mid)
    os.makedirs(d, exist_ok=True)
    os.chmod(d, 0o700)
    p = os.path.join(d, f"item_{idx}_abcdef012345.bin")
    with open(p, "wb") as f:
        f.write(data)
    os.chmod(p, 0o600)
    return p


def test_helper_unit():
    tmp = tempfile.mkdtemp(prefix="dpw-unit-")
    restore = _patch_dirs(tmp)
    try:
        docx = _make_docx_bytes()
        mid, idx = "930001", 0
        landed = _landed_file(tmp, mid, idx, docx)
        saved = {"path": landed, "size": len(docx), "status": "saved"}
        ev = bridge.doc_evidence_path_for(landed, idx)
        check("wiring: evidence name is doc_evidence_<idx>.json next to file",
              ev == os.path.join(os.path.dirname(landed), "doc_evidence_0.json"))
        st = bridge.maybe_parse_inbound_doc(saved, idx)
        check("wiring: real DOCX .bin parses to completed evidence",
              st == "parsed" and os.path.isfile(ev))
        data = json.load(open(ev, encoding="utf-8"))
        texts = [a["text"] for a in data.get("anchors", [])]
        check("wiring: evidence anchors carry paragraph + table cells",
              data.get("status") == "completed"
              and "wiring paragraph text" in texts
              and "cell-one" in texts and "cell-two" in texts)
        check("wiring: evidence file is 0600",
              (os.stat(ev).st_mode & 0o777) == 0o600)
        mtime = os.path.getmtime(ev)
        time.sleep(0.05)
        st2 = bridge.maybe_parse_inbound_doc(saved, idx)
        check("wiring: existing evidence skips without re-running",
              st2 == "skipped_exists" and os.path.getmtime(ev) == mtime)

        # Unknown bytes: clean failure evidence, no raise.
        mid2, idx2 = "930002", 0
        landed2 = _landed_file(tmp, mid2, idx2, b"\x00\x01\x02garbage-not-a-doc")
        ev2 = bridge.doc_evidence_path_for(landed2, idx2)
        st3 = bridge.maybe_parse_inbound_doc({"path": landed2}, idx2)
        d2 = json.load(open(ev2, encoding="utf-8")) if os.path.isfile(ev2) else {}
        check("wiring: unknown format -> failed evidence, no raise",
              st3 == "failed" and d2.get("status") == "failed"
              and d2.get("failure_code") == "unsupported_format")

        # Pre-existing evidence: untouched.
        mid3, idx3 = "930003", 2
        landed3 = _landed_file(tmp, mid3, idx3, docx)
        ev3 = bridge.doc_evidence_path_for(landed3, idx3)
        with open(ev3, "w", encoding="utf-8") as f:
            f.write("SENTINEL")
        st4 = bridge.maybe_parse_inbound_doc({"path": landed3}, idx3)
        check("wiring: pre-existing evidence left untouched",
              st4 == "skipped_exists"
              and open(ev3, encoding="utf-8").read() == "SENTINEL")

        # Parser missing: wiring failure evidence, no raise.
        old_root = bridge.ROOT
        bridge.ROOT = tempfile.mkdtemp(prefix="dpw-noroot-")
        try:
            mid4, idx4 = "930004", 0
            landed4 = _landed_file(tmp, mid4, idx4, docx)
            ev4 = bridge.doc_evidence_path_for(landed4, idx4)
            st5 = bridge.maybe_parse_inbound_doc({"path": landed4}, idx4)
            d4 = json.load(open(ev4, encoding="utf-8")) if os.path.isfile(ev4) else {}
            check("wiring: missing parser -> wiring failure evidence, no raise",
                  st5 == "failed" and d4.get("status") == "failed"
                  and d4.get("failure_code") == "wiring_failed")
        finally:
            bridge.ROOT = old_root

        # No usable saved path: failed, no raise, no evidence file.
        st6 = bridge.maybe_parse_inbound_doc({"path": "/nonexistent/x.bin"}, 0)
        st7 = bridge.maybe_parse_inbound_doc({}, 0)
        check("wiring: missing file / empty saved -> failed, no raise",
              st6 == "failed" and st7 == "failed")

        # Timeout: sleep hook + patched fuses -> timeout evidence.
        old_parse, old_caller = (bridge.DOC_PARSE_TIMEOUT_SECONDS,
                                 bridge.DOC_PARSE_CALLER_TIMEOUT_SECONDS)
        old_env = {k: os.environ.get(k) for k in
                   ("DOC_PARSER_TEST_MODE", "DOC_PARSER_TEST_SLEEP_SECONDS")}
        bridge.DOC_PARSE_TIMEOUT_SECONDS = 1
        bridge.DOC_PARSE_CALLER_TIMEOUT_SECONDS = 5
        os.environ["DOC_PARSER_TEST_MODE"] = "1"
        os.environ["DOC_PARSER_TEST_SLEEP_SECONDS"] = "5"
        try:
            mid5, idx5 = "930005", 0
            landed5 = _landed_file(tmp, mid5, idx5, docx)
            ev5 = bridge.doc_evidence_path_for(landed5, idx5)
            st8 = bridge.maybe_parse_inbound_doc({"path": landed5}, idx5)
            d5 = json.load(open(ev5, encoding="utf-8")) if os.path.isfile(ev5) else {}
            check("wiring: parser timeout -> timeout evidence, no raise",
                  st8 == "failed" and d5.get("status") == "failed"
                  and d5.get("failure_code") == "timeout")
        finally:
            bridge.DOC_PARSE_TIMEOUT_SECONDS = old_parse
            bridge.DOC_PARSE_CALLER_TIMEOUT_SECONDS = old_caller
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
    finally:
        restore()


def _key_item(url, key, name="file.bin", plain_len=None):
    item = {"media": {"full_url": url,
                      "aes_key": base64.b64encode(key).decode(),
                      "encrypt_type": 1},
            "file_name": name}
    if plain_len is not None:
        item["len"] = str(plain_len)
    return item


def _file_msg(mid, items, text=None, user="USER1"):
    item_list = [{"type": 4, "file_item": it} for it in items]
    msg = {"message_id": mid, "from_user_id": user, "context_token": "CTX",
           "create_time_ms": int(time.time() * 1000), "item_list": item_list}
    if text is not None:
        msg["item_list"].insert(0, {"type": 1, "text_item": {"text": text}})
    return msg


def _run_serve_batch(tmp, messages, session_user="USER1", mode_on=False):
    restore = _patch_dirs(tmp)
    old_post = bridge.post_json
    old_send = bridge.send_direct_or_outbox
    old_ticket = bridge.get_typing_ticket
    sent_calls = []
    try:
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
        restore()


def test_serve_loop(server):
    docx = _make_docx_bytes()
    key = os.urandom(16)
    cipher = bridge.aes_ecb_encrypt(docx, key)
    ROUTES["/doc.bin"] = (200, {"Content-Length": str(len(cipher))}, cipher)
    item = _key_item(server.url("/doc.bin"), key, "report.docx", len(docx))

    tmp = tempfile.mkdtemp(prefix="dpw-serve-")
    rc, sent, inbox_dir, base = _run_serve_batch(
        tmp, [_file_msg("930101", [item], text="请看这份文件")])
    rec_path = os.path.join(inbox_dir, "930101.json")
    check("wiring serve: file message still reaches inbox",
          os.path.isfile(rec_path))
    rec = json.load(open(rec_path, encoding="utf-8")) if os.path.isfile(rec_path) else {}
    files = rec.get("files", [])
    check("wiring serve: inbox files[] saved",
          len(files) == 1 and os.path.isfile(files[0]["path"]))
    ev = None
    if files:
        # The file's original item_list index is 1 here (text is 0), so
        # the landed file is item_1_*.bin and the evidence must be
        # doc_evidence_1.json -- the stable original index, not a
        # renumbered per-file counter.
        idx = int(files[0]["attachment_id"].split(":")[1])
        ev = bridge.doc_evidence_path_for(files[0]["path"], idx)
        check("wiring serve: evidence index matches landed item index",
              os.path.basename(files[0]["path"]).startswith(f"item_{idx}_")
              and os.path.basename(ev) == f"doc_evidence_{idx}.json")
    ev_data = json.load(open(ev, encoding="utf-8")) if ev and os.path.isfile(ev) else {}
    texts = [a["text"] for a in ev_data.get("anchors", [])]
    check("wiring serve: completed doc_evidence_<idx>.json next to landed file",
          ev_data.get("status") == "completed"
          and "wiring paragraph text" in texts and "cell-one" in texts)
    check("wiring serve: evidence file is 0600",
          ev is not None and os.path.isfile(ev)
          and (os.stat(ev).st_mode & 0o777) == 0o600)

    # Local-only mode: refusal, zero downloads, no evidence anywhere.
    tmp2 = tempfile.mkdtemp(prefix="dpw-serve-mode-")
    HITS.clear()
    rc2, sent2, inbox_dir2, _ = _run_serve_batch(
        tmp2, [_file_msg("930102", [item])], mode_on=True)
    found_evidence = []
    for root, _dirs, names in os.walk(tmp2):
        found_evidence += [n for n in names if n.startswith("doc_evidence_")]
    check("wiring serve: local-only refusal downloads nothing, parses nothing",
          not os.path.exists(os.path.join(inbox_dir2, "930102.json"))
          and HITS.get("/doc.bin", 0) == 0
          and found_evidence == [])


test_helper_unit()
with FakeServer() as server:
    test_serve_loop(server)

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
