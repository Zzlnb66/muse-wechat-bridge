"""Offline tests for the Gate 3 step 1 storage extraction.

bridge_storage.py holds only pure storage mechanics (root-derived
paths, atomic writes, safe JSON reads). bridge.py imports it and
keeps the same module-level names, so existing callers and tests
that rebind bridge.STATE etc. see no change.

Coverage:
- atomic_write_json / atomic_write_text: mode 0600, exact bytes,
  no tmp residue after success, failed write leaves original intact;
- read_json_dict: missing / bad JSON / non-dict -> {}, valid dict
  parsed; bridge._load_json_dict delegates with identical results;
- paths_for_root: every path under its root, two roots disjoint,
  bridge module attributes equal the derivation for its own ROOT;
- bridge.ensure_dirs with rebound globals creates the dirs;
- end-to-end fake flow through the new module: inbox record ->
  pending job -> drain -> sent archive, all via bridge_storage
  writers, temp dirs, fake send, no network, never touching the
  real running bridge.

Run: python3 tests/test_storage_module.py (exit 0 = all pass).
"""

import json
import os
import stat
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402
import bridge_storage  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def _tmp_files(d):
    return [n for n in os.listdir(d) if ".tmp" in n]


def test_atomic_writers(tmp):
    d = os.path.join(tmp, "w")
    os.makedirs(d)
    p = os.path.join(d, "a.json")
    bridge_storage.atomic_write_json(p, {"k": "v", "n": 1})
    with open(p, encoding="utf-8") as f:
        check("atomic json round-trip", json.load(f) == {"k": "v", "n": 1})
    check("atomic json mode 0600", _mode(p) == 0o600)
    check("atomic json no tmp residue", _tmp_files(d) == [])
    # Failed write leaves the original untouched.
    with open(p, "wb") as f:
        f.write(b"ORIGINAL")
    try:
        bridge_storage.atomic_write_json(p, {"bad": {1, 2}})
        raised = False
    except TypeError:
        raised = True
    check("atomic json unserializable raises", raised)
    with open(p, "rb") as f:
        check("atomic json failed write keeps original", f.read() == b"ORIGINAL")
    # Separate dir: the failed JSON write above legitimately leaves
    # its tmp file behind (original semantics), which would pollute
    # a whole-dir tmp check here.
    d2 = os.path.join(tmp, "w2")
    os.makedirs(d2)
    t = os.path.join(d2, "b.txt")
    bridge_storage.atomic_write_text(t, "hello\n")
    with open(t, encoding="utf-8") as f:
        check("atomic text round-trip", f.read() == "hello\n")
    check("atomic text mode 0600", _mode(t) == 0o600)
    check("atomic text no tmp residue", _tmp_files(d2) == [])
    check("bridge re-exports storage writers",
          bridge.atomic_write_json is bridge_storage.atomic_write_json
          and bridge.atomic_write_text is bridge_storage.atomic_write_text)


def test_safe_read(tmp):
    d = os.path.join(tmp, "r")
    os.makedirs(d)
    missing = os.path.join(d, "missing.json")
    bad = os.path.join(d, "bad.json")
    with open(bad, "w", encoding="utf-8") as f:
        f.write("{not json")
    lst = os.path.join(d, "list.json")
    with open(lst, "w", encoding="utf-8") as f:
        f.write("[1, 2]")
    good = os.path.join(d, "good.json")
    with open(good, "w", encoding="utf-8") as f:
        json.dump({"a": 1}, f)
    for path, want in ((missing, {}), (bad, {}), (lst, {}), (good, {"a": 1})):
        check(f"read_json_dict {os.path.basename(path)}",
              bridge_storage.read_json_dict(path) == want)
        check(f"bridge._load_json_dict delegates {os.path.basename(path)}",
              bridge._load_json_dict(path) == want)


def test_paths(tmp):
    a = bridge_storage.paths_for_root(os.path.join(tmp, "rootA"))
    b = bridge_storage.paths_for_root(os.path.join(tmp, "rootB"))
    fields = [k for k in vars(a) if k != "root"]
    check("paths: every value under its root",
          all(getattr(a, k).startswith(os.path.join(tmp, "rootA") + os.sep)
              for k in fields))
    check("paths: two roots disjoint",
          all(getattr(a, k) != getattr(b, k) for k in fields))
    mine = bridge_storage.paths_for_root(bridge.ROOT)
    mapping = {
        "state": "STATE", "inbox": "INBOX", "running": "RUNNING", "done": "DONE",
        "outbox_pending": "OUTBOX_PENDING", "outbox_sent": "OUTBOX_SENT",
        "outbox_failed": "OUTBOX_FAILED", "media_inbound": "MEDIA_INBOUND",
        "media_outbound": "MEDIA_OUTBOUND", "analysis_dir": "ANALYSIS_DIR",
        "local_vault_dir": "LOCAL_VAULT_DIR", "failures_dir": "FAILURES_DIR",
        "file_inbound_dir": "FILE_INBOUND_DIR",
        "local_only_mode_file": "LOCAL_ONLY_MODE_FILE",
        "pending_local_queries_file": "PENDING_LOCAL_QUERIES_FILE",
        "ttl_index_file": "TTL_INDEX_FILE", "tombstone_file": "TOMBSTONE_FILE",
        "image_event_file": "IMAGE_EVENT_FILE",
        "file_receipts_file": "FILE_RECEIPTS_FILE",
        "session_file": "SESSION_FILE", "cursor_file": "CURSOR_FILE",
        "processed_file": "PROCESSED_FILE",
        "needs_relogin_file": "NEEDS_RELOGIN_FILE",
        "login_pending_file": "LOGIN_PENDING_FILE",
        "lock_file": "LOCK_FILE",
        "channel_status_file": "CHANNEL_STATUS_FILE",
        "serve_run_file": "SERVE_RUN_FILE",
        "analyzer_runs_file": "ANALYZER_RUNS_FILE",
        "safe_view_file": "SAFE_VIEW_FILE",
    }
    check("paths: bridge attrs equal derivation",
          all(getattr(bridge, mod) == getattr(mine, field)
              for field, mod in mapping.items()))


def test_ensure_dirs_rebound(tmp):
    names = ("STATE", "INBOX", "RUNNING", "DONE", "OUTBOX_PENDING",
             "OUTBOX_SENT", "OUTBOX_FAILED", "MEDIA_INBOUND",
             "MEDIA_OUTBOUND", "FAILURES_DIR", "ANALYSIS_DIR",
             "LOCAL_VAULT_DIR")
    old = {n: getattr(bridge, n) for n in names}
    try:
        for n in names:
            setattr(bridge, n, os.path.join(tmp, "rebound", n.lower()))
        bridge.ensure_dirs()
        check("ensure_dirs creates rebound dirs",
              all(os.path.isdir(getattr(bridge, n)) for n in names))
    finally:
        for n, v in old.items():
            setattr(bridge, n, v)


def test_e2e_fake_flow(tmp):
    names = ("STATE", "INBOX", "RUNNING", "DONE", "OUTBOX_PENDING",
             "OUTBOX_SENT", "OUTBOX_FAILED", "MEDIA_INBOUND",
             "MEDIA_OUTBOUND", "FAILURES_DIR", "ANALYSIS_DIR",
             "LOCAL_VAULT_DIR")
    old = {n: getattr(bridge, n) for n in names}
    try:
        for n in names:
            d = os.path.join(tmp, "flow", n.lower())
            os.makedirs(d, exist_ok=True)
            setattr(bridge, n, d)
        bridge._OBS_QUEUED_SEEN.clear()
        bridge._TERMINAL_TASK_IDS.clear()
        bridge._IN_MEMORY_HALTED_FILES.clear()
        try:
            bridge._SEND_COOLDOWN.clear()
        except Exception:
            pass
        # Receive: inbox record written via the storage module.
        mid = "900001"
        bridge.atomic_write_json(
            os.path.join(bridge.INBOX, mid + ".json"),
            {"message_id": mid, "text": "hi", "from_user_id": "user-T"})
        # Worker task: pending job written via the storage module.
        bridge.atomic_write_json(
            os.path.join(bridge.OUTBOX_PENDING, mid + ".json"),
            {"text": "hello storage", "to_user_id": "user-T"})
        calls = []

        def _fake_send_text(session, text, to_user_id=None, context_token="", client_id=None):
            calls.append(text)
            return {}

        orig = (bridge.send_text, bridge.send_file, bridge.send_image)
        bridge.send_text = _fake_send_text
        bridge.send_file = lambda *a, **k: {}
        bridge.send_image = lambda *a, **k: {}
        try:
            sent, failed = bridge.drain_outbox({"ilink_user_id": "user-T"})
        finally:
            bridge.send_text, bridge.send_file, bridge.send_image = orig
        check("e2e: fake send called once with job text",
              calls == ["hello storage"])
        check("e2e: job reported sent", mid + ".json" in sent and not failed)
        sent_path = os.path.join(bridge.OUTBOX_SENT, mid + ".json")
        check("e2e: sent archive exists via storage paths",
              os.path.exists(sent_path))
        with open(sent_path, encoding="utf-8") as f:
            check("e2e: sent archive parses with job text",
                  json.load(f).get("text") == "hello storage")
        check("e2e: sent archive mode 0600", _mode(sent_path) == 0o600)
        check("e2e: no tmp residue in pending/sent",
              _tmp_files(bridge.OUTBOX_PENDING) == []
              and _tmp_files(bridge.OUTBOX_SENT) == [])
    finally:
        for n, v in old.items():
            setattr(bridge, n, v)


def main():
    tmp = tempfile.mkdtemp(prefix="storage-module-")
    test_atomic_writers(tmp)
    test_safe_read(tmp)
    test_paths(tmp)
    test_ensure_dirs_rebound(tmp)
    test_e2e_fake_flow(tmp)
    failed = [n for n, c in ok if not c]
    print(f"\nstorage_module: {len(ok) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
