"""v1.0.4 P0 tests (converged user + GPT review, 2026-10-29).
Offline, synthetic data only, no WeChat, no real keys.
Run: python3 tests/test_v104.py /path/to/bridge.py

1. Channel status (separate from process liveness):
   - success resets consecutive_failures to 0 and sets last_success_ts;
     a successful EMPTY getupdates response counts as success, so the
     caller records True on check_ret success, not on message count.
   - failure increments consecutive_failures, keeps the previous
     last_success_ts, and never stores a token / full URL in last_error.
   - _now_iso returns a timezone-aware ISO timestamp (offset present).
2. Concurrent startup regression (GPT counterexample, now disproven
   by code order): a second serve_loop that cannot take serve.lock
   MUST return 2 and MUST NOT overwrite state/serve.pid. PID is only
   written after flock succeeds in serve_loop.
3. Timestamped log lines preserve old grep substrings ("lock
   acquired", "getupdates error", "queued inbox/") after the timestamp.
4. Review patch (Finding 1): _init_channel_status() overwrites an old
   ok/0 status with pending for a NEW run_id before serve.pid is
   written, so an old instance's success can never be read together
   with a new PID. serve_run.json carries the same run_id, and
   _record_channel_status preserves it (including PID reuse).
5. Review patch (Finding 4): a channel_status file with valid JSON
   but a wrong-typed consecutive_failures (e.g. "invalid") must NOT
   raise out of _record_channel_status; it resets to 0 with a warning
   and counts the current failure as 1.
"""
import importlib.util, json, os, sys, tempfile

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}

def load(source):
    spec = importlib.util.spec_from_file_location("bridge_v104", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-v104-")
    old = m.ROOT
    for name, value in list(vars(m).items()):
        if isinstance(value, str) and (value == old or value.startswith(old + "/")):
            setattr(m, name, td.name + value[len(old):])
    m.ensure_dirs(); m.save_session(dict(SESSION))
    return td

def _read_status(m):
    with open(m.CHANNEL_STATUS_FILE, encoding="utf-8") as f:
        return json.load(f)

def main():
    source = sys.argv[1] if len(sys.argv) > 1 else "bridge.py"
    m = load(source)
    td = isolated(m)
    try:
        # 1a. timestamp format
        ts = m._now_iso()
        assert ("+" in ts or ts.endswith("Z")) and "T" in ts, ts
        # 1b. success then failure then success
        s = m._record_channel_status(True)
        assert s["status"] == "ok" and s["consecutive_failures"] == 0
        assert s["last_success_ts"], s
        first_success = s["last_success_ts"]
        s = m._record_channel_status(False, "URLError(ConnectionRefusedError: errno=111)")
        assert s["status"] == "error" and s["consecutive_failures"] == 1
        assert s["last_success_ts"] == first_success, s
        s = m._record_channel_status(False, "URLError(ConnectionRefusedError: errno=111)")
        assert s["consecutive_failures"] == 2, s
        # 1c. no secret / URL in last_error, even if the caller passes raw text
        s = m._record_channel_status(False, 'curl failed, bot_token=SYNTHETIC_BOT_TOKEN https://example.invalid/x?authkey=abc')
        assert "SYNTHETIC_BOT_TOKEN" not in s["last_error"], s
        assert "authkey=abc" not in s["last_error"], s
        assert "https://example.invalid/x?authkey=abc" not in s["last_error"], s
        s = m._record_channel_status(True)
        assert s["consecutive_failures"] == 0 and s["last_error"] is None, s
        # file on disk matches, and is atomically written (readable JSON)
        assert _read_status(m)["status"] == "ok"
        # 2. concurrent startup: hold the lock, then serve_loop must
        # return 2 WITHOUT touching serve.pid.
        import fcntl
        pid_path = os.path.join(m.STATE, "serve.pid")
        with open(pid_path, "w", encoding="utf-8") as f:
            f.write("999999\n")
        lock_f = open(m.LOCK_FILE, "a+")
        fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            rc = m.serve_loop()
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)
            lock_f.close()
        assert rc == 2, rc
        with open(pid_path, encoding="utf-8") as f:
            assert f.read().strip() == "999999", "PID overwritten by failed starter!"
        # 4. instance isolation: old ok/0 must not survive a new init
        old_status = {"status": "ok", "last_attempt_ts": m._now_iso(),
                      "last_success_ts": m._now_iso(), "consecutive_failures": 0,
                      "last_error": None, "process_pid": 999999,
                      "run_id": "old-run-id-for-test", "process_started_ts": "2020-01-01T00:00:00+00:00"}
        m.atomic_write_json(m.CHANNEL_STATUS_FILE, old_status, mode=0o600)
        init = m._init_channel_status()
        assert init["status"] == "pending" and init["last_success_ts"] is None, init
        assert init["run_id"] and init["run_id"] != "old-run-id-for-test", init
        assert init["process_pid"] == os.getpid(), init
        with open(m.SERVE_RUN_FILE, encoding="utf-8") as f:
            run_file = json.load(f)
        assert run_file["run_id"] == init["run_id"] and run_file["pid"] == os.getpid(), run_file
        # A later success keeps the same run_id, even if PID were reused.
        s = m._record_channel_status(True)
        assert s["run_id"] == init["run_id"] and s["process_started_ts"] == init["process_started_ts"], s
        # 5. wrong-typed failures field must not raise (Finding 4)
        m.atomic_write_json(m.CHANNEL_STATUS_FILE, {"consecutive_failures": "invalid"}, mode=0o600)
        s = m._record_channel_status(False, "synthetic transport failure")
        assert s["consecutive_failures"] == 1 and s["status"] == "error", s
        m.atomic_write_json(m.CHANNEL_STATUS_FILE, {"consecutive_failures": -3}, mode=0o600)
        s = m._record_channel_status(False, "synthetic transport failure")
        assert s["consecutive_failures"] == 1, s
        m.atomic_write_json(m.CHANNEL_STATUS_FILE, ["not", "an", "object"], mode=0o600)
        s = m._record_channel_status(False, "synthetic transport failure")
        assert s["consecutive_failures"] == 1, s
        # 3. timestamped log substrings preserved
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            m._log_ts("serve: lock acquired, polling")
            m._log_ts("serve: getupdates error, backing off 5s: test")
            m._log_ts("serve: queued inbox/123.json")
        out = buf.getvalue()
        for sub in ("lock acquired", "getupdates error", "queued inbox/"):
            assert sub in out, out
        print("test_v104: PASS (channel status, concurrent-start PID, timestamp substrings, instance isolation, invalid-field robustness)")
    finally:
        td.cleanup()

if __name__ == "__main__":
    main()
