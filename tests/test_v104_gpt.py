"""GPT ruling tests for v1.0.4 (blocking items only).
Offline, synthetic data only, no WeChat, no real keys.
Run: python3 tests/test_v104_gpt.py /path/to/bridge.py

1. Kernel identity binding: _init_channel_status writes serve_run.json
   with run_id, pid == current pid, boot_id == current boot_id, and
   proc_starttime == /proc/self/stat field 22 for the current process.
   A pre-placed old matched file (old run_id, ok/0, proc_starttime=1)
   with a PID equal to the test process PID must NOT survive init:
   after a successful init, channel_status is pending for the NEW
   run_id, not the old ok/0.
2. Fault injection matrix (serve_loop, mocked atomic_write_json):
   - fail on 1st init write (serve_run) -> serve_loop returns 1,
     serve.pid is NOT written / sentinel unchanged.
   - fail on 2nd init write (channel_status) -> same: return 1,
     serve.pid unchanged. A partial state (new serve_run, old
     channel_status) is left, but no PID is published, so a Worker
     must not treat it as a live instance.
   - no injection, first poll pending -> serve_loop publishes the
     new PID, serve_run and channel_status share the new run_id,
     channel_status is pending (not ok). This is the "process
     recovered, channel pending confirmation" state, not recovery.
   - no injection, first poll returns an empty success -> channel
     becomes ok/0 for the new run_id. Only then may a Worker confirm
     channel recovery.
3. Startup vs runtime failure policy split: init failure aborts
   (return 1, no PID); a runtime _record_channel_status write failure
   warns and continues (does not raise out of _record).
"""
import importlib.util, json, os, sys, tempfile
from unittest.mock import patch

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}

def load(source):
    spec = importlib.util.spec_from_file_location("bridge_v104_gpt", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-v104-gpt-")
    old = m.ROOT
    for name, value in list(vars(m).items()):
        if isinstance(value, str) and (value == old or value.startswith(old + "/")):
            setattr(m, name, td.name + value[len(old):])
    m.ensure_dirs(); m.save_session(dict(SESSION))
    return td

def _read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

class EndProbe(BaseException):
    pass

def main():
    source = sys.argv[1] if len(sys.argv) > 1 else "bridge.py"
    m = load(source)
    td = isolated(m)
    try:
        # 1. Kernel binding + old matched file does not survive init
        old_boot = m._boot_id()
        old_start = m._proc_starttime()
        assert old_boot and old_boot != "unknown", old_boot
        assert isinstance(old_start, int) and old_start > 0, old_start
        # Simulate PID reuse: old files claim this very PID, with a
        # tiny old proc_starttime, so a PID-only check would pass.
        m.atomic_write_json(m.SERVE_RUN_FILE,
            {"run_id": "old-uuid-999", "pid": os.getpid(), "boot_id": old_boot,
             "proc_starttime": 1, "started_ts": "2020-01-01T00:00:00+00:00"}, mode=0o600)
        m.atomic_write_json(m.CHANNEL_STATUS_FILE,
            {"status": "ok", "consecutive_failures": 0, "last_success_ts": "2026-10-29T10:00:00Z",
             "run_id": "old-uuid-999", "process_pid": os.getpid(),
             "process_started_ts": "2020-01-01T00:00:00+00:00",
             "boot_id": old_boot, "proc_starttime": 1}, mode=0o600)
        init = m._init_channel_status()
        assert init["status"] == "pending" and init["last_success_ts"] is None, init
        assert init["run_id"] != "old-uuid-999", init
        run_file = _read_json(m.SERVE_RUN_FILE)
        assert run_file["run_id"] == init["run_id"], run_file
        assert run_file["pid"] == os.getpid(), run_file
        assert run_file["boot_id"] == old_boot, run_file
        assert run_file["proc_starttime"] == old_start, run_file
        assert init["boot_id"] == old_boot and init["proc_starttime"] == old_start, init

        # 2. Fault injection: fail 1st and 2nd init writes
        for fail_call in (1, 2):
            pid_path = os.path.join(m.STATE, "serve.pid")
            with open(pid_path, "w", encoding="utf-8") as f:
                f.write("999999\n")
            # Pre-place old matched files again, so a partial state
            # would be visible if serve_loop wrongly published a PID.
            m.atomic_write_json(m.SERVE_RUN_FILE,
                {"run_id": "old-uuid-999", "pid": os.getpid(), "boot_id": old_boot,
                 "proc_starttime": 1}, mode=0o600)
            m.atomic_write_json(m.CHANNEL_STATUS_FILE,
                {"status": "ok", "consecutive_failures": 0, "run_id": "old-uuid-999",
                 "process_pid": os.getpid()}, mode=0o600)
            orig_atomic = m.atomic_write_json
            calls = {"n": 0}
            def failing_atomic(path, obj, mode=0o600):
                # Only fail the init writes: serve_run is call 1,
                # channel_status is call 2, in _init order. Other
                # writes (e.g. save_session in isolated setup) are
                # not in this window because serve_loop is already
                # past session load; count only STATE writes for
                # serve_run/channel_status.
                if path in (m.SERVE_RUN_FILE, m.CHANNEL_STATUS_FILE):
                    calls["n"] += 1
                    if calls["n"] == fail_call:
                        raise IOError("synthetic init write failure")
                return orig_atomic(path, obj, mode=mode)
            with patch.object(m, "atomic_write_json", side_effect=failing_atomic):
                rc = m.serve_loop()
            assert rc == 1, f"fail_call={fail_call} rc={rc}, expected 1 (startup abort, not token expiry 14, not lock 2)"
            with open(pid_path, encoding="utf-8") as f:
                assert f.read().strip() == "999999", f"fail_call={fail_call}: serve.pid was published despite init failure!"


        # 2b. PID-reuse failure variant: serve.pid already equals the
        # test process PID, with old matched files claiming it.
        # Init failure must still return 1 and must NOT overwrite
        # the PID file, and the production checker must NOT recover
        # (test process cmdline is not bridge.py serve, so the
        # reason is cmdline_mismatch, which is still unconfirmed).
        pid_path2 = os.path.join(m.STATE, "serve.pid")
        with open(pid_path2, "w", encoding="utf-8") as f:
            f.write(f"{os.getpid()}\n")
        m.atomic_write_json(m.SERVE_RUN_FILE,
            {"run_id": "old-uuid-999", "pid": os.getpid(), "boot_id": m._boot_id(),
             "proc_starttime": 1}, mode=0o600)
        m.atomic_write_json(m.CHANNEL_STATUS_FILE,
            {"status": "ok", "consecutive_failures": 0, "run_id": "old-uuid-999",
             "process_pid": os.getpid()}, mode=0o600)
        orig_atomic3 = m.atomic_write_json
        def failing_atomic3(path, obj, mode=0o600):
            if path in (m.SERVE_RUN_FILE, m.CHANNEL_STATUS_FILE):
                raise IOError("synthetic init write failure (PID reuse)")
            return orig_atomic3(path, obj, mode=mode)
        with patch.object(m, "atomic_write_json", side_effect=failing_atomic3):
            rc = m.serve_loop()
        assert rc == 1, rc
        with open(pid_path2, encoding="utf-8") as f:
            assert f.read().strip() == str(os.getpid()), "PID file changed on init failure (PID reuse)!"
        r = m.verify_channel_recovery()
        assert r["recovered"] is False, r

        # 2c. Replace-stage failure: os.replace raises on the channel
        # file during _init. _init must propagate it, serve_loop must
        # return 1, PID unchanged, and the old channel JSON must
        # still parse (atomic write never truncates it in place).
        with open(pid_path2, "w", encoding="utf-8") as f:
            f.write("999999\n")
        m.atomic_write_json(m.CHANNEL_STATUS_FILE,
            {"status": "ok", "consecutive_failures": 0, "run_id": "old-uuid-999",
             "process_pid": 999999}, mode=0o600)
        orig_replace = os.replace
        def failing_replace(src, dst):
            if dst == m.CHANNEL_STATUS_FILE:
                raise OSError("synthetic replace failure")
            return orig_replace(src, dst)
        with patch.object(os, "replace", side_effect=failing_replace):
            rc = m.serve_loop()
        assert rc == 1, rc
        with open(pid_path2, encoding="utf-8") as f:
            assert f.read().strip() == "999999", "PID changed on replace failure!"
        # Old channel JSON must still be readable (not truncated).
        with open(m.CHANNEL_STATUS_FILE, encoding="utf-8") as f:
            assert json.load(f)["run_id"] == "old-uuid-999"

        # 3. No injection: pending first, then empty success confirms
        import io, contextlib
        attempts = {"n": 0}
        def poll_pending_then_success(*args, **kwargs):
            attempts["n"] += 1
            if attempts["n"] == 1:
                # First poll hangs: simulate by raising EndProbe AFTER
                # checking that PID + run files are published pending.
                run_file = _read_json(m.SERVE_RUN_FILE)
                chan = _read_json(m.CHANNEL_STATUS_FILE)
                pid_val = open(os.path.join(m.STATE, "serve.pid"), encoding="utf-8").read().strip()
                assert pid_val == str(os.getpid()), pid_val
                assert run_file["run_id"] == chan["run_id"], (run_file, chan)
                assert chan["status"] == "pending" and chan["last_success_ts"] is None, chan
                raise EndProbe()
            return {"ret": 0, "msgs": []}
        # Patch sender loop + sleep + post_json, like the reviewer probes.
        with patch.object(m, "post_json", side_effect=poll_pending_then_success), \
             patch.object(m, "_sender_loop", lambda *args: None), \
             patch.object(m.time, "sleep", lambda seconds: None), \
             contextlib.redirect_stdout(io.StringIO()):
            try:
                m.serve_loop()
            except EndProbe:
                pass
        # Second run: empty success should flip to ok/0 for the new run.
        attempts2 = {"n": 0}
        def poll_success(*args, **kwargs):
            attempts2["n"] += 1
            if attempts2["n"] > 1:
                raise EndProbe()
            return {"ret": 0, "msgs": []}
        with patch.object(m, "post_json", side_effect=poll_success), \
             patch.object(m, "_sender_loop", lambda *args: None), \
             patch.object(m.time, "sleep", lambda seconds: None), \
             contextlib.redirect_stdout(io.StringIO()):
            try:
                m.serve_loop()
            except EndProbe:
                pass
        chan = _read_json(m.CHANNEL_STATUS_FILE)
        run_file = _read_json(m.SERVE_RUN_FILE)
        assert chan["status"] == "ok" and chan["consecutive_failures"] == 0, chan
        assert chan["run_id"] == run_file["run_id"], (chan, run_file)
        assert chan["last_success_ts"] is not None, chan

        # 4. Runtime write failure warns and continues (does not raise)
        orig_atomic2 = m.atomic_write_json
        def runtime_failing(path, obj, mode=0o600):
            if path == m.CHANNEL_STATUS_FILE:
                raise IOError("synthetic runtime write failure")
            return orig_atomic2(path, obj, mode=mode)
        with patch.object(m, "atomic_write_json", side_effect=runtime_failing):
            s = m._record_channel_status(False, "synthetic failure")
            assert s["status"] == "error", s  # returned, not raised
        print("test_v104_gpt: PASS (kernel binding, init-failure abort x2, pending-then-success, runtime warn-and-continue)")
    finally:
        td.cleanup()

if __name__ == "__main__":
    main()
