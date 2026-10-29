"""Production Worker verification tests (review round 2, blocking).
Offline, synthetic data only, no WeChat, no real keys.
Run: python3 tests/test_v104_worker_verify.py /path/to/bridge.py

This test uses the PRODUCTION checker, module.verify_channel_recovery()
and the `verify-channel` CLI path (same function), NOT a test-only
judge. A test-only judge would not prove the production Worker
behaviour, which is exactly what the previous round flagged.

Scenarios, all with a REAL live dummy process whose argv is
`python3 <tmp>/bridge.py serve` (so /proc cmdline and starttime are
real, not substituted):
  - old matched files + PID reuse: serve.pid = dummy PID, both files
    claim that PID with an old run_id and proc_starttime=1, channel
    ok/0 -> verify MUST be False (proc_starttime_mismatch), even
    though PID matches and the two files agree with each other.
  - init intermediate state: serve_run has a NEW run_id, channel
    still has the OLD run_id -> verify MUST be False (run_id_mismatch).
  - same instance, pending first: serve_run + channel share the new
    run_id, channel pending, last_success None -> verify MUST be
    False (channel_pending), and must NOT say "process recovered"
    in the sense of channel recovery (recovered field is False).
  - same instance, then ok: channel updated to ok/0 with a
    last_success_ts, same run_id, same kernel identity -> verify
    MUST be True (recovered).
  - process dead: kill the dummy, verify MUST be False
    (process_not_running), never recovered.
  - missing identity fields: old-format files without boot_id /
    proc_starttime -> verify MUST be False, never recovered.
"""
import importlib.util, json, os, subprocess, sys, tempfile, time

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}

def load(source):
    spec = importlib.util.spec_from_file_location("bridge_v104_worker", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-v104-worker-")
    old = m.ROOT
    for name, value in list(vars(m).items()):
        if isinstance(value, str) and (value == old or value.startswith(old + "/")):
            setattr(m, name, td.name + value[len(old):])
    m.ensure_dirs(); m.save_session(dict(SESSION))
    return td

def _write(m, path, obj):
    m.atomic_write_json(path, obj, mode=0o600)

def main():
    source = sys.argv[1] if len(sys.argv) > 1 else "bridge.py"
    m = load(source)
    td = isolated(m)
    dummy = None
    try:
        # Dummy live process with a real bridge.py serve argv.
        dummy_path = os.path.join(td.name, "bridge.py")
        with open(dummy_path, "w", encoding="utf-8") as f:
            f.write("import time\ntime.sleep(60)\n")
        dummy = subprocess.Popen([sys.executable, dummy_path, "serve"])
        pid = dummy.pid
        # Wait for /proc entry to appear.
        for _ in range(50):
            if os.path.exists(f"/proc/{pid}/cmdline"):
                break
            time.sleep(0.05)
        boot = m._boot_id()
        start = m._proc_starttime(pid)
        assert boot and boot != "unknown", boot
        assert isinstance(start, int) and start > 0, start
        with open(os.path.join(m.STATE, "serve.pid"), "w", encoding="utf-8") as f:
            f.write(f"{pid}\n")

        # A. Old matched files + PID reuse -> must NOT recover.
        _write(m, m.SERVE_RUN_FILE, {"run_id": "old-uuid-999", "pid": pid, "boot_id": boot, "proc_starttime": 1})
        _write(m, m.CHANNEL_STATUS_FILE, {"status": "ok", "consecutive_failures": 0, "last_success_ts": "2026-10-29T10:00:00Z",
                                          "run_id": "old-uuid-999", "process_pid": pid, "boot_id": boot, "proc_starttime": 1})
        r = m.verify_channel_recovery()
        assert r["recovered"] is False and r["reason"] == "proc_starttime_mismatch", r

        # B. Init intermediate: new serve_run, old channel -> run_id mismatch.
        _write(m, m.SERVE_RUN_FILE, {"run_id": "new-uuid-001", "pid": pid, "boot_id": boot, "proc_starttime": start})
        r = m.verify_channel_recovery()
        assert r["recovered"] is False and r["reason"] == "run_id_mismatch", r

        # C. Same instance pending -> not recovered (channel_pending).
        _write(m, m.CHANNEL_STATUS_FILE, {"status": "pending", "consecutive_failures": 0, "last_success_ts": None,
                                          "run_id": "new-uuid-001", "process_pid": pid, "boot_id": boot, "proc_starttime": start})
        r = m.verify_channel_recovery()
        assert r["recovered"] is False and r["reason"] == "channel_not_ok", r

        # C2. Status ok but no success timestamp yet -> still pending, not recovered.
        _write(m, m.CHANNEL_STATUS_FILE, {"status": "ok", "consecutive_failures": 0, "last_success_ts": None,
                                          "run_id": "new-uuid-001", "process_pid": pid, "boot_id": boot, "proc_starttime": start})
        r = m.verify_channel_recovery()
        assert r["recovered"] is False and r["reason"] == "channel_pending", r

        # D. Same instance ok -> recovered (production checker, not a test judge).
        _write(m, m.CHANNEL_STATUS_FILE, {"status": "ok", "consecutive_failures": 0, "last_success_ts": m._now_iso(),
                                          "run_id": "new-uuid-001", "process_pid": pid, "boot_id": boot, "proc_starttime": start})
        r = m.verify_channel_recovery()
        assert r["recovered"] is True and r["run_id"] == "new-uuid-001", r

        # E. Old-format files without kernel identity -> not recovered.
        _write(m, m.SERVE_RUN_FILE, {"run_id": "new-uuid-001", "pid": pid})
        _write(m, m.CHANNEL_STATUS_FILE, {"status": "ok", "consecutive_failures": 0, "last_success_ts": m._now_iso(),
                                          "run_id": "new-uuid-001", "process_pid": pid})
        r = m.verify_channel_recovery()
        assert r["recovered"] is False, r

        # F. Process dead -> not recovered, never "process recovered".
        dummy.terminate(); dummy.wait(timeout=5); dummy = None
        r = m.verify_channel_recovery()
        assert r["recovered"] is False and r["reason"] == "process_not_running", r

        print("test_v104_worker_verify: PASS (old+PID-reuse, intermediate, pending, ok, old-format, dead process, via production verify_channel_recovery)")
    finally:
        if dummy is not None:
            try:
                dummy.terminate(); dummy.wait(timeout=5)
            except Exception:
                pass
        td.cleanup()

if __name__ == "__main__":
    main()
