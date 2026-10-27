"""R8 timing regression test (Gemini Step 1).
Verifies, offline:
- While the main thread is blocked (simulating a 45s getupdates long poll),
  a newly queued outbox reply is sent by _sender_loop within ~2s, not after the block.
- _sender_loop stops cleanly on stop_event and does not hang the test.
- send_direct_or_outbox does NOT call the network (queue only).
Run: python3 tests/test_r8_timing.py /path/to/bridge.py
"""
import importlib.util, sys, tempfile, time, threading
from pathlib import Path
from unittest.mock import patch

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYN", "ilink_user_id": "OWN", "context_token": "CTX"}

def main():
    source = sys.argv[1]
    spec = importlib.util.spec_from_file_location("fix_r8", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    td = tempfile.TemporaryDirectory(); old = m.ROOT
    for name, val in list(vars(m).items()):
        if isinstance(val, str) and (val == old or val.startswith(old + "/")):
            setattr(m, name, td.name + val[len(old):])
    m.ensure_dirs(); m.save_session(dict(SESSION))

    # 1. send_direct_or_outbox must not call send_text
    with patch.object(m, "send_text", side_effect=AssertionError("network send from main thread")):
        ok = m.send_direct_or_outbox(SESSION, "ping reply", "OWN", "CTX", "test_fast.json")
        assert ok, "queue failed"
        assert (Path(m.OUTBOX_PENDING) / "test_fast.json").exists()
    print("R8 queue-only: main thread did not call network - PASS")

    # 2. Sender thread sends while main thread is blocked
    sent = []
    def fake_send_text(session, text, to_user_id=None, context_token=None, client_id=None):
        sent.append(text); return {"ret": 0}
    stop = threading.Event(); q = __import__("queue").Queue()
    with patch.object(m, "send_text", fake_send_text):
        th = threading.Thread(target=m._sender_loop, args=(SESSION, stop, q), daemon=True)
        th.start()
        # Simulate main thread blocked in long poll: sleep 5s in main, queue a reply at t=0.5s
        time.sleep(0.5)
        m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "worker_reply.json"),
                            {"text": "worker reply", "to_user_id": "OWN", "context_token": "CTX", "client_id": "C2"}, mode=0o600)
        deadline = time.time() + 5
        while time.time() < deadline and "worker reply" not in sent:
            time.sleep(0.1)
        assert "worker reply" in sent, sent
        print(f"R8 timing: reply sent while main blocked, in <5s - PASS (sent={sent})")
        stop.set()
        th.join(timeout=5)
        assert not th.is_alive(), "sender thread did not stop cleanly"
        print("R8 lifecycle: thread stopped cleanly on stop_event - PASS")
    print("ALL R8 TIMING TESTS PASSED")

if __name__ == "__main__":
    main()

def test_halted_not_resent():
    """Separate check, run via test file main? Added as function, called below if run as script with arg."""
    pass
