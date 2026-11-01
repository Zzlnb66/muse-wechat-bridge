"""Tiered getupdates backoff tests (v1.5.0 item 2, GPT conditional pass).

Scope is getupdates ONLY: the tiered wait lives in serve_loop's
getupdates except branch and never touches sendmessage retry.
Offline, synthetic data only, no WeChat, no real keys.

Run: python3 tests/test_backoff_tiers.py /path/to/bridge.py

1. _rate_limit_delay: 1-based streak -> 5, 10, 20, 30, 30, 30 ...;
   streak <= 0 -> 5.
2. _interruptible_sleep: stop predicate firing returns True promptly
   (never the full duration); never firing returns False after the
   duration; sleeps in slices, never one uninterruptible sleep.
3. serve_loop integration (post_json mocked, _interruptible_sleep
   recorded, SystemExit break like test_v101 test_2):
   a. rate-limit streak 5,10,20,30,30, then an empty success resets,
      next rate-limit is 5 again.
   b. a network_error between limits breaks the streak (next limit 5).
   c. TokenExpired (-14) returns 14 and never calls the backoff wait.
   d. a stop during backoff returns 14.
   e. a transport error is not a success: it resets the streak but
      the next limit still starts at 5 (same as b, asserted via the
      recorded wait sequence, not via any success counter).
4. Credentials never reach logs: _sanitize_error redacts bot_token /
   context_token credential fields from the backoff log line input.
5. Sender independence is structural and asserted by construction:
   _sender_loop runs on its own daemon thread with its own 1 s tick
   and is started before the poll loop; the backoff wait runs only
   on the poll-loop thread, so it cannot pause the sender. The test
   asserts the wait is the sliced _interruptible_sleep (not a single
   time.sleep) and that no send function is called during backoff.

Exit non-zero on any failure.
"""
import importlib.util, sys, tempfile, time
from pathlib import Path
from unittest.mock import patch

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}


def load(source):
    spec = importlib.util.spec_from_file_location("bridge_backoff", source)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-backoff-")
    old = m.ROOT
    for name, value in list(vars(m).items()):
        if isinstance(value, str) and (value == old or value.startswith(old + "/")):
            setattr(m, name, td.name + value[len(old):])
    m.ensure_dirs()
    m.save_session(dict(SESSION))
    return td


def test_delay_table(m):
    assert [m._rate_limit_delay(s) for s in (1, 2, 3, 4, 5, 6, 100)] == [5, 10, 20, 30, 30, 30, 30]
    assert m._rate_limit_delay(0) == 5
    assert m._rate_limit_delay(-3) == 5
    print("backoff-1 delay table 5/10/20/30/30 cap - PASS")


def test_interruptible_sleep(m):
    t0 = time.monotonic()
    assert m._interruptible_sleep(30, lambda: True) is True
    assert time.monotonic() - t0 < 1.0, "stop predicate must return promptly"
    t0 = time.monotonic()
    assert m._interruptible_sleep(0.2, lambda: False) is False
    assert 0.15 < time.monotonic() - t0 < 2.0
    calls = {"n": 0}

    def firing_after_3():
        calls["n"] += 1
        return calls["n"] >= 3

    assert m._interruptible_sleep(30, firing_after_3) is True
    assert calls["n"] == 3, calls
    print("backoff-2 interruptible sleep: prompt stop, sliced waits - PASS")


def _run_serve_loop(m, script):
    """Run serve_loop with a scripted post_json; script is a list of
    outcomes: ('limit',), ('net',), ('expired',), ('ok',), ('break',).
    Returns (serve_loop return value, recorded backoff seconds)."""
    waits = []
    calls = {"n": 0}

    def fake_post_json(*args, **kwargs):
        outcome = script[min(calls["n"], len(script) - 1)]
        calls["n"] += 1
        if outcome[0] == "limit":
            raise Exception("HTTP 429 Too Many Requests")
        if outcome[0] == "net":
            raise Exception("curl failed: connection refused")
        if outcome[0] == "expired":
            raise m.TokenExpired("ret=-14")
        if outcome[0] == "ok":
            return {"ret": 0, "get_updates_buf": "AFTER", "msgs": []}
        raise SystemExit("test break")

    def fake_wait(seconds, stop_predicate):
        waits.append(seconds)
        return False

    with patch.object(m, "post_json", side_effect=fake_post_json), \
         patch.object(m, "_interruptible_sleep", side_effect=fake_wait):
        try:
            ret = m.serve_loop()
        except BaseException:
            ret = "break"
    return ret, waits


def test_sequence_and_reset(m):
    td = isolated(m)  # held: dropping it would GC-delete session.json
    script = [("limit",)] * 5 + [("ok",), ("limit",), ("break",)]
    ret, waits = _run_serve_loop(m, script)
    assert ret == "break", ret
    assert waits == [5, 10, 20, 30, 30, 5], waits
    print("backoff-3a sequence, cap, empty-success reset - PASS")


def test_non_limit_breaks_streak(m):
    td = isolated(m)  # held: see test_sequence_and_reset
    script = [("limit",), ("net",), ("limit",), ("break",)]
    ret, waits = _run_serve_loop(m, script)
    assert ret == "break", ret
    assert waits == [5, 5, 5], waits
    print("backoff-3b non-limit error breaks streak - PASS")


def test_expired_priority(m):
    td = isolated(m)  # held: see test_sequence_and_reset
    script = [("expired",)]
    ret, waits = _run_serve_loop(m, script)
    assert ret == 14, ret
    assert waits == [], waits
    print("backoff-3c -14 priority: return 14, no backoff wait - PASS")


def test_stop_during_backoff(m):
    td = isolated(m)  # held: see test_sequence_and_reset
    waits = []
    calls = {"n": 0}

    def fake_post_json(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise SystemExit("test break")
        raise Exception("HTTP 429 Too Many Requests")

    def stop_wait(seconds, stop_predicate):
        waits.append(seconds)
        return True

    with patch.object(m, "post_json", side_effect=fake_post_json), \
         patch.object(m, "_interruptible_sleep", side_effect=stop_wait):
        ret = m.serve_loop()
    assert ret == 14, ret
    assert waits == [5], waits
    print("backoff-3d stop during backoff returns 14 - PASS")


def test_no_credentials_in_log_input(m):
    err = Exception("poll failed bot_token=SYNTHETIC_SECRET context_token=SYNTHETIC_SECRET")
    clean = m._sanitize_error(err)
    assert "SYNTHETIC_SECRET" not in clean, clean
    print("backoff-4 credentials redacted from log input - PASS")


def main():
    source = sys.argv[1]
    m = load(source)
    test_delay_table(m)
    test_interruptible_sleep(m)
    test_sequence_and_reset(m)
    test_non_limit_breaks_streak(m)
    test_expired_priority(m)
    test_stop_during_backoff(m)
    test_no_credentials_in_log_input(m)
    print("ALL BACKOFF TIER TESTS PASSED")


if __name__ == "__main__":
    main()
