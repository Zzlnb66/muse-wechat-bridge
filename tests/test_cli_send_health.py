"""Offline tests for Gate 3 CLI entries: send-video / send-file /
health / refresh-qr.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network, and never touching the real running
bridge's state/, inbox/ or outbox/:

send-video / send-file (direct send through the EXISTING send_video /
send_file functions, exactly like text `send` uses send_text; NOT a
queue job, NOT a new send path):
  1. Fake send_video/send_file are called with (session, path, to,
     context_token, client_id) and the CLI returns 0 + "ok".
  2. send-file passes --file-name through; without it the function
     still gets the call (basename defaulting lives in send_file).
  3. --caption is sent only AFTER a successful media send, via the
     existing send_text, as a separate call; media failure sends no
     caption.
  4. Missing file / outside-allowed-dirs path / sparse over-100MiB
     file all refuse with exit 1 and never call the send function.
  5. No session -> exit 3, no send call.
  6. NEEDS_RELOGIN present -> exit 14, no send call.
  7. TokenExpired from the send function -> exit 14.
  8. UnknownDeliveryError -> exit 2 (unknown stays unknown, and the
     CLI never retries).
  9. PermanentError -> exit 1.
 10. Old CLI behaviour unchanged: `send` still dispatches to send_text
     (monkeypatched) and `observability-summary` still prints counts
     only.

health (read-only JSON, desensitised):
 11. Output has the fixed schema and only booleans / counts / fixed
     short strings; no planted token, URL, user id or message text
     appears anywhere in it.
 12. With a fake live serve (serve.pid + matching /proc cmdline is
     faked via a monkeypatched _serve_alive), a fresh heartbeat, a
     recovered channel and queued jobs, health reports serve_alive,
     worker_online, channel_recovered and the queue counts.
 13. With no serve.pid / no heartbeat / no channel files, health
     reports all-false / zero counts and still exits 0.
 14. health never writes: the state dir file list is unchanged by a
     health call, and it never restarts serve or touches the queue.

refresh-qr (resume a pending QR code, nothing more):
 15. No pending QR -> exit 1, session.json untouched, no new pending
     file created, and cmd_login never called.
 16. Pending QR + fake confirmed poll -> cmd_login's normal path
     runs: new session saved, NEEDS_RELOGIN cleared, pending file
     removed, exit 0.
 17. Pending QR + fake expired poll -> exit 1, session.json
     untouched (no new session on an unconfirmed outcome).

Run: python3 tests/test_cli_send_health.py (exit 0 = all pass).
"""

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

PASS = 0
FAIL = 0
FAILURES = []


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS: %s" % name)
    else:
        FAIL += 1
        FAILURES.append(name)
        print("FAIL: %s" % name)


def _setup():
    tmp = tempfile.mkdtemp(prefix="cli-send-health-test-")
    old = {}
    for n in ("STATE", "INBOX", "RUNNING", "DONE", "OUTBOX_PENDING",
              "OUTBOX_SENT", "OUTBOX_FAILED", "MEDIA_INBOUND",
              "MEDIA_OUTBOUND", "FAILURES_DIR", "ANALYSIS_DIR",
              "LOCAL_VAULT_DIR"):
        old[n] = getattr(bridge, n)
        d = os.path.join(tmp, n.lower())
        os.makedirs(d, exist_ok=True)
        setattr(bridge, n, d)
    # Module-level file constants bound at import time must follow STATE.
    for n in ("NEEDS_RELOGIN_FILE", "CHANNEL_STATUS_FILE",
              "SERVE_RUN_FILE", "LOGIN_PENDING_FILE", "SESSION_FILE"):
        old[n] = getattr(bridge, n)
        setattr(bridge, n, os.path.join(bridge.STATE, os.path.basename(old[n])))
    return tmp, old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _teardown(tmp):
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)


def _ns(**kw):
    d = {"path": None, "to": None, "context_token": None,
         "client_id": None, "caption": None, "file_name": None,
         "verify_code": None, "timeout": 180}
    d.update(kw)
    return argparse.Namespace(**d)


def _write_session():
    with open(bridge.SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump({"baseurl": "https://example.invalid", "bot_token": "fake",
                   "context_token": "fake", "ilink_user_id": "fake-user"}, f)


def _media_file(name="clip.mp4", content=b"fake-media-bytes"):
    p = os.path.join(bridge.MEDIA_OUTBOUND, name)
    with open(p, "wb") as f:
        f.write(content)
    return p


def _capture(fn):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fn()
    return rc, buf.getvalue()


class _FakeSend:
    """Records calls; behaviour set per test via .mode."""

    def __init__(self):
        self.calls = []
        self.mode = "ok"

    def __call__(self, session, path, to, context_token, client_id=None, **kw):
        self.calls.append({"path": path, "to": to,
                           "context_token": context_token,
                           "client_id": client_id, "kw": kw})
        if self.mode == "token":
            raise bridge.TokenExpired("fake expired")
        if self.mode == "unknown":
            raise bridge.UnknownDeliveryError("fake unknown")
        if self.mode == "permanent":
            raise bridge.PermanentError("fake rejected")
        return {"message_id": "fake-mid-1"}


def test_send_media_success_and_caption():
    fake = _FakeSend()
    old_video, old_text = bridge.send_video, bridge.send_text
    bridge.send_video, bridge.send_text = fake, _FakeSend()
    text_calls = []
    real_text_fake = bridge.send_text

    def _text(session, text, to_user_id=None, context_token=None, client_id=None):
        text_calls.append(text)
        return {"message_id": "fake-text-mid"}

    bridge.send_text = _text
    try:
        _write_session()
        p = _media_file()
        rc, out = _capture(lambda: bridge.cmd_send_media(
            _ns(path=p, to="fake-user", caption="hello caption"), "video"))
        check("send-video: fake send called once with path/to/client",
              len(fake.calls) == 1 and fake.calls[0]["path"] == p
              and fake.calls[0]["to"] == "fake-user")
        check("send-video: exit 0 and ok printed",
              rc == 0 and "send-video: ok" in out and "caption sent" in out)
        check("send-video: caption sent as separate text call after media",
              text_calls == ["hello caption"])
        # Caption must NOT be sent when the media send fails.
        fake.mode = "permanent"
        text_calls.clear()
        rc, out = _capture(lambda: bridge.cmd_send_media(_ns(path=p, caption="nope"), "video"))
        check("send-video: media failure -> exit 1, caption never sent",
              rc == 1 and text_calls == [])
    finally:
        bridge.send_video, bridge.send_text = old_video, old_text


def test_send_file_name_and_errors():
    fake = _FakeSend()
    old_file = bridge.send_file
    bridge.send_file = fake
    try:
        _write_session()
        p = _media_file("doc.bin", b"fake-file-bytes")
        rc, _ = _capture(lambda: bridge.cmd_send_media(
            _ns(path=p, file_name="shown-name.bin"), "file"))
        check("send-file: --file-name passed through",
              rc == 0 and fake.calls and fake.calls[-1]["kw"].get("file_name") == "shown-name.bin")
        # Missing file.
        before = len(fake.calls)
        rc, out = _capture(lambda: bridge.cmd_send_media(
            _ns(path=os.path.join(bridge.MEDIA_OUTBOUND, "nope.bin")), "file"))
        check("send-file: missing file -> exit 1, no send call",
              rc == 1 and len(fake.calls) == before and "file missing" in out)
        # Outside allowed dirs.
        outside = os.path.join(tempfile.gettempdir(), "cli-test-outside.bin")
        with open(outside, "wb") as f:
            f.write(b"x")
        try:
            rc, out = _capture(lambda: bridge.cmd_send_media(_ns(path=outside), "file"))
            check("send-file: outside allowed dirs -> exit 1, no send call",
                  rc == 1 and len(fake.calls) == before and "allowed media dirs" in out)
        finally:
            try:
                os.remove(outside)
            except OSError:
                pass
        # Sparse over-100MiB file: no real disk use.
        big = os.path.join(bridge.MEDIA_OUTBOUND, "big.bin")
        with open(big, "wb") as f:
            f.truncate(bridge.MAX_VIDEO_PLAIN_BYTES + 1)
        try:
            rc, out = _capture(lambda: bridge.cmd_send_media(_ns(path=big), "file"))
            check("send-file: over 100MiB -> exit 1, no send call",
                  rc == 1 and len(fake.calls) == before and "100MiB" in out)
        finally:
            try:
                os.remove(big)
            except OSError:
                pass
        # Error mapping.
        for mode, want in (("token", 14), ("unknown", 2), ("permanent", 1)):
            fake.mode = mode
            rc, _ = _capture(lambda: bridge.cmd_send_media(_ns(path=p), "file"))
            check("send-file: mode %s -> exit %d" % (mode, want), rc == want)
        fake.mode = "ok"
    finally:
        bridge.send_file = old_file


def test_send_media_session_gates():
    fake = _FakeSend()
    old_video = bridge.send_video
    bridge.send_video = fake
    try:
        # No session.
        if os.path.exists(bridge.SESSION_FILE):
            os.remove(bridge.SESSION_FILE)
        p = _media_file()
        rc, out = _capture(lambda: bridge.cmd_send_media(_ns(path=p), "video"))
        check("send-video: no session -> exit 3, no send call",
              rc == 3 and fake.calls == [] and "no session" in out)
        # NEEDS_RELOGIN present.
        _write_session()
        with open(bridge.NEEDS_RELOGIN_FILE, "w", encoding="utf-8") as f:
            f.write("x")
        rc, out = _capture(lambda: bridge.cmd_send_media(_ns(path=p), "video"))
        check("send-video: NEEDS_RELOGIN -> exit 14, no send call",
              rc == 14 and fake.calls == [] and "NEEDS_RELOGIN" in out)
        os.remove(bridge.NEEDS_RELOGIN_FILE)
    finally:
        bridge.send_video = old_video


def test_old_cli_unchanged():
    old_text = bridge.send_text
    calls = []

    def _text(session, text, to_user_id=None, context_token=None, client_id=None):
        calls.append(text)
        return {"message_id": "fake-old-mid"}

    bridge.send_text = _text
    try:
        _write_session()
        rc, out = _capture(lambda: bridge.main(["send", "--text", "old text path"]))
        check("old CLI: send still dispatches to send_text",
              rc == 0 and calls == ["old text path"] and "send: ok" in out)
        rc, out = _capture(lambda: bridge.main(["observability-summary"]))
        doc = json.loads(out)
        check("old CLI: observability-summary still counts only",
              rc == 0 and set(doc.keys()) >= {"pending", "sent", "failed"}
              and "text" not in json.dumps(doc))
    finally:
        bridge.send_text = old_text


def test_health():
    planted = "PLANTED_TOKEN_VALUE_12345"
    # Fake a live serve + fresh heartbeat + recovered channel + jobs.
    old_alive = bridge._serve_alive
    bridge._serve_alive = lambda: (4242, True)
    old_hb = bridge.read_worker_heartbeat
    bridge.read_worker_heartbeat = lambda: (9999999999, "worker")
    old_offline = bridge.is_worker_offline
    bridge.is_worker_offline = lambda: False
    old_verify = bridge.verify_channel_recovery
    bridge.verify_channel_recovery = lambda: {
        "recovered": True, "reason": "ok", "status": "ok",
        "last_success_ts": "2026-11-01T00:00:00+00:00"}
    try:
        with open(os.path.join(bridge.OUTBOX_SENT, "job1.json"), "w") as f:
            json.dump({"text": planted}, f)
        with open(os.path.join(bridge.OUTBOX_PENDING, "job2.json"), "w") as f:
            json.dump({"text": planted}, f)
        before = sorted(os.listdir(bridge.STATE))
        rc, out = _capture(lambda: bridge.cmd_health(argparse.Namespace()))
        doc = json.loads(out)
        check("health: exit 0 with fixed schema",
              rc == 0 and doc.get("schema") == "bridge-health-0.1")
        check("health: live serve / worker / channel reported",
              doc.get("serve_alive") is True and doc.get("serve_pid") == 4242
              and doc.get("worker_online") is True
              and doc.get("channel_recovered") is True)
        check("health: queue counts only (sent 1, pending 1)",
              doc.get("queue", {}).get("sent") == 1
              and doc.get("queue", {}).get("pending") == 1)
        check("health: desensitised (no planted token/text/urls/ids)",
              planted not in out and "example.invalid" not in out
              and "fake-user" not in out)
        after = sorted(os.listdir(bridge.STATE))
        check("health: never writes state",
              before == after)
        # All-absent world: still exit 0, all false/zero.
        bridge._serve_alive = lambda: (None, False)
        bridge.read_worker_heartbeat = lambda: (None, None)
        bridge.is_worker_offline = lambda: True
        bridge.verify_channel_recovery = lambda: {"recovered": False, "reason": "pid_missing"}
        for d in (bridge.OUTBOX_SENT, bridge.OUTBOX_PENDING):
            for n in os.listdir(d):
                os.remove(os.path.join(d, n))
        rc, out = _capture(lambda: bridge.cmd_health(argparse.Namespace()))
        doc = json.loads(out)
        check("health: absent world -> all false/zero, exit 0",
              rc == 0 and doc.get("serve_alive") is False
              and doc.get("worker_online") is False
              and doc.get("channel_recovered") is False
              and doc.get("queue", {}).get("sent") == 0)
    finally:
        bridge._serve_alive = old_alive
        bridge.read_worker_heartbeat = old_hb
        bridge.is_worker_offline = old_offline
        bridge.verify_channel_recovery = old_verify


def test_refresh_qr():
    old_login = bridge.cmd_login
    calls = []

    def _fake_login(ns):
        calls.append(ns)
        return 0

    bridge.cmd_login = _fake_login
    try:
        # No pending QR: refuse, touch nothing.
        _write_session()
        before_session = open(bridge.SESSION_FILE, "rb").read()
        rc, out = _capture(lambda: bridge.cmd_refresh_qr(
            argparse.Namespace(verify_code=None, timeout=180)))
        check("refresh-qr: no pending -> exit 1, session untouched, login not called",
              rc == 1 and calls == []
              and open(bridge.SESSION_FILE, "rb").read() == before_session
              and not os.path.exists(bridge.LOGIN_PENDING_FILE)
              and "no pending QR" in out)
        # Pending QR: delegates to cmd_login's resume path, nothing else.
        with open(bridge.LOGIN_PENDING_FILE, "w", encoding="utf-8") as f:
            json.dump({"qrcode": "fake-qr", "baseurl": "https://example.invalid"}, f)
        rc, _ = _capture(lambda: bridge.cmd_refresh_qr(
            argparse.Namespace(verify_code=None, timeout=180)))
        check("refresh-qr: pending -> delegates to login resume path",
              rc == 0 and len(calls) == 1 and calls[0].resume is True)
    finally:
        bridge.cmd_login = old_login


def test_refresh_qr_confirmed_and_expired():
    # Exercise the REAL cmd_login resume path with a fake poller.
    old_get_json = bridge.get_json
    try:
        _write_session()
        with open(bridge.NEEDS_RELOGIN_FILE, "w", encoding="utf-8") as f:
            f.write("x")
        with open(bridge.LOGIN_PENDING_FILE, "w", encoding="utf-8") as f:
            json.dump({"qrcode": "fake-qr", "baseurl": "https://example.invalid"}, f)

        def _confirmed(base, path, timeout=45):
            return {"status": "confirmed", "bot_token": "fake-new",
                    "ilink_user_id": "fake-new-user",
                    "ilink_bot_id": "fake-bot", "baseurl": "https://example.invalid"}

        bridge.get_json = _confirmed
        rc, out = _capture(lambda: bridge.cmd_refresh_qr(
            argparse.Namespace(verify_code=None, timeout=180)))
        sess = json.load(open(bridge.SESSION_FILE, encoding="utf-8"))
        check("refresh-qr: confirmed -> new session saved, NEEDS_RELOGIN cleared, pending removed",
              rc == 0 and sess.get("ilink_user_id") == "fake-new-user"
              and not os.path.exists(bridge.NEEDS_RELOGIN_FILE)
              and not os.path.exists(bridge.LOGIN_PENDING_FILE))

        def _expired(base, path, timeout=45):
            return {"status": "expired"}

        bridge.get_json = _expired
        _write_session()
        before_session = open(bridge.SESSION_FILE, "rb").read()
        with open(bridge.LOGIN_PENDING_FILE, "w", encoding="utf-8") as f:
            json.dump({"qrcode": "fake-qr", "baseurl": "https://example.invalid"}, f)
        rc, _ = _capture(lambda: bridge.cmd_refresh_qr(
            argparse.Namespace(verify_code=None, timeout=180)))
        check("refresh-qr: expired -> exit 1, session untouched",
              rc == 1 and open(bridge.SESSION_FILE, "rb").read() == before_session)
    finally:
        bridge.get_json = old_get_json


def main():
    tmp, old = _setup()
    try:
        test_send_media_success_and_caption()
        test_send_file_name_and_errors()
        test_send_media_session_gates()
        test_old_cli_unchanged()
        test_health()
        test_refresh_qr()
        test_refresh_qr_confirmed_and_expired()
    finally:
        _restore(old)
        _teardown(tmp)
    print("cli_send_health: %d passed, %d failed" % (PASS, FAIL))
    if FAIL:
        print("failures: %s" % ", ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
