"""Regression tests for R1-R8 fixes. Offline, synthetic data only, no WeChat.
Run: python3 tests/test_regression.py /path/to/bridge.py
Each test asserts CORRECT behaviour (not the old defect reproduction).
"""
import importlib.util, io, json, subprocess, sys, tempfile, contextlib
from pathlib import Path
from unittest.mock import patch
import socket

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}

def load(source):
    spec = importlib.util.spec_from_file_location("bridge_fix", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-fix-")
    old = m.ROOT
    for name, value in list(vars(m).items()):
        if isinstance(value, str) and (value == old or value.startswith(old + "/")):
            setattr(m, name, td.name + value[len(old):])
    m.ensure_dirs(); m.save_session(dict(SESSION))
    return td

def test_r1_no_second_post(m, td):
    attempts = []
    def first(url, headers, data, timeout):
        attempts.append("curl"); raise m.ApiError("lost response")
    def second(req, **kwargs):
        attempts.append("urllib"); return None
    with patch.object(m, "_post_via_curl", first), patch.object(m.urllib.request, "urlopen", second):
        try:
            m.send_text(SESSION, "hi", client_id="X")
            assert False, "should raise UnknownDeliveryError"
        except m.UnknownDeliveryError:
            pass
    assert attempts == ["curl"], attempts
    print("R1 final send: only 1 POST, UnknownDeliveryError - PASS")

def test_r1_direct_unknown_not_pending(m, td):
    # Gemini R8 update: send_direct_or_outbox is now queue-only, so
    # Unknown handling is tested at drain_outbox level, not direct.
    sent=[]
    def sender(*args, **kwargs):
        sent.append(1)
        if len(sent)==1: raise m.UnknownDeliveryError("unknown")
        return {"ret":0}
    # Queue a job, then drain with Unknown send -> needs_check, no resend
    m.atomic_write_json(str(Path(m.OUTBOX_PENDING)/"notice.json"),
                        {"text":"notice","to_user_id":SESSION["ilink_user_id"],"context_token":SESSION["context_token"],"client_id":"C9"}, mode=0o600)
    with patch.object(m, "send_text", sender):
        m.drain_outbox(SESSION)
        assert not (Path(m.OUTBOX_PENDING)/"notice.json").exists()
        assert (Path(m.OUTBOX_FAILED)/"notice.json").exists()
        # Second drain must not resend the failed job
        m.drain_outbox(SESSION)
    assert len(sent)==1, sent
    print("R1 drain unknown: goes to failed/needs_check, not pending, no resend - PASS")

def test_r3_no_key_in_error(m, td):
    key = bytes.fromhex("12"*16)
    def fail(cmd, **kwargs): raise subprocess.TimeoutExpired(cmd, 30)
    with patch.object(m.subprocess, "run", fail):
        try:
            m.aes_ecb_decrypt(b"0"*16, key)
        except Exception as e:
            s = m._sanitize_error(e)
            assert key.hex() not in s, s
            # v1.0.1: aes_ecb_* now converts TimeoutExpired to a key-free
            # ApiError ("openssl decrypt timeout") instead of letting the
            # raw TimeoutExpired (argv incl. -K) escape; _sanitize_error
            # on a raw TimeoutExpired still returns "TimeoutExpired".
            assert "timeout" in s.lower(), s
    print("R3 TimeoutExpired sanitized, no key - PASS")

def test_r4_tmp_permissions(m, td):
    import os
    # atomic_write_json should create tmp with 0600 from creation; check final and tmp pattern
    p = Path(m.STATE)/"test.json"
    m.atomic_write_json(str(p), {"a":1})
    assert oct(p.stat().st_mode & 0o777) == "0o600"
    print("R4 atomic write final 0600 - PASS (tmp creation mode is in _open_private)")

def test_r6_login_clears_flag(m, td):
    from types import SimpleNamespace
    Path(m.NEEDS_RELOGIN_FILE).write_text("old")
    m.atomic_write_json(m.LOGIN_PENDING_FILE, {"qrcode":"Q","baseurl":SESSION["baseurl"]})
    resp={"status":"confirmed", **SESSION}
    with patch.object(m, "get_json", return_value=resp):
        rc=m.cmd_login(SimpleNamespace(resume=True, timeout=10, verify_code=None))
    assert rc==0 and not Path(m.NEEDS_RELOGIN_FILE).exists()
    print("R6 login clears NEEDS_RELOGIN - PASS")

def test_r2_cursor_not_advanced_on_crash(m, td):
    batch={"ret":0, "get_updates_buf":"AFTER_BATCH", "msgs":[
        {"message_id":"101","from_user_id":SESSION["ilink_user_id"],"context_token":"C","item_list":[{"type":2,"image_item":{"media":{"full_url":"https://example.invalid/a"}}}]},
        {"message_id":"102","from_user_id":SESSION["ilink_user_id"],"item_list":[{"type":1,"text_item":{"text":"second"}}]},
    ]}
    with patch.object(m, "post_json", return_value=batch), patch.object(m, "process_inbound_image", side_effect=SystemExit("crash")):
        try: m.serve_loop()
        except SystemExit: pass
    assert m.load_cursor() != "AFTER_BATCH", m.load_cursor()
    assert m.load_processed() == set()
    print("R2 crash mid-batch: cursor NOT advanced, processed empty - PASS")

def main():
    source=sys.argv[1]
    m=load(source)
    td=isolated(m)
    # R8 is structural: sender thread exists and serve_loop starts it (checked by code inspection + import)
    assert hasattr(m, "_sender_loop")
    print("R8 _sender_loop exists - PASS (full thread test requires serve_loop mock, see code)")
    test_r1_no_second_post(m, td)
    test_r1_direct_unknown_not_pending(m, td)
    test_r3_no_key_in_error(m, td)
    test_r4_tmp_permissions(m, td)
    test_r6_login_clears_flag(m, td)
    # R2 test needs fresh isolated state (processed/cursor from previous tests would pollute) - use new temp dir
    td2=isolated(m)
    test_r2_cursor_not_advanced_on_crash(m, td2)
    print("ALL REGRESSION TESTS PASSED")

if __name__=="__main__":
    main()
