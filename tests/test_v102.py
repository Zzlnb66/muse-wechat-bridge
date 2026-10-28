"""v1.0.2 regression tests (user review of the v1.0.1 package, 2026-10-28).
Offline, synthetic data only, no WeChat, no real keys.
Run: python3 tests/test_v102.py /path/to/bridge.py

1. Bearer / JSON credential sanitization, at the call site:
   - _sanitize_error redacts 'Authorization: Bearer <token>' and
     JSON "aeskey"/"context_token" fields.
   - _post_via_curl / _get_via_curl timeouts raise a parameter-free
     ApiError, never the argv text.
   - send_image CDN upload timeout raises RetryableError("upload
     curl timeout") with no upload_full_url in the message.
2. Disk terminal recognition after a simulated process restart:
   - unknown_restart jobs, all-terminal jobs, and old-format
     image_path/image_status jobs are NOT resent after
     _TERMINAL_TASK_IDS is cleared (memory set gone, disk remains).
   - Sub-item interception: failed/needs_check text and attachment
     items are skipped, while a still-pending attachment in the
     same job is still sent once.
3. Dual-path errors: post_json/get_json keep both curl_err and
   urllib_err, plus desensitized target and proxy host:port only
   (no proxy password, no token).
"""
import importlib.util, json, os, subprocess, sys, tempfile
from pathlib import Path
from unittest.mock import patch

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}

def load(source):
    spec = importlib.util.spec_from_file_location("bridge_v102", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-v102-")
    old = m.ROOT
    for name, value in list(vars(m).items()):
        if isinstance(value, str) and (value == old or value.startswith(old + "/")):
            setattr(m, name, td.name + value[len(old):])
    m.ensure_dirs(); m.save_session(dict(SESSION))
    return td

def _img(m, name="t.jpg"):
    from PIL import Image
    p = str(Path(m.MEDIA_INBOUND) / name)
    Image.new("RGB", (8, 8), "red").save(p, "JPEG")
    return p

def test_1_sanitization(m, td):
    # 1a. Sanitizer defence in depth.
    raw = Exception("curl POST failed: ['curl', '-H', 'Authorization: Bearer SECRET_TOKEN_XYZ', "
                    "'-d', '{\"aeskey\": \"SECRET_AES_123\", \"context_token\": \"SECRET_CTX_456\"}']")
    s = m._sanitize_error(raw)
    for secret in ("SECRET_TOKEN_XYZ", "SECRET_AES_123", "SECRET_CTX_456"):
        assert secret not in s, s
    assert "Bearer [redacted]" in s, s
    # 1b. Call-site timeouts carry no argv.
    cmd = ["curl", "-s", "-H", "Authorization: Bearer SECRET_TOKEN_XYZ",
           "-d", '{"context_token": "SECRET_CTX_456"}', "https://example.invalid/x"]
    def timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd, 30)
    with patch.object(m.subprocess, "run", timeout_run):
        try:
            m._post_via_curl("https://example.invalid/x", {"Authorization": "Bearer SECRET_TOKEN_XYZ"}, b"{}", 5)
            assert False, "should raise"
        except Exception as e:
            assert str(e) == "curl POST timeout", str(e)
            assert "SECRET_TOKEN_XYZ" not in str(e) and "SECRET_CTX_456" not in str(e)
        try:
            m._get_via_curl("https://example.invalid/x", {}, 5)
            assert False, "should raise"
        except Exception as e:
            assert str(e) == "curl GET timeout", str(e)
    # 1c. send_image upload timeout -> RetryableError, no URL.
    img = _img(m, "up.jpg")
    orig_run = m.subprocess.run
    def selective_run(cmd, **kwargs):
        if cmd and cmd[0] == "curl":
            raise subprocess.TimeoutExpired(cmd, 40)
        return orig_run(cmd, **kwargs)
    fake_url = "https://cdn.example.invalid/upload?secret=SECRET_URL_PARAM"
    with patch.object(m, "aes_ecb_encrypt", return_value=b"ciphertext1234"), \
         patch.object(m, "post_json", return_value={"data": {"upload_full_url": fake_url}}), \
         patch.object(m.subprocess, "run", selective_run):
        try:
            m.send_image(SESSION, img, SESSION["ilink_user_id"], SESSION["context_token"])
            assert False, "should raise"
        except Exception as e:
            assert isinstance(e, m.RetryableError), f"{type(e)}: {e}"
            assert str(e) == "upload curl timeout", str(e)
            assert "SECRET_URL_PARAM" not in str(e) and "cdn.example.invalid" not in str(e)
    print("v1.0.2-1 Bearer/upload sanitization: sanitizer, curl timeouts, upload timeout - PASS")

def test_2_terminal_after_restart(m, td):
    # 2a. unknown_restart, no terminal flag on disk (pre-v1.0.2 recovery
    # shape), move to failed/ fails, then memory set cleared = new process.
    job = {"text": "hi", "text_status": "pending", "delivery_state": "unknown_restart",
           "to_user_id": SESSION["ilink_user_id"], "context_token": SESSION["context_token"],
           "client_id": "C201"}
    m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "job201.json"), job, mode=0o600)
    sent = []
    def sender(*args, **kwargs):
        sent.append(1); raise m.PermanentError("permanent (synthetic)")
    orig_replace = m.os.replace
    def failing_replace(src, dst):
        if "job201" in str(src) and "failed" in str(dst):
            raise OSError("move failed (synthetic)")
        return orig_replace(src, dst)
    with patch.object(m, "send_text", sender), patch.object(m.os, "replace", side_effect=failing_replace):
        m.drain_outbox(SESSION)
        assert sent == [], f"unknown_restart job was sent! {sent}"
        assert "job201.json" in m._TERMINAL_TASK_IDS
        # Simulate a process restart: memory set gone, disk unchanged.
        m._TERMINAL_TASK_IDS.clear()
        m.drain_outbox(SESSION)
        assert sent == [], f"unknown_restart job was sent after restart! {sent}"
        assert "job201.json" in m._TERMINAL_TASK_IDS
    print("v1.0.2-2a unknown_restart: no send before or after memory clear - PASS")
    # 2b. All-terminal job (text failed + attachment needs_check), same
    # move-failure + memory-clear simulation.
    img = _img(m, "term.jpg")
    job2 = {"text": "hi", "text_status": "failed", "delivery_state": "failed",
            "to_user_id": SESSION["ilink_user_id"], "context_token": SESSION["context_token"],
            "client_id": "C202",
            "attachments": [{"path": img, "type": "image", "status": "needs_check", "client_id": "A202"}]}
    m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "job202.json"), job2, mode=0o600)
    sent2, sent_img = [], []
    def failing_replace2(src, dst):
        if "job202" in str(src) and "failed" in str(dst):
            raise OSError("move failed (synthetic)")
        return orig_replace(src, dst)
    with patch.object(m, "send_text", lambda *a, **k: sent2.append(1)), \
         patch.object(m, "send_image", lambda *a, **k: sent_img.append(1)), \
         patch.object(m.os, "replace", side_effect=failing_replace2):
        m.drain_outbox(SESSION)
        assert sent2 == [] and sent_img == [], (sent2, sent_img)
        m._TERMINAL_TASK_IDS.clear()
        m.drain_outbox(SESSION)
        assert sent2 == [] and sent_img == [], (sent2, sent_img)
    print("v1.0.2-2b all-terminal job: no send before or after memory clear - PASS")
    # 2c. Old-format single image: image_path + image_status needs_check.
    img2 = _img(m, "old.jpg")
    job3 = {"image_path": img2, "image_status": "needs_check",
            "to_user_id": SESSION["ilink_user_id"], "context_token": SESSION["context_token"]}
    m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "job203.json"), job3, mode=0o600)
    sent_img3 = []
    with patch.object(m, "send_image", lambda *a, **k: sent_img3.append(1)):
        m.drain_outbox(SESSION)
        assert sent_img3 == [], sent_img3
        m._TERMINAL_TASK_IDS.clear()
        # After the first drain the job was archived to failed/ (replace
        # succeeded here), so the second drain has nothing to send.
        # To prove disk recognition, re-create the same old-format job
        # in pending/ and drain again with memory cleared.
        m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "job203.json"), job3, mode=0o600)
        m._TERMINAL_TASK_IDS.clear()
        m.drain_outbox(SESSION)
        assert sent_img3 == [], sent_img3
    print("v1.0.2-2c old-format image_path needs_check: no send - PASS")
    # 2d. Sub-item interception: failed text + needs_check attachment
    # must NOT be sent, but a pending attachment in the same job IS sent.
    img3, img4 = _img(m, "bad.jpg"), _img(m, "good.jpg")
    job4 = {"text": "hi", "text_status": "failed", "delivery_state": "failed",
            "to_user_id": SESSION["ilink_user_id"], "context_token": SESSION["context_token"],
            "client_id": "C204",
            "attachments": [{"path": img3, "type": "image", "status": "needs_check", "client_id": "A204a"},
                            {"path": img4, "type": "image", "status": "pending", "client_id": "A204b"}]}
    m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "job204.json"), job4, mode=0o600)
    sent_text, sent_paths = [], []
    def fake_send_image(session, path, *a, **k):
        sent_paths.append(path)
        return {"ret": 0}
    with patch.object(m, "send_text", lambda *a, **k: sent_text.append(1)), \
         patch.object(m, "send_image", fake_send_image):
        m.drain_outbox(SESSION)
    assert sent_text == [], sent_text
    assert sent_paths == [img4], f"only the pending attachment should be sent, got {sent_paths}"
    print("v1.0.2-2d sub-item interception: failed items skipped, pending item sent once - PASS")

def test_3_dual_path_errors(m, td):
    import urllib.error
    def curl_fail(*args, **kwargs):
        raise m.ApiError("curl POST timeout")
    def urlopen_fail(*args, **kwargs):
        raise urllib.error.URLError("Connection refused (synthetic)")
    env = {"https_proxy": "http://user:SECRET_PROXY_PASS@127.0.0.1:7897"}
    with patch.object(m, "_post_via_curl", curl_fail), \
         patch.object(m.urllib.request, "urlopen", urlopen_fail), \
         patch.dict(os.environ, env, clear=False):
        try:
            m.post_json("https://example.invalid", "/ilink/bot/getupdates", {}, session=SESSION, authenticated=True)
            assert False, "should raise"
        except Exception as e:
            msg = str(e)
            assert "curl" in msg and "urllib" in msg, msg
            assert "example.invalid:443" in msg, msg
            assert "127.0.0.1:7897" in msg, msg
            assert "SECRET_PROXY_PASS" not in msg and "SECRET_TOKEN" not in msg, msg
    def curl_get_fail(*args, **kwargs):
        raise m.ApiError("curl GET timeout")
    with patch.object(m, "_get_via_curl", curl_get_fail), \
         patch.object(m.urllib.request, "urlopen", urlopen_fail), \
         patch.dict(os.environ, env, clear=False):
        try:
            m.get_json("https://example.invalid", "/x")
            assert False, "should raise"
        except Exception as e:
            msg = str(e)
            assert "curl" in msg and "urllib" in msg, msg
            assert "SECRET_PROXY_PASS" not in msg, msg
    print("v1.0.2-3 dual-path errors: curl+urllib, desensitized target/proxy - PASS")

def test_4_retryable_not_lost_by_failed_sibling(m, td):
    # Boundary found in user review of v1.0.2: attachment1 Retryable
    # (pending, attempts=1) + attachment2 Permanent (failed) must NOT
    # archive the whole job to failed/ on the first drain. The job
    # must stay in pending/, and the next drain must retry only the
    # pending attachment (failed one skipped), then archive.
    img1, img2 = _img(m, "retry.jpg"), _img(m, "perm.jpg")
    job = {"to_user_id": SESSION["ilink_user_id"], "context_token": SESSION["context_token"],
           "attachments": [{"path": img1, "type": "image", "status": "pending", "client_id": "A301"},
                           {"path": img2, "type": "image", "status": "pending", "client_id": "A302"}]}
    m.atomic_write_json(str(Path(m.OUTBOX_PENDING) / "job301.json"), job, mode=0o600)
    def first_send(session, path, *a, **k):
        if path == img1:
            raise m.RetryableError("transient (synthetic)")
        raise m.PermanentError("permanent (synthetic)")
    with patch.object(m, "send_image", first_send):
        m.drain_outbox(SESSION)
    pending = Path(m.OUTBOX_PENDING) / "job301.json"
    failed = Path(m.OUTBOX_FAILED) / "job301.json"
    assert pending.exists() and not failed.exists(), "job must stay in pending, not be archived to failed/"
    data = json.loads(pending.read_text())
    assert data.get("terminal") is not True, "terminal flag must not be set while a sub-item is pending"
    atts = {a["path"]: a["status"] for a in data["attachments"]}
    assert atts[img1] == "pending" and atts[img2] == "failed", atts
    # Second drain: pending attachment succeeds, failed one skipped.
    sent = []
    def second_send(session, path, *a, **k):
        sent.append(path); return {"ret": 0}
    with patch.object(m, "send_image", second_send):
        m.drain_outbox(SESSION)
    assert sent == [img1], sent
    assert failed.exists() and not pending.exists(), "job must archive to failed/ only after all sub-items are terminal"
    print("v1.0.2-4 retryable sibling: pending item keeps its retries, failed sibling skipped - PASS")

def main():
    source = sys.argv[1]
    m = load(source)
    td = isolated(m)
    test_1_sanitization(m, td)
    td2 = isolated(m)
    test_2_terminal_after_restart(m, td2)
    td3 = isolated(m)
    test_3_dual_path_errors(m, td3)
    td4 = isolated(m)
    test_4_retryable_not_lost_by_failed_sibling(m, td4)
    print("ALL v1.0.2 TESTS PASSED")

if __name__ == "__main__":
    main()
