"""v1.0.3 regression tests (user independent review of v1.0.2, 2026-10-29).
Offline, synthetic data only, no WeChat, no real keys.
Run: python3 tests/test_v103.py /path/to/bridge.py

1. urllib error detail: URLError(ConnectionRefusedError, errno 111)
   and URLError(TimeoutError) must be distinguishable in the raised
   ApiError, without the request URL.
2. NO_PROXY bypass: _effective_proxy_host_port(target_url) must
   return "none (no_proxy bypass)" when urllib.request.proxy_bypass
   says the target is bypassed, even when a proxy is configured.
3. Exception-chain cut: the 6 call sites that wrap a subprocess
   TimeoutExpired / generic exception must raise with
   __suppress_context__ is True (from None), and a full
   traceback.format_exception() string must contain no token,
   AES key hex, or upload URL, and no "During handling" chain.
   (aes_ecb_encrypt/decrypt, _post_via_curl, _get_via_curl,
   send_image curl upload, send_image urllib fallback.)
"""
import importlib.util, os, subprocess, sys, tempfile, traceback, urllib.error
from pathlib import Path
from unittest.mock import patch

SESSION = {"baseurl": "https://example.invalid", "bot_token": "SYNTHETIC_BOT_TOKEN",
           "ilink_user_id": "SYNTHETIC_OWNER", "context_token": "SYNTHETIC_CONTEXT"}

def load(source):
    spec = importlib.util.spec_from_file_location("bridge_v103", source)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m

def isolated(m):
    td = tempfile.TemporaryDirectory(prefix="wechat-v103-")
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

def _assert_no_leak(m, e, secrets):
    # Python semantics correction: "raise ... from None" sets
    # __suppress_context__ = True and __cause__ = None, but it does
    # NOT clear __context__ itself. What matters is the printed
    # traceback: with suppression, format_exception must NOT print
    # "During handling of the above exception" nor the original
    # TimeoutExpired argv. Asserting __context__ is None would fail
    # for correct from-None code, so assert suppression + clean output.
    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    assert e.__suppress_context__ is True, f"__suppress_context__ not set: {e.__suppress_context__}"
    assert "During handling of the above exception" not in tb, tb
    for s in secrets:
        assert s not in tb, f"LEAKED {s!r} in traceback:\n{tb}"
        assert s not in str(e), f"LEAKED {s!r} in exception string: {e}"
    return tb

def test_1_urllib_detail_and_no_proxy(m, td):
    # 1a. Refused vs timeout must differ.
    refused = urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
    timeout = urllib.error.URLError(TimeoutError("timed out"))
    d_refused = m._describe_urllib_error(refused)
    d_timeout = m._describe_urllib_error(timeout)
    assert "ConnectionRefusedError" in d_refused and "111" in d_refused, d_refused
    assert "TimeoutError" in d_timeout, d_timeout
    assert d_refused != d_timeout, (d_refused, d_timeout)
    assert "example.invalid" not in d_refused and "example.invalid" not in d_timeout
    # 1b. post_json dual-path error carries the detail, not just "URLError".
    def curl_fail(*a, **k):
        raise m.ApiError("curl POST timeout")
    def urlopen_refused(*a, **k):
        raise urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
    with patch.object(m, "_post_via_curl", curl_fail), \
         patch.object(m.urllib.request, "urlopen", urlopen_refused):
        try:
            m.post_json("https://example.invalid", "/ilink/bot/getupdates", {}, session=SESSION, authenticated=True)
            assert False, "should raise"
        except Exception as e:
            assert "ConnectionRefusedError" in str(e) and "111" in str(e), str(e)
            assert "curl POST timeout" in str(e), str(e)
    # 1c. NO_PROXY bypass beats a configured proxy.
    env = {"https_proxy": "http://user:SECRET_PROXY_PASS@127.0.0.1:7897",
           "no_proxy": "example.invalid"}
    with patch.dict(os.environ, env, clear=False):
        got = m._effective_proxy_host_port("https://example.invalid/x")
        assert got == "none (no_proxy bypass)", got
        got2 = m._effective_proxy_host_port("https://other.example/x")
        assert got2 == "127.0.0.1:7897", got2
        assert "SECRET_PROXY_PASS" not in got2
    # 1d. P2: no proxy configured at all -> "none", not "none (no_proxy bypass)",
    # even when NO_PROXY=* is set.
    env2 = {"no_proxy": "*"}
    cleared = {k: "" for k in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY")}
    with patch.dict(os.environ, {**cleared, **env2}, clear=False):
        # patch.dict with "" does not unset; unset explicitly via a nested patch
        pass
    import os as _os
    saved = {k: _os.environ.pop(k, None) for k in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY")}
    try:
        _os.environ["no_proxy"] = "*"
        assert m._effective_proxy_host_port("https://example.invalid/x") == "none"
    finally:
        _os.environ.pop("no_proxy", None)
        for k, v in saved.items():
            if v is not None:
                _os.environ[k] = v
    print("v1.0.3-1 urllib detail + NO_PROXY bypass - PASS")

def test_2_traceback_cut(m, td):
    key = bytes.fromhex("cd" * 16)
    token = "SECRET_TOKEN_XYZ"
    secret_url = "https://cdn.example.invalid/upload?secret=SECRET_URL_PARAM"
    # aes_ecb_encrypt / decrypt
    def timeout_run(cmd, **k):
        raise subprocess.TimeoutExpired(cmd, 30)
    with patch.object(m.subprocess, "run", timeout_run):
        for fn in (m.aes_ecb_encrypt, m.aes_ecb_decrypt):
            try:
                fn(b"hello world 1234", key); assert False, "should raise"
            except Exception as e:
                _assert_no_leak(m, e, [key.hex(), token, secret_url])
    # _post_via_curl / _get_via_curl
    cmd = ["curl", "-s", "-H", f"Authorization: Bearer {token}", "https://example.invalid/x"]
    with patch.object(m.subprocess, "run", timeout_run):
        for fn, args in ((m._post_via_curl, ("https://example.invalid/x", {"Authorization": f"Bearer {token}"}, b"{}", 5)),
                         (m._get_via_curl, ("https://example.invalid/x", {}, 5))):
            try:
                fn(*args); assert False, "should raise"
            except Exception as e:
                _assert_no_leak(m, e, [token, secret_url])
    # send_image curl upload + urllib fallback
    img = _img(m, "up103.jpg")
    orig_run = m.subprocess.run
    def selective_run(cmd, **k):
        if cmd and cmd[0] == "curl":
            raise subprocess.TimeoutExpired(cmd, 40)
        return orig_run(cmd, **k)
    with patch.object(m, "aes_ecb_encrypt", return_value=b"ciphertext1234"), \
         patch.object(m, "post_json", return_value={"data": {"upload_full_url": secret_url}}), \
         patch.object(m.subprocess, "run", selective_run):
        try:
            m.send_image(SESSION, img, SESSION["ilink_user_id"], SESSION["context_token"])
            assert False, "should raise"
        except Exception as e:
            _assert_no_leak(m, e, [secret_url, token])
            assert isinstance(e, m.RetryableError)
    # send_image urllib fallback (no curl binary)
    with patch.object(m, "aes_ecb_encrypt", return_value=b"ciphertext1234"), \
         patch.object(m, "post_json", return_value={"data": {"upload_full_url": secret_url}}), \
         patch.object(m.shutil, "which", return_value=None), \
         patch.object(m.urllib.request, "urlopen", side_effect=urllib.error.URLError("boom")):
        try:
            m.send_image(SESSION, img, SESSION["ilink_user_id"], SESSION["context_token"])
            assert False, "should raise"
        except Exception as e:
            _assert_no_leak(m, e, [secret_url, token])
    print("v1.0.3-2 exception chain cut: 6 call sites, no token/key/URL in traceback - PASS")

def main():
    source = sys.argv[1]
    m = load(source)
    td = isolated(m)
    test_1_urllib_detail_and_no_proxy(m, td)
    td2 = isolated(m)
    test_2_traceback_cut(m, td2)
    print("ALL v1.0.3 TESTS PASSED")

if __name__ == "__main__":
    main()
