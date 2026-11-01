"""Offline tests for Gate 2 -14 anomaly counting + ops manual.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
getupdates failures and no network, and never touching the real
running bridge's state/, inbox/ or outbox/:

Classifier (fixed enums only, exception text never stored):
  1. TokenExpired -> login_expired / ret_minus_14.
  2. "HTTP 429 ..." -> rate_limited / http_429.
  3. errmsg with a rate-limit phrase -> rate_limited / rate_signal.
  4. post_json transport wrapper ("curl failed: ...; urllib failed:
     ...") -> network_error / transport, even when the curl half
     failed on an empty body (transport dominates).
  5. Empty body / non-JSON / non-dict messages -> result_unknown /
     empty_or_nonjson.
  6. HTTP non-429 error -> result_unknown / http_error.
  7. Explicit non-zero ret without a rate-limit signal -> NOT
     rate_limited: result_unknown / server_rejection (no numeric
     rate-limit code is assumed from upstream).
  8. Anything else -> result_unknown / unclassified.

-14 safety (unchanged):
  9. check_ret on ret -14 still writes NEEDS_RELOGIN and raises
     TokenExpired (the serve_loop exit path itself is unchanged;
     only a desensitised count is added beside it).

Counting file (state/getupdates_anomalies.json, 0600, atomic):
 10. Per-category increments, last_seen timestamps, last_detail
     enums, and schema are correct; counts are cumulative.
 11. Desensitisation: exception messages planted with a fake token,
     a URL carrying a credential query, a user identifier and message
     text never appear in the counting file or in CLI output.
 12. A counting write failure never raises (returns False) and never
     blocks the caller.
 13. The failures marker written on a counting write failure is
     itself desensitised (no planted credential value or URL).
 14. A corrupt pre-existing counting file resets to defaults
     instead of raising; the read-only CLI reports it unreadable
     with zero counts rather than fabricating numbers.
 15. The read-only CLI on a missing file reports present=False with
     zero counts, returns 0, and does NOT create the file.
 16. The read-only CLI output matches the recorded counts.
 17. Recording anomalies never touches channel_status (the existing
     success/failure buckets are unaffected).
 18. An unknown category/detail is normalised to result_unknown /
     unclassified, never stored verbatim.

Run: python3 tests/test_getupdates_anomalies.py (exit 0 = all pass).
"""

import contextlib
import io
import json
import os
import stat
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
    tmp = tempfile.mkdtemp(prefix="anomalies-test-")
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
    for n in ("NEEDS_RELOGIN_FILE", "CHANNEL_STATUS_FILE"):
        old[n] = getattr(bridge, n)
        setattr(bridge, n, os.path.join(bridge.STATE, os.path.basename(old[n])))
    return tmp, old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _teardown(tmp):
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)


def _anomalies_path():
    return os.path.join(bridge.STATE, "getupdates_anomalies.json")


def _read_counts_file():
    with open(_anomalies_path(), encoding="utf-8") as f:
        return json.load(f)


def _file_text():
    with open(_anomalies_path(), encoding="utf-8") as f:
        return f.read()


def test_classifier():
    c = bridge.classify_getupdates_error
    check("classify: TokenExpired -> login_expired/ret_minus_14",
          c(bridge.TokenExpired("getupdates: token expired")) == ("login_expired", "ret_minus_14"))
    check("classify: HTTP 429 -> rate_limited/http_429",
          c(bridge.ApiError("HTTP 429 for /ilink/bot/getupdates: {} (curl_err=none)")) == ("rate_limited", "http_429"))
    check("classify: rate-limit phrase -> rate_limited/rate_signal",
          c(bridge.ApiError("getupdates: ret=1001 errcode=1001 errmsg=Rate limit exceeded, retry later")) == ("rate_limited", "rate_signal"))
    check("classify: transport wrapper -> network_error/transport",
          c(bridge.ApiError("/ilink/bot/getupdates: curl failed: empty response body; urllib failed: URLError(TimeoutError); target=h:1; proxy=h:2")) == ("network_error", "transport"))
    check("classify: empty body -> result_unknown/empty_or_nonjson",
          c(bridge.ApiError("/ilink/bot/getupdates: empty response body, not treated as success (curl_err=none)")) == ("result_unknown", "empty_or_nonjson"))
    check("classify: non-JSON -> result_unknown/empty_or_nonjson",
          c(bridge.ApiError("/ilink/bot/getupdates: non-JSON response, not treated as success")) == ("result_unknown", "empty_or_nonjson"))
    check("classify: non-dict check_ret -> result_unknown/empty_or_nonjson",
          c(bridge.ApiError("getupdates: empty or non-JSON response, not treated as success")) == ("result_unknown", "empty_or_nonjson"))
    check("classify: HTTP 500 -> result_unknown/http_error",
          c(bridge.ApiError("HTTP 500 for /ilink/bot/getupdates: b'{}' (curl_err=none)")) == ("result_unknown", "http_error"))
    check("classify: explicit ret without rate signal is NOT rate_limited",
          c(bridge.ApiError("getupdates: ret=500 errcode=500 errmsg=internal error")) == ("result_unknown", "server_rejection"))
    check("classify: generic failure -> result_unknown/unclassified",
          c(bridge.ApiError("something completely different")) == ("result_unknown", "unclassified"))


def test_minus14_still_stops():
    try:
        bridge.check_ret({"ret": -14, "errcode": None}, "getupdates")
        raised = False
    except bridge.TokenExpired:
        raised = True
    check("-14: check_ret still raises TokenExpired", raised)
    check("-14: NEEDS_RELOGIN still written",
          os.path.exists(bridge.NEEDS_RELOGIN_FILE))
    if os.path.exists(bridge.NEEDS_RELOGIN_FILE):
        with open(bridge.NEEDS_RELOGIN_FILE, encoding="utf-8") as f:
            doc = json.load(f)
        check("-14: NEEDS_RELOGIN records the getupdates op",
              doc.get("op") == "getupdates")


def test_record_counts_and_desensitisation():
    planted_token = "tok_PLANTED_abc123"
    planted_url = "https://example.invalid/secret?context_token=PLANTED_TOKEN_VALUE"
    planted_user = "user_PLANTED_999"
    planted_text = "PLANTED_SECRET_TEXT"
    # Exceptions carrying planted material; only enums may survive.
    bridge.record_getupdates_anomaly(*bridge.classify_getupdates_error(
        bridge.TokenExpired("getupdates token %s %s" % (planted_token, planted_user))))
    bridge.record_getupdates_anomaly(*bridge.classify_getupdates_error(
        bridge.ApiError("/ilink/bot/getupdates: curl failed: x; urllib failed: y %s" % planted_url)))
    bridge.record_getupdates_anomaly(*bridge.classify_getupdates_error(
        bridge.ApiError("/ilink/bot/getupdates: curl failed: x; urllib failed: y")))
    bridge.record_getupdates_anomaly(*bridge.classify_getupdates_error(
        bridge.ApiError("HTTP 429 for /ilink/bot/getupdates: %s" % planted_text)))
    bridge.record_getupdates_anomaly(*bridge.classify_getupdates_error(
        bridge.ApiError("getupdates: ret=500 errmsg=%s" % planted_text)))
    doc = _read_counts_file()
    check("counts: schema correct", doc.get("schema") == "getupdates-anomalies-0.1")
    check("counts: per-category totals correct",
          doc.get("counts") == {"login_expired": 1, "network_error": 2, "rate_limited": 1, "result_unknown": 1})
    check("counts: last_seen set for every seen category",
          all(isinstance(doc.get("last_seen", {}).get(k), str) for k in ("login_expired", "network_error", "rate_limited", "result_unknown")))
    check("counts: last_detail enums correct",
          doc.get("last_detail") == {"login_expired": "ret_minus_14", "network_error": "transport", "rate_limited": "http_429", "result_unknown": "server_rejection"})
    check("counts: file mode is 0600",
          stat.S_IMODE(os.stat(_anomalies_path()).st_mode) == 0o600)
    body = _file_text()
    check("counts: file contains no planted token/user/text/URL",
          planted_token not in body and planted_user not in body
          and planted_text not in body and "example.invalid" not in body
          and "PLANTED_TOKEN_VALUE" not in body)


def test_write_failure_and_marker():
    tmp_blocker = os.path.join(bridge.STATE, "blocker-file")
    with open(tmp_blocker, "w", encoding="utf-8") as f:
        f.write("not a dir")
    old_state = bridge.STATE
    bridge.STATE = tmp_blocker
    try:
        ok = bridge.record_getupdates_anomaly("network_error", "transport")
        check("write failure: record returns False, never raises", ok is False)
    finally:
        bridge.STATE = old_state
    # Marker path: fail ONLY the counting-file write with a planted
    # error, so the marker itself is written and can be inspected.
    real_write = bridge.atomic_write_json

    def _boom(path, obj, mode=0o600):
        if path.endswith("getupdates_anomalies.json"):
            raise bridge.ApiError("boom context_token=PLANTED_MARKER_TOKEN https://example.invalid/marker?context_token=PLANTED_MARKER_TOKEN")
        return real_write(path, obj, mode=mode)

    bridge.atomic_write_json = _boom
    try:
        ok = bridge.record_getupdates_anomaly("network_error", "transport")
        check("marker: record still returns False, never raises", ok is False)
    finally:
        bridge.atomic_write_json = real_write
    marker = os.path.join(bridge.FAILURES_DIR, "getupdates_anomalies_write_failed.json")
    check("marker: failures marker written", os.path.exists(marker))
    if os.path.exists(marker):
        with open(marker, encoding="utf-8") as f:
            mbody = f.read()
        check("marker: no planted credential value or URL",
              "PLANTED_MARKER_TOKEN" not in mbody and "example.invalid" not in mbody)
        check("marker: kind is fixed", "getupdates_anomalies_write_failed" in mbody)


def test_corrupt_file_and_cli():
    with open(_anomalies_path(), "w", encoding="utf-8") as f:
        f.write("{not json")
    out = bridge.read_getupdates_anomalies()
    check("corrupt: read reports present but unreadable, zero counts",
          out.get("present") is True and out.get("readable") is False
          and out.get("counts") == {"login_expired": 0, "network_error": 0, "rate_limited": 0, "result_unknown": 0})
    ok = bridge.record_getupdates_anomaly("network_error", "transport")
    doc = _read_counts_file()
    check("corrupt: record resets to defaults instead of raising",
          ok is True and doc.get("counts", {}).get("network_error") == 1
          and doc.get("counts", {}).get("login_expired") == 0)


def test_cli_readonly_and_channel_untouched():
    # Fresh temp state: no counting file yet.
    for p in (_anomalies_path(), bridge.CHANNEL_STATUS_FILE):
        if os.path.exists(p):
            os.remove(p)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = bridge.main(["getupdates-anomalies"])
    out = json.loads(buf.getvalue())
    check("cli: missing file reports present=False, zero counts, rc 0",
          rc == 0 and out.get("present") is False
          and out.get("counts") == {"login_expired": 0, "network_error": 0, "rate_limited": 0, "result_unknown": 0})
    check("cli: missing file is NOT created by the read-only query",
          not os.path.exists(_anomalies_path()))
    bridge.record_getupdates_anomaly("rate_limited", "http_429")
    bridge.record_getupdates_anomaly("rate_limited", "http_429")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = bridge.main(["getupdates-anomalies"])
    out = json.loads(buf.getvalue())
    check("cli: output matches recorded counts",
          rc == 0 and out.get("counts", {}).get("rate_limited") == 2)
    check("cli: output contains no tokens or user identifiers",
          "context_token" not in buf.getvalue() and "bot_token" not in buf.getvalue())
    check("channel_status untouched by anomaly recording",
          not os.path.exists(bridge.CHANNEL_STATUS_FILE))


def test_normalisation():
    bridge.record_getupdates_anomaly("bogus_category", "bogus_detail")
    doc = _read_counts_file()
    check("normalisation: unknown category/detail -> result_unknown/unclassified",
          doc.get("last_detail", {}).get("result_unknown") == "unclassified")
    check("normalisation: no verbatim bogus keys stored",
          "bogus_category" not in json.dumps(doc) and "bogus_detail" not in json.dumps(doc))


def main():
    tmp, old = _setup()
    try:
        test_classifier()
        test_minus14_still_stops()
        test_record_counts_and_desensitisation()
        test_write_failure_and_marker()
        test_corrupt_file_and_cli()
        test_cli_readonly_and_channel_untouched()
        test_normalisation()
    finally:
        _restore(old)
        _teardown(tmp)
    print("getupdates_anomalies: %d passed, %d failed" % (PASS, FAIL))
    if FAIL:
        print("failures: %s" % ", ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
