"""Offline tests for Gate 1 item 2: observability.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network, and never touching the real running
bridge's state/, inbox/ or outbox/:

Unit-level (timeline helpers, no drain):
  1. A recorded event carries exactly schema/job/stage/ts/source
     (+ failure_code/reason only for terminal events), with a UTC
     ISO timestamp; two events in order have non-decreasing ts.
  2. Bad stage, unsafe job key (path separators, empty, dot-file)
     and unknown failure code are all rejected (False, no line).
  3. A timeline write failure never raises (False only).
  4. The reader returns None for a stage that never happened
     (never a fabricated time), and worker_started is always None:
     the bridge has no signal for Worker start, and the queue
     notice (bridge-written) must not be read as Worker start.
  5. Earliest event per stage wins when a stage is recorded twice.
  6. Failure-code mapping is stable: recipient_mismatch,
     text_plan_mismatch, part retry_limit/unknown_delivery,
     unknown attachment type, text too long, restart unknown,
     and the generic delivery_failed/delivery_unknown fallbacks.
  7. A bare message-id view links analysis dirs by name + state
     only (never content); a failed analyzer state adds the
     view-level analysis_failed code.

Drain-level (drain_outbox with fake senders):
  8. Normal send: queued (drain_first_seen) + sent events, view
     outcome sent, stages queued/sent present, worker_started
     None, failure_codes empty, and neither the timeline file
     nor the view/summary JSON contains planted text or token.
  9. PermanentError: failed event with send_permanent, view
     outcome failed, summary failed=1 and needs_check=0.
 10. UnknownDeliveryError: needs_check event with send_unknown,
     view outcome needs_check, summary needs_check=1, failed=0.
 11. RetryableError with attempts left: job stays pending, no
     terminal event, view outcome pending, summary pending=1.
 12. Sanitize-empty text: skipped_empty event, job in failed/,
     view outcome skipped_empty, summary skipped_empty=1 and
     sent=0 (needs_check stays 0).
 13. Long-text plan: exactly one sent event after all parts,
     view outcome sent.
 14. Recipient mismatch: needs_check event with
     recipient_mismatch, job parked in failed/, never sent.
 15. Timeline write failure does not block sending: with STATE
     pointing at a regular file, drain still sends the job.
 16. Summary buckets stay separate when jobs are placed
     directly (pending/sent/failed/needs_check/skipped_empty/
     unreadable each counted once, nothing else), and the
     summary JSON contains no planted text or token.
 17. Read-only CLI dispatch: bridge.main(["observability",
     <job>]) and main(["observability-summary"]) print the same
     view/summary JSON and return 0.

Run: python3 tests/test_observability.py (exit 0 = all pass).
"""

import contextlib
import io
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


class _Fakes:
    def __init__(self, fail_text=None):
        self.text_calls = []
        self.file_calls = []
        self.image_calls = []
        self.fail_text = fail_text

    def send_text(self, session, text, to_user_id=None, context_token=None, client_id=None):
        if self.fail_text is not None:
            raise self.fail_text("simulated")
        self.text_calls.append(text)
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        self.file_calls.append(path)
        return {}

    def send_image(self, session, path, to_user_id, token, client_id=None):
        self.image_calls.append(path)
        return {}


def _setup(tmp):
    names = ("STATE", "INBOX", "RUNNING", "DONE", "OUTBOX_PENDING",
             "OUTBOX_SENT", "OUTBOX_FAILED", "MEDIA_INBOUND",
             "MEDIA_OUTBOUND", "FAILURES_DIR", "ANALYSIS_DIR",
             "LOCAL_VAULT_DIR")
    old = {}
    for n in names:
        old[n] = getattr(bridge, n)
        d = os.path.join(tmp, n.lower())
        os.makedirs(d, exist_ok=True)
        setattr(bridge, n, d)
    bridge._OBS_QUEUED_SEEN.clear()
    return old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _drain(job_name, job, fakes):
    bridge._OBS_QUEUED_SEEN.clear()
    _write_json(os.path.join(bridge.OUTBOX_PENDING, job_name), job)
    orig = (bridge.send_text, bridge.send_file, bridge.send_image)
    bridge.send_text, bridge.send_file, bridge.send_image = (
        fakes.send_text, fakes.send_file, fakes.send_image)
    try:
        sent, failed = bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file, bridge.send_image = orig
    return sent, failed


def _job(text, **extra):
    job = {"text": text, "to_user_id": "user-B", "client_id": "c-obs"}
    job.update(extra)
    return job


def _timeline(tmp_state):
    p = os.path.join(tmp_state, "message_timeline.jsonl")
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ---------------------------------------------------------------- unit
def test_unit_record_and_read():
    tmp = tempfile.mkdtemp(prefix="obs-unit-")
    old = _setup(tmp)
    try:
        state = bridge.STATE
        check("unit: record received ok",
              bridge.record_timeline_event("7001.json", "received", source="serve_inbox_write") is True)
        check("unit: record queued ok",
              bridge.record_timeline_event("7001.json", "queued", source="drain_first_seen") is True)
        evs = _timeline(state)
        check("unit: two events, exact fields, no text/token",
              len(evs) == 2
              and all(set(e.keys()) <= {"schema", "job", "stage", "ts", "source",
                                        "failure_code", "reason"} for e in evs)
              and evs[0] == {"schema": "message-timeline-0.1", "job": "7001",
                             "stage": "received", "ts": evs[0]["ts"],
                             "source": "serve_inbox_write"}
              and evs[1]["stage"] == "queued")
        check("unit: UTC ISO ts, non-decreasing",
              evs[0]["ts"].endswith("+00:00") and evs[1]["ts"] >= evs[0]["ts"])
        check("unit: bad stage rejected",
              bridge.record_timeline_event("7001.json", "bogus") is False)
        check("unit: unsafe job key rejected",
              bridge.record_timeline_event("../7001.json", "received") is False
              and bridge.record_timeline_event("", "received") is False
              and bridge.record_timeline_event(".hidden.json", "received") is False)
        check("unit: unknown failure code rejected",
              bridge.record_timeline_event("7001.json", "failed", failure_code="nope") is False)
        check("unit: no extra lines after rejections", len(_timeline(state)) == 2)
        view = bridge.get_message_observability("7001")
        check("unit: reader stages received/queued present, worker_started None, sent None",
              view["stages"]["received"] == evs[0]["ts"]
              and view["stages"]["queued"] == evs[1]["ts"]
              and view["stages"]["worker_started"] is None
              and view["stages"]["sent"] is None
              and view["stage_notes"]["worker_started"] == "not_observed_by_bridge"
              and view["outcome"] == "unknown"
              and view["event_count"] == 2)
        check("unit: unknown job view is all-missing, never fabricated",
              bridge.get_message_observability("7999")["stages"] ==
              {"received": None, "queued": None, "worker_started": None, "sent": None}
              and bridge.get_message_observability("7999")["outcome"] == "unknown")
        # Earliest wins.
        first_q = evs[1]["ts"]
        bridge.record_timeline_event("7001.json", "queued", source="drain_first_seen")
        check("unit: earliest queued wins on re-record",
              bridge.get_message_observability("7001")["stages"]["queued"] == first_q)
    finally:
        _restore(old)


def test_unit_write_failure_never_raises():
    tmp = tempfile.mkdtemp(prefix="obs-fail-")
    old = _setup(tmp)
    try:
        blocker = os.path.join(tmp, "not-a-dir")
        with open(blocker, "w", encoding="utf-8") as f:
            f.write("x")
        bridge.STATE = blocker
        check("unit: timeline write failure returns False, never raises",
              bridge.record_timeline_event("7002.json", "received") is False)
        check("unit: reader with broken STATE returns []",
              bridge.read_message_timeline("7002") == [])
    finally:
        _restore(old)


def test_unit_failure_codes():
    tmp = tempfile.mkdtemp(prefix="obs-codes-")
    old = _setup(tmp)
    try:
        f = bridge._obs_failure_code_for_job
        check("code: recipient_mismatch",
              f({"needs_check_reason": "recipient_mismatch"}) == "recipient_mismatch")
        check("code: text_plan_mismatch",
              f({"needs_check_reason": "text_plan_mismatch"}) == "text_plan_mismatch")
        check("code: part retry_limit -> send_retry_limit",
              f({"text": "x", "text_plan_version": 1,
                 "text_parts": [{"status": "failed", "error_code": "retry_limit"}]}) == "send_retry_limit")
        check("code: part unknown_delivery -> send_unknown",
              f({"text": "x", "text_plan_version": 1,
                 "text_parts": [{"status": "needs_check", "error_code": "unknown_delivery"}]}) == "send_unknown")
        check("code: unknown attachment type",
              f({"text": "x", "attachments": [{"status": "failed",
                 "error": "unknown attachment type: 'weird'"}]}) == "unknown_attachment_type")
        check("code: text too long",
              f({"text": "x", "text_status": "failed",
                 "text_error": "text too long: 21 parts, deliver as file instead"}) == "text_too_long")
        check("code: unknown_restart",
              f({"delivery_state": "unknown_restart"}) == "send_restart_unknown")
        check("code: generic failed/needs_check fallbacks",
              f({"text": "x", "text_status": "failed"}) == "delivery_failed"
              and f({"text": "x", "text_status": "needs_check"}) == "delivery_unknown"
              and f({"text": "x", "text_status": "sent"}) is None)
    finally:
        _restore(old)


def test_unit_analysis_link():
    tmp = tempfile.mkdtemp(prefix="obs-analysis-")
    old = _setup(tmp)
    try:
        d = os.path.join(bridge.ANALYSIS_DIR, "7003_0")
        os.makedirs(d, exist_ok=True)
        _write_json(os.path.join(d, "state.json"), {"state": "failed"})
        view = bridge.get_message_observability("7003")
        check("unit: analysis linked by dir+state only, failed adds code",
              view["analysis_states"] == [{"dir": "7003_0", "state": "failed"}]
              and "analysis_failed" in view["failure_codes"])
        check("unit: analysis view carries no content keys",
              all(set(a.keys()) == {"dir", "state"} for a in view["analysis_states"]))
    finally:
        _restore(old)


# ---------------------------------------------------------------- drain
def test_drain_normal_send():
    tmp = tempfile.mkdtemp(prefix="obs-send-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        sent, _ = _drain("7001.json",
                         _job("hello world SECRET-TEXT-XYZ",
                              context_token="SECRET-TOKEN-XYZ"),
                         fakes)
        check("drain-send: sent, text delivered",
              "7001.json" in sent and fakes.text_calls == ["hello world SECRET-TEXT-XYZ"])
        check("drain-send: job archived in sent/",
              os.path.exists(os.path.join(bridge.OUTBOX_SENT, "7001.json")))
        evs = _timeline(bridge.STATE)
        check("drain-send: queued + sent events, no other terminal event",
              [e["stage"] for e in evs] == ["queued", "sent"]
              and evs[0]["source"] == "drain_first_seen"
              and evs[1]["source"] == "drain_archive")
        view = bridge.get_message_observability("7001")
        check("drain-send: view outcome sent, worker_started None, no codes",
              view["outcome"] == "sent" and view["stages"]["queued"] is not None
              and view["stages"]["sent"] is not None
              and view["stages"]["worker_started"] is None
              and view["failure_codes"] == [])
        blob = open(os.path.join(bridge.STATE, "message_timeline.jsonl"),
                    encoding="utf-8").read()
        check("drain-send: timeline/view/summary carry no text or token",
              "SECRET-TEXT-XYZ" not in blob and "SECRET-TOKEN-XYZ" not in blob
              and "SECRET-TEXT-XYZ" not in json.dumps(view)
              and "SECRET-TOKEN-XYZ" not in json.dumps(view)
              and "SECRET-TEXT-XYZ" not in json.dumps(bridge.get_observability_summary())
              and "SECRET-TOKEN-XYZ" not in json.dumps(bridge.get_observability_summary()))
    finally:
        _restore(old)


def test_drain_permanent_and_unknown():
    tmp = tempfile.mkdtemp(prefix="obs-failures-")
    old = _setup(tmp)
    try:
        fakes = _Fakes(fail_text=bridge.PermanentError)
        _drain("7011.json", _job("boom"), fakes)
        evs = [e for e in _timeline(bridge.STATE) if e["job"] == "7011"]
        check("drain-permanent: failed event send_permanent, view failed",
              [e["stage"] for e in evs] == ["queued", "failed"]
              and evs[1]["failure_code"] == "send_permanent"
              and bridge.get_message_observability("7011")["outcome"] == "failed"
              and bridge.get_message_observability("7011")["failure_codes"] == ["send_permanent"])
        fakes2 = _Fakes(fail_text=bridge.UnknownDeliveryError)
        _drain("7012.json", _job("huh?"), fakes2)
        evs2 = [e for e in _timeline(bridge.STATE) if e["job"] == "7012"]
        check("drain-unknown: needs_check event send_unknown, view needs_check",
              [e["stage"] for e in evs2] == ["queued", "needs_check"]
              and evs2[1]["failure_code"] == "send_unknown"
              and bridge.get_message_observability("7012")["outcome"] == "needs_check")
        s = bridge.get_observability_summary()
        check("drain-failures: summary failed=1 needs_check=1 sent=0",
              s["failed"] == 1 and s["needs_check"] == 1 and s["sent"] == 0)
    finally:
        _restore(old)


def test_drain_retryable_stays_pending():
    tmp = tempfile.mkdtemp(prefix="obs-retry-")
    old = _setup(tmp)
    try:
        fakes = _Fakes(fail_text=bridge.RetryableError)
        _sent, _failed = _drain("7021.json", _job("try again"), fakes)
        check("drain-retryable: still in pending, no terminal event",
              os.path.exists(os.path.join(bridge.OUTBOX_PENDING, "7021.json"))
              and [e["stage"] for e in _timeline(bridge.STATE)] == ["queued"])
        check("drain-retryable: view pending, summary pending=1",
              bridge.get_message_observability("7021")["outcome"] == "pending"
              and bridge.get_observability_summary()["pending"] == 1)
    finally:
        _restore(old)


def test_drain_skipped_empty():
    tmp = tempfile.mkdtemp(prefix="obs-skip-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        _drain("7031.json", _job("[[hatch_widget:only]]"), fakes)
        check("drain-skip: never sent, queued then skipped_empty event",
              fakes.text_calls == []
              and [e["stage"] for e in _timeline(bridge.STATE)] == ["queued", "skipped_empty"]
              and _timeline(bridge.STATE)[1]["failure_code"] == "skipped_empty")
        view = bridge.get_message_observability("7031")
        s = bridge.get_observability_summary()
        check("drain-skip: view skipped_empty, summary skipped=1 sent=0 needs_check=0",
              view["outcome"] == "skipped_empty" and view["text_skipped"] is True
              and s["skipped_empty"] == 1 and s["sent"] == 0 and s["needs_check"] == 0)
    finally:
        _restore(old)


def test_drain_long_text_plan():
    tmp = tempfile.mkdtemp(prefix="obs-plan-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        sent, _ = _drain("7041.json", _job("word " * 500), fakes)
        evs = _timeline(bridge.STATE)
        check("drain-plan: all parts sent, exactly one sent event",
              "7041.json" in sent and len(fakes.text_calls) == 2
              and [e["stage"] for e in evs if e["stage"] == "sent"] == ["sent"]
              and bridge.get_message_observability("7041")["outcome"] == "sent")
    finally:
        _restore(old)


def test_drain_recipient_mismatch():
    tmp = tempfile.mkdtemp(prefix="obs-mismatch-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        job = _job("wrong account")
        job["to_user_id"] = "user-A"
        _drain("7051.json", job, fakes)
        evs = [e for e in _timeline(bridge.STATE) if e["job"] == "7051"]
        check("drain-mismatch: needs_check recipient_mismatch, never sent",
              fakes.text_calls == []
              and [e["stage"] for e in evs] == ["queued", "needs_check"]
              and evs[1]["failure_code"] == "recipient_mismatch"
              and os.path.exists(os.path.join(bridge.OUTBOX_FAILED, "7051.json")))
    finally:
        _restore(old)


def test_drain_timeline_failure_does_not_block():
    tmp = tempfile.mkdtemp(prefix="obs-noblock-")
    old = _setup(tmp)
    try:
        blocker = os.path.join(tmp, "not-a-dir")
        with open(blocker, "w", encoding="utf-8") as f:
            f.write("x")
        bridge.STATE = blocker
        fakes = _Fakes()
        sent, _ = _drain("7061.json", _job("still goes out"), fakes)
        check("drain-noblock: send proceeds despite broken STATE timeline",
              "7061.json" in sent and fakes.text_calls == ["still goes out"])
    finally:
        _restore(old)


def test_summary_direct_placement():
    tmp = tempfile.mkdtemp(prefix="obs-summary-")
    old = _setup(tmp)
    try:
        _write_json(os.path.join(bridge.OUTBOX_PENDING, "7101.json"),
                    {"text": "retry me", "to_user_id": "user-B",
                     "text_status": "pending", "delivery_state": "pending"})
        _write_json(os.path.join(bridge.OUTBOX_SENT, "7102.json"),
                    {"text": "done", "delivery_state": "sent"})
        _write_json(os.path.join(bridge.OUTBOX_FAILED, "7103.json"),
                    {"text": "bad", "text_status": "failed", "delivery_state": "failed"})
        _write_json(os.path.join(bridge.OUTBOX_FAILED, "7104.json"),
                    {"text": "check me SECRET-TEXT-XYZ", "context_token": "SECRET-TOKEN-XYZ",
                     "delivery_state": "needs_check", "needs_check_reason": "recipient_mismatch"})
        _write_json(os.path.join(bridge.OUTBOX_FAILED, "7105.json"),
                    {"text": "", "text_status": "skipped_empty",
                     "delivery_state": "skipped_empty", "terminal": True})
        with open(os.path.join(bridge.OUTBOX_FAILED, "7106.json"), "w", encoding="utf-8") as f:
            f.write("{not json")
        s = bridge.get_observability_summary()
        check("summary: each bucket exactly once",
              s["pending"] == 1 and s["sent"] == 1 and s["failed"] == 1
              and s["needs_check"] == 1 and s["skipped_empty"] == 1
              and s["unreadable"] == 1)
        check("summary: counts only, no planted text or token",
              "SECRET-TEXT-XYZ" not in json.dumps(s)
              and "SECRET-TOKEN-XYZ" not in json.dumps(s))
    finally:
        _restore(old)


def test_cli_dispatch():
    tmp = tempfile.mkdtemp(prefix="obs-cli-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        _drain("7201.json", _job("cli check"), fakes)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = bridge.main(["observability", "7201.json"])
        view = json.loads(buf.getvalue())
        check("cli: observability prints view, exit 0",
              rc == 0 and view["job"] == "7201" and view["outcome"] == "sent")
        buf2 = io.StringIO()
        with contextlib.redirect_stdout(buf2):
            rc2 = bridge.main(["observability-summary"])
        s = json.loads(buf2.getvalue())
        check("cli: observability-summary prints counts, exit 0",
              rc2 == 0 and s["sent"] == 1 and s["schema"] == "observability-summary-0.1")
    finally:
        _restore(old)


def main():
    test_unit_record_and_read()
    test_unit_write_failure_never_raises()
    test_unit_failure_codes()
    test_unit_analysis_link()
    test_drain_normal_send()
    test_drain_permanent_and_unknown()
    test_drain_retryable_stays_pending()
    test_drain_skipped_empty()
    test_drain_long_text_plan()
    test_drain_recipient_mismatch()
    test_drain_timeline_failure_does_not_block()
    test_summary_direct_placement()
    test_cli_dispatch()
    failed = [n for n, c in ok if not c]
    print(f"\n{len(ok) - len(failed)}/{len(ok)} passed")
    if failed:
        print("FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
