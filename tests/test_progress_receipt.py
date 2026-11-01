"""Offline tests for Gate 1 item 3: progress receipt for slow replies.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and an injected clock (no real sleep for the 5s
threshold), and never touching the real running bridge's state/,
inbox/ or outbox/:

  1. A formal reply within 5s (including exactly 5.0s) triggers
     no progress receipt.
  2. A message overdue past 5s with no formal job gets exactly
     one progress receipt: fixed text, stable client_id
     "progress-<mid>", one timeline "progress" event on the
     formal job's timeline, and a persistent marker; repeat
     calls (including after a simulated restart, i.e. a fresh
     load from disk) never queue a second one.
  3. A formal job in pending/, sent/ or failed/ suppresses the
     progress receipt, and the progress call leaves the formal
     job file byte-identical.
  4. A progress receipt sent first does not change the formal
     reply: after drain, the formal job's text is intact in
     sent/, no evidence-audit row is written for either job,
     and the summary counts the formal job but not the
     progress job as sent.
  5. A progress queue-write failure is recorded (marker
     ok=False), is never retried, and does not block the
     formal reply.
  6. The per-message view shows progress_at separately from
     stages.sent, and the summary excludes *_progress.json
     from every bucket.
  7. The progress text is exactly the fixed sentence: it
     contains no user text and no token.
  8. Timeline lines with an unparsable timestamp, and received
     events for non-numeric job keys, are ignored, never
     treated as overdue, and never raise.
  9. A session without ilink_user_id triggers nothing.

Run: python3 tests/test_progress_receipt.py (exit 0 = all pass).
"""

import datetime
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
    # Gate 1 item 4 (grace revision, 2026-11-01): progress
    # suppression is PER MESSAGE (offline ack/job), never a
    # global heartbeat gate, so this harness deliberately seeds
    # NO heartbeat file: these tests prove progress fires even
    # when the heartbeat reads stale, which is exactly the
    # within-grace behavior. The per-message offline path is
    # covered by test_offline_receipt.py and by the two tests
    # at the end of this file.
    return old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _iso(epoch):
    return datetime.datetime.fromtimestamp(
        epoch, tz=datetime.timezone.utc).isoformat(timespec="seconds")


def _write_timeline(state, rows):
    """rows: list of (job, stage, ts_epoch). Written verbatim."""
    p = os.path.join(state, "message_timeline.jsonl")
    with open(p, "a", encoding="utf-8") as f:
        for job, stage, ts in rows:
            f.write(json.dumps({"schema": "message-timeline-0.1",
                                "job": job, "stage": stage,
                                "ts": _iso(ts), "source": "test"},
                               ensure_ascii=False) + "\n")


def _timeline(state):
    p = os.path.join(state, "message_timeline.jsonl")
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _session():
    return {"ilink_user_id": "user-B", "context_token": "CTX-FAKE"}


def _progress_path(tmp_pending, mid):
    return os.path.join(tmp_pending, "%s_progress.json" % mid)


class _Fakes:
    def __init__(self):
        self.text_calls = []

    def send_text(self, session, text, to_user_id=None, context_token=None, client_id=None):
        self.text_calls.append(text)
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        return {}

    def send_image(self, session, path, to_user_id, token, client_id=None):
        return {}


def _drain(job_name, job, fakes):
    bridge._OBS_QUEUED_SEEN.clear()
    with open(os.path.join(bridge.OUTBOX_PENDING, job_name), "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False)
    orig = (bridge.send_text, bridge.send_file, bridge.send_image)
    bridge.send_text, bridge.send_file, bridge.send_image = (
        fakes.send_text, fakes.send_file, fakes.send_image)
    try:
        return bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file, bridge.send_image = orig


BASE = 1793475000.0  # fixed injected clock base, not wall time


def test_within_5s_no_receipt():
    tmp = tempfile.mkdtemp(prefix="prog-fast-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        check("fast: within 5s no receipt",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 3.0) == [])
        check("fast: exactly 5.0s still no receipt",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 5.0) == [])
        check("fast: no progress job file",
              not os.path.exists(_progress_path(bridge.OUTBOX_PENDING, "7001")))
        check("fast: no progress timeline event",
              [e for e in _timeline(bridge.STATE) if e["stage"] == "progress"] == [])
    finally:
        _restore(old)


def test_overdue_exactly_once():
    tmp = tempfile.mkdtemp(prefix="prog-once-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        got = bridge.maybe_send_progress_receipts(_session(), now=BASE + 6.0)
        check("once: overdue queues one receipt", got == ["7001"])
        p = _progress_path(bridge.OUTBOX_PENDING, "7001")
        job = _read_json(p) if os.path.exists(p) else {}
        check("once: progress job fixed text + stable client_id",
              job.get("text") == bridge.PROGRESS_RECEIPT_TEXT
              and job.get("client_id") == "progress-7001"
              and job.get("to_user_id") == "user-B")
        evs = [e for e in _timeline(bridge.STATE) if e["stage"] == "progress"]
        check("once: one progress timeline event on formal job",
              len(evs) == 1 and evs[0]["job"] == "7001"
              and evs[0]["source"] == "bridge_progress")
        marker = bridge._load_progress_receipts().get("7001", {})
        check("once: marker persisted ok",
              marker.get("ok") is True and marker.get("client_id") == "progress-7001")
        check("once: repeat call queues nothing",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        check("once: still exactly one progress job file",
              os.path.exists(p) and len(
                  [n for n in os.listdir(bridge.OUTBOX_PENDING)
                   if n.endswith("_progress.json")]) == 1)
        # Simulated restart: fresh load from disk (the function
        # loads markers from disk on every call) still sends none.
        check("once: post-restart call queues nothing",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 600.0) == [])
    finally:
        _restore(old)


def test_formal_job_suppresses():
    tmp = tempfile.mkdtemp(prefix="prog-supp-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        formal = {"text": "formal reply", "to_user_id": "user-B",
                  "client_id": "c-formal"}
        fpath = os.path.join(bridge.OUTBOX_PENDING, "7001.json")
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(formal, f, ensure_ascii=False)
        before = open(fpath, encoding="utf-8").read()
        check("suppress: formal in pending suppresses",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        check("suppress: formal job file untouched",
              open(fpath, encoding="utf-8").read() == before)
        os.replace(fpath, os.path.join(bridge.OUTBOX_SENT, "7001.json"))
        check("suppress: formal in sent suppresses",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        os.replace(os.path.join(bridge.OUTBOX_SENT, "7001.json"),
                   os.path.join(bridge.OUTBOX_FAILED, "7001.json"))
        check("suppress: formal in failed suppresses",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        check("suppress: no progress job anywhere",
              not os.path.exists(_progress_path(bridge.OUTBOX_PENDING, "7001"))
              and not os.path.exists(_progress_path(bridge.OUTBOX_SENT, "7001")))
    finally:
        _restore(old)


def test_progress_then_formal_unaffected():
    tmp = tempfile.mkdtemp(prefix="prog-formal-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        bridge.maybe_send_progress_receipts(_session(), now=BASE + 6.0)
        fakes = _Fakes()
        sent, _failed = _drain("7001.json",
                               {"text": "formal reply intact",
                                "to_user_id": "user-B", "client_id": "c-formal"},
                               fakes)
        # Drain again so the progress job itself is sent too.
        orig = (bridge.send_text, bridge.send_file, bridge.send_image)
        bridge.send_text, bridge.send_file, bridge.send_image = (
            fakes.send_text, fakes.send_file, fakes.send_image)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            bridge.send_text, bridge.send_file, bridge.send_image = orig
        formal = _read_json(os.path.join(bridge.OUTBOX_SENT, "7001.json"))
        check("formal: text intact after progress + drain",
              formal.get("text") == "formal reply intact"
              and "formal reply intact" in fakes.text_calls)
        check("formal: no evidence-audit row for either job",
              not os.path.exists(os.path.join(bridge.STATE, "evidence_audit.jsonl")))
        s = bridge.get_observability_summary()
        check("formal: summary counts formal, not progress, as sent",
              s["sent"] == 1 and s["pending"] == 0)
        view = bridge.get_message_observability("7001")
        # Timestamps have second precision, so a progress event and
        # a formal sent in the same second are equal, not strictly
        # ordered; the contract is separate fields, not "<".
        check("formal: view sent + progress_at separate, outcome sent",
              view["outcome"] == "sent" and view["stages"]["sent"] is not None
              and view["progress_at"] is not None
              and view["progress_at"] <= view["stages"]["sent"])
    finally:
        _restore(old)


def test_progress_failure_no_block_no_retry():
    tmp = tempfile.mkdtemp(prefix="prog-fail-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        orig_atomic = bridge.atomic_write_json

        def _flaky(path, obj, mode=0o600):
            if os.path.basename(path).endswith("_progress.json"):
                raise OSError("simulated queue failure")
            return orig_atomic(path, obj, mode=mode)

        bridge.atomic_write_json = _flaky
        try:
            got = bridge.maybe_send_progress_receipts(_session(), now=BASE + 6.0)
        finally:
            bridge.atomic_write_json = orig_atomic
        marker = bridge._load_progress_receipts().get("7001", {})
        check("fail: failed queue returns nothing, marker ok=False",
              got == [] and marker.get("ok") is False)
        check("fail: no progress timeline event on failure",
              [e for e in _timeline(bridge.STATE)
               if e["stage"] == "progress"] == [])
        check("fail: marker blocks retry",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        fakes = _Fakes()
        sent, _failed = _drain("7001.json",
                               {"text": "formal still works",
                                "to_user_id": "user-B", "client_id": "c-formal"},
                               fakes)
        check("fail: formal reply unaffected by progress failure",
              "7001.json" in sent and "formal still works" in fakes.text_calls)
    finally:
        _restore(old)


def test_view_and_summary_exclude_progress():
    tmp = tempfile.mkdtemp(prefix="prog-view-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE),
                                       ("7001", "progress", BASE + 6.0),
                                       ("7001", "sent", BASE + 9.0)])
        with open(os.path.join(bridge.OUTBOX_SENT, "7001.json"), "w", encoding="utf-8") as f:
            json.dump({"text": "x", "to_user_id": "user-B",
                       "delivery_state": "sent"}, f, ensure_ascii=False)
        with open(os.path.join(bridge.OUTBOX_SENT, "7001_progress.json"), "w", encoding="utf-8") as f:
            json.dump({"text": bridge.PROGRESS_RECEIPT_TEXT,
                       "to_user_id": "user-B", "delivery_state": "sent"},
                      f, ensure_ascii=False)
        view = bridge.get_message_observability("7001")
        check("view: progress_at separate from stages.sent",
              view["progress_at"] == _iso(BASE + 6.0)
              and view["stages"]["sent"] == _iso(BASE + 9.0)
              and view["stages"] == {"received": _iso(BASE),
                                     "queued": None, "worker_started": None,
                                     "sent": _iso(BASE + 9.0)})
        s = bridge.get_observability_summary()
        check("summary: progress job in sent/ not counted as sent",
              s["sent"] == 1)
    finally:
        _restore(old)


def test_fixed_text_only():
    tmp = tempfile.mkdtemp(prefix="prog-text-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        bridge.maybe_send_progress_receipts(_session(), now=BASE + 6.0)
        job = _read_json(_progress_path(bridge.OUTBOX_PENDING, "7001"))
        check("text: progress text is exactly the fixed sentence",
              job.get("text") == "还在处理中，稍等一下，我会把正式回复发给你。"
              and "SECRET" not in job.get("text", ""))
    finally:
        _restore(old)


def test_bad_timeline_lines_ignored():
    tmp = tempfile.mkdtemp(prefix="prog-bad-")
    old = _setup(tmp)
    try:
        p = os.path.join(bridge.STATE, "message_timeline.jsonl")
        with open(p, "w", encoding="utf-8") as f:
            f.write(json.dumps({"schema": "message-timeline-0.1", "job": "7001",
                                "stage": "received", "ts": "not-a-time",
                                "source": "test"}, ensure_ascii=False) + "\n")
            f.write(json.dumps({"schema": "message-timeline-0.1", "job": "7001_queue",
                                "stage": "received", "ts": _iso(BASE),
                                "source": "test"}, ensure_ascii=False) + "\n")
            f.write("this is not json\n")
        check("bad: unparsable/non-digit lines ignored, no raise",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 600.0) == [])
    finally:
        _restore(old)


def test_no_session_user():
    tmp = tempfile.mkdtemp(prefix="prog-nouser-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        check("nouser: session without user id triggers nothing",
              bridge.maybe_send_progress_receipts({}, now=BASE + 60.0) == []
              and bridge.maybe_send_progress_receipts({"ilink_user_id": ""},
                                                      now=BASE + 60.0) == [])
    finally:
        _restore(old)


def test_offline_ack_suppresses_progress():
    # Grace revision: suppression is per message. A message with
    # a persisted offline ack gets no progress note, even though
    # the heartbeat here is stale (no heartbeat file at all) —
    # the ack, not the global heartbeat state, is the guard.
    tmp = tempfile.mkdtemp(prefix="prog-offack-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        bridge._save_offline_receipts(
            {"7001": {"ts": _iso(BASE), "client_id": "offline-7001",
                      "ok": True}})
        check("offack: progress suppressed for acked message",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        check("offack: no progress job for acked message",
              not os.path.exists(_progress_path(bridge.OUTBOX_PENDING, "7001")))
    finally:
        _restore(old)


def test_offline_job_suppresses_progress():
    # Same guard via the job file alone (ack record lost).
    tmp = tempfile.mkdtemp(prefix="prog-offjob-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        with open(os.path.join(bridge.OUTBOX_PENDING, "7001_offline.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"text": bridge.OFFLINE_RECEIPT_TEXT,
                       "to_user_id": "user-B",
                       "client_id": "offline-7001"}, f, ensure_ascii=False)
        check("offjob: progress suppressed for offline-job message",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0) == [])
        check("offjob: no progress job for offline-job message",
              not os.path.exists(_progress_path(bridge.OUTBOX_PENDING, "7001")))
    finally:
        _restore(old)


def test_stale_heartbeat_progress_still_fires():
    # Grace revision, the key behavior change: with NO heartbeat
    # file at all (stale by definition) and no offline ack/job,
    # progress still fires. The old global gate would have
    # silenced it; the per-message guard does not.
    tmp = tempfile.mkdtemp(prefix="prog-stale-")
    old = _setup(tmp)
    try:
        check("stale: no heartbeat file reads offline",
              bridge.is_worker_offline(now=BASE + 60.0) is True)
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        check("stale: progress still queues despite stale heartbeat",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0)
              == ["7001"])
    finally:
        _restore(old)


def main():
    test_within_5s_no_receipt()
    test_overdue_exactly_once()
    test_formal_job_suppresses()
    test_progress_then_formal_unaffected()
    test_progress_failure_no_block_no_retry()
    test_view_and_summary_exclude_progress()
    test_fixed_text_only()
    test_bad_timeline_lines_ignored()
    test_no_session_user()
    test_offline_ack_suppresses_progress()
    test_offline_job_suppresses_progress()
    test_stale_heartbeat_progress_still_fires()
    bad = [n for n, c in ok if not c]
    print(f"{len(ok) - len(bad)}/{len(ok)} passed")
    if bad:
        print("FAILED:", bad)
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
