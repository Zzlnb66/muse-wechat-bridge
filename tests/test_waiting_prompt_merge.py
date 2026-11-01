"""Offline tests for the waiting-prompt merge (2026-11-01).

One user message used to yield THREE WeChat bubbles: the serve's
queue notice ("coming, preparing a reply..."), the progress
receipt (over 5s only), and the formal reply. The queue notice
and the progress receipt say the same thing ("please wait"), so
the user decided to merge them into ONE waiting prompt: keep the
progress receipt, drop the queue notice.

After the merge, against the fix-copy bridge.py, all in temp
dirs with fake send functions and an injected clock, never
touching the real running bridge's state/, inbox/ or outbox/:

  1. The serve queue-notice call site is gone: bridge.py source
     mentions the queue-notice helper only in its definition.
     The helper itself is kept, unused, so the old notice is a
     one-call revert.
  2. Slow message (over 5s, no formal job): progress receipt +
     formal reply only, never a *_queue.json file.
  3. Fast message (formal job within 5s): formal reply only,
     neither a queue file nor a progress file.
  4. The formal job's timeline "queued" stage still comes from
     drain first sighting (source drain_first_seen) even with
     no queue notice; stages stay received/queued/sent in order.
  5. Summary counting: *_queue.json is excluded from every
     bucket like *_progress.json and *_offline.json, so "sent"
     counts formal deliveries and genuine replies (fast/local)
     only, never queue notices.
  6. Offline behavior is unchanged without queue files: an
     overdue inbox message with a stale heartbeat still gets
     exactly one offline receipt, and progress for that message
     is then suppressed.

Run: python3 tests/test_waiting_prompt_merge.py (exit 0 = all pass).
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
    return old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _iso(epoch):
    return datetime.datetime.fromtimestamp(
        epoch, tz=datetime.timezone.utc).isoformat(timespec="seconds")


def _write_timeline(state, rows):
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


def _write_json(path, doc):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False)


def _session():
    return {"ilink_user_id": "user-B", "context_token": "CTX-FAKE"}


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
    _write_json(os.path.join(bridge.OUTBOX_PENDING, job_name), job)
    orig = (bridge.send_text, bridge.send_file, bridge.send_image)
    bridge.send_text, bridge.send_file, bridge.send_image = (
        fakes.send_text, fakes.send_file, fakes.send_image)
    try:
        return bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file, bridge.send_image = orig


def _queue_files():
    found = []
    for d in (bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT, bridge.OUTBOX_FAILED):
        if os.path.isdir(d):
            found += [n for n in os.listdir(d) if n.endswith("_queue.json")]
    return found


BASE = 1793475000.0  # fixed injected clock base, not wall time


def test_no_queue_callsite():
    src_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bridge.py")
    with open(src_path, encoding="utf-8") as f:
        src = f.read()
    check("callsite: helper defined exactly once",
          src.count("def next_queue_notice():") == 1)
    check("callsite: no serve call to the helper remains",
          "send_direct_or_outbox(session, next_queue_notice()" not in src)
    check("callsite: helper name appears only in its definition",
          src.count("next_queue_notice(") == 1)


def test_slow_message_progress_plus_formal_no_queue():
    tmp = tempfile.mkdtemp(prefix="merge-slow-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        got = bridge.maybe_send_progress_receipts(_session(), now=BASE + 6.0)
        check("slow: overdue queues exactly one progress receipt",
              got == ["7001"])
        check("slow: no queue file after progress",
              _queue_files() == [])
        fakes = _Fakes()
        _drain("7001.json",
               {"text": "formal reply", "to_user_id": "user-B",
                "client_id": "c-formal"},
               fakes)
        # Drain again so the progress job itself is sent too.
        orig = (bridge.send_text, bridge.send_file, bridge.send_image)
        bridge.send_text, bridge.send_file, bridge.send_image = (
            fakes.send_text, fakes.send_file, fakes.send_image)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            bridge.send_text, bridge.send_file, bridge.send_image = orig
        check("slow: still no queue file after formal drain",
              _queue_files() == [])
        check("slow: formal text intact, progress text sent",
              "formal reply" in fakes.text_calls
              and bridge.PROGRESS_RECEIPT_TEXT in fakes.text_calls)
        s = bridge.get_observability_summary()
        check("slow: summary sent counts formal only",
              s["sent"] == 1 and s["pending"] == 0 and s["failed"] == 0)
    finally:
        _restore(old)


def test_fast_message_formal_only():
    tmp = tempfile.mkdtemp(prefix="merge-fast-")
    old = _setup(tmp)
    try:
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        _write_json(os.path.join(bridge.OUTBOX_PENDING, "7001.json"),
                    {"text": "formal reply", "to_user_id": "user-B",
                     "client_id": "c-formal"})
        check("fast: formal within 5s queues no progress",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 3.0) == [])
        fakes = _Fakes()
        _drain("7001.json",
               {"text": "formal reply", "to_user_id": "user-B",
                "client_id": "c-formal"},
               fakes)
        check("fast: no queue file, no progress file",
              _queue_files() == []
              and [n for n in os.listdir(bridge.OUTBOX_SENT)
                   if n.endswith("_progress.json")] == [])
        check("fast: only the formal text was sent",
              fakes.text_calls == ["formal reply"])
        s = bridge.get_observability_summary()
        check("fast: summary sent counts exactly the formal job",
              s["sent"] == 1)
    finally:
        _restore(old)


def test_formal_timeline_queued_intact():
    tmp = tempfile.mkdtemp(prefix="merge-timeline-")
    old = _setup(tmp)
    try:
        # Drain normalizes job keys (strips ".json"), so the
        # seeded received event and the drain's queued/sent
        # events all land under job "7001".
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        fakes = _Fakes()
        _drain("7001.json",
               {"text": "formal reply", "to_user_id": "user-B",
                "client_id": "c-formal"},
               fakes)
        evs = [e for e in _timeline(bridge.STATE) if e["job"] == "7001"]
        stages = {e["stage"]: e for e in evs}
        check("timeline: received/queued/sent all present",
              all(k in stages for k in ("received", "queued", "sent")))
        check("timeline: queued comes from drain first sighting",
              stages["queued"]["source"] == "drain_first_seen")
        check("timeline: stages in order received<=queued<=sent",
              stages["received"]["ts"] <= stages["queued"]["ts"]
              <= stages["sent"]["ts"])
        view = bridge.get_message_observability("7001")
        check("timeline: view stages all non-null, outcome sent",
              view["stages"]["received"] is not None
              and view["stages"]["queued"] is not None
              and view["stages"]["sent"] is not None
              and view["outcome"] == "sent")
    finally:
        _restore(old)


def test_summary_excludes_queue_files():
    tmp = tempfile.mkdtemp(prefix="merge-summary-")
    old = _setup(tmp)
    try:
        for name, doc in (
                ("7001.json", {"text": "formal", "to_user_id": "user-B"}),
                ("7002_queue.json", {"text": "old queue notice",
                                     "to_user_id": "user-B"}),
                ("7003_progress.json", {"text": "progress",
                                        "to_user_id": "user-B"}),
                ("7004_offline.json", {"text": "offline",
                                       "to_user_id": "user-B"}),
                ("7005_fast.json", {"text": "fast reply",
                                    "to_user_id": "user-B"})):
            _write_json(os.path.join(bridge.OUTBOX_SENT, name), doc)
        _write_json(os.path.join(bridge.OUTBOX_FAILED, "7006_queue.json"),
                    {"text": "old failed queue notice", "to_user_id": "user-B"})
        s = bridge.get_observability_summary()
        check("summary: sent counts formal + fast, not queue/progress/offline",
              s["sent"] == 2)
        check("summary: old queue file in failed counts in no bucket",
              s["failed"] == 0 and s["needs_check"] == 0
              and s["skipped_empty"] == 0 and s["unreadable"] == 0
              and s["pending"] == 0)
    finally:
        _restore(old)


def test_offline_unchanged_without_queue():
    tmp = tempfile.mkdtemp(prefix="merge-offline-")
    old = _setup(tmp)
    try:
        # No heartbeat file at all: heartbeat reads stale.
        _write_json(os.path.join(bridge.INBOX, "7001.json"),
                    {"text": "user hello"})
        _write_timeline(bridge.STATE, [("7001", "received", BASE)])
        got = bridge.maybe_send_offline_receipts(_session(), now=BASE + 200.0)
        check("offline: overdue inbox message still gets one offline receipt",
              got == ["7001"])
        check("offline: no queue file involved",
              _queue_files() == [])
        check("offline: inbox record untouched",
              os.path.exists(os.path.join(bridge.INBOX, "7001.json")))
        check("offline: progress for that message now suppressed",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 300.0) == [])
    finally:
        _restore(old)


def main():
    test_no_queue_callsite()
    test_slow_message_progress_plus_formal_no_queue()
    test_fast_message_formal_only()
    test_formal_timeline_queued_intact()
    test_summary_excludes_queue_files()
    test_offline_unchanged_without_queue()
    failed = [n for n, c in ok if not c]
    print("%d/%d passed" % (len(ok) - len(failed), len(ok)))
    if failed:
        print("FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
