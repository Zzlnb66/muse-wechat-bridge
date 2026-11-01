"""Offline tests for Gate 1 item 4 prerequisite: Worker heartbeat
reading + bridge-side offline receipt, GRACE REVISION
(2026-11-01): an offline receipt fires only after a message
has waited strictly more than OFFLINE_RECEIPT_GRACE_SECONDS
(180s) since its earliest timeline 'received' event, with a
stale heartbeat and no formal job. This fixes the
idle-false-alarm defect of the pre-grace version, where a
stale heartbeat right after arrival (normal after any idle gap
over 2 minutes, given per-message Worker wakeup plus ~60-75s
pickup latency) would spam an offline receipt for every normal
message before the Worker could pick it up.

Covers, against the fix-copy bridge.py, all in temp dirs with
fake send functions and an injected clock (no real waiting for
the heartbeat timeout or the grace window), and never touching
the real running bridge's state/, inbox/ or outbox/:

  1. A fresh heartbeat (within the stale threshold) means
     online: no offline receipt, inbox untouched — even for a
     message that has waited far past the grace window (a live
     Worker that is merely backlogged is not offline).
  2. A missing/stale heartbeat means offline, but the receipt
     still waits out the grace window: 179s waited no receipt,
     exactly 180s waited no receipt (strictly greater), 181s
     waited receipt. No 'received' event means the wait cannot
     be computed: never guess, never send.
  3. A stale heartbeat means offline; the boundary is exact:
     exactly 120s old is still online, 121s old is offline.
     An unreadable heartbeat (bad JSON, non-dict, missing ts,
     naive ts without timezone) means offline, never a crash.
  4. A burst of distinct inbox messages gets at most one
     receipt per message, never a repeated flood.
  5. A formal job in pending/ suppresses the offline receipt,
     and the formal job file is left byte-identical.
  6. Recovery: after an offline receipt, the original inbox
     record is untouched, and once the heartbeat turns fresh,
     a formal reply drains normally with its text intact and
     no evidence-audit row for either job.
  7. Mutual exclusion, PER MESSAGE (grace revision): while
     stale-but-within-grace, progress receipts still fire
     (the Worker may be working; "still working" is true);
     once a message has an offline ack/job, its progress is
     suppressed; progress-first then offline is still allowed
     (a genuine status change, not a duplicate).
  8. An offline queue-write failure is recorded (ack ok=False),
     is never retried, earns no timeline event, and does not
     block the formal reply.
  9. The offline text is exactly the fixed sentence: it
     contains no user text and no token.
 10. The per-message view shows offline_receipt_at separately
     from stages.sent, and the summary excludes *_offline.json
     from every bucket.
 11. A session without ilink_user_id triggers nothing, and
     non-numeric inbox files are ignored without raising.

Run: python3 tests/test_offline_receipt.py (exit 0 = all pass).
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


def _write_heartbeat(state, doc):
    p = os.path.join(state, "worker_heartbeat.json")
    with open(p, "w", encoding="utf-8") as f:
        if isinstance(doc, str):
            f.write(doc)
        else:
            json.dump(doc, f, ensure_ascii=False)


def _write_inbox(inbox, mid, text="user hello"):
    p = os.path.join(inbox, "%s.json" % mid)
    with open(p, "w", encoding="utf-8") as f:
        json.dump({"text": text}, f, ensure_ascii=False)
    return p


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


def _offline_path(outbox_dir, mid):
    return os.path.join(outbox_dir, "%s_offline.json" % mid)


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


def test_fresh_heartbeat_online():
    tmp = tempfile.mkdtemp(prefix="off-fresh-")
    old = _setup(tmp)
    try:
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE - 10),
                                        "source": "worker"})
        inbox_before = _write_inbox(bridge.INBOX, "8001")
        before = open(inbox_before, encoding="utf-8").read()
        check("fresh: heartbeat 10s old is online",
              bridge.is_worker_offline(now=BASE) is False)
        check("fresh: no offline receipt when online",
              bridge.maybe_send_offline_receipts(_session(), now=BASE) == [])
        check("fresh: no offline job file",
              not os.path.exists(_offline_path(bridge.OUTBOX_PENDING, "8001")))
        check("fresh: inbox record untouched",
              open(inbox_before, encoding="utf-8").read() == before)
    finally:
        _restore(old)


def test_missing_heartbeat_offline_once():
    tmp = tempfile.mkdtemp(prefix="off-missing-")
    old = _setup(tmp)
    try:
        check("missing: no heartbeat file is offline",
              bridge.is_worker_offline(now=BASE) is True)
        _write_inbox(bridge.INBOX, "8001", text="secret user words")
        # Grace: received 200s ago, so the 180s wait is satisfied.
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        got = bridge.maybe_send_offline_receipts(_session(), now=BASE)
        check("missing: overdue inbox message queues one receipt",
              got == ["8001"])
        p = _offline_path(bridge.OUTBOX_PENDING, "8001")
        job = _read_json(p) if os.path.exists(p) else {}
        check("missing: offline job fixed text + stable client_id",
              job.get("text") == bridge.OFFLINE_RECEIPT_TEXT
              and job.get("client_id") == "offline-8001"
              and job.get("to_user_id") == "user-B")
        check("missing: offline text is the fixed sentence only",
              job.get("text") == "蛋挞现在不在线，消息已收到，回来后会回复你。"
              and "secret user words" not in job.get("text", "")
              and "CTX-FAKE" not in job.get("text", ""))
        evs = [e for e in _timeline(bridge.STATE)
               if e["stage"] == "offline_receipt"]
        check("missing: one offline timeline event on formal job",
              len(evs) == 1 and evs[0]["job"] == "8001"
              and evs[0]["source"] == "bridge_offline")
        ack = bridge._load_offline_receipts().get("8001", {})
        check("missing: ack persisted ok",
              ack.get("ok") is True and ack.get("client_id") == "offline-8001")
        check("missing: repeat call queues nothing",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 600.0) == [])
        check("missing: still exactly one offline job file",
              os.path.exists(p) and len(
                  [n for n in os.listdir(bridge.OUTBOX_PENDING)
                   if n.endswith("_offline.json")]) == 1)
        check("missing: post-restart call queues nothing",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 3600.0) == [])
        check("missing: inbox record still present",
              os.path.exists(os.path.join(bridge.INBOX, "8001.json")))
    finally:
        _restore(old)


def test_stale_boundary_and_bad_heartbeats():
    tmp = tempfile.mkdtemp(prefix="off-boundary-")
    old = _setup(tmp)
    try:
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE - 120),
                                        "source": "worker"})
        check("boundary: exactly 120s old is still online",
              bridge.is_worker_offline(now=BASE) is False)
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE - 121),
                                        "source": "worker"})
        check("boundary: 121s old is offline",
              bridge.is_worker_offline(now=BASE) is True)
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE - 119),
                                        "source": "worker"})
        check("boundary: 119s old is online",
              bridge.is_worker_offline(now=BASE) is False)
        for label, doc in (
                ("bad-json", "{not json"),
                ("non-dict", [1, 2, 3]),
                ("missing-ts", {"schema": "worker-heartbeat-0.1",
                                "source": "worker"}),
                ("naive-ts", {"schema": "worker-heartbeat-0.1",
                              "ts": "2026-11-01T03:00:00",
                              "source": "worker"}),
                ("garbage-ts", {"schema": "worker-heartbeat-0.1",
                                "ts": "yesterday-ish",
                                "source": "worker"})):
            _write_heartbeat(bridge.STATE, doc)
            check("bad-heartbeat: %s is offline, no raise" % label,
                  bridge.is_worker_offline(now=BASE) is True)
        # Valid ts without a schema field is still liveness: the
        # reader is deliberately lenient on schema so a writer
        # version skew cannot flip a live Worker offline; only
        # the timestamp decides.
        _write_heartbeat(bridge.STATE, {"ts": _iso(BASE - 5),
                                        "source": "worker"})
        check("bad-heartbeat: valid ts without schema still online",
              bridge.is_worker_offline(now=BASE) is False)
    finally:
        _restore(old)


def test_burst_one_per_message():
    tmp = tempfile.mkdtemp(prefix="off-burst-")
    old = _setup(tmp)
    try:
        for mid in ("8001", "8002", "8003"):
            _write_inbox(bridge.INBOX, mid)
            _write_timeline(bridge.STATE, [(mid, "received", BASE - 200)])
        got = bridge.maybe_send_offline_receipts(_session(), now=BASE)
        check("burst: one receipt per distinct message",
              got == ["8001", "8002", "8003"])
        check("burst: three offline jobs, no more",
              len([n for n in os.listdir(bridge.OUTBOX_PENDING)
                   if n.endswith("_offline.json")]) == 3)
        check("burst: repeat call queues nothing",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 60.0) == [])
    finally:
        _restore(old)


def test_formal_job_suppresses():
    tmp = tempfile.mkdtemp(prefix="off-supp-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        # Received long ago, so the grace gate is satisfied and
        # only the formal-job rule can suppress here.
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        formal = {"text": "formal reply", "to_user_id": "user-B",
                  "client_id": "c-formal"}
        fpath = os.path.join(bridge.OUTBOX_PENDING, "8001.json")
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(formal, f, ensure_ascii=False)
        before = open(fpath, encoding="utf-8").read()
        check("suppress: formal in pending suppresses offline receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE) == [])
        check("suppress: formal job file untouched",
              open(fpath, encoding="utf-8").read() == before)
        check("suppress: no offline job anywhere",
              not os.path.exists(_offline_path(bridge.OUTBOX_PENDING, "8001"))
              and not os.path.exists(_offline_path(bridge.OUTBOX_SENT, "8001")))
    finally:
        _restore(old)


def test_recovery_formal_flows_normally():
    tmp = tempfile.mkdtemp(prefix="off-recover-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        bridge.maybe_send_offline_receipts(_session(), now=BASE)
        check("recovery: offline receipt queued while offline",
              os.path.exists(_offline_path(bridge.OUTBOX_PENDING, "8001")))
        check("recovery: inbox record still present after receipt",
              os.path.exists(os.path.join(bridge.INBOX, "8001.json")))
        # Worker comes back: fresh heartbeat, then a formal reply.
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE + 10),
                                        "source": "worker"})
        check("recovery: fresh heartbeat is online, no new receipt",
              bridge.maybe_send_offline_receipts(
                  _session(), now=BASE + 10.0) == [])
        fakes = _Fakes()
        sent, _failed = _drain("8001.json",
                               {"text": "formal reply intact",
                                "to_user_id": "user-B", "client_id": "c-formal"},
                               fakes)
        formal = _read_json(os.path.join(bridge.OUTBOX_SENT, "8001.json"))
        check("recovery: formal reply drains intact after offline receipt",
              "8001.json" in sent and formal.get("text") == "formal reply intact"
              and "formal reply intact" in fakes.text_calls)
        check("recovery: no evidence-audit row for either job",
              not os.path.exists(os.path.join(bridge.STATE, "evidence_audit.jsonl")))
        view = bridge.get_message_observability("8001")
        check("recovery: view shows offline_receipt_at, formal sent",
              view["offline_receipt_at"] is not None
              and view["stages"]["sent"] is not None
              and view["outcome"] == "sent")
    finally:
        _restore(old)


def test_grace_boundaries():
    tmp = tempfile.mkdtemp(prefix="off-grace-")
    old = _setup(tmp)
    try:
        # No heartbeat file: stale by definition. All three
        # messages were received at BASE.
        for mid in ("8001", "8002", "8003"):
            _write_inbox(bridge.INBOX, mid)
            _write_timeline(bridge.STATE, [(mid, "received", BASE)])
        check("grace: 179s waited, no receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 179.0) == [])
        check("grace: exactly 180s waited, no receipt (strictly greater)",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 180.0) == [])
        check("grace: 181s waited, receipts queue",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 181.0)
              == ["8001", "8002", "8003"])
    finally:
        _restore(old)


def test_fresh_heartbeat_long_wait_never_offline():
    tmp = tempfile.mkdtemp(prefix="off-freshlong-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        _write_timeline(bridge.STATE, [("8001", "received", BASE)])
        # Heartbeat 10s old at call time: fresh, even though the
        # message has waited 1000s. A live-but-backlogged Worker
        # is not offline; saying so would be a lie.
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE + 990),
                                        "source": "worker"})
        check("fresh-long: fresh heartbeat + 1000s wait, no receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 1000.0) == [])
        check("fresh-long: no offline job file",
              not os.path.exists(_offline_path(bridge.OUTBOX_PENDING, "8001")))
    finally:
        _restore(old)


def test_no_received_event_never_offline():
    tmp = tempfile.mkdtemp(prefix="off-norecv-")
    old = _setup(tmp)
    try:
        # No heartbeat file (stale) but no timeline at all: the
        # wait cannot be computed, so never guess, never send.
        _write_inbox(bridge.INBOX, "8001")
        check("no-received: no timeline at all, no receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 10000.0) == [])
        # A garbage line and a non-received stage still leave
        # no 'received' event for this message.
        with open(os.path.join(bridge.STATE, "message_timeline.jsonl"),
                  "a", encoding="utf-8") as f:
            f.write("not json at all\n")
        _write_timeline(bridge.STATE, [("8001", "progress", BASE)])
        check("no-received: garbage + non-received rows, no receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 10000.0) == [])
        check("no-received: no offline job file",
              not os.path.exists(_offline_path(bridge.OUTBOX_PENDING, "8001")))
    finally:
        _restore(old)


def test_mutual_exclusion_with_progress():
    tmp = tempfile.mkdtemp(prefix="off-excl-")
    old = _setup(tmp)
    try:
        # Grace revision: suppression is PER MESSAGE, not global.
        # 1. Stale (no heartbeat file) + within grace: progress
        # fires ("still working" is true; the Worker may be on
        # its way), offline does not.
        _write_inbox(bridge.INBOX, "8001")
        _write_timeline(bridge.STATE, [("8001", "received", BASE)])
        check("exclusion: stale within grace, progress still queues",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0)
              == ["8001"])
        check("exclusion: stale within grace, no offline receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 60.0) == [])
        check("exclusion: progress job exists for within-grace message",
              os.path.exists(
                  os.path.join(bridge.OUTBOX_PENDING, "8001_progress.json")))
        # 2. Same message past grace: offline fires, and from
        # then on that message's progress is suppressed (the
        # "will reply on return" promise stands; no third note).
        check("exclusion: past grace, offline receipt queues",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 200.0)
              == ["8001"])
        check("exclusion: after offline ack, progress suppressed",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 200.0) == [])
        # 3. Fresh heartbeat + long wait: never offline, and
        # progress still works (the online path is intact).
        _write_heartbeat(bridge.STATE, {"schema": "worker-heartbeat-0.1",
                                        "ts": _iso(BASE + 1000),
                                        "source": "worker"})
        _write_timeline(bridge.STATE, [("8002", "received", BASE)])
        check("exclusion: fresh heartbeat long wait, no offline receipt",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 1000.0) == [])
        check("exclusion: fresh heartbeat long wait, progress queues",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 1000.0)
              == ["8002"])
        # 4. Asymmetric reverse, preserved: progress first (within
        # grace, heartbeat now stale again), then offline past
        # grace is still allowed — a genuine status change.
        os.remove(os.path.join(bridge.STATE, "worker_heartbeat.json"))
        _write_inbox(bridge.INBOX, "8003")
        _write_timeline(bridge.STATE, [("8003", "received", BASE)])
        check("exclusion: progress-first message queues progress",
              bridge.maybe_send_progress_receipts(_session(), now=BASE + 60.0)
              == ["8003"])
        check("exclusion: progress-first message still gets offline",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 200.0)
              == ["8003"])
    finally:
        _restore(old)


def test_offline_failure_no_block_no_retry():
    tmp = tempfile.mkdtemp(prefix="off-fail-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        orig_atomic = bridge.atomic_write_json

        def _flaky(path, obj, mode=0o600):
            if os.path.basename(path).endswith("_offline.json"):
                raise OSError("simulated queue failure")
            return orig_atomic(path, obj, mode=mode)

        bridge.atomic_write_json = _flaky
        try:
            got = bridge.maybe_send_offline_receipts(_session(), now=BASE)
        finally:
            bridge.atomic_write_json = orig_atomic
        ack = bridge._load_offline_receipts().get("8001", {})
        check("fail: failed queue returns nothing, ack ok=False",
              got == [] and ack.get("ok") is False)
        check("fail: no offline timeline event on failure",
              [e for e in _timeline(bridge.STATE)
               if e["stage"] == "offline_receipt"] == [])
        check("fail: ack blocks retry",
              bridge.maybe_send_offline_receipts(_session(), now=BASE + 60.0) == [])
        check("fail: inbox record still present",
              os.path.exists(os.path.join(bridge.INBOX, "8001.json")))
        fakes = _Fakes()
        sent, _failed = _drain("8001.json",
                               {"text": "formal still works",
                                "to_user_id": "user-B", "client_id": "c-formal"},
                               fakes)
        check("fail: formal reply unaffected by offline failure",
              "8001.json" in sent and "formal still works" in fakes.text_calls)
    finally:
        _restore(old)


def test_view_and_summary_exclude_offline():
    tmp = tempfile.mkdtemp(prefix="off-view-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        bridge.maybe_send_offline_receipts(_session(), now=BASE)
        view = bridge.get_message_observability("8001")
        check("view: offline_receipt_at set, stages.sent stays None",
              view["offline_receipt_at"] is not None
              and view["stages"]["sent"] is None)
        # Move the offline job to sent/ and add one formal sent job:
        # the summary must count only the formal one as sent, and
        # a pending offline job must not count as pending.
        os.replace(_offline_path(bridge.OUTBOX_PENDING, "8001"),
                    _offline_path(bridge.OUTBOX_SENT, "8001"))
        with open(os.path.join(bridge.OUTBOX_SENT, "8002.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"text": "formal", "to_user_id": "user-B"}, f)
        with open(os.path.join(bridge.OUTBOX_PENDING, "8003_offline.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"text": bridge.OFFLINE_RECEIPT_TEXT,
                       "to_user_id": "user-B"}, f)
        s = bridge.get_observability_summary()
        check("summary: offline jobs excluded from sent and pending",
              s["sent"] == 1 and s["pending"] == 0)
        check("summary: offline jobs in no bucket",
              s["failed"] == 0 and s["needs_check"] == 0
              and s["skipped_empty"] == 0 and s["unreadable"] == 0)
    finally:
        _restore(old)


def test_session_and_junk_guards():
    tmp = tempfile.mkdtemp(prefix="off-guard-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        with open(os.path.join(bridge.INBOX, "not-a-number.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"text": "junk"}, f)
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        check("guard: session without user id triggers nothing",
              bridge.maybe_send_offline_receipts({}, now=BASE) == []
              and bridge.maybe_send_offline_receipts(
                  {"context_token": "CTX-FAKE"}, now=BASE) == [])
        got = bridge.maybe_send_offline_receipts(_session(), now=BASE)
        check("guard: non-numeric inbox file ignored, numeric served",
              got == ["8001"])
        check("guard: no offline job for junk file",
              not os.path.exists(
                  os.path.join(bridge.OUTBOX_PENDING, "not-a-number_offline.json")))
    finally:
        _restore(old)


def test_offline_job_file_suppresses_without_ack():
    tmp = tempfile.mkdtemp(prefix="off-noack-")
    old = _setup(tmp)
    try:
        _write_inbox(bridge.INBOX, "8001")
        _write_timeline(bridge.STATE, [("8001", "received", BASE - 200)])
        # Ack file deleted/lost, but the offline job already
        # exists in sent/: still no second receipt (double guard).
        with open(_offline_path(bridge.OUTBOX_SENT, "8001"),
                  "w", encoding="utf-8") as f:
            json.dump({"text": bridge.OFFLINE_RECEIPT_TEXT,
                       "to_user_id": "user-B",
                       "client_id": "offline-8001"}, f)
        check("noack: existing offline job suppresses without ack",
              bridge.maybe_send_offline_receipts(_session(), now=BASE) == [])
        check("noack: still exactly one offline job file",
              len([n for n in os.listdir(bridge.OUTBOX_SENT)
                   if n.endswith("_offline.json")]) == 1)
    finally:
        _restore(old)


def main():
    test_fresh_heartbeat_online()
    test_grace_boundaries()
    test_fresh_heartbeat_long_wait_never_offline()
    test_no_received_event_never_offline()
    test_missing_heartbeat_offline_once()
    test_stale_boundary_and_bad_heartbeats()
    test_burst_one_per_message()
    test_formal_job_suppresses()
    test_recovery_formal_flows_normally()
    test_mutual_exclusion_with_progress()
    test_offline_failure_no_block_no_retry()
    test_view_and_summary_exclude_offline()
    test_session_and_junk_guards()
    test_offline_job_file_suppresses_without_ack()
    failed = [n for n, c in ok if not c]
    print(f"{len(ok) - len(failed)}/{len(ok)} passed")
    if failed:
        print("FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
