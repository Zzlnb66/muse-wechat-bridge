"""Offline tests for the evidence audit log (item: evidence audit).

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network, and never touching the real running
bridge's state/, inbox/ or outbox/:

  1. Completed evidence -> evidence_before_reply true, hashes match,
     schema evidence-audit-0.1, reply hash/len match, audit file 0600.
  2. Missing evidence -> evidence_before_reply false, hashes null.
  3. Partial evidence -> evidence_before_reply true, state partial.
  4. Broken state.json -> state unknown, no exception, job still sent.
  5. Audit write failure -> job still sent with delivery sent, a
     failures/ marker exists with kind + error type only, and the
     marker contains no reply text.
  6. Non-video reply -> no audit file at all.
  7. Local-only video reply -> no audit, even with completed evidence.
  8. Segmented long-text job -> audited exactly once, after all
     parts and the attachment are sent, with the full text hash/len.
  9. Failed job (permanent send error) -> no audit, job to failed/.

Honesty rule, repeated in the implementation docstring: an audit
record proves only that evidence existed at send time, with its
state and hash. It does NOT prove that the Worker actually read
that evidence, and no test here claims otherwise.

Run: python3 tests/test_evidence_audit.py (exit 0 = all pass).
"""

import hashlib
import json
import os
import stat
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


class _Fakes:
    def __init__(self, fail_text=False):
        self.text_calls = []
        self.file_calls = []
        self.fail_text = fail_text

    def send_text(self, session, text, to_user_id=None, context_token=None, client_id=None):
        if self.fail_text:
            raise bridge.PermanentError("simulated permanent")
        self.text_calls.append(text)
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        self.file_calls.append(path)
        return {}


def _setup(tmp):
    """Patch every bridge directory global to tmp subdirs. Returns old."""
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
    return old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _write_job(pend, name, job):
    _write_json(os.path.join(pend, name), job)


def _read_job(d, name):
    with open(os.path.join(d, name), encoding="utf-8") as f:
        return json.load(f)


def _analysis(mid, idx, state, evidence=None, narrative=None, broken_state=False):
    outdir = os.path.join(bridge.ANALYSIS_DIR, "%s_%d" % (mid, idx))
    os.makedirs(outdir, exist_ok=True)
    if broken_state:
        with open(os.path.join(outdir, "state.json"), "w", encoding="utf-8") as f:
            f.write("{not json")
    elif state is not None:
        _write_json(os.path.join(outdir, "state.json"), {"state": state})
    if evidence is not None:
        with open(os.path.join(outdir, "evidence.json"), "w", encoding="utf-8") as f:
            f.write(evidence)
    if narrative is not None:
        with open(os.path.join(outdir, "narrative.md"), "w", encoding="utf-8") as f:
            f.write(narrative)
    return outdir


def _done(mid, videos=True, local_only=False):
    rec = {"message_id": int(mid), "text": "", "from_user_id": "user-B",
           "history_class": "user"}
    if videos:
        rec["videos"] = [{"path": "/tmp/fake-video.mp4"}]
    if local_only:
        rec["local_only"] = True
    _write_json(os.path.join(bridge.DONE, "%s.json" % mid), rec)


def _audit_lines():
    path = os.path.join(bridge.STATE, "evidence_audit.jsonl")
    if not os.path.exists(path) or os.path.isdir(path):
        return None
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _drain(job_name, job, fakes):
    _write_job(bridge.OUTBOX_PENDING, job_name, job)
    orig = (bridge.send_text, bridge.send_file)
    bridge.send_text, bridge.send_file = fakes.send_text, fakes.send_file
    try:
        sent, failed = bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file = orig
    return sent, failed


def _job(text):
    return {"text": text, "to_user_id": "user-B", "client_id": "c-audit"}


# ---------------------------------------------------------------- 1
def test_completed():
    tmp = tempfile.mkdtemp(prefix="ea-completed-")
    old = _setup(tmp)
    try:
        mid = "1001"
        _done(mid)
        _analysis(mid, 0, "completed", evidence='{"tracks": []}', narrative="# n")
        sent, _ = _drain("1001.json", _job("video reply here"), _Fakes())
        lines = _audit_lines()
        ev_hash = hashlib.sha256(b'{"tracks": []}').hexdigest()
        nar_hash = hashlib.sha256(b"# n").hexdigest()
        txt = "video reply here"
        check("completed: job sent", "1001.json" in sent)
        check("completed: exactly one audit line", lines is not None and len(lines) == 1)
        if lines:
            r = lines[0]
            check("completed: schema + message_id", r["schema"] == "evidence-audit-0.1" and r["message_id"] == "1001")
            check("completed: evidence_before_reply true", r["evidence_before_reply"] is True)
            check("completed: hashes match", r["evidence_dirs"][0]["evidence_sha256"] == ev_hash
                  and r["evidence_dirs"][0]["narrative_sha256"] == nar_hash
                  and r["evidence_dirs"][0]["state_status"] == "completed")
            check("completed: reply hash/len match, no reply text stored",
                  r["reply_text_sha256"] == hashlib.sha256(txt.encode("utf-8")).hexdigest()
                  and r["reply_text_len"] == len(txt)
                  and txt not in json.dumps(r, ensure_ascii=False))
        mode = stat.S_IMODE(os.stat(os.path.join(bridge.STATE, "evidence_audit.jsonl")).st_mode)
        check("completed: audit file 0600", mode == 0o600)
    finally:
        _restore(old)


# ---------------------------------------------------------------- 2, 3, 4
def test_missing_partial_broken():
    tmp = tempfile.mkdtemp(prefix="ea-states-")
    old = _setup(tmp)
    try:
        # 2. Missing evidence: state completed but no evidence.json.
        _done("2001")
        _analysis("2001", 0, "completed", evidence=None, narrative=None)
        _drain("2001.json", _job("reply two"), _Fakes())
        lines = _audit_lines()
        check("missing: false + null hashes",
              lines is not None and len(lines) == 1
              and lines[0]["evidence_before_reply"] is False
              and lines[0]["evidence_dirs"][0]["evidence_sha256"] is None
              and lines[0]["evidence_dirs"][0]["narrative_sha256"] is None)
        # 3. Partial evidence.
        _done("2002")
        _analysis("2002", 0, "partial", evidence='{"p": 1}', narrative=None)
        _drain("2002.json", _job("reply three"), _Fakes())
        lines = _audit_lines()
        check("partial: true + state partial",
              lines is not None and len(lines) == 2
              and lines[1]["evidence_before_reply"] is True
              and lines[1]["evidence_dirs"][0]["state_status"] == "partial")
        # 4. Broken state.json: unknown, no exception, job still sent.
        _done("2003")
        _analysis("2003", 0, None, evidence='{"x": 1}', broken_state=True)
        sent, _ = _drain("2003.json", _job("reply four"), _Fakes())
        lines = _audit_lines()
        check("broken: unknown + job sent, no raise",
              "2003.json" in sent and lines is not None and len(lines) == 3
              and lines[2]["evidence_dirs"][0]["state_status"] == "unknown"
              and lines[2]["evidence_before_reply"] is False)
    finally:
        _restore(old)


# ---------------------------------------------------------------- 5
def test_audit_write_failure():
    tmp = tempfile.mkdtemp(prefix="ea-failwrite-")
    old = _setup(tmp)
    try:
        mid = "3001"
        _done(mid)
        _analysis(mid, 0, "completed", evidence='{"ok": 1}')
        # Make the audit path itself a directory so the append fails,
        # while the failures/ directory still works.
        os.makedirs(os.path.join(bridge.STATE, "evidence_audit.jsonl"), exist_ok=True)
        sent, _ = _drain("3001.json", _job("secret-reply-text-xyz"), _Fakes())
        check("write-fail: job still sent", "3001.json" in sent)
        archived = _read_job(bridge.OUTBOX_SENT, "3001.json")
        check("write-fail: delivery sent unchanged", archived.get("delivery_state") == "sent")
        fail_files = [n for n in os.listdir(bridge.FAILURES_DIR) if "evidence_audit" in n]
        check("write-fail: failures marker exists", len(fail_files) >= 1)
        if fail_files:
            marker = _read_job(bridge.FAILURES_DIR, fail_files[0])
            blob = json.dumps(marker, ensure_ascii=False)
            check("write-fail: marker has kind only, no reply text",
                  marker.get("kind") == "evidence_audit_failed"
                  and "secret-reply-text-xyz" not in blob)
    finally:
        _restore(old)


# ---------------------------------------------------------------- 6, 7, 9
def test_skips_and_failed():
    tmp = tempfile.mkdtemp(prefix="ea-skips-")
    old = _setup(tmp)
    try:
        # 6. Non-video reply: no done videos, no analysis dirs.
        _done("4001", videos=False)
        _drain("4001.json", _job("just text"), _Fakes())
        check("non-video: no audit file",
              not os.path.exists(os.path.join(bridge.STATE, "evidence_audit.jsonl")))
        # 7. Local-only video: completed evidence present, still skipped.
        _done("4002", videos=True, local_only=True)
        _analysis("4002", 0, "completed", evidence='{"ok": 1}')
        _drain("4002.json", _job("local generic reply"), _Fakes())
        check("local-only: no audit file",
              not os.path.exists(os.path.join(bridge.STATE, "evidence_audit.jsonl")))
        # 9. Failed job: permanent send error, job to failed/, no audit.
        _done("4003")
        _analysis("4003", 0, "completed", evidence='{"ok": 1}')
        _, failed = _drain("4003.json", _job("will fail"), _Fakes(fail_text=True))
        check("failed-job: no audit + job failed",
              not os.path.exists(os.path.join(bridge.STATE, "evidence_audit.jsonl"))
              and os.path.exists(os.path.join(bridge.OUTBOX_FAILED, "4003.json")))
    finally:
        _restore(old)


# ---------------------------------------------------------------- 8
def test_segmented_once():
    tmp = tempfile.mkdtemp(prefix="ea-segmented-")
    old = _setup(tmp)
    try:
        mid = "5001"
        _done(mid)
        _analysis(mid, 0, "completed", evidence='{"seg": 1}')
        text = ("段落内容。" * 400) + "\n\n" + ("更多内容。" * 400) + "\n"
        assert len(text) > 1800
        job = {"text": text, "to_user_id": "user-B", "client_id": "c-seg",
               "attachments": [{"path": "/tmp/fake-att.bin", "type": "file",
                                "status": "pending", "client_id": "att-seg"}]}
        sent, _ = _drain("5001.json", job, _Fakes())
        lines = _audit_lines()
        check("segmented: job sent once", "5001.json" in sent)
        check("segmented: exactly one audit line",
              lines is not None and len(lines) == 1)
        if lines:
            check("segmented: full text hash/len, evidence true",
                  lines[0]["reply_text_sha256"] == hashlib.sha256(text.encode("utf-8")).hexdigest()
                  and lines[0]["reply_text_len"] == len(text)
                  and lines[0]["evidence_before_reply"] is True)
    finally:
        _restore(old)


test_completed()
test_missing_partial_broken()
test_audit_write_failure()
test_skips_and_failed()
test_segmented_once()

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
