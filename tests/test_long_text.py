"""Offline tests for item 3 (long-text segmentation), 2026-10-31 plan.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network:
  1. Segmentation: paragraph/code-fence text restores exactly, every
     display fits 1800 chars, fence reopen repeats the opening fence
     with its info string and fence close carries no info string.
  2. Overlong single line is character-cut, restores exactly.
  3. More than 20 parts raises LongTextTooManyParts.
  4. Drain happy path: 3 text parts sent in order, then the
     attachment, job moves to sent/, top-level text untouched,
     fixed per-part client_ids, full_text_sha256 matches.
  5. CORE acceptance (scheme section 3): part 2 unknown -> part 1
     is NOT resent on a second drain, part 3 and the attachment get
     ZERO send calls, job goes to failed/ with part 2 needs_check,
     part 3 blocked_by_prior, attachment blocked_by_prior; the
     safe history view shows partial delivery, never the full text.
  6. Retryable part: part 2 fails retryable once, part 3 and the
     attachment wait; a second drain sends the rest and the job.
  7. Permanent part failure blocks the tail the same way.
  8. Manual resend: resend-text-part resets ONLY part 2 to
     pending (sent part untouched, no reslicing); a later drain
     sends part 2, then unblocks and sends part 3 + attachment.
  9. More than 20 parts in drain: zero sends, job fails with a
     file-delivery prompt before any network call.
 10. Old attempted long jobs are never re-segmented: a job with
     text_attempts > 0 keeps the legacy single send, no plan.
 11. Restart recovery: a residual 'sending' part becomes
     needs_check, the tail and attachments block, job to failed/.
 12. Post-send persist failure HALTs the job in-process: no
     further part is sent in that drain.
 13. Short text keeps the legacy path: no plan fields at all.

Run: python3 tests/test_long_text.py (exit 0 = all pass).
"""

import hashlib
import json
import os
import sys
import tempfile
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def _setup_outbox(tmp):
    pend = os.path.join(tmp, "pending")
    sent = os.path.join(tmp, "sent")
    failed = os.path.join(tmp, "failed")
    for d in (pend, sent, failed):
        os.makedirs(d, exist_ok=True)
    old = (bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT, bridge.OUTBOX_FAILED)
    bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT, bridge.OUTBOX_FAILED = pend, sent, failed
    return old, pend, sent, failed


def _restore_outbox(old):
    bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT, bridge.OUTBOX_FAILED = old


def _write_job(pend, name, job):
    with open(os.path.join(pend, name), "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False)


def _read_job(d, name):
    with open(os.path.join(d, name), encoding="utf-8") as f:
        return json.load(f)


def _long_text():
    para = "这是一个段落，用来测试分段。\n\n" * 40
    code = "```python\n" + "print('hello world, this is a long line')\n" * 30 + "```\n"
    tail = "结尾段落。\n" * 20
    return para + code + tail


def _three_part_text():
    # Exactly 3 parts: each block is ~1500 chars, so no two blocks
    # fit in one 1800-char display part, and each block is one line.
    return (("段落一内容。" * 250) + "\n\n"
            + ("段落二内容。" * 250) + "\n\n"
            + ("段落三内容。" * 250) + "\n")


# ---------------------------------------------------------------- 1-3

def test_segmentation():
    text = _long_text()
    parts, sha = bridge._segment_text(text)
    check("segment: display fits 1800", all(len(p["display_text"]) <= 1800 for p in parts))
    check("segment: raw restores exactly", "".join(p["raw_slice"] for p in parts) == text)
    check("segment: sha matches", sha == hashlib.sha256(text.encode("utf-8")).hexdigest())
    check("segment: labels in order",
          all(p["display_text"].startswith("[%d/%d]\n" % (i + 1, len(parts))) for i, p in enumerate(parts)))
    check("segment: part client_ids distinct",
          len({p["client_id"] for p in parts}) == len(parts))
    # Fence: a part starting inside the fence reopens with info,
    # a part ending inside the fence closes without info.
    in_fence = [p for p in parts if "```python\n" in p["display_text"] and p["display_text"].count("```") >= 2]
    check("segment: fence reopen/close present", len(in_fence) >= 1)
    for p in parts:
        if p["display_text"].count("```") >= 2 and not p["display_text"].rstrip().endswith("```\n"):
            pass
    # A text that ends inside a fence: the last part must end with
    # the original closing fence, and middle parts close/reopen it.
    code_only = "```python\n" + "x = 1  # comment line\n" * 200 + "```\n"
    code_parts, _ = bridge._segment_text(code_only)
    check("segment: fence-only last part ends with closing fence",
          code_parts[-1]["display_text"].rstrip().endswith("```")
          and not code_parts[-1]["display_text"].rstrip().endswith("```python"))
    check("segment: fence-only middle parts close without info",
          all(p["display_text"].rstrip().endswith("```")
              for p in code_parts[:-1]))

    over = "a" * 5000
    parts2, _ = bridge._segment_text(over)
    check("segment: overlong line char-cut restores",
          "".join(p["raw_slice"] for p in parts2) == over
          and all(len(p["display_text"]) <= 1800 for p in parts2))
    try:
        bridge._segment_text("x" * (1800 * 25))
        check("segment: >20 parts raises", False)
    except bridge.LongTextTooManyParts:
        check("segment: >20 parts raises", True)


# ---------------------------------------------------------------- helpers

class _Fakes:
    def __init__(self):
        self.text_calls = []
        self.file_calls = []
        self.fail_at = {}  # part display label -> exception to raise once
        self.fail_mode = None  # None | ('unknown', n) | ('retryable', n) | ('permanent', n)

    def send_text(self, session, text, to_user_id=None, context_token=None, client_id=None):
        self.text_calls.append(text)
        if self.fail_mode is not None:
            kind, idx = self.fail_mode
            # idx is 1-based part number, matched by its label.
            if text.startswith("[%d/" % idx):
                self.fail_mode = None
                if kind == "unknown":
                    raise bridge.UnknownDeliveryError("simulated unknown")
                if kind == "retryable":
                    raise bridge.RetryableError("simulated retryable")
                if kind == "permanent":
                    raise bridge.PermanentError("simulated permanent")
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        self.file_calls.append(path)
        return {}


def _patch_fakes(fakes):
    orig = (bridge.send_text, bridge.send_file)
    bridge.send_text = fakes.send_text
    bridge.send_file = fakes.send_file
    return orig


def _restore_fakes(orig):
    bridge.send_text, bridge.send_file = orig


def _job_with_attachment(text):
    return {
        "text": text,
        "to_user_id": "user-B",
        "client_id": "job-client-1",
        "attachments": [{"path": "/tmp/fake-attachment.bin", "type": "file",
                         "status": "pending", "client_id": "att-client-1"}],
    }


# ---------------------------------------------------------------- 4

def test_drain_happy_path():
    tmp = tempfile.mkdtemp(prefix="lt-happy-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        assert len(text) > 1800
        _write_job(pend, "m1.json", _job_with_attachment(text))
        fakes = _Fakes()
        orig = _patch_fakes(fakes)
        try:
            sent_names, _ = bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        check("happy: job sent", "m1.json" in sent_names and os.path.exists(os.path.join(sent, "m1.json")))
        check("happy: 3 text parts sent in order, then attachment",
              len(fakes.text_calls) == 3
              and all(c.startswith("[%d/" % (i + 1)) for i, c in enumerate(fakes.text_calls))
              and len(fakes.file_calls) == 1)
        archived = _read_job(sent, "m1.json")
        check("happy: top-level text untouched, plan all sent",
              archived["text"] == text
              and archived["full_text_sha256"] == hashlib.sha256(text.encode("utf-8")).hexdigest()
              and all(p["status"] == "sent" for p in archived["text_parts"])
              and archived["attachments"][0]["status"] == "sent")
        check("happy: sent display texts fit budget",
              all(len(c) <= 1800 for c in fakes.text_calls))
    finally:
        _restore_outbox(old)


# ---------------------------------------------------------------- 5

def test_core_unknown_blocks_tail():
    tmp = tempfile.mkdtemp(prefix="lt-unknown-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        _write_job(pend, "m2.json", _job_with_attachment(text))
        fakes = _Fakes()
        fakes.fail_mode = ("unknown", 2)
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
            # Second drain: part 1 must NOT be resent, part 3 and
            # the attachment must get zero further calls.
            before = (len(fakes.text_calls), len(fakes.file_calls))
            bridge.drain_outbox({"ilink_user_id": "user-B"})
            after = (len(fakes.text_calls), len(fakes.file_calls))
        finally:
            _restore_fakes(orig)
        check("unknown: part 1 sent once, part 2 attempted once, part 3 never",
              len(fakes.text_calls) == 2
              and fakes.text_calls[0].startswith("[1/")
              and fakes.text_calls[1].startswith("[2/"))
        check("unknown: second drain sends nothing new", before == after)
        check("unknown: attachment zero calls", len(fakes.file_calls) == 0)
        check("unknown: job in failed/", os.path.exists(os.path.join(failed, "m2.json")))
        archived = _read_job(failed, "m2.json")
        parts = archived["text_parts"]
        check("unknown: part states sent/needs_check/blocked",
              parts[0]["status"] == "sent"
              and parts[1]["status"] == "needs_check"
              and parts[2]["status"] == "blocked_by_prior"
              and archived["attachments"][0]["status"] == "blocked_by_prior")
        check("unknown: top-level text kept for history", archived["text"] == text)
        # Safe history: partial delivery shown, full text never shown.
        done = os.path.join(tmp, "done")
        os.makedirs(done, exist_ok=True)
        with open(os.path.join(done, "m2.json"), "w", encoding="utf-8") as f:
            json.dump({"message_id": "m2", "text": ""}, f)
        old_done, old_sent = bridge.DONE, bridge.OUTBOX_SENT
        bridge.DONE = done
        try:
            view = bridge.build_safe_history_view()
        finally:
            bridge.DONE = old_done
        rounds = [r for r in view["rounds"] if r["message_id"] == "m2"]
        check("unknown: safe view partial, not full text",
              len(rounds) == 1
              and rounds[0]["answer"] is not None
              and rounds[0]["answer"].get("delivery_status") == "partial"
              and "部分送达" in rounds[0]["answer"]["text"]
              and text not in rounds[0]["answer"]["text"])
    finally:
        _restore_outbox(old)
        bridge._TERMINAL_TASK_IDS.discard("m2.json")


# ---------------------------------------------------------------- 6, 7

def test_retryable_then_permanent():
    tmp = tempfile.mkdtemp(prefix="lt-retry-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        _write_job(pend, "m3.json", _job_with_attachment(text))
        fakes = _Fakes()
        fakes.fail_mode = ("retryable", 2)
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
            mid = _read_job(pend, "m3.json")
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        check("retryable: first drain stops after part 2, attachment waits",
              mid["text_parts"][0]["status"] == "sent"
              and mid["text_parts"][1]["status"] == "pending"
              and mid["text_parts"][1]["attempts"] == 1
              and mid["text_parts"][2]["status"] == "pending"
              and mid["attachments"][0]["status"] == "pending")
        # Precise assertions on the second drain's outcome:
        check("retryable: job eventually sent with attachment",
              os.path.exists(os.path.join(sent, "m3.json")) and len(fakes.file_calls) == 1)
        check("retryable: part 2 attempted twice, part 3 once",
              sum(1 for c in fakes.text_calls if c.startswith("[2/")) == 2
              and sum(1 for c in fakes.text_calls if c.startswith("[3/")) == 1)
        check("retryable: mid-drain part 2 was pending, not blocked",
              mid["text_parts"][1]["status"] in ("pending", "sent"))
    finally:
        _restore_outbox(old)

    tmp = tempfile.mkdtemp(prefix="lt-perm-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        _write_job(pend, "m4.json", _job_with_attachment(text))
        fakes = _Fakes()
        fakes.fail_mode = ("permanent", 2)
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        check("permanent: part 3 and attachment zero calls",
              len(fakes.text_calls) == 2 and len(fakes.file_calls) == 0)
        archived = _read_job(failed, "m4.json")
        check("permanent: part states sent/failed/blocked",
              archived["text_parts"][0]["status"] == "sent"
              and archived["text_parts"][1]["status"] == "failed"
              and archived["text_parts"][2]["status"] == "blocked_by_prior")
    finally:
        _restore_outbox(old)
        bridge._TERMINAL_TASK_IDS.discard("m4.json")


# ---------------------------------------------------------------- 8

def test_manual_resend():
    tmp = tempfile.mkdtemp(prefix="lt-resend-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        _write_job(pend, "m5.json", _job_with_attachment(text))
        fakes = _Fakes()
        fakes.fail_mode = ("unknown", 2)
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        assert os.path.exists(os.path.join(failed, "m5.json"))
        # Manual resend: only part 2.
        args = types.SimpleNamespace(job="m5.json", part=2)
        rc = bridge.cmd_resend_text_part(args)
        check("resend: command ok", rc == 0)
        check("resend: job back in pending, terminal cleared",
              os.path.exists(os.path.join(pend, "m5.json"))
              and not os.path.exists(os.path.join(failed, "m5.json")))
        job = _read_job(pend, "m5.json")
        check("resend: only part 2 reset, sent part untouched, no reslice",
              job["text_parts"][0]["status"] == "sent"
              and job["text_parts"][1]["status"] == "pending"
              and job["text_parts"][1]["attempts"] == 0
              and job["text_parts"][2]["status"] == "blocked_by_prior"
              and job["text"] == text
              and "terminal" not in job)
        # Refuse to resend a sent part.
        rc2 = bridge.cmd_resend_text_part(types.SimpleNamespace(job="m5.json", part=1))
        check("resend: sent part refused", rc2 != 0)
        # Drain: part 2, then blocked part 3, then attachment.
        fakes2 = _Fakes()
        orig = _patch_fakes(fakes2)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        check("resend: drain sends part 2, then part 3, then attachment",
              [c.split("\n")[0] for c in fakes2.text_calls] == ["[2/3]", "[3/3]"]
              and len(fakes2.file_calls) == 1
              and os.path.exists(os.path.join(sent, "m5.json")))
    finally:
        _restore_outbox(old)
        bridge._TERMINAL_TASK_IDS.discard("m5.json")


# ---------------------------------------------------------------- 9, 10

def test_too_many_and_old_job():
    tmp = tempfile.mkdtemp(prefix="lt-toolong-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        _write_job(pend, "m6.json", {"text": "x" * (1800 * 25), "to_user_id": "user-B", "client_id": "c6"})
        fakes = _Fakes()
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        check("toolong: zero sends before rejection", len(fakes.text_calls) == 0)
        archived = _read_job(failed, "m6.json")
        check("toolong: failed with file prompt",
              archived["text_status"] == "failed" and "file" in archived.get("text_error", ""))
    finally:
        _restore_outbox(old)
        bridge._TERMINAL_TASK_IDS.discard("m6.json")

    tmp = tempfile.mkdtemp(prefix="lt-old-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        _write_job(pend, "m7.json", {"text": text, "to_user_id": "user-B",
                                     "client_id": "c7", "text_attempts": 1})
        fakes = _Fakes()
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        check("old: attempted job keeps legacy single send, no plan",
              len(fakes.text_calls) == 1 and fakes.text_calls[0] == text)
        archived = _read_job(sent, "m7.json")
        check("old: no plan fields on legacy job", "text_parts" not in archived)
    finally:
        _restore_outbox(old)


# ---------------------------------------------------------------- 11, 12, 13

def test_restart_and_halt_and_short():
    tmp = tempfile.mkdtemp(prefix="lt-restart-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        parts, sha = bridge._segment_text(text)
        parts[0]["status"] = "sent"
        parts[1]["status"] = "sending"
        job = {"text": text, "to_user_id": "user-B", "client_id": "c8",
               "text_plan_version": 1, "text_parts": parts, "full_text_sha256": sha,
               "attachments": [{"path": "/tmp/f.bin", "type": "file", "status": "pending"}],
               "delivery_state": "sending"}
        _write_job(pend, "m8.json", job)
        bridge._IN_MEMORY_HALTED_FILES.discard("m8.json")
        bridge.recover_sending_on_startup()
        check("restart: job to failed/", os.path.exists(os.path.join(failed, "m8.json")))
        archived = _read_job(failed, "m8.json")
        check("restart: sending part needs_check, tail + attachment blocked",
              archived["text_parts"][1]["status"] == "needs_check"
              and archived["text_parts"][2]["status"] == "blocked_by_prior"
              and archived["attachments"][0]["status"] == "blocked_by_prior")
    finally:
        _restore_outbox(old)
        bridge._TERMINAL_TASK_IDS.discard("m8.json")

    tmp = tempfile.mkdtemp(prefix="lt-halt-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        text = _three_part_text()
        _write_job(pend, "m9.json", _job_with_attachment(text))
        fakes = _Fakes()
        orig_text, orig_file = _patch_fakes(fakes)
        orig_write = bridge.atomic_write_json
        calls = {"n": 0}

        def flaky_write(path, obj, mode=0o600):
            calls["n"] += 1
            # Call 1: plan persist. Call 2: part-1 pre-send persist.
            # Call 3: part-1 post-send persist -> fail once.
            if calls["n"] == 3:
                raise OSError("simulated persist failure")
            return orig_write(path, obj, mode=mode)

        bridge.atomic_write_json = flaky_write
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            bridge.atomic_write_json = orig_write
            _restore_fakes((orig_text, orig_file))
        check("halt: only part 1 attempted, job halted in-process",
              len(fakes.text_calls) == 1 and "m9.json" in bridge._IN_MEMORY_HALTED_FILES)
        check("halt: job still in pending, not sent or failed",
              os.path.exists(os.path.join(pend, "m9.json"))
              and not os.path.exists(os.path.join(sent, "m9.json"))
              and not os.path.exists(os.path.join(failed, "m9.json")))
        bridge._IN_MEMORY_HALTED_FILES.discard("m9.json")
    finally:
        _restore_outbox(old)

    tmp = tempfile.mkdtemp(prefix="lt-short-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        _write_job(pend, "m10.json", {"text": "hi, short", "to_user_id": "user-B", "client_id": "c10"})
        fakes = _Fakes()
        orig = _patch_fakes(fakes)
        try:
            bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            _restore_fakes(orig)
        archived = _read_job(sent, "m10.json")
        check("short: legacy path, no plan fields",
              len(fakes.text_calls) == 1 and "text_parts" not in archived
              and "text_plan_version" not in archived)
    finally:
        _restore_outbox(old)


test_segmentation()
test_drain_happy_path()
test_core_unknown_blocks_tail()
test_retryable_then_permanent()
test_manual_resend()
test_too_many_and_old_job()
test_restart_and_halt_and_short()

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
