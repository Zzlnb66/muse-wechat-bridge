"""Offline tests for the two parent-decided SQLite switch amendments
(2026-11-01, before any dual soak). New file on purpose: the old
test_sqlite_switch.py pins the PRE-amendment gaps and is left
untouched, so its one compare-gap check is EXPECTED to fail after
fix 2 (see that file's test_compare_gap_inbound_processed_lag) --
the parent decision resolves the gap, and only the parent updates
the pin. Nothing here weakens an old expectation.

Fix 1 -- retryable reset edge: a drain pass that keeps a job
pending on an explicitly-undelivered Retryable failure resets the
shadow row sending_in_flight -> ready_to_send (lease cleared), so
the next drain can begin_send again under JSON's immediate
3-attempt retry. Unknown / timeout / interrupt failures never
reset (they go terminal -> needs_check); terminal rows are never
downgraded; segment rows are descriptive mirrors only.

Fix 2 -- benign inbound lag: compare_shadow reports the exact
triple (shadow received, JSON processed, equal record hash) under
"benign_lag" (count + message-id list), NOT as a mismatch, so a
dual soak can be compare-clean while the Worker moves inbox ->
done. Any hash inequality is still a true mismatch; every other
state combination is unchanged; compare stays read-only and adds
no done-sweep write point.

Everything runs in temp dirs with fake sends. Never touches the
real running bridge, never sends a real WeChat message, never
reaches the real upstream.

Run: python3 tests/test_sqlite_switch_fixes.py (exit 0 = all pass).
"""

import json
import os
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402
import bridge_sqlite  # noqa: E402
import test_sqlite_switch as base  # noqa: E402

check = base.check
_Ctx = base._Ctx
_text_job = base._text_job


class _FailFakes:
    """Fake senders that fail on demand with a chosen error class."""

    def __init__(self, text_exc=None, image_exc=None):
        self.text_exc = text_exc
        self.image_exc = image_exc
        self.text_calls = []

    def send_text(self, session, text, to_user_id=None, context_token="",
                  client_id=None):
        self.text_calls.append(text)
        if self.text_exc is not None:
            raise self.text_exc
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        return {}

    def send_image(self, session, path, to_user_id, token, client_id=None):
        if self.image_exc is not None:
            raise self.image_exc
        return {}


def _read_pending(ctx, name):
    with open(os.path.join(bridge.OUTBOX_PENDING, name),
              encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------- fix 1
def test_retryable_reset_text(mode):
    ctx = _Ctx()
    try:
        ctx.set_mode(mode)
        failing = _FailFakes(text_exc=bridge.RetryableError("upload down"))
        ctx.write_pending("920101.json", _text_job())
        sent, _failed = ctx.drain(failing)
        check("%s retryable: not sent on failing pass" % mode, sent == [])
        job = _read_pending(ctx, "920101.json")
        check("%s retryable: JSON still pending, 1 attempt" % mode,
              job.get("text_status") == "pending"
              and int(job.get("text_attempts") or 0) == 1)
        row = ctx.job_row("920101")
        check("%s retryable: shadow reset to ready_to_send, lease cleared"
              % mode,
              row is not None and row["state"] == "ready_to_send"
              and row["lease_until"] is None)
        ok_fakes = _FailFakes()
        sent, _failed = ctx.drain(ok_fakes)
        check("%s retryable: next drain sends again" % mode,
              sent == ["920101.json"])
        row = ctx.job_row("920101")
        check("%s retryable: shadow row sent after retry" % mode,
              row is not None and row["state"] == "sent")
    finally:
        ctx.restore()


def test_retryable_reset_attachment(mode):
    ctx = _Ctx()
    try:
        ctx.set_mode(mode)
        failing = _FailFakes(image_exc=bridge.RetryableError("upload down"))
        job = {"image_path": "/tmp/never-read.jpg", "to_user_id": "user-B",
               "task_id": "t1"}
        ctx.write_pending("920102.json", job)
        sent, _failed = ctx.drain(failing)
        check("%s attachment retryable: not sent on failing pass" % mode,
              sent == [])
        pending = _read_pending(ctx, "920102.json")
        atts = pending.get("attachments") or []
        check("%s attachment retryable: attachment still pending, 1 attempt"
              % mode,
              len(atts) == 1 and atts[0].get("status") == "pending"
              and int(atts[0].get("attempts") or 0) == 1)
        row = ctx.job_row("920102")
        check("%s attachment retryable: shadow reset to ready_to_send"
              % mode,
              row is not None and row["state"] == "ready_to_send"
              and row["lease_until"] is None)
        sent, _failed = ctx.drain(_FailFakes())
        check("%s attachment retryable: next drain sends again" % mode,
              sent == ["920102.json"])
    finally:
        ctx.restore()


def test_unknown_never_resets(mode):
    ctx = _Ctx()
    try:
        ctx.set_mode(mode)
        failing = _FailFakes(
            text_exc=bridge.UnknownDeliveryError("post broke"))
        ctx.write_pending("920103.json", _text_job())
        sent, _failed = ctx.drain(failing)
        check("%s unknown: not sent on failing pass" % mode, sent == [])
        check("%s unknown: job archived to failed/" % mode,
              os.path.isfile(os.path.join(bridge.OUTBOX_FAILED,
                                          "920103.json")))
        row = ctx.job_row("920103")
        check("%s unknown: shadow locked to needs_check, never reset"
              % mode,
              row is not None and row["state"] == "needs_check")
        sent, _failed = ctx.drain(_FailFakes())
        check("%s unknown: second drain never resends" % mode, sent == [])
    finally:
        ctx.restore()


def test_reset_refuses_terminal_and_plain_transition():
    tmp = tempfile.mkdtemp(prefix="sqlite-switch-reset-api-")
    try:
        db = os.path.join(tmp, "t.db")
        conn = bridge_sqlite.connect_shadow(db)
        try:
            bridge_sqlite.upsert_job_ready(conn, "r1", "sha1")
            bridge_sqlite.begin_send(conn, "r1")
            row, action = bridge_sqlite.reset_for_retry(conn, "r1")
            check("api reset: in-flight -> ready_to_send, lease cleared",
                  action == "reset" and row["state"] == "ready_to_send"
                  and row["lease_until"] is None)
            row, action = bridge_sqlite.reset_for_retry(conn, "r1")
            check("api reset: already-ready is a no-op success",
                  action == "already_ready"
                  and row["state"] == "ready_to_send")
            bridge_sqlite.begin_send(conn, "r1")
            try:
                bridge_sqlite.transition_job(conn, "r1", "ready_to_send")
                plain_ok = True
            except bridge_sqlite.IllegalTransition:
                plain_ok = False
            check("api reset: plain transition without via_retry refused",
                  plain_ok is False)
            row, action = bridge_sqlite.reset_for_retry(conn, "r1")
            check("api reset: via_retry path still works after refusal",
                  action == "reset" and row["state"] == "ready_to_send")
            for target, label in (("sent", "sent"),
                                  ("needs_check", "needs_check"),
                                  ("dead", "dead")):
                key = "t_" + label
                bridge_sqlite.upsert_job_ready(conn, key, "sha1")
                bridge_sqlite.mirror_job_terminal(conn, key, target)
                try:
                    bridge_sqlite.reset_for_retry(conn, key)
                    refused = False
                except bridge_sqlite.IllegalTransition:
                    refused = True
                check("api reset: terminal %s row never downgraded" % label,
                      refused is True
                      and bridge_sqlite.get_job(conn, key)["state"] == target)
            try:
                bridge_sqlite.reset_for_retry(conn, "missing-job")
                missing_ok = True
            except bridge_sqlite.ShadowError:
                missing_ok = False
            check("api reset: missing row raises, never invented",
                  missing_ok is False)
            conn.commit()
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_dual_reset_failure_fail_open():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        os.makedirs(ctx.db_path(), exist_ok=True)
        failing = _FailFakes(text_exc=bridge.RetryableError("upload down"))
        ctx.write_pending("920104.json", _text_job())
        sent, _failed = ctx.drain(failing)
        check("dual reset failure: not sent on failing pass", sent == [])
        job = _read_pending(ctx, "920104.json")
        check("dual reset failure: JSON retry preserved (still pending)",
              job.get("text_status") == "pending"
              and int(job.get("text_attempts") or 0) == 1)
        markers = [n for n in os.listdir(bridge.FAILURES_DIR)
                   if n.endswith("_shadow.json")]
        check("dual reset failure: desensitized shadow marker written",
              len(markers) >= 1)
    finally:
        ctx.restore()


def test_segments_untouched_by_reset():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        failing = _FailFakes(text_exc=bridge.RetryableError("upload down"))
        ctx.write_pending("920105.json", _text_job())
        ctx.drain(failing)
        conn = sqlite3.connect(ctx.db_path())
        try:
            rows = conn.execute(
                "SELECT part_index, state FROM outbound_segments"
                " WHERE job_key = ?", ("920105",)).fetchall()
        finally:
            conn.close()
        check("segments: reset leaves descriptive mirror untouched",
              rows == [(0, "pending")])
    finally:
        ctx.restore()


# ---------------------------------------------------------------- fix 2
def _compare(ctx):
    return bridge_sqlite.compare_shadow(ctx.tmp, ctx.db_path())


def test_compare_benign_lag():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        record = {"message_id": "920201", "text": "hi"}
        bridge._shadow_upsert_inbound("920201", record)
        with open(os.path.join(bridge.DONE, "920201.json"),
                  "w", encoding="utf-8") as f:
            json.dump(record, f)
        report = _compare(ctx)
        check("benign: same-hash received->processed is benign, not mismatch",
              report["match"] is True and report["mismatches"] == []
              and report["benign_lag"]["count"] == 1
              and report["benign_lag"]["message_ids"] == ["920201"])
    finally:
        ctx.restore()


def test_compare_hash_differs_is_real_mismatch():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        bridge._shadow_upsert_inbound(
            "920202", {"message_id": "920202", "text": "hi"})
        with open(os.path.join(bridge.DONE, "920202.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"message_id": "920202", "text": "different"}, f)
        report = _compare(ctx)
        check("benign: hash differs -> true mismatch, benign empty",
              report["match"] is False
              and any("920202" in m for m in report["mismatches"])
              and report["benign_lag"]["count"] == 0)
    finally:
        ctx.restore()


def test_compare_other_combos_unchanged():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        rec_a = {"message_id": "920203", "text": "a"}
        bridge._shadow_upsert_inbound("920203", rec_a)
        with open(os.path.join(bridge.INBOX, "920203.json"),
                  "w", encoding="utf-8") as f:
            json.dump(rec_a, f)
        rec_b = {"message_id": "920204", "text": "b"}
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_inbound(
                conn, "920204", "processed",
                bridge_sqlite.record_sha256(rec_b))
            conn.commit()
        finally:
            conn.close()
        with open(os.path.join(bridge.DONE, "920204.json"),
                  "w", encoding="utf-8") as f:
            json.dump(rec_b, f)
        rec_c = {"message_id": "920205", "text": "c"}
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_inbound(
                conn, "920205", "processed",
                bridge_sqlite.record_sha256(rec_c))
            conn.commit()
        finally:
            conn.close()
        with open(os.path.join(bridge.INBOX, "920205.json"),
                  "w", encoding="utf-8") as f:
            json.dump(rec_c, f)
        report = _compare(ctx)
        check("benign: received->received and processed->processed clean",
              report["benign_lag"]["count"] == 0
              and not any("920203" in m or "920204" in m
                          for m in report["mismatches"]))
        check("benign: reverse (shadow processed, JSON received) still mismatch",
              any("920205" in m for m in report["mismatches"]))
    finally:
        ctx.restore()


def main():
    for mode in ("dual", "shadow"):
        test_retryable_reset_text(mode)
        test_retryable_reset_attachment(mode)
        test_unknown_never_resets(mode)
    test_reset_refuses_terminal_and_plain_transition()
    test_dual_reset_failure_fail_open()
    test_segments_untouched_by_reset()
    test_compare_benign_lag()
    test_compare_hash_differs_is_real_mismatch()
    test_compare_other_combos_unchanged()
    failed = [n for n, c in base.ok if not c]
    print("\nsqlite_switch_fixes: %d passed, %d failed"
          % (len(base.ok) - len(failed), len(failed)))
    if failed:
        print("FAILED:", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
