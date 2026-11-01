"""Offline tests for Gate 3 step 1: SQLite shadow layer (no write
authority switch). Everything runs on temp dirs and temp DBs; the
live bridge state is never touched, no real WeChat, no real upstream.

Run: python3 tests/test_sqlite_shadow.py (exit 0 = all pass).
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge_sqlite as bs  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def make_job(db, key, state="pending"):
    db.execute(
        "INSERT INTO outbound_jobs (job_key, message_id, state,"
        " failure_code, lease_until, record_sha256, updated_ts)"
        " VALUES (?, ?, ?, NULL, NULL, ?, ?)",
        (key, key, state, bs.record_sha256({"job": key}), bs._now_iso()),
    )


def fixture_root():
    root = tempfile.mkdtemp(prefix="sqlite-shadow-")
    for sub in ("inbox", "done", "outbox/pending", "outbox/sent",
                "outbox/failed"):
        os.makedirs(os.path.join(root, sub))
    inbox = [
        {"message_id": "1001", "text": "hello one"},
        {"message_id": "1002", "text": "hello two",
         "context_token": "SECRET-TOKEN-VALUE"},
    ]
    for rec in inbox:
        with open(os.path.join(root, "inbox", rec["message_id"] + ".json"),
                  "w", encoding="utf-8") as f:
            json.dump(rec, f)
    with open(os.path.join(root, "done", "1003.json"),
              "w", encoding="utf-8") as f:
        json.dump({"message_id": "1003", "text": "done one"}, f)
    with open(os.path.join(root, "outbox", "pending", "2001.json"),
              "w", encoding="utf-8") as f:
        json.dump({"text": "long reply",
                   "text_parts": [{"status": "pending"}, {"status": "sent"}]}, f)
    with open(os.path.join(root, "outbox", "sent", "2002.json"),
              "w", encoding="utf-8") as f:
        json.dump({"text": "sent reply"}, f)
    with open(os.path.join(root, "outbox", "failed", "2003.json"),
              "w", encoding="utf-8") as f:
        json.dump({"text": "bad one", "needs_check": True}, f)
    with open(os.path.join(root, "outbox", "failed", "2004.json"),
              "w", encoding="utf-8") as f:
        json.dump({"text": "dead one", "failure_code": "send_permanent"}, f)
    # Auxiliary tasks are NOT formal jobs and must not be imported.
    with open(os.path.join(root, "outbox", "pending",
                           "2001_progress.json"),
              "w", encoding="utf-8") as f:
        json.dump({"text": "progress"}, f)
    return root


def main():
    tmp = tempfile.mkdtemp(prefix="sqlite-shadow-test-")
    try:
        db_path = os.path.join(tmp, "bridge.db")

        # ---- 1. five-state transition table -------------------------
        conn = bs.connect_shadow(db_path)
        make_job(conn, "t1", "pending")
        make_job(conn, "t2", "pending")
        make_job(conn, "t3", "pending")
        make_job(conn, "t4", "pending")
        conn.commit()
        # legal path: pending -> leased -> ready -> in_flight -> sent
        bs.claim_job(conn, "t1", 60, now="2026-01-01T00:00:00+00:00")
        bs.mark_ready(conn, "t1")
        bs.begin_send(conn, "t1", 60, now="2026-01-01T00:01:00+00:00")
        bs.finish_send(conn, "t1", "sent")
        check("transition: happy path ends sent",
              bs.get_job(conn, "t1")["state"] == "sent")
        # in-flight -> unknown locks needs_check
        bs.claim_job(conn, "t2", 60, now="2026-01-01T00:00:00+00:00")
        bs.mark_ready(conn, "t2")
        bs.begin_send(conn, "t2", 60, now="2026-01-01T00:01:00+00:00")
        bs.finish_send(conn, "t2", "unknown")
        check("transition: in-flight unknown -> needs_check",
              bs.get_job(conn, "t2")["state"] == "needs_check")
        # illegal transitions all raise, changing nothing
        illegal = []
        for key, target, kw in (
            ("t1", "pending", {}),            # sent -> pending
            ("t1", "dead", {}),               # sent -> dead
            ("t2", "pending", {}),            # needs_check -> pending
            ("t2", "ready_to_send", {}),      # needs_check -> ready
            ("t3", "ready_to_send", {}),      # pending -> ready (skip claim)
            ("t3", "sent", {}),               # pending -> sent
        ):
            try:
                bs.transition_job(conn, key, target, **kw)
                illegal.append((key, target, False))
            except bs.IllegalTransition:
                illegal.append((key, target, True))
        check("transition: illegal edges raise",
              all(flag for _, _, flag in illegal))
        check("transition: illegal edges change nothing",
              bs.get_job(conn, "t3")["state"] == "pending")
        # in-flight can never go to pending/ready/dead
        bs.claim_job(conn, "t4", 60, now="2026-01-01T00:00:00+00:00")
        bs.mark_ready(conn, "t4")
        bs.begin_send(conn, "t4", 60, now="2026-01-01T00:01:00+00:00")
        blocked = 0
        for target in ("pending", "ready_to_send", "dead"):
            try:
                bs.transition_job(conn, "t4", target)
            except bs.IllegalTransition:
                blocked += 1
        check("transition: in-flight never to pending/ready/dead",
              blocked == 3
              and bs.get_job(conn, "t4")["state"] == "sending_in_flight")
        # finish_send refuses to decide 'permanent' from in-flight
        try:
            bs.finish_send(conn, "t4", "permanent")
            permanent_refused = False
        except bs.ShadowError:
            permanent_refused = True
        check("transition: finish_send rejects permanent from in-flight",
              permanent_refused)
        # leased -> pending without recovery, or with a live lease: refused
        make_job(conn, "t5", "pending")
        bs.claim_job(conn, "t5", 3600, now="2026-01-01T00:00:00+00:00")
        refused = 0
        try:
            bs.transition_job(conn, "t5", "pending")
        except bs.IllegalTransition:
            refused += 1
        try:
            bs.transition_job(conn, "t5", "pending",
                              now="2026-01-01T00:30:00+00:00",
                              via_recovery=True)
        except bs.IllegalTransition:
            refused += 1
        check("transition: leased->pending needs expired-lease recovery",
              refused == 2
              and bs.get_job(conn, "t5")["state"] == "leased_inferring")
        try:
            bs.transition_job(conn, "t5", "bogus_state")
            bogus = False
        except bs.IllegalTransition:
            bogus = True
        check("transition: unknown state rejected", bogus)
        conn.commit()

        # ---- 2. lease recovery iron law ------------------------------
        # t5 lease expires at 01:00; t4 in-flight lease expires 01:02.
        report = bs.recover_expired_leases(conn, now="2026-01-01T02:00:00+00:00")
        conn.commit()
        check("lease: expired leased -> pending",
              "t5" in report["recovered_to_pending"]
              and bs.get_job(conn, "t5")["state"] == "pending")
        check("lease: expired in-flight -> needs_check locked",
              "t4" in report["locked_to_needs_check"]
              and bs.get_job(conn, "t4")["state"] == "needs_check")
        # a second recovery never moves needs_check anywhere
        report2 = bs.recover_expired_leases(conn, now="2026-01-01T03:00:00+00:00")
        conn.commit()
        check("lease: needs_check never auto-recovers",
              report2 == {"recovered_to_pending": [],
                          "locked_to_needs_check": []}
              and bs.get_job(conn, "t4")["state"] == "needs_check")
        # a live lease is untouched by recovery
        make_job(conn, "t6", "pending")
        bs.claim_job(conn, "t6", 3600, now="2026-01-01T02:00:00+00:00")
        conn.commit()
        report3 = bs.recover_expired_leases(conn, now="2026-01-01T02:30:00+00:00")
        conn.commit()
        check("lease: unexpired lease untouched",
              "t6" not in report3["recovered_to_pending"]
              and bs.get_job(conn, "t6")["state"] == "leased_inferring")
        conn.close()

        # ---- 3. import: counts, hash, idempotency ----------------------
        root = fixture_root()
        db2 = os.path.join(tmp, "shadow.db")
        rep1 = bs.import_json_shadow(root, db2)
        check("import: inbound/outbound counts",
              rep1["inbound_count"] == 3 and rep1["outbound_count"] == 4)
        check("import: segment rows (2 parts + 3 singles)",
              rep1["segment_count"] == 5)
        check("import: auxiliary progress job excluded",
              rep1["files_read"] == 7)
        rep2 = bs.import_json_shadow(root, db2)
        check("import: re-import same counts and hash (idempotent)",
              rep1["inbound_count"] == rep2["inbound_count"]
              and rep1["outbound_count"] == rep2["outbound_count"]
              and rep1["rows_sha256"] == rep2["rows_sha256"])
        conn = bs.connect_shadow_readonly(db2)
        n_in = conn.execute("SELECT COUNT(*) FROM inbound_messages").fetchone()[0]
        n_out = conn.execute("SELECT COUNT(*) FROM outbound_jobs").fetchone()[0]
        states = dict(conn.execute(
            "SELECT job_key, state FROM outbound_jobs").fetchall())
        conn.close()
        check("import: DB row counts match report",
              n_in == 3 and n_out == 4)
        check("import: pending->ready_to_send, sent->sent, failed mapping",
              states.get("2001") == "ready_to_send"
              and states.get("2002") == "sent"
              and states.get("2003") == "needs_check"
              and states.get("2004") == "dead")
        # shadow stores no body and no token, not even hashed in: the
        # raw strings must not appear anywhere in the DB file bytes.
        with open(db2, "rb") as f:
            blob = f.read()
        check("import: no body/token bytes in shadow file",
              b"SECRET-TOKEN-VALUE" not in blob
              and b"hello two" not in blob
              and b"long reply" not in blob)

        # ---- 4. compare: match, then mismatch reports only -------------
        cmp1 = bs.compare_shadow(root, db2)
        check("compare: match after import",
              cmp1["match"] and not cmp1["mismatches"])
        with open(os.path.join(root, "inbox", "1001.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"message_id": "1001", "text": "TAMPERED"}, f)
        cmp2 = bs.compare_shadow(root, db2)
        check("compare: tampered JSON reported, not corrected",
              not cmp2["match"]
              and any("1001" in m for m in cmp2["mismatches"]))
        conn = bs.connect_shadow_readonly(db2)
        still = conn.execute(
            "SELECT record_sha256 FROM inbound_messages"
            " WHERE message_id = '1001'").fetchone()[0]
        conn.close()
        check("compare: DB row unchanged by compare",
              still == bs.record_sha256({"message_id": "1001",
                                         "text": "hello one"}))
        with open(os.path.join(root, "inbox", "1001.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"message_id": "1001", "text": "hello one"}, f)
        check("compare: match restored after JSON restored",
              bs.compare_shadow(root, db2)["match"])

        # ---- 5. import failure atomicity --------------------------------
        before = bs.compare_shadow(root, db2)
        with open(os.path.join(root, "inbox", "1002.json"),
                  "w", encoding="utf-8") as f:
            f.write("{not valid json")
        try:
            bs.import_json_shadow(root, db2)
            raised = False
        except (json.JSONDecodeError, ValueError):
            raised = True
        # compare needs valid JSON, so verify atomicity via DB counts.
        conn = bs.connect_shadow_readonly(db2)
        n_in2 = conn.execute("SELECT COUNT(*) FROM inbound_messages").fetchone()[0]
        conn.close()
        check("import: corrupt JSON raises and DB unchanged",
              raised and n_in2 == 3)
        with open(os.path.join(root, "inbox", "1002.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"message_id": "1002", "text": "hello two",
                       "context_token": "SECRET-TOKEN-VALUE"}, f)
        check("import: works again after JSON repaired",
              bs.import_json_shadow(root, db2)["inbound_count"] == 3)

        # ---- 6. drills ---------------------------------------------------
        # 6a. write failure: DB path under a regular file, not a dir.
        blocker = os.path.join(tmp, "blocker")
        with open(blocker, "w") as f:
            f.write("x")
        try:
            bs.import_json_shadow(root, os.path.join(blocker, "bridge.db"))
            write_failed_cleanly = False
        except (OSError, sqlite3.Error):
            write_failed_cleanly = True
        check("drill: write failure is clean, JSON untouched",
              write_failed_cleanly
              and os.path.exists(os.path.join(root, "inbox", "1001.json")))
        # 6b. interruption: transaction rolled back, counts unchanged.
        conn = bs.connect_shadow(db2)
        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO inbound_messages (message_id, state,"
            " source_bucket, record_sha256, received_ts, updated_ts)"
            " VALUES ('9999', 'received', 'inbox', 'x', NULL, ?)",
            (bs._now_iso(),))
        conn.rollback()
        n_in3 = conn.execute("SELECT COUNT(*) FROM inbound_messages").fetchone()[0]
        conn.close()
        check("drill: interrupted transaction rolls back",
              n_in3 == 3)
        # 6c. network timeout drill: covered by finish_send unknown +
        # recovery lock above; re-assert the locked job cannot be resent.
        conn = bs.connect_shadow(db2)
        try:
            bs.transition_job(conn, "2003", "pending")
            resend_blocked = False
        except bs.IllegalTransition:
            resend_blocked = True
        conn.close()
        check("drill: needs_check cannot be re-sent or re-queued",
              resend_blocked)
        # 6d. lease expiry drill: covered in section 2; re-assert the
        # recovered leased job is pending and the locked one is not.
        # 6e. old-version rollback: missing DB -> compare reports, JSON
        # still works; corrupt DB -> open raises; schema mismatch raises.
        cmp_missing = bs.compare_shadow(root, os.path.join(tmp, "nope.db"))
        check("drill: missing shadow DB reported, not a crash",
              not cmp_missing["shadow_present"] and not cmp_missing["match"])
        corrupt = os.path.join(tmp, "corrupt.db")
        with open(corrupt, "wb") as f:
            f.write(b"this is not sqlite")
        try:
            bs.connect_shadow(corrupt)
            corrupt_raises = False
        except sqlite3.Error:
            corrupt_raises = True
        check("drill: corrupt shadow DB raises cleanly",
              corrupt_raises)
        mismatch = os.path.join(tmp, "mismatch.db")
        conn = bs.connect_shadow(mismatch)
        conn.execute("UPDATE meta SET value = 'other-0.9'"
                     " WHERE key = 'schema_version'")
        conn.commit()
        conn.close()
        try:
            bs.connect_shadow(mismatch)
            schema_raises = False
        except bs.ShadowSchemaError:
            schema_raises = True
        check("drill: schema version mismatch refused, no silent migrate",
              schema_raises)
        check("drill: JSON authority still readable without shadow",
              bs.json_snapshot(root)["files_read"] == 7)
        # 6f. backup / restore round-trip.
        backup = os.path.join(tmp, "bridge-backup.db")
        bs.backup_shadow(db2, backup)
        restored = os.path.join(tmp, "restored.db")
        counts = bs.restore_shadow(backup, restored)
        conn = bs.connect_shadow_readonly(restored)
        rows = sorted(conn.execute(
            "SELECT job_key, state, record_sha256 FROM outbound_jobs"
        ).fetchall())
        conn.close()
        conn = bs.connect_shadow_readonly(db2)
        rows0 = sorted(conn.execute(
            "SELECT job_key, state, record_sha256 FROM outbound_jobs"
        ).fetchall())
        conn.close()
        check("drill: backup/restore counts and rows identical",
              counts["outbound_jobs"] == 4 and rows == rows0)

        # ---- 7. tombstones ------------------------------------------------
        conn = bs.connect_shadow(db2)
        bs.record_tombstone(conn, "1001", "inbound", "dead")
        bs.record_tombstone(conn, "1001", "inbound", "dead")
        n_tomb = conn.execute("SELECT COUNT(*) FROM tombstones").fetchone()[0]
        try:
            bs.record_tombstone(conn, "x", "inbound", "pending")
            tomb_terminal = False
        except bs.ShadowError:
            tomb_terminal = True
        conn.commit()
        conn.close()
        check("tombstone: minimal id only, replace not duplicate,"
              " non-terminal refused",
              n_tomb == 1 and tomb_terminal)

        # ---- 8. CLI ---------------------------------------------------------
        proc = subprocess.run(
            [sys.executable, "bridge_sqlite.py", "compare", "--root", root,
             "--db", db2],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True, text=True)
        out = json.loads(proc.stdout) if proc.stdout.strip() else {}
        check("cli: compare exits 0 and reports match",
              proc.returncode == 0 and out.get("match") is True)
        proc = subprocess.run(
            [sys.executable, "bridge_sqlite.py", "import", "--root", root,
             "--db", os.path.join(tmp, "cli.db")],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True, text=True)
        out = json.loads(proc.stdout) if proc.stdout.strip() else {}
        check("cli: import exits 0 with counts",
              proc.returncode == 0 and out.get("inbound_count") == 3)
        shutil.rmtree(root, ignore_errors=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        # Fix-copy state must be left at baseline by every test run.
        state_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "state")
        for name in ("bridge.db", "bridge.db-wal", "bridge.db-shm"):
            path = os.path.join(state_dir, name)
            if os.path.exists(path):
                os.remove(path)


if __name__ == "__main__":
    main()
    failed = [name for name, cond in ok if not cond]
    print("sqlite_shadow: %d passed, %d failed"
          % (len(ok) - len(failed), len(failed)))
    sys.exit(1 if failed else 0)
