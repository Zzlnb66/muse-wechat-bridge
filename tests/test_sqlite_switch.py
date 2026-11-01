"""Offline tests for the SQLite write-authority switch implementation
(Gate 3 step 2, design docs/sqlite-write-switch-design-DRAFT.md).

Scope discipline for this file:
- Everything runs in temp dirs with fake sends. Never touches the
  real running bridge, never sends a real WeChat message, never
  reaches the real upstream.
- Default mode json is inert: drain behaves exactly as before and
  no bridge.db file is ever created.
- dual mode dual-writes (JSON first, shadow second; shadow-first
  only for send-start/send-terminal) and fails open on shadow
  errors. shadow mode fails closed ONLY at send-start.
- needs_check CLI, third gear, and soak duration are NOT
  implemented (user-nod items); they are not tested here.
- Two design consequences are pinned by tests, not hidden:
  (a) a job that entered sending_in_flight and then ends
  retryable-pending is skipped until its lease expires, then
  locked to needs_check (no immediate 3-attempt retry in
  dual/shadow -- a design amendment would be needed to change it);
  (b) a Worker inbox -> done move leaves the shadow inbound row at
  "received" while JSON says "processed", which compare reports as
  a difference (lag, not contradiction; the bridge never observes
  the Worker move, and section 3.1 forbids off-table write points).

Run: python3 tests/test_sqlite_switch.py (exit 0 = all pass).
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402
import bridge_sqlite  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


class _Fakes:
    def __init__(self):
        self.text_calls = []

    def send_text(self, session, text, to_user_id=None, context_token="", client_id=None):
        self.text_calls.append(text)
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        return {}

    def send_image(self, session, path, to_user_id, token, client_id=None):
        return {}


class _Ctx:
    """Fresh temp bridge root with real-layout dirs, globals rebound."""

    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="sqlite-switch-")
        self.old = {}
        layout = {
            "STATE": "state",
            "INBOX": "inbox",
            "RUNNING": "running",
            "DONE": "done",
            "OUTBOX_PENDING": os.path.join("outbox", "pending"),
            "OUTBOX_SENT": os.path.join("outbox", "sent"),
            "OUTBOX_FAILED": os.path.join("outbox", "failed"),
            "MEDIA_INBOUND": os.path.join("media", "inbound"),
            "MEDIA_OUTBOUND": os.path.join("media", "outbound"),
            "FAILURES_DIR": "failures",
            "ANALYSIS_DIR": "analysis",
            "LOCAL_VAULT_DIR": "local_vault",
        }
        for name, rel in layout.items():
            self.old[name] = getattr(bridge, name)
            d = os.path.join(self.tmp, rel)
            os.makedirs(d, exist_ok=True)
            setattr(bridge, name, d)
        self.old_mode = bridge._SQLITE_AUTHORITY_MODE
        self.old_tick = bridge._SHADOW_LAST_LEASE_TICK
        self.old_mod = bridge._SHADOW_MOD
        bridge._SQLITE_AUTHORITY_MODE = None
        bridge._SHADOW_LAST_LEASE_TICK = 0.0
        self._clear_drain_state()

    def _clear_drain_state(self):
        bridge._OBS_QUEUED_SEEN.clear()
        bridge._TERMINAL_TASK_IDS.clear()
        bridge._IN_MEMORY_HALTED_FILES.clear()
        try:
            bridge._SEND_COOLDOWN.clear()
        except Exception:
            pass

    def restore(self):
        for name, value in self.old.items():
            setattr(bridge, name, value)
        bridge._SQLITE_AUTHORITY_MODE = self.old_mode
        bridge._SHADOW_LAST_LEASE_TICK = self.old_tick
        bridge._SHADOW_MOD = self.old_mod
        shutil.rmtree(self.tmp, ignore_errors=True)

    def set_mode(self, mode):
        if mode is None:
            p = os.path.join(bridge.STATE, "sqlite_authority.json")
            if os.path.exists(p):
                os.remove(p)
        else:
            with open(os.path.join(bridge.STATE, "sqlite_authority.json"),
                      "w", encoding="utf-8") as f:
                json.dump({"mode": mode}, f)
        bridge._SQLITE_AUTHORITY_MODE = None
        got = bridge.refresh_sqlite_authority()
        assert got == mode or (mode is None and got == "json"), (got, mode)
        return got

    def db_path(self):
        return os.path.join(bridge.STATE, "bridge.db")

    def remove_db(self):
        """Remove the shadow DB whether it is a file or a directory
        (broken-DB tests create it as a directory)."""
        p = self.db_path()
        if os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
        elif os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass
        for suffix in ("-wal", "-shm"):
            try:
                os.remove(p + suffix)
            except OSError:
                pass

    def db_exists(self):
        return os.path.exists(self.db_path())

    def job_row(self, key):
        if not self.db_exists():
            return None
        conn = sqlite3.connect(self.db_path())
        try:
            row = conn.execute(
                "SELECT job_key, message_id, state, failure_code,"
                " lease_until FROM outbound_jobs WHERE job_key = ?",
                (key,)).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {"job_key": row[0], "message_id": row[1], "state": row[2],
                "failure_code": row[3], "lease_until": row[4]}

    def inbound_row(self, mid):
        if not self.db_exists():
            return None
        conn = sqlite3.connect(self.db_path())
        try:
            row = conn.execute(
                "SELECT message_id, state FROM inbound_messages"
                " WHERE message_id = ?",
                (mid,)).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {"message_id": row[0], "state": row[1]}

    def write_pending(self, name, job):
        with open(os.path.join(bridge.OUTBOX_PENDING, name),
                  "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False)

    def read_json(self, path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def drain(self, fakes):
        self._clear_drain_state()
        orig = (bridge.send_text, bridge.send_file, bridge.send_image)
        bridge.send_text, bridge.send_file, bridge.send_image = (
            fakes.send_text, fakes.send_file, fakes.send_image)
        try:
            return bridge.drain_outbox(
                {"ilink_user_id": "user-B", "context_token": ""})
        finally:
            bridge.send_text, bridge.send_file, bridge.send_image = orig

    def shadow_conn(self):
        return bridge_sqlite.connect_shadow(self.db_path())


def _text_job(text="hello", to="user-B"):
    return {"text": text, "to_user_id": to, "client_id": "c1"}


# ---------------------------------------------------------------- mode parsing
def test_mode_parsing():
    ctx = _Ctx()
    try:
        check("mode: missing file -> json", ctx.set_mode(None) == "json")
        with open(os.path.join(bridge.STATE, "sqlite_authority.json"),
                  "w", encoding="utf-8") as f:
            f.write("{not json")
        bridge._SQLITE_AUTHORITY_MODE = None
        check("mode: broken json -> json",
              bridge.refresh_sqlite_authority() == "json")
        with open(os.path.join(bridge.STATE, "sqlite_authority.json"),
                  "w", encoding="utf-8") as f:
            json.dump(["dual"], f)
        bridge._SQLITE_AUTHORITY_MODE = None
        check("mode: non-dict -> json",
              bridge.refresh_sqlite_authority() == "json")
        with open(os.path.join(bridge.STATE, "sqlite_authority.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"mode": "shadow2"}, f)
        bridge._SQLITE_AUTHORITY_MODE = None
        check("mode: invalid value -> json, never silently shadow",
              bridge.refresh_sqlite_authority() == "json")
        check("mode: dual accepted", ctx.set_mode("dual") == "dual")
        check("mode: shadow accepted", ctx.set_mode("shadow") == "shadow")
        # Restart-only semantics: file change without refresh is inert.
        ctx.set_mode("dual")
        with open(os.path.join(bridge.STATE, "sqlite_authority.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"mode": "json"}, f)
        check("mode: file change without refresh is inert",
              bridge.get_sqlite_mode() == "dual")
        check("mode: refresh picks up the change",
              bridge.refresh_sqlite_authority() == "json")
    finally:
        ctx.restore()


# ---------------------------------------------------------------- json mode inert
def test_json_mode_inert():
    ctx = _Ctx()
    try:
        ctx.set_mode(None)
        fakes = _Fakes()
        ctx.write_pending("910001.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("json: normal job sent", sent == ["910001.json"]
              and fakes.text_calls == ["hello"])
        check("json: no bridge.db created", not ctx.db_exists())
        check("json: job archived to sent/",
              os.path.isfile(os.path.join(bridge.OUTBOX_SENT, "910001.json")))
        summary = bridge.get_observability_summary()
        check("json: summary sent=1 pending=0",
              summary["sent"] == 1 and summary["pending"] == 0)
    finally:
        ctx.restore()


# ---------------------------------------------------------------- dual mode
def test_dual_normal_and_segments():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        fakes = _Fakes()
        ctx.write_pending("910002.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("dual: normal job sent", sent == ["910002.json"])
        row = ctx.job_row("910002")
        check("dual: shadow row sent",
              row is not None and row["state"] == "sent")
        conn = sqlite3.connect(ctx.db_path())
        try:
            segs = conn.execute(
                "SELECT part_index, state FROM outbound_segments"
                " WHERE job_key = ?", ("910002",)).fetchall()
        finally:
            conn.close()
        # NOTE: _segment_states_for_job yields "pending" for plain
        # (plan-less) jobs regardless of delivery state -- that is
        # the pre-existing import mapping, and compare ignores
        # segment rows, so the mirror matches import here.
        check("dual: plain-job segment mirrored (import mapping)",
              segs == [(0, "pending")])
    finally:
        ctx.restore()


def test_dual_plan_job():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        fakes = _Fakes()
        ctx.write_pending("910003.json", _text_job("word " * 600))
        sent, _failed = ctx.drain(fakes)
        check("dual: plan job sent", sent == ["910003.json"])
        row = ctx.job_row("910003")
        check("dual: plan job shadow row sent",
              row is not None and row["state"] == "sent")
        conn = sqlite3.connect(ctx.db_path())
        try:
            segs = conn.execute(
                "SELECT part_index, state FROM outbound_segments"
                " WHERE job_key = ? ORDER BY part_index",
                ("910003",)).fetchall()
        finally:
            conn.close()
        check("dual: plan segments all sent",
              len(segs) == 2 and all(s == "sent" for _, s in segs))
        import re as _re
        _bodies = [_re.sub(r"^\[\d+/\d+\]\n", "", c)
                   for c in fakes.text_calls]
        # Long-line cuts may split a word across parts, so splice
        # restore is exact concatenation, not token rejoining.
        check("dual: plan job sent in order, splice intact",
              len(fakes.text_calls) == 2
              and "".join(_bodies) == "word " * 600)
    finally:
        ctx.restore()


def test_dual_gate_park_and_fill():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        fakes = _Fakes()
        # No receipt record -> parked, never sent, shadow needs_check.
        ctx.write_pending("910004.json",
                          {"text": "foreign?", "client_id": "c1"})
        sent, failed = ctx.drain(fakes)
        check("dual gate: recipient-less foreign job not sent",
              sent == [] and fakes.text_calls == [])
        check("dual gate: foreign job parked to failed/",
              os.path.isfile(os.path.join(bridge.OUTBOX_FAILED, "910004.json")))
        row = ctx.job_row("910004")
        check("dual gate: shadow row needs_check + recipient_missing",
              row is not None and row["state"] == "needs_check"
              and row["failure_code"] == "recipient_missing")
        # Receipt record present -> filled and sent (gate unchanged).
        with open(os.path.join(bridge.INBOX, "910005.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"message_id": "910005", "text": "hi"}, f)
        ctx.write_pending("910005.json",
                          {"text": "local reply", "client_id": "c1"})
        sent, _failed = ctx.drain(fakes)
        check("dual gate: local recipient-less job filled and sent",
              sent == ["910005.json"] and fakes.text_calls == ["local reply"])
        row = ctx.job_row("910005")
        check("dual gate: local job shadow row sent",
              row is not None and row["state"] == "sent")
    finally:
        ctx.restore()


def test_dual_first_seen_terminal_and_inflight():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        fakes = _Fakes()
        # Pre-existing shadow sent row -> archived to sent/, no send.
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_job_ready(conn, "910006", "sha-x")
            bridge_sqlite.mirror_job_terminal(conn, "910006", "sent")
            conn.commit()
        finally:
            conn.close()
        ctx.write_pending("910006.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("dual first-seen: shadow-sent job archived without send",
              sent == ["910006.json"] and fakes.text_calls == [])
        check("dual first-seen: shadow-sent job in sent/",
              os.path.isfile(os.path.join(bridge.OUTBOX_SENT, "910006.json")))
        # Pre-existing shadow needs_check row -> failed/, no send.
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_job_ready(conn, "910007", "sha-x")
            bridge_sqlite.mirror_job_terminal(
                conn, "910007", "needs_check", "delivery_unknown")
            conn.commit()
        finally:
            conn.close()
        ctx.write_pending("910007.json", _text_job())
        sent, failed = ctx.drain(fakes)
        check("dual first-seen: shadow-needs_check job to failed/, no send",
              sent == [] and fakes.text_calls == []
              and os.path.isfile(os.path.join(bridge.OUTBOX_FAILED, "910007.json")))
        # In-flight, lease live -> skipped, stays pending, no send.
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_job_ready(conn, "910008", "sha-x")
            bridge_sqlite.begin_send(conn, "910008", lease_seconds=3600)
            conn.commit()
        finally:
            conn.close()
        ctx.write_pending("910008.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("dual first-seen: live in-flight skipped, stays pending",
              sent == [] and fakes.text_calls == []
              and os.path.isfile(os.path.join(bridge.OUTBOX_PENDING, "910008.json")))
        # In-flight, lease expired -> locked to needs_check, no send.
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_job_ready(conn, "910009", "sha-x")
            bridge_sqlite.begin_send(conn, "910009", lease_seconds=-60)
            conn.commit()
        finally:
            conn.close()
        ctx.write_pending("910009.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        row = ctx.job_row("910009")
        check("dual first-seen: expired in-flight locked to needs_check",
              sent == [] and fakes.text_calls == []
              and os.path.isfile(os.path.join(bridge.OUTBOX_FAILED, "910009.json"))
              and row is not None and row["state"] == "needs_check"
              and row["failure_code"] == "lease_expired_in_flight")
        # Leased -> skipped for manual confirmation, no send.
        # (A leased row can only arise via pending -> claim, since
        # ready -> leased has no edge; insert the pending row
        # directly, then claim it.)
        conn = ctx.shadow_conn()
        try:
            conn.execute(
                "INSERT INTO outbound_jobs (job_key, message_id, state,"
                " failure_code, lease_until, record_sha256, updated_ts)"
                " VALUES (?, ?, 'pending', NULL, NULL, ?, ?)",
                ("910010", "910010", "sha-x", "2026-01-01T00:00:00+00:00"))
            bridge_sqlite.claim_job(conn, "910010", 3600)
            conn.commit()
        finally:
            conn.close()
        ctx.write_pending("910010.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("dual first-seen: leased job skipped, stays pending",
              sent == [] and fakes.text_calls == []
              and os.path.isfile(os.path.join(bridge.OUTBOX_PENDING, "910010.json")))
    finally:
        ctx.restore()


def test_dual_fail_open_broken_db_and_schema():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        fakes = _Fakes()
        # DB path is a directory: every shadow open fails, JSON still sends.
        os.makedirs(ctx.db_path(), exist_ok=True)
        ctx.write_pending("910011.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("dual fail-open: broken DB still sends per JSON",
              sent == ["910011.json"] and fakes.text_calls == ["hello"])
        markers = [n for n in os.listdir(bridge.FAILURES_DIR)
                   if n.endswith("_shadow.json")]
        check("dual fail-open: desensitized shadow marker written",
              len(markers) >= 1)
        shutil.rmtree(ctx.db_path())
        # Tampered schema version: connect refuses, drain still sends.
        conn = ctx.shadow_conn()
        conn.execute("UPDATE meta SET value = 'x' WHERE key = 'schema_version'")
        conn.commit()
        conn.close()
        try:
            bridge_sqlite.connect_shadow(ctx.db_path())
            schema_refused = False
        except bridge_sqlite.ShadowSchemaError:
            schema_refused = True
        check("shadow: schema mismatch refuses open, never migrates",
              schema_refused)
        ctx.write_pending("910012.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("dual fail-open: schema-mismatch DB still sends per JSON",
              sent == ["910012.json"])
    finally:
        ctx.restore()


def test_dual_inbound_and_untracked_aux():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        bridge._shadow_upsert_inbound("910013", {"message_id": "910013"})
        row = ctx.inbound_row("910013")
        check("dual: inbound row received",
              row is not None and row["state"] == "received")
        fakes = _Fakes()
        # Bridge-written receipt jobs are untracked: no shadow row.
        ctx.write_pending("910013_progress.json",
                          {"text": "wait", "to_user_id": "user-B",
                           "client_id": "c1"})
        sent, _failed = ctx.drain(fakes)
        check("dual: progress receipt sent",
              sent == ["910013_progress.json"])
        check("dual: progress receipt has NO shadow row",
              ctx.job_row("910013_progress") is None)
        # Even with a broken DB, untracked receipts are never gated.
        ctx.remove_db()
        os.makedirs(ctx.db_path(), exist_ok=True)
        ctx.write_pending("910014_progress.json",
                          {"text": "wait2", "to_user_id": "user-B",
                           "client_id": "c1"})
        sent, _failed = ctx.drain(fakes)
        check("dual: untracked receipt sends despite broken DB",
              sent == ["910014_progress.json"])
        ctx.remove_db()
    finally:
        ctx.restore()


# ---------------------------------------------------------------- shadow mode
def test_shadow_healthy_and_fail_closed():
    ctx = _Ctx()
    try:
        ctx.set_mode("shadow")
        fakes = _Fakes()
        ctx.write_pending("910020.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("shadow: healthy job sent", sent == ["910020.json"])
        row = ctx.job_row("910020")
        check("shadow: row sent via begin_send + finish",
              row is not None and row["state"] == "sent")
        # Broken DB: send-start gate blocks, job stays pending,
        # non-terminal, never sent.
        ctx.remove_db()
        os.makedirs(ctx.db_path(), exist_ok=True)
        ctx.write_pending("910021.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        pending_path = os.path.join(bridge.OUTBOX_PENDING, "910021.json")
        job = ctx.read_json(pending_path) if os.path.isfile(pending_path) else {}
        check("shadow fail-closed: broken DB blocks send, stays pending",
              sent == [] and fakes.text_calls == ["hello"]
              and os.path.isfile(pending_path)
              and not job.get("terminal"))
        check("shadow fail-closed: blocked job is NOT terminal",
              job.get("terminal") is not True
              and job.get("delivery_state") != "needs_check")
        # DB fixed: next drain sends it.
        ctx.remove_db()
        sent, _failed = ctx.drain(fakes)
        check("shadow fail-closed: after DB fix, job sends",
              sent == ["910021.json"])
    finally:
        ctx.restore()


def test_shadow_terminal_mirror_fail_open():
    ctx = _Ctx()
    try:
        ctx.set_mode("shadow")
        fakes = _Fakes()
        orig = bridge_sqlite.mirror_job_terminal

        def _boom(*args, **kwargs):
            raise RuntimeError("mirror boom")

        bridge_sqlite.mirror_job_terminal = _boom
        try:
            ctx.write_pending("910022.json", _text_job())
            sent, _failed = ctx.drain(fakes)
        finally:
            bridge_sqlite.mirror_job_terminal = orig
        check("shadow: terminal mirror failure never blocks archive",
              sent == ["910022.json"]
              and os.path.isfile(os.path.join(bridge.OUTBOX_SENT, "910022.json")))
    finally:
        ctx.restore()


def test_shadow_module_missing_falls_back_to_json():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        bridge._SHADOW_MOD = None
        bridge._SQLITE_AUTHORITY_MODE = None
        check("shadow: missing module forces json",
              bridge.refresh_sqlite_authority() == "json")
        fakes = _Fakes()
        ctx.write_pending("910023.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("shadow: missing module still sends per JSON",
              sent == ["910023.json"])
        check("shadow: missing module creates no DB", not ctx.db_exists())
    finally:
        ctx.restore()


# ---------------------------------------------------------------- startup + leases
def test_startup_reconcile_and_lease_tick():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        # (a) pending + shadow sent -> sent/
        # (b) pending + shadow needs_check -> failed/
        # (c) pending + shadow in-flight expired -> failed/ + row locked
        # (d) pending + shadow in-flight live -> stays pending
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_job_ready(conn, "910030", "sha-x")
            bridge_sqlite.mirror_job_terminal(conn, "910030", "sent")
            bridge_sqlite.upsert_job_ready(conn, "910031", "sha-x")
            bridge_sqlite.mirror_job_terminal(
                conn, "910031", "needs_check", "delivery_unknown")
            bridge_sqlite.upsert_job_ready(conn, "910032", "sha-x")
            bridge_sqlite.begin_send(conn, "910032", lease_seconds=-60)
            bridge_sqlite.upsert_job_ready(conn, "910033", "sha-x")
            bridge_sqlite.begin_send(conn, "910033", lease_seconds=3600)
            conn.commit()
        finally:
            conn.close()
        for mid in ("910030", "910031", "910032", "910033"):
            ctx.write_pending(mid + ".json", _text_job())
        bridge._shadow_startup_sync()
        check("reconcile: shadow-sent job moved to sent/",
              os.path.isfile(os.path.join(bridge.OUTBOX_SENT, "910030.json")))
        check("reconcile: shadow-needs_check job moved to failed/",
              os.path.isfile(os.path.join(bridge.OUTBOX_FAILED, "910031.json")))
        row = ctx.job_row("910032")
        check("reconcile: expired in-flight locked + moved to failed/",
              os.path.isfile(os.path.join(bridge.OUTBOX_FAILED, "910032.json"))
              and row is not None and row["state"] == "needs_check")
        check("reconcile: live in-flight left pending",
              os.path.isfile(os.path.join(bridge.OUTBOX_PENDING, "910033.json")))
        row = ctx.job_row("910033")
        check("reconcile: live in-flight row untouched",
              row is not None and row["state"] == "sending_in_flight")
        # Lease tick: expired in-flight row -> needs_check, then quiet.
        conn = ctx.shadow_conn()
        try:
            bridge_sqlite.upsert_job_ready(conn, "910034", "sha-x")
            bridge_sqlite.begin_send(conn, "910034", lease_seconds=-30)
            conn.commit()
        finally:
            conn.close()
        bridge._SHADOW_LAST_LEASE_TICK = 0.0
        bridge._shadow_lease_tick()
        row = ctx.job_row("910034")
        check("lease tick: expired in-flight locked to needs_check",
              row is not None and row["state"] == "needs_check")
    finally:
        ctx.restore()


# ---------------------------------------------------------------- import guard + repair
def test_import_guard_and_repair():
    ctx = _Ctx()
    try:
        with open(os.path.join(bridge.INBOX, "910040.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"message_id": "910040", "text": "hi"}, f)
        # Library guard.
        try:
            bridge_sqlite.import_json_shadow(
                ctx.tmp, ctx.db_path(), authority_mode="dual")
            blocked = False
        except bridge_sqlite.ShadowImportBlocked:
            blocked = True
        check("import: dual mode library import refused", blocked)
        # CLI guard: dual -> exit 2, DB untouched; json -> exit 0.
        ctx.set_mode("dual")
        conn = ctx.shadow_conn()
        conn.close()
        before = ctx.job_row("910040")
        proc = subprocess.run(
            [sys.executable,
             os.path.join(os.path.dirname(os.path.dirname(
                 os.path.abspath(__file__))), "bridge_sqlite.py"),
             "import", "--root", ctx.tmp],
            capture_output=True, text=True)
        check("import: dual mode CLI import refused (exit 2)",
              proc.returncode == 2)
        check("import: refused CLI import left DB untouched",
              ctx.job_row("910040") == before)
        ctx.set_mode("json")
        proc = subprocess.run(
            [sys.executable,
             os.path.join(os.path.dirname(os.path.dirname(
                 os.path.abspath(__file__))), "bridge_sqlite.py"),
             "import", "--root", ctx.tmp],
            capture_output=True, text=True)
        check("import: json mode CLI import allowed (exit 0)",
              proc.returncode == 0)
        row = ctx.inbound_row("910040")
        check("import: json mode CLI import wrote the row",
              row is not None and row["state"] == "received")
        # Repair API: pending JSON + missing row -> inserted ready.
        ctx.write_pending("910041.json", _text_job())
        conn = ctx.shadow_conn()
        try:
            rep = bridge_sqlite.repair_job_row(conn, ctx.tmp, "910041")
            conn.commit()
        finally:
            conn.close()
        check("repair: pending + missing row -> inserted ready",
              rep["action"] == "inserted"
              and ctx.job_row("910041")["state"] == "ready_to_send")
        conn = ctx.shadow_conn()
        try:
            rep = bridge_sqlite.repair_job_row(conn, ctx.tmp, "910041")
            conn.commit()
        finally:
            conn.close()
        check("repair: pending + row -> kept (no downgrade path)",
              rep["action"] == "kept")
        # Sent JSON + ready row -> sent.
        os.replace(os.path.join(bridge.OUTBOX_PENDING, "910041.json"),
                   os.path.join(bridge.OUTBOX_SENT, "910041.json"))
        conn = ctx.shadow_conn()
        try:
            rep = bridge_sqlite.repair_job_row(conn, ctx.tmp, "910041")
            conn.commit()
        finally:
            conn.close()
        check("repair: sent JSON + ready row -> sent",
              rep["action"] in ("finished_sent", "direct_terminal")
              and ctx.job_row("910041")["state"] == "sent")
        # Failed JSON + sent row -> terminal never downgraded.
        os.replace(os.path.join(bridge.OUTBOX_SENT, "910041.json"),
                   os.path.join(bridge.OUTBOX_FAILED, "910041.json"))
        with open(os.path.join(bridge.OUTBOX_FAILED, "910041.json"),
                  "w", encoding="utf-8") as f:
            json.dump({"text": "x", "delivery_state": "failed",
                       "terminal": True}, f)
        conn = ctx.shadow_conn()
        try:
            rep = bridge_sqlite.repair_job_row(conn, ctx.tmp, "910041")
            conn.commit()
        finally:
            conn.close()
        check("repair: terminal row never downgraded by failed JSON",
              rep["action"] == "kept_terminal"
              and ctx.job_row("910041")["state"] == "sent")
    finally:
        ctx.restore()


# ---------------------------------------------------------------- API units + gaps
def test_shadow_api_units():
    tmp = tempfile.mkdtemp(prefix="sqlite-switch-api-")
    try:
        db = os.path.join(tmp, "t.db")
        conn = bridge_sqlite.connect_shadow(db)
        try:
            try:
                bridge_sqlite.upsert_inbound(conn, "m1", "bogus", "sha")
                bad_state = False
            except bridge_sqlite.ShadowError:
                bad_state = True
            check("api: inbound bad state rejected", bad_state)
            row, action = bridge_sqlite.upsert_job_ready(conn, "j1", "sha1")
            check("api: upsert ready inserts",
                  action == "inserted" and row["state"] == "ready_to_send")
            row, action = bridge_sqlite.upsert_job_ready(conn, "j1", "sha2")
            check("api: upsert ready keeps ready row",
                  action == "kept" and row["state"] == "ready_to_send")
            row, action = bridge_sqlite.mirror_job_terminal(
                conn, "j1", "needs_check", "recipient_mismatch")
            check("api: ready -> needs_check direct terminal",
                  action == "direct_terminal"
                  and row["state"] == "needs_check"
                  and row["failure_code"] == "recipient_mismatch")
            row, action = bridge_sqlite.mirror_job_terminal(
                conn, "j1", "dead", "other")
            check("api: terminal row never downgraded",
                  action == "kept_terminal" and row["state"] == "needs_check")
            bridge_sqlite.upsert_job_ready(conn, "j2", "sha1")
            bridge_sqlite.begin_send(conn, "j2")
            row, action = bridge_sqlite.mirror_job_terminal(conn, "j2", "sent")
            check("api: in-flight -> sent via finish",
                  action == "finished_sent" and row["state"] == "sent")
            bridge_sqlite.upsert_job_ready(conn, "j3", "sha1")
            try:
                bridge_sqlite.finish_send(conn, "j3", "permanent")
                perm_rejected = False
            except bridge_sqlite.ShadowError:
                perm_rejected = True
            check("api: finish_send rejects permanent from in-flight",
                  perm_rejected)
            conn.commit()
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_merge_staging_and_timeline_not_shadow_written():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        staged_ok, _first = bridge.stage_merge_message(
            "910050", "user-B", {"message_id": "910050", "text": "hi"}, "")
        check("merge: staging call itself succeeds", staged_ok is True)
        check("merge: staged-but-unpromoted message has NO shadow row",
              ctx.inbound_row("910050") is None)
        # Timeline is JSONL only: no timeline table can exist in the DB.
        bridge._shadow_upsert_inbound("910051", {"message_id": "910051"})
        conn = sqlite3.connect(ctx.db_path())
        try:
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        finally:
            conn.close()
        check("timeline: no timeline table in shadow schema",
              "message_timeline" not in tables
              and "timeline" not in " ".join(tables))
    finally:
        ctx.restore()


def test_compare_gap_inbound_processed_lag():
    """RESOLVED BY PARENT DECISION 2026-11-01 (was a pinned gap): Worker
    inbox -> done moves are invisible to the bridge, so the shadow row
    stays 'received' while JSON says 'processed'. Same-hash
    received->processed is now classified benign_lag (listed
    separately, never a mismatch); only a hash difference is a real
    mismatch. No done-sweep write point was added."""
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        record = {"message_id": "910060", "text": "hi"}
        bridge._shadow_upsert_inbound("910060", record)
        with open(os.path.join(bridge.DONE, "910060.json"),
                  "w", encoding="utf-8") as f:
            json.dump(record, f)
        report = bridge_sqlite.compare_shadow(ctx.tmp, ctx.db_path())
        check("gap: same-hash received->processed is benign_lag, not mismatch",
              report.get("match") is True
              and not any("910060" in m for m in report["mismatches"])
              and "910060" in report.get("benign_lag", {}).get("message_ids", []))
    finally:
        ctx.restore()


def test_mode_flip_back_to_json():
    ctx = _Ctx()
    try:
        ctx.set_mode("dual")
        fakes = _Fakes()
        ctx.write_pending("910070.json", _text_job())
        ctx.drain(fakes)
        check("flip: dual drain wrote a row",
              ctx.job_row("910070") is not None)
        ctx.set_mode("json")
        ctx.write_pending("910071.json", _text_job())
        sent, _failed = ctx.drain(fakes)
        check("flip: json drain sends per JSON",
              sent == ["910071.json"])
        check("flip: json drain wrote NO new shadow row",
              ctx.job_row("910071") is None)
    finally:
        ctx.restore()


def main():
    test_mode_parsing()
    test_json_mode_inert()
    test_dual_normal_and_segments()
    test_dual_plan_job()
    test_dual_gate_park_and_fill()
    test_dual_first_seen_terminal_and_inflight()
    test_dual_fail_open_broken_db_and_schema()
    test_dual_inbound_and_untracked_aux()
    test_shadow_healthy_and_fail_closed()
    test_shadow_terminal_mirror_fail_open()
    test_shadow_module_missing_falls_back_to_json()
    test_startup_reconcile_and_lease_tick()
    test_import_guard_and_repair()
    test_shadow_api_units()
    test_merge_staging_and_timeline_not_shadow_written()
    test_compare_gap_inbound_processed_lag()
    test_mode_flip_back_to_json()
    failed = [name for name, passed in ok if not passed]
    print(f"\nsqlite_switch: {len(ok) - len(failed)} passed, "
          f"{len(failed)} failed")
    if failed:
        print("FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
