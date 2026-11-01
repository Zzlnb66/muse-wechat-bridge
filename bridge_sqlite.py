"""SQLite shadow layer for the WeChat bridge (Gate 3 step 1).

Scope of this step, stated plainly so it cannot be misread later:

* Authority stages (Gate 3 step 2, mode file state/sqlite_authority.json,
  read by serve at startup; a missing file means json):
  - json (default): JSON files are the SOLE authority; the serve/sender
    loops never read the shadow DB. The dual/shadow code paths in
    bridge.py exist but are inert in this mode: zero behavior change.
  - dual: serve/drain dual-write JSON-then-shadow (shadow-first only
    for send-start/send-terminal); send decisions still follow JSON,
    and any shadow write failure is fail-open.
  - shadow: drain must obtain the sending_in_flight transition before
    calling sendmessage; only that step is fail-closed. Flipping the
    mode file to dual/shadow is a SEPARATE operator step that needs
    its own confirmation; this module never flips it.
* SQLite (state/bridge.db, WAL mode, 0600) rows still store metadata
  and hashes ONLY (see below).
* The five-state outbound lifecycle below is copied from the handover
  supplement (Gate 3 section 4), not invented here:
    1. pending           -- awaiting claim
    2. leased_inferring  -- Worker claimed it and is inferring; no
                            WeChat network send has started
    3. ready_to_send     -- Worker produced a reply, awaiting the
                            sender thread
    4. sending_in_flight -- sender thread is about to call, or is
                            calling, iLink sendmessage
    5. terminal group: sent (explicit success response received) /
       needs_check (send timed out or delivery result unknown) /
       dead (permanent failure)
  The supplement calls these "five states" with the terminal group as
  the fifth slot; the state column therefore holds seven literal
  values, and the transition table below is what makes the model exact.

Iron law (R1, supplement Gate 3 section 4 + main Gate 3 section 4):

* ONLY a job in leased_inferring whose lease has expired (Worker
  crashed without producing a result) may automatically fall back to
  pending for re-inference.
* Once a job has entered sending_in_flight, a timeout or interruption
  MUST lock it to needs_check for human review. Automatic re-send is
  forbidden, and needs_check NEVER returns to pending automatically.
  The SINGLE exception (parent decision 2026-11-01): a failure the
  bridge classified Retryable -- explicitly undelivered, the send
  function failed before any sendmessage post -- resets the row to
  ready_to_send via reset_for_retry (lease cleared), never to
  pending, so JSON's immediate 3-attempt retry survives dual/shadow.
* An unknown delivery result always goes to needs_check, never to a
  blind resend and never to dead (dead is only for permanent failures
  decided BEFORE any send attempt, or for jobs permanently rejected
  before claim -- never for an in-flight outcome).

Privacy rules for the shadow:

* Shadow rows store metadata and hashes ONLY. No message body, no
  token, no user identifier, no URL, no raw error text is ever written
  to SQLite. Record hashes are computed over a canonical JSON form
  with any key whose name contains "token" dropped first.
* Tombstones store the minimum anti-resurrection identifiers only:
  the message/job id, its kind, and its terminal state. See
  docs/sqlite-shadow-design.md for the retention semantics
  ("local save" vs "complete delete").

Run: python3 bridge_sqlite.py <init|import|compare|recover|backup>
  --root <bridge root> [--db <path>] [--backup <path>]
All commands are offline and never touch the live bridge's JSON files
except import, which only READS them.
"""

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

SCHEMA_VERSION = "bridge-sqlite-0.1"
DB_FILENAME = "bridge.db"

# ---------------------------------------------------------------- states
PENDING = "pending"
LEASED_INFERRING = "leased_inferring"
READY_TO_SEND = "ready_to_send"
SENDING_IN_FLIGHT = "sending_in_flight"
SENT = "sent"
NEEDS_CHECK = "needs_check"
DEAD = "dead"

OUTBOUND_STATES = (
    PENDING,
    LEASED_INFERRING,
    READY_TO_SEND,
    SENDING_IN_FLIGHT,
    SENT,
    NEEDS_CHECK,
    DEAD,
)
TERMINAL_STATES = (SENT, NEEDS_CHECK, DEAD)

INBOUND_RECEIVED = "received"
INBOUND_PROCESSED = "processed"
INBOUND_STATES = (INBOUND_RECEIVED, INBOUND_PROCESSED)

# Segment states mirror the per-part statuses of a long-text job.
SEGMENT_STATES = ("pending", "sent", "failed", NEEDS_CHECK)

# Failure codes: reuse the bridge's fixed vocabulary where one fits.
# lease_expired_in_flight is shadow-only (the bridge has no such code).
FAIL_LEASE_EXPIRED_IN_FLIGHT = "lease_expired_in_flight"

# Transition table: current state -> set of allowed next states.
# Every edge NOT listed here is forbidden and raises IllegalTransition.
# Notes on the deliberate edges:
#  - pending -> dead: ONLY for a permanent rejection decided before any
#    claim (e.g. recipient mismatch found before claim). Never for a
#    send outcome.
#  - leased_inferring -> pending: ONLY via lease-timeout recovery
#    (see transition_job's via_recovery + lease-expired check).
#  - leased_inferring -> dead: ONLY for a permanent failure reported
#    by the Worker before any send attempt.
#  - ready_to_send -> dead: ONLY for a permanent rejection found at
#    send preparation (e.g. recipient mismatch). Never after a send.
#  - sending_in_flight -> sent / needs_check, plus ONE gated edge:
#    sending_in_flight -> ready_to_send, ONLY via a Retryable reset
#    (see reset_for_retry): the send function failed BEFORE any
#    sendmessage post could deliver (explicitly undelivered), so the
#    job may retry under JSON's immediate 3-attempt rule. It can
#    NEVER go to pending (pending is the pre-claim state; the bridge
#    does not manufacture claims) or dead (dead is never decided
#    from an in-flight outcome). Timeout / interrupt / unknown
#    outcomes still go to needs_check, never through this edge.
#  - needs_check / sent / dead are terminal: NOTHING leaves them
#    automatically. A human resolves needs_check outside this module.
ALLOWED_TRANSITIONS = {
    PENDING: {LEASED_INFERRING, DEAD},
    LEASED_INFERRING: {PENDING, READY_TO_SEND, DEAD},
    READY_TO_SEND: {SENDING_IN_FLIGHT, DEAD},
    SENDING_IN_FLIGHT: {SENT, NEEDS_CHECK, READY_TO_SEND},
    SENT: set(),
    NEEDS_CHECK: set(),
    DEAD: set(),
}


class ShadowError(Exception):
    """Base class for shadow-layer errors (never a send decision)."""


class IllegalTransition(ShadowError):
    """A state transition the table forbids."""


class ShadowSchemaError(ShadowError):
    """The database file has an unexpected schema version."""


class ShadowImportBlocked(ShadowError):
    """A full re-import was refused because the authority mode is
    dual/shadow: import is DELETE-and-rebuild, which would wipe
    in-flight leases. Sanctioned repair path: stop serve, mode back
    to json, import, compare clean, flip back."""


# Authority modes (state/sqlite_authority.json, {"mode": ...}).
# Invalid values are NEVER silently treated as shadow: they fall back
# to json (fail-safe) and the caller reports the invalid value.
AUTHORITY_MODES = ("json", "dual", "shadow")
AUTHORITY_MODE_FILE = "sqlite_authority.json"


# ---------------------------------------------------------------- time
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return datetime.fromisoformat(value)


def _lease_expired(lease_until, now):
    if lease_until is None:
        return False
    until = _parse_iso(lease_until)
    at = _parse_iso(now) if now is not None else datetime.now(timezone.utc)
    return until <= at


# ---------------------------------------------------------------- hashing
def _scrubbed(obj):
    """Drop token-bearing keys recursively (B2-lite: tokens never land
    in any record, and are not even hashed into the shadow)."""
    if isinstance(obj, dict):
        return {
            k: _scrubbed(v)
            for k, v in obj.items()
            if "token" not in k.lower()
        }
    if isinstance(obj, list):
        return [_scrubbed(v) for v in obj]
    return obj


def record_sha256(obj):
    """Canonical sha256 of a JSON record (sorted keys, no token keys)."""
    canonical = json.dumps(
        _scrubbed(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def rows_sha256(rows):
    """sha256 over sorted (key, state, record_sha) triples."""
    canonical = json.dumps(sorted(rows), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- schema
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inbound_messages (
    message_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    source_bucket TEXT NOT NULL,
    record_sha256 TEXT NOT NULL,
    received_ts TEXT,
    updated_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbound_jobs (
    job_key TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    state TEXT NOT NULL,
    failure_code TEXT,
    lease_until TEXT,
    record_sha256 TEXT NOT NULL,
    updated_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbound_segments (
    job_key TEXT NOT NULL,
    part_index INTEGER NOT NULL,
    state TEXT NOT NULL,
    record_sha256 TEXT NOT NULL,
    updated_ts TEXT NOT NULL,
    PRIMARY KEY (job_key, part_index)
);
CREATE TABLE IF NOT EXISTS tombstones (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    terminal_state TEXT NOT NULL,
    created_ts TEXT NOT NULL
);
"""


def _state_dir_for(bridge_root):
    return os.path.join(bridge_root, "state")


def db_path_for(bridge_root):
    return os.path.join(_state_dir_for(bridge_root), DB_FILENAME)


def authority_mode_path_for(bridge_root):
    return os.path.join(_state_dir_for(bridge_root), AUTHORITY_MODE_FILE)


def read_authority_mode(bridge_root):
    """Read the authority mode file. Returns (mode, valid).

    Missing file -> ("json", True): json is the default. Unreadable
    JSON, a non-dict body, or a value outside AUTHORITY_MODES ->
    ("json", False): fail-safe to json, NEVER silently shadow; the
    caller reports the invalid value loudly. This function never
    writes the file: mode changes are operator actions."""
    path = authority_mode_path_for(bridge_root)
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return "json", True
    if not isinstance(doc, dict):
        return "json", False
    mode = doc.get("mode")
    if mode not in AUTHORITY_MODES:
        return "json", False
    return mode, True


def _chmod_0600(path):
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def connect_shadow(db_path):
    """Open (creating if needed) a shadow DB: WAL mode, 0600, schema
    checked. Raises ShadowSchemaError on a version mismatch; never
    migrates silently."""
    existed = os.path.exists(db_path)
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA_SQL)
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
            conn.commit()
        elif row[0] != SCHEMA_VERSION:
            raise ShadowSchemaError(
                "schema_version mismatch: db has %r, expected %r"
                % (row[0], SCHEMA_VERSION)
            )
    except Exception:
        conn.close()
        raise
    if not existed:
        _chmod_0600(db_path)
    return conn


def connect_shadow_readonly(db_path):
    """Open an existing shadow DB read-only (compare never writes)."""
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    return conn


# ---------------------------------------------------------------- transitions
def get_job(conn, job_key):
    row = conn.execute(
        "SELECT job_key, message_id, state, failure_code, lease_until,"
        " record_sha256, updated_ts FROM outbound_jobs WHERE job_key = ?",
        (job_key,),
    ).fetchone()
    if row is None:
        return None
    return {
        "job_key": row[0],
        "message_id": row[1],
        "state": row[2],
        "failure_code": row[3],
        "lease_until": row[4],
        "record_sha256": row[5],
        "updated_ts": row[6],
    }


def transition_job(conn, job_key, new_state, now=None, via_recovery=False,
                   via_retry=False, failure_code=None, lease_seconds=None):
    """Move one outbound job, enforcing ALLOWED_TRANSITIONS plus the
    iron law. The leased_inferring -> pending edge additionally
    requires via_recovery=True AND an expired lease; the
    sending_in_flight -> ready_to_send edge additionally requires
    via_retry=True (a Retryable reset, see reset_for_retry);
    anything else raises IllegalTransition and changes nothing.
    Callers commit."""
    if new_state not in OUTBOUND_STATES:
        raise IllegalTransition("unknown state %r" % (new_state,))
    job = get_job(conn, job_key)
    if job is None:
        raise ShadowError("unknown job %r" % (job_key,))
    current = job["state"]
    if new_state not in ALLOWED_TRANSITIONS[current]:
        raise IllegalTransition(
            "forbidden transition %s -> %s for job %s"
            % (current, new_state, job_key)
        )
    if current == LEASED_INFERRING and new_state == PENDING:
        if not via_recovery:
            raise IllegalTransition(
                "leased_inferring -> pending requires lease-timeout"
                " recovery, not a plain transition"
            )
        if not _lease_expired(job["lease_until"], now):
            raise IllegalTransition(
                "leased_inferring -> pending requires an expired lease"
            )
    if current == SENDING_IN_FLIGHT and new_state == READY_TO_SEND:
        if not via_retry:
            raise IllegalTransition(
                "sending_in_flight -> ready_to_send requires a"
                " Retryable reset (via_retry), not a plain transition"
            )
    lease_until = job["lease_until"]
    if lease_seconds is not None:
        base = _parse_iso(now) if now is not None else datetime.now(timezone.utc)
        from datetime import timedelta

        lease_until = (base + timedelta(seconds=lease_seconds)).isoformat()
    elif new_state in (PENDING, READY_TO_SEND, SENT, NEEDS_CHECK, DEAD):
        # The inference lease ends when the result is produced; the
        # sender takes its own lease at begin_send.
        lease_until = None
    conn.execute(
        "UPDATE outbound_jobs SET state = ?, failure_code = ?,"
        " lease_until = ?, updated_ts = ? WHERE job_key = ?",
        (
            new_state,
            failure_code if failure_code is not None else job["failure_code"],
            lease_until,
            _now_iso(),
            job_key,
        ),
    )
    return get_job(conn, job_key)


def claim_job(conn, job_key, lease_seconds, now=None):
    """pending -> leased_inferring with a lease deadline."""
    return transition_job(
        conn, job_key, LEASED_INFERRING, now=now, lease_seconds=lease_seconds
    )


def mark_ready(conn, job_key):
    """leased_inferring -> ready_to_send (Worker produced a result)."""
    return transition_job(conn, job_key, READY_TO_SEND)


def begin_send(conn, job_key, lease_seconds=120, now=None):
    """ready_to_send -> sending_in_flight with a send lease."""
    return transition_job(
        conn, job_key, SENDING_IN_FLIGHT, now=now, lease_seconds=lease_seconds
    )


def finish_send(conn, job_key, outcome):
    """Close an in-flight job. outcome is 'sent' (explicit success),
    'unknown' (timeout/interrupt/unknown -> needs_check, locked), or
    'permanent' -- which is REJECTED here: a permanent failure can
    never be decided from an in-flight outcome, because delivery may
    already have happened. Use needs_check instead."""
    if outcome == "sent":
        return transition_job(conn, job_key, SENT)
    if outcome == "unknown":
        return transition_job(
            conn, job_key, NEEDS_CHECK, failure_code="delivery_unknown"
        )
    raise ShadowError(
        "finish_send outcome %r is not allowed from sending_in_flight;"
        " use 'unknown' (needs_check) for any non-success outcome" % (outcome,)
    )


def reset_for_retry(conn, job_key):
    """Retryable-reset edge (parent decision 2026-11-01, dual/shadow
    soak): sending_in_flight -> ready_to_send, ONLY for a failure
    the bridge classified Retryable -- the send function failed
    before any sendmessage post could deliver (explicitly
    undelivered: upload / getuploadurl failures, never a post-send
    ambiguity). transition_job clears the send lease on this edge
    (ready_to_send is in its lease-clearing set), so the next drain
    can begin_send again under JSON's immediate 3-attempt rule.
    Unknown / timeout / interrupt outcomes must NEVER call this:
    they go to needs_check via finish_send. Returns (row, action):
    "reset", or "already_ready" when a dual fail-open begin_send
    left the row ready_to_send. Raises IllegalTransition for
    terminal rows or any other non-resettable state, and
    ShadowError for a missing row. Callers commit."""
    row = get_job(conn, job_key)
    if row is None:
        raise ShadowError("unknown job %r" % (job_key,))
    if row["state"] == READY_TO_SEND:
        return row, "already_ready"
    return transition_job(
        conn, job_key, READY_TO_SEND, via_retry=True), "reset"


def recover_expired_leases(conn, now=None):
    """The ONLY automatic recovery in the shadow layer (iron law):

    * leased_inferring with an expired lease -> pending (Worker
      crashed without producing a result; re-inference is safe
      because no send was ever attempted).
    * sending_in_flight with an expired lease -> needs_check (locked
      for a human; NEVER back to pending, NEVER re-sent).
    * sent / needs_check / dead are never touched.
    Returns a report dict; callers commit."""
    at = _parse_iso(now) if now is not None else datetime.now(timezone.utc)
    report = {"recovered_to_pending": [], "locked_to_needs_check": []}
    rows = conn.execute(
        "SELECT job_key, state, lease_until FROM outbound_jobs"
        " WHERE state IN (?, ?)",
        (LEASED_INFERRING, SENDING_IN_FLIGHT),
    ).fetchall()
    for job_key, state, lease_until in rows:
        if not _lease_expired(lease_until, at.isoformat()):
            continue
        if state == LEASED_INFERRING:
            transition_job(
                conn, job_key, PENDING, now=at.isoformat(), via_recovery=True
            )
            report["recovered_to_pending"].append(job_key)
        else:
            transition_job(
                conn, job_key, NEEDS_CHECK, now=at.isoformat(),
                failure_code=FAIL_LEASE_EXPIRED_IN_FLIGHT,
            )
            report["locked_to_needs_check"].append(job_key)
    return report


# ---------------------------------------------------------------- dual-write API
# Gate 3 step 2 (switch implementation). These are the ONLY shadow
# write entry points the serve/drain loops may use, and every one of
# them is called through bridge.py's guarded wrapper (which commits
# on success, rolls back on error, and never lets an exception reach
# the serve/drain loops). Direct transition_job/finish_send calls
# from bridge.py are NOT used for live mirroring, because the
# transition table has no edge for the pre-send terminal cases
# (ready_to_send -> needs_check, e.g. recipient-gate parking, and
# ready_to_send -> dead for skipped_empty): mirror_job_terminal
# below handles those with a direct, audited update that NEVER moves
# a row OUT of a terminal state. Callers commit.
def get_inbound(conn, message_id):
    row = conn.execute(
        "SELECT message_id, state, source_bucket, record_sha256,"
        " received_ts, updated_ts FROM inbound_messages"
        " WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "message_id": row[0],
        "state": row[1],
        "source_bucket": row[2],
        "record_sha256": row[3],
        "received_ts": row[4],
        "updated_ts": row[5],
    }


def upsert_inbound(conn, message_id, state, record_sha256_value,
                   source_bucket="inbox"):
    """Insert or refresh one inbound row. received_ts is set only on
    insert (an upsert must not rewrite history); state/sha follow the
    latest JSON record. Raises ShadowError on an unknown state."""
    if state not in INBOUND_STATES:
        raise ShadowError("unknown inbound state %r" % (state,))
    conn.execute(
        "INSERT INTO inbound_messages (message_id, state, source_bucket,"
        " record_sha256, received_ts, updated_ts)"
        " VALUES (?, ?, ?, ?, ?, ?)"
        " ON CONFLICT(message_id) DO UPDATE SET state = excluded.state,"
        " source_bucket = excluded.source_bucket,"
        " record_sha256 = excluded.record_sha256,"
        " updated_ts = excluded.updated_ts",
        (message_id, state, source_bucket, record_sha256_value,
         _now_iso(), _now_iso()),
    )
    return get_inbound(conn, message_id)


def upsert_job_ready(conn, job_key, record_sha256_value):
    """Drain-first-seen convergence for one outbound job.

    Returns (row, action):
    - missing row -> INSERT ready_to_send ("inserted");
    - pending row -> direct UPDATE to ready_to_send
      ("upgraded_pending": the pending-dir JSON file proves the task
      was already produced, so the stale pending row is converged;
      the transition table has no pending -> ready_to_send edge
      because claim is Worker-side, but a JSON task file in
      pending/ IS the production fact);
    - ready_to_send -> kept ("kept");
    - sending_in_flight / leased_inferring / terminal -> kept
      ("kept"): the caller decides skip/lock/archive from the row
      state, this function never downgrades or advances them."""
    row = get_job(conn, job_key)
    if row is None:
        conn.execute(
            "INSERT INTO outbound_jobs (job_key, message_id, state,"
            " failure_code, lease_until, record_sha256, updated_ts)"
            " VALUES (?, ?, ?, NULL, NULL, ?, ?)",
            (job_key, job_key.split(":")[0], READY_TO_SEND,
             record_sha256_value, _now_iso()),
        )
        return get_job(conn, job_key), "inserted"
    if row["state"] == PENDING:
        conn.execute(
            "UPDATE outbound_jobs SET state = ?, record_sha256 = ?,"
            " lease_until = NULL, updated_ts = ? WHERE job_key = ?",
            (READY_TO_SEND, record_sha256_value, _now_iso(), job_key),
        )
        return get_job(conn, job_key), "upgraded_pending"
    return row, "kept"


def upsert_job_segments(conn, job_key, job_dict):
    """Mirror a job's segment states (descriptive only; segment rows
    NEVER participate in send decisions, and compare ignores them).
    Returns the number of segment rows written."""
    count = 0
    sha = record_sha256(job_dict)
    for part_index, seg_state in _segment_states_for_job(job_dict):
        conn.execute(
            "INSERT INTO outbound_segments (job_key, part_index, state,"
            " record_sha256, updated_ts) VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(job_key, part_index) DO UPDATE SET"
            " state = excluded.state,"
            " record_sha256 = excluded.record_sha256,"
            " updated_ts = excluded.updated_ts",
            (job_key, part_index, seg_state, sha, _now_iso()),
        )
        count += 1
    return count


def update_job_record_sha(conn, job_key, record_sha256_value):
    """Refresh one job row's record hash (used when the bridge moves
    a JSON file after mutating its terminal flags, so the shadow row
    keeps matching the archived file for compare). State untouched.
    Returns True when a row was updated."""
    cur = conn.execute(
        "UPDATE outbound_jobs SET record_sha256 = ?, updated_ts = ?"
        " WHERE job_key = ?",
        (record_sha256_value, _now_iso(), job_key),
    )
    return cur.rowcount > 0


def mirror_job_terminal(conn, job_key, target_state, failure_code=None,
                        record_sha256_value=None):
    """Mirror a job-level terminal disposition into the shadow row.

    Returns (row, action):
    - missing row -> INSERT terminal ("inserted");
    - already terminal -> kept, NEVER downgraded ("kept_terminal");
    - sending_in_flight + sent -> finish_send("sent")
      ("finished_sent"); + needs_check -> finish_send("unknown")
      ("finished_unknown"); + dead -> direct UPDATE to dead
      ("direct_dead": finish_send refuses permanent outcomes from
      in-flight, but a failed-dir disposition is an established
      JSON fact, and import maps it to dead the same way);
    - ready_to_send / pending / leased_inferring + any terminal ->
      direct UPDATE ("direct_terminal": the transition table has
      no ready -> needs_check edge, yet pre-send dispositions
      (recipient gate, sanitize, plan mismatch) must land in the
      same terminal state the JSON snapshot mapping reports, or
      compare would disagree forever).
    The bridge failure code is stored when given (finish_send's
    delivery_unknown is overwritten with it, so the shadow row
    carries the same code the operator sees on the JSON job).
    This function never moves a row OUT of a terminal state and
    never sends anything."""
    if target_state not in TERMINAL_STATES:
        raise ShadowError("mirror target must be terminal, got %r"
                          % (target_state,))
    row = get_job(conn, job_key)
    if row is None:
        conn.execute(
            "INSERT INTO outbound_jobs (job_key, message_id, state,"
            " failure_code, lease_until, record_sha256, updated_ts)"
            " VALUES (?, ?, ?, ?, NULL, ?, ?)",
            (job_key, job_key.split(":")[0], target_state, failure_code,
             record_sha256_value if record_sha256_value is not None
             else "", _now_iso()),
        )
        return get_job(conn, job_key), "inserted"
    if row["state"] in TERMINAL_STATES:
        return row, "kept_terminal"
    if row["state"] == SENDING_IN_FLIGHT and target_state == SENT:
        finish_send(conn, job_key, "sent")
        if failure_code is not None or record_sha256_value is not None:
            conn.execute(
                "UPDATE outbound_jobs SET failure_code = COALESCE(?, failure_code),"
                " record_sha256 = COALESCE(?, record_sha256)"
                " WHERE job_key = ?",
                (failure_code, record_sha256_value, job_key),
            )
        return get_job(conn, job_key), "finished_sent"
    if row["state"] == SENDING_IN_FLIGHT and target_state == NEEDS_CHECK:
        finish_send(conn, job_key, "unknown")
        if failure_code is not None or record_sha256_value is not None:
            conn.execute(
                "UPDATE outbound_jobs SET failure_code = COALESCE(?, failure_code),"
                " record_sha256 = COALESCE(?, record_sha256)"
                " WHERE job_key = ?",
                (failure_code, record_sha256_value, job_key),
            )
        return get_job(conn, job_key), "finished_unknown"
    conn.execute(
        "UPDATE outbound_jobs SET state = ?, failure_code = ?,"
        " lease_until = NULL, record_sha256 = COALESCE(?, record_sha256),"
        " updated_ts = ? WHERE job_key = ?",
        (target_state, failure_code, record_sha256_value, _now_iso(),
         job_key),
    )
    return get_job(conn, job_key), "direct_dead" \
        if target_state == DEAD and row["state"] == SENDING_IN_FLIGHT \
        else "direct_terminal"


def _is_shadow_tracked_job_name(name):
    """Mirror json_snapshot's auxiliary-job exclusion: bridge-written
    receipt jobs (_progress/_offline/_queue) are NOT shadow-tracked.
    Untracked jobs get no shadow rows at all, in any mode."""
    return not ("_progress" in name or "_offline" in name
                or "_queue" in name)


def reconcile_on_startup(conn, bridge_root):
    """Startup reconciliation (design section 2.4). SHADOW SIDE ONLY:
    it performs no JSON file move and sends nothing. It first runs
    the lease recovery (expired in-flight -> needs_check, expired
    leased -> pending), then inspects every tracked pending-dir JSON
    job and reports what the BRIDGE should do with the file:

    - shadow sent -> move JSON to sent/ (already delivered, no resend);
    - shadow needs_check/dead -> move JSON to failed/;
    - shadow sending_in_flight, lease unexpired -> leave alone;
    - shadow leased_inferring / missing row / unreadable JSON ->
      report only, never auto-move.
    Callers commit. The bridge applies the moves (guarded, never
    raising) and records timeline events itself."""
    report = recover_expired_leases(conn)
    report.update({
        "move_to_sent": [],
        "move_to_failed": [],
        "left_inflight": [],
        "missing_row": [],
        "skipped": [],
    })
    pending_dir = os.path.join(bridge_root, "outbox", "pending")
    if not os.path.isdir(pending_dir):
        return report
    for name in sorted(os.listdir(pending_dir)):
        if not name.endswith(".json") or not _is_shadow_tracked_job_name(name):
            continue
        key = _job_id_from_filename(name)
        try:
            with open(os.path.join(pending_dir, name),
                      encoding="utf-8") as f:
                json.load(f)
        except (OSError, ValueError):
            report["skipped"].append(key)
            continue
        row = get_job(conn, key)
        if row is None:
            report["missing_row"].append(key)
        elif row["state"] == SENT:
            report["move_to_sent"].append(key)
        elif row["state"] in (NEEDS_CHECK, DEAD):
            report["move_to_failed"].append({
                "job_key": key,
                "state": row["state"],
                "failure_code": row["failure_code"],
            })
        elif row["state"] == SENDING_IN_FLIGHT:
            if key not in report["locked_to_needs_check"]:
                report["left_inflight"].append(key)
        else:
            report["skipped"].append(key)
    return report


def repair_job_row(conn, bridge_root, job_key):
    """Targeted post-switch repair (design section 5.2). The ONLY
    sanctioned row fix while a dual/shadow serve runs: it aligns ONE
    shadow row with the JSON archive fact and moves NO JSON file.

    - JSON in sent/ -> shadow sent;
    - JSON in failed/ -> shadow needs_check/dead per the same mapping
      import uses (needs_check wins when marked, else dead);
    - JSON in pending/ + missing row -> insert ready_to_send;
    - JSON in pending/ + any row -> no-op (in-flight rows wait out
      their lease; terminal rows are history, never rewritten);
    - JSON nowhere -> no-op (nothing to align to).
    Terminal rows are never downgraded. Returns a report dict; the
    caller commits and records the repair (job_key, before/after)
    in a desensitized failures/ marker -- the audit row lives
    outside SQLite by design (no schema change in this switch)."""
    outbox_dir = os.path.join(bridge_root, "outbox")
    before = get_job(conn, job_key)
    before_state = before["state"] if before else None
    sent_path = os.path.join(outbox_dir, "sent", job_key + ".json")
    failed_path = os.path.join(outbox_dir, "failed", job_key + ".json")
    pending_path = os.path.join(outbox_dir, "pending", job_key + ".json")
    if os.path.isfile(sent_path):
        row, action = mirror_job_terminal(conn, job_key, SENT)
        return {"job_key": job_key, "json_side": "sent",
                "before_state": before_state,
                "after_state": row["state"], "action": action}
    if os.path.isfile(failed_path):
        try:
            record = _read_json_file(failed_path)
        except (OSError, ValueError):
            return {"job_key": job_key, "json_side": "failed",
                    "before_state": before_state,
                    "after_state": before_state,
                    "action": "skipped_unreadable"}
        state, failure_code = _job_state_from_failed_dict(record)
        row, action = mirror_job_terminal(conn, job_key, state, failure_code)
        return {"job_key": job_key, "json_side": "failed",
                "before_state": before_state,
                "after_state": row["state"], "action": action}
    if os.path.isfile(pending_path):
        if before is None:
            try:
                record = _read_json_file(pending_path)
            except (OSError, ValueError):
                return {"job_key": job_key, "json_side": "pending",
                        "before_state": before_state,
                        "after_state": before_state,
                        "action": "skipped_unreadable"}
            row, action = upsert_job_ready(
                conn, job_key, record_sha256(record))
            return {"job_key": job_key, "json_side": "pending",
                    "before_state": before_state,
                    "after_state": row["state"], "action": action}
        return {"job_key": job_key, "json_side": "pending",
                "before_state": before_state,
                "after_state": before_state, "action": "kept"}
    return {"job_key": job_key, "json_side": "missing",
            "before_state": before_state,
            "after_state": before_state, "action": "kept"}


# ---------------------------------------------------------------- JSON snapshot
def _read_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _job_id_from_filename(filename):
    return filename[:-5] if filename.endswith(".json") else filename


def _job_state_from_failed_dict(job):
    """Map a failed-dir job dict to a shadow terminal state.

    needs_check wins whenever the job explicitly marks it; otherwise a
    failed-dir job is a permanent disposition (dead). skipped_empty is
    a permanent non-send, also dead, with its own failure code."""
    if job.get("needs_check") is True:
        return NEEDS_CHECK, job.get("failure_code") or "needs_check"
    for field in ("delivery_state", "terminal", "terminal_state", "outcome"):
        if job.get(field) == NEEDS_CHECK:
            return NEEDS_CHECK, job.get("failure_code") or "needs_check"
    if job.get("skipped_empty") is True or job.get("terminal") == "skipped_empty":
        return DEAD, "skipped_empty"
    return DEAD, job.get("failure_code") or job.get("error_code")


def _segment_states_for_job(job):
    """Yield (part_index, state) for a job's text segments.

    A plain job has one segment (index 0, state from the job's own
    delivery status). A long-text plan job has one row per part, keyed
    by the plan's part status (sending/sent -> sent only when the whole
    job is sent; failed/unknown -> failed/needs_check). Segment states
    are descriptive mirror data; the iron law lives on the job row."""
    parts = job.get("text_parts")
    if isinstance(parts, list) and parts:
        for i, part in enumerate(parts):
            status = part.get("status") if isinstance(part, dict) else None
            if status == "sent":
                yield i, "sent"
            elif status in ("failed", "unknown"):
                yield i, "failed"
            elif status == NEEDS_CHECK:
                yield i, NEEDS_CHECK
            else:
                yield i, "pending"
        return
    yield 0, "pending"


def json_snapshot(bridge_root):
    """Read-only snapshot of the JSON authority.

    Returns {inbound: {message_id: (state, sha)}, outbound: {job_key:
    (state, failure_code, sha)}, segments: {(job_key, part_index):
    state}, files_read: int}. Raises on unreadable/corrupt JSON so an
    import can roll back instead of mirroring half a tree."""
    inbound = {}
    outbound = {}
    segments = {}
    files_read = 0
    inbox_dir = os.path.join(bridge_root, "inbox")
    done_dir = os.path.join(bridge_root, "done")
    outbox_dir = os.path.join(bridge_root, "outbox")
    for directory, state in ((inbox_dir, INBOUND_RECEIVED),
                             (done_dir, INBOUND_PROCESSED)):
        if not os.path.isdir(directory):
            continue
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".json"):
                continue
            path = os.path.join(directory, name)
            record = _read_json_file(path)
            mid = str(record.get("message_id", _job_id_from_filename(name)))
            inbound[mid] = (state, record_sha256(record))
            files_read += 1
    for bucket, state in (("pending", READY_TO_SEND), ("sent", SENT)):
        directory = os.path.join(outbox_dir, bucket)
        if not os.path.isdir(directory):
            continue
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".json") or "_progress" in name \
                    or "_offline" in name or "_queue" in name:
                continue
            path = os.path.join(directory, name)
            record = _read_json_file(path)
            key = _job_id_from_filename(name)
            outbound[key] = (
                state,
                record.get("failure_code") or record.get("error_code"),
                record_sha256(record),
            )
            for i, seg_state in _segment_states_for_job(record):
                segments[(key, i)] = seg_state
            files_read += 1
    failed_dir = os.path.join(outbox_dir, "failed")
    if os.path.isdir(failed_dir):
        for name in sorted(os.listdir(failed_dir)):
            if not name.endswith(".json") or "_progress" in name \
                    or "_offline" in name or "_queue" in name:
                continue
            path = os.path.join(failed_dir, name)
            record = _read_json_file(path)
            key = _job_id_from_filename(name)
            state, failure_code = _job_state_from_failed_dict(record)
            outbound[key] = (state, failure_code, record_sha256(record))
            for i, seg_state in _segment_states_for_job(record):
                segments[(key, i)] = seg_state
            files_read += 1
    return {
        "inbound": inbound,
        "outbound": outbound,
        "segments": segments,
        "files_read": files_read,
    }


# ---------------------------------------------------------------- import
def import_json_shadow(bridge_root, db_path, authority_mode=None):
    """Import the JSON snapshot into the shadow DB in ONE transaction.

    Idempotent: re-importing the same tree yields the same row counts
    and the same rows hash (PRIMARY KEY upserts). On any error the
    transaction rolls back and the JSON files are untouched (they are
    only ever read). Returns a report dict.

    authority_mode "dual"/"shadow" REFUSES the import (raises
    ShadowImportBlocked): import is DELETE-and-rebuild, which would
    wipe in-flight leases while a dual/shadow serve is running.
    authority_mode None keeps the historical behavior (allowed) so
    json-mode callers and drills need no change; the CLI always
    passes the mode read from the mode file."""
    if authority_mode in ("dual", "shadow"):
        raise ShadowImportBlocked(
            "full re-import refused in %r mode: stop serve, mode back"
            " to json, import, compare clean, flip back" % authority_mode
        )
    snapshot = json_snapshot(bridge_root)
    conn = connect_shadow(db_path)
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM inbound_messages")
        conn.execute("DELETE FROM outbound_jobs")
        conn.execute("DELETE FROM outbound_segments")
        now = _now_iso()
        for mid, (state, sha) in snapshot["inbound"].items():
            conn.execute(
                "INSERT INTO inbound_messages (message_id, state,"
                " source_bucket, record_sha256, received_ts, updated_ts)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (mid, state,
                 "inbox" if state == INBOUND_RECEIVED else "done",
                 sha, None, now),
            )
        for key, (state, failure_code, sha) in snapshot["outbound"].items():
            conn.execute(
                "INSERT INTO outbound_jobs (job_key, message_id, state,"
                " failure_code, lease_until, record_sha256, updated_ts)"
                " VALUES (?, ?, ?, ?, NULL, ?, ?)",
                (key, key.split(":")[0], state, failure_code, sha, now),
            )
        for (key, part_index), seg_state in snapshot["segments"].items():
            conn.execute(
                "INSERT INTO outbound_segments (job_key, part_index,"
                " state, record_sha256, updated_ts) VALUES (?, ?, ?, ?, ?)",
                (key, part_index, seg_state,
                 snapshot["outbound"][key][2], now),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    rows = (
        [(k, v[0], v[1]) for k, v in snapshot["inbound"].items()]
        + [(k, v[0], v[2]) for k, v in snapshot["outbound"].items()]
    )
    return {
        "inbound_count": len(snapshot["inbound"]),
        "outbound_count": len(snapshot["outbound"]),
        "segment_count": len(snapshot["segments"]),
        "files_read": snapshot["files_read"],
        "rows_sha256": rows_sha256(rows),
    }


# ---------------------------------------------------------------- compare
def compare_shadow(bridge_root, db_path):
    """Read-only shadow comparison. NEVER writes to the DB and NEVER
    corrects a mismatch: mismatches are reported for a human (or a
    later explicit re-import) to resolve. Returns a report dict.

    Benign lag (parent decision 2026-11-01, soak criterion): the
    bridge never observes the Worker moving inbox/ -> done/, so a
    shadow inbound row stays "received" while JSON says "processed".
    When -- and ONLY when -- the shadow state is received, the JSON
    state is processed, AND the two record hashes are exactly equal,
    that row is listed under "benign_lag" (count + message-id list)
    and does NOT count as a mismatch, so "match" stays true. Any
    hash inequality is still a true mismatch, and every other state
    combination is unchanged. No done-sweep write point is added:
    compare stays read-only and never changes authority or rows."""
    snapshot = json_snapshot(bridge_root)
    conn = connect_shadow_readonly(db_path)
    if conn is None:
        return {
            "shadow_present": False,
            "match": False,
            "mismatches": ["shadow database missing: %s" % db_path],
            "benign_lag": {"count": 0, "message_ids": []},
            "json": {
                "inbound_count": len(snapshot["inbound"]),
                "outbound_count": len(snapshot["outbound"]),
            },
            "sqlite": {"inbound_count": 0, "outbound_count": 0},
        }
    try:
        db_inbound = {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT message_id, state, record_sha256"
                " FROM inbound_messages"
            ).fetchall()
        }
        db_outbound = {
            row[0]: (row[1], row[2])
            for row in conn.execute(
                "SELECT job_key, state, record_sha256 FROM outbound_jobs"
            ).fetchall()
        }
    finally:
        conn.close()
    mismatches = []
    benign_lag = []
    for mid, (state, sha) in snapshot["inbound"].items():
        if mid not in db_inbound:
            mismatches.append("inbound missing in shadow: %s" % mid)
        elif db_inbound[mid] != (state, sha):
            db_state, db_sha = db_inbound[mid]
            if db_state == INBOUND_RECEIVED and state == INBOUND_PROCESSED \
                    and db_sha == sha:
                benign_lag.append(mid)
            else:
                mismatches.append("inbound differs in shadow: %s" % mid)
    for mid in db_inbound:
        if mid not in snapshot["inbound"]:
            mismatches.append("inbound extra in shadow: %s" % mid)
    for key, (state, _fc, sha) in snapshot["outbound"].items():
        if key not in db_outbound:
            mismatches.append("outbound missing in shadow: %s" % key)
        elif db_outbound[key] != (state, sha):
            mismatches.append("outbound differs in shadow: %s" % key)
    for key in db_outbound:
        if key not in snapshot["outbound"]:
            mismatches.append("outbound extra in shadow: %s" % key)
    return {
        "shadow_present": True,
        "match": not mismatches,
        "mismatches": mismatches,
        "benign_lag": {
            "count": len(benign_lag),
            "message_ids": sorted(benign_lag),
        },
        "json": {
            "inbound_count": len(snapshot["inbound"]),
            "outbound_count": len(snapshot["outbound"]),
        },
        "sqlite": {
            "inbound_count": len(db_inbound),
            "outbound_count": len(db_outbound),
        },
    }


# ---------------------------------------------------------------- backup
def backup_shadow(db_path, backup_path):
    """SQLite online backup (handles WAL correctly). Read-only for
    the source database's logical content."""
    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(backup_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    _chmod_0600(backup_path)


def restore_shadow(backup_path, db_path):
    """Restore a backup file over a shadow DB path (file copy; the
    caller owns the decision -- this is a drill/ops helper, not an
    automatic recovery). Verifies the restored file opens and reports
    its row counts."""
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    shutil.copyfile(backup_path, db_path)
    _chmod_0600(db_path)
    conn = connect_shadow_readonly(db_path)
    counts = {}
    try:
        for table in ("inbound_messages", "outbound_jobs",
                      "outbound_segments", "tombstones"):
            counts[table] = conn.execute(
                "SELECT COUNT(*) FROM %s" % table
            ).fetchone()[0]
    finally:
        conn.close()
    return counts


# ---------------------------------------------------------------- tombstones
def record_tombstone(conn, tomb_id, kind, terminal_state):
    """Minimal anti-resurrection identifier ONLY: id + kind + terminal
    state + timestamp. No body, no token, no content hash. Tombstones
    are never auto-cleared (see the design doc: 'complete delete'
    removes content rows but keeps the tombstone)."""
    if terminal_state not in TERMINAL_STATES:
        raise ShadowError(
            "tombstones only record terminal states, got %r" % (terminal_state,)
        )
    conn.execute(
        "INSERT OR REPLACE INTO tombstones (id, kind, terminal_state,"
        " created_ts) VALUES (?, ?, ?, ?)",
        (tomb_id, kind, terminal_state, _now_iso()),
    )


# ---------------------------------------------------------------- CLI
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=[
        "init", "import", "compare", "recover", "backup",
    ])
    parser.add_argument("--root", default=None,
                        help="bridge root (default: parent of this file)")
    parser.add_argument("--db", default=None)
    parser.add_argument("--backup", default=None)
    args = parser.parse_args(argv)
    root = args.root or os.path.dirname(os.path.abspath(__file__))
    db_path = args.db or db_path_for(root)
    if args.command == "init":
        conn = connect_shadow(db_path)
        conn.close()
        print(json.dumps({"db": db_path, "schema": SCHEMA_VERSION}))
    elif args.command == "import":
        mode, _valid = read_authority_mode(root)
        if mode in ("dual", "shadow"):
            print(json.dumps({
                "error": "import refused in %r mode: stop serve, mode"
                         " back to json, import, compare clean, flip"
                         " back" % mode,
            }, indent=2, sort_keys=True))
            return 2
        print(json.dumps(import_json_shadow(root, db_path), indent=2,
                         sort_keys=True))
    elif args.command == "compare":
        print(json.dumps(compare_shadow(root, db_path), indent=2,
                         sort_keys=True))
    elif args.command == "recover":
        conn = connect_shadow(db_path)
        try:
            report = recover_expired_leases(conn)
            conn.commit()
        finally:
            conn.close()
        print(json.dumps(report, indent=2, sort_keys=True))
    elif args.command == "backup":
        if not args.backup:
            parser.error("--backup is required for backup")
        backup_shadow(db_path, args.backup)
        print(json.dumps({"backup": args.backup}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
