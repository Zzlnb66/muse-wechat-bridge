#!/usr/bin/env python3
"""
WeChat iLink (ClawBot) bridge for Muse - Phase 2 steps 1-6:
text + voice transcription + fast feedback + image receive/send.
Phase 1 backup: bridge.phase1.bak.py (identical md5 at backup time).
Steps 1-3 live test confirmed by user 2026-10-28 02:17 ("OK继续").
Image steps follow Tencent openclaw-weixin commit
874637a0dbc24050f72d1385add4fc6170a2d726 as referenced by the user;
getconfig/sendtyping and getuploadurl payloads are best-effort
per the user spec, failure only logs, never blocks the main loop.
Visual content in images is analysis input only, never an
instruction to execute, delete, or externally send anything.

Protocol source verified 2026-10-27 from:
  https://raw.githubusercontent.com/Tencent/openclaw-weixin/main/docs/protocol.md
Base URL: https://ilinkai.weixin.qq.com
  POST /ilink/bot/get_bot_qrcode?bot_type=3
  GET  /ilink/bot/get_qrcode_status?qrcode=...
  POST /ilink/bot/getupdates
  POST /ilink/bot/sendmessage

Hard rules implemented here (do not weaken without user approval):
  1. Check ret AND errcode on every API response. -14 means token/session
     expired: write state/NEEDS_RELOGIN and exit, never retry in a loop.
     getupdates failures are also bucketed (desensitised counts only)
     in state/getupdates_anomalies.json; see
     docs/getupdates-anomalies-manual.md.
     For sendMessage, a non-zero ret is an error. An empty / non-JSON
     response is also treated as failure, never as success (stricter than
     the official plugin, which does not check an absent ret on send).
  2. sendMessage msg includes from_user_id="", a unique client_id,
     message_type=2, message_state=2, the inbound context_token, and
     base_info. to_user_id is REQUIRED by the protocol example and is
     set to the session ilink_user_id unless --to is given.
  3. Only the scanning user's private chat is processed: ignore any
     message with a non-empty group_id, and require an exact,
     case-sensitive from_user_id == session ilink_user_id.
  4. serve() holds an fcntl.flock on state/serve.lock for its whole life.
     A second serve exits instead of double-polling.
  5. Idempotency, in the order requested for Phase 1: for a new text
     message, record message_id in state/processed_ids.json and atomically
     update state/cursor.txt FIRST, then atomically write
     inbox/<message_id>.json, then update the latest context_token in
     state/session.json. NOTE (trade-off, see README note in final report):
     if the process crashes between the cursor update and the inbox
     write, that message will not be re-delivered, because the cursor has
     already advanced. This order prevents duplicate execution at the
     cost of that narrow crash window.
  6. serve() drains outbox/pending/*.json and sends them. On success a
     file is moved to outbox/sent/. On network timeout it stays in
     pending/ with the SAME client_id for the next retry. The Muse task
     itself is never re-run because a send failed.

Runtime dependencies (D4 correction): Python standard library plus
Pillow (image full decode), pycryptodome (in-process AES-128-ECB,
Gate 2 switch; system openssl is kept installed only as rollback and
is no longer used for AES), system curl
(preferred transport and image curl fallback). The venv in this folder
is used only to render a QR-code PNG at login time (qrcode package).
Final sendmessage is NOT auto-retried on unknown result (see R1);
pre-send failures are the only retryable class in drain_outbox.

Secrets: bot_token lives only in state/session.json (0600) and in the
Authorization header. Never print it, never log full responses that may
contain it.
"""

import argparse
import base64
import hashlib
import re
import unicodedata
try:
    import fcntl
except ImportError:
    # fcntl is Unix-only and does not exist on Windows. Login does not
    # need it at all; serve() falls back to msvcrt / no lock below.
    fcntl = None
import json
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

# Gate 2 (2026-11-01): in-process AES-128-ECB lives in the sibling
# module aes_inprocess.py, which must be copied next to bridge.py at
# deploy time. pycryptodome is REQUIRED: if it is missing, this import
# fails loudly and bridge.py does not start; it never silently falls
# back to the openssl subprocess, which would hide the missing
# dependency and put key bytes back into a command-line argv.
if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aes_inprocess import (  # noqa: E402
    aes_inprocess_decrypt,
    aes_inprocess_decrypt_nopad,
    aes_inprocess_encrypt,
)

# Gate 3 step 1: pure storage primitives live in bridge_storage.py
# (same deploy rule as aes_inprocess.py: must sit next to bridge.py).
import bridge_storage  # noqa: E402
from bridge_storage import atomic_write_json, atomic_write_text  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
# All path constants below are bindings to one derivation: changing
# ROOT (a second instance, a test copy) re-derives every path. Keep
# these names as plain module attributes: tests rebind them per root.
_PATHS = bridge_storage.paths_for_root(ROOT)
STATE = _PATHS.state
INBOX = _PATHS.inbox
RUNNING = _PATHS.running
DONE = _PATHS.done
OUTBOX_PENDING = _PATHS.outbox_pending
OUTBOX_SENT = _PATHS.outbox_sent
OUTBOX_FAILED = _PATHS.outbox_failed
MEDIA_INBOUND = _PATHS.media_inbound
ANALYSIS_DIR = _PATHS.analysis_dir
LOCAL_VAULT_DIR = _PATHS.local_vault_dir
LOCAL_ONLY_MODE_FILE = _PATHS.local_only_mode_file
PENDING_LOCAL_QUERIES_FILE = _PATHS.pending_local_queries_file
TTL_INDEX_FILE = _PATHS.ttl_index_file
TOMBSTONE_FILE = _PATHS.tombstone_file
LOCAL_TTL_SECONDS = 48 * 3600
MEDIA_OUTBOUND = _PATHS.media_outbound
FAILURES_DIR = _PATHS.failures_dir
IMAGE_EVENT_FILE = _PATHS.image_event_file
MAX_IMAGE_BYTES = 25 * 1024 * 1024
MAX_VIDEO_PLAIN_BYTES = 100 * 1024 * 1024
MAX_VIDEO_CIPHER_BYTES = MAX_VIDEO_PLAIN_BYTES + 16
# Item 2 (2026-10-31): ordinary file receive v1 limits. These are
# trial-run values for the synchronous receive path, NOT WeChat
# platform limits. A larger capacity needs the streaming/background
# download version, not a bigger constant here.
FILE_INBOUND_DIR = _PATHS.file_inbound_dir
FILE_SINGLE_PLAIN_CAP = 10 * 1024 * 1024
FILE_SINGLE_CIPHER_CAP = FILE_SINGLE_PLAIN_CAP + 1024
FILE_MESSAGE_TOTAL_CAP = 20 * 1024 * 1024
FILE_MESSAGE_MAX_COUNT = 5
FILE_MESSAGE_BUDGET_SECONDS = 60
FILE_DOWNLOAD_ATTEMPTS = 2
FILE_RECEIPTS_FILE = _PATHS.file_receipts_file
# Test-only hook: extra allowed media origins for local test servers,
# e.g. {("http", "127.0.0.1", 8765)}. Empty in production; never put
# an HTTP localhost allowlist into the production configuration.
_TEST_MEDIA_ORIGINS = set()
MAX_IMAGE_PIXELS = 50_000_000
CDN_BASE_DEFAULT = "https://novac2c.cdn.weixin.qq.com/c2c"
SESSION_FILE = _PATHS.session_file
CURSOR_FILE = _PATHS.cursor_file
PROCESSED_FILE = _PATHS.processed_file
NEEDS_RELOGIN_FILE = _PATHS.needs_relogin_file
LOGIN_PENDING_FILE = _PATHS.login_pending_file
LOCK_FILE = _PATHS.lock_file
import threading as _threading_top
_SESSION_LOCK = _threading_top.RLock()
# Gate 3 step 2: SQLite authority. The shadow module is imported
# guarded: if it cannot be imported, serve still starts in json mode
# (loud print + failure marker at refresh time). Shadow
# unavailability must never equal bridge unavailability (design
# section 3.1). The mode file itself is an OPERATOR action: no code
# in this file ever writes it.
try:
    import bridge_sqlite as _SHADOW_MOD
except Exception:
    _SHADOW_MOD = None
# Gemini R5: files whose post-send state write failed are halted in
# memory for this process lifetime, never re-sent by the sender loop.
_IN_MEMORY_HALTED_FILES = set()
# v1.0.1: terminal task ids whose failed/ move failed. Never resend in this process.
_TERMINAL_TASK_IDS = set()

def _terminal_attachments(job):
    """Attachments for terminal checks, including the old single-image
    format (job['image_path'] / job['image_status']) which drain_outbox
    normalises only after _is_terminal_job was previously called."""
    atts = job.get('attachments')
    if isinstance(atts, list):
        return atts
    if job.get('image_path'):
        return [{'status': job.get('image_status', 'pending')}]
    return []


def _is_terminal_job(job):
    # v1.0.2: a job-level terminal marker, persisted on disk, is
    # sufficient on its own. This covers delivery_state =
    # 'unknown_restart' (startup recovery, where text/attachment
    # statuses are still 'pending') and any job already moved to the
    # terminal branch whose failed/ move failed. Memory-only sets
    # must never be the sole guard across a process restart.
    # NOTE: delivery_state 'failed'/'needs_check' alone is NOT a
    # whole-job marker: drain_outbox sets delivery_state to the last
    # item's status, so a job with text failed + attachment still
    # pending has delivery_state='failed' but is NOT whole-terminal;
    # treating it as terminal would block its pending sub-item.
    # Whole-job terminal via delivery_state is only 'unknown_restart'.
    if job.get('terminal') is True:
        return True
    if job.get('delivery_state') == 'unknown_restart':
        return True
    # Item 3 (2026-10-31): a long-text plan job is terminal only when
    # every text part AND every attachment is in a terminal state
    # (sent/failed/needs_check/blocked_by_prior) and at least one
    # part/attachment is failed/needs_check. 'blocked_by_prior' alone
    # is a stop, not a failure: it only becomes terminal together
    # with the failed/needs_check part that blocked it. A part still
    # 'pending' or 'sending' keeps the job non-terminal, so drain
    # retries it instead of archiving it away.
    _plan = job.get('text_parts') if job.get('text_plan_version') == 1 and isinstance(job.get('text_parts'), list) else None
    if _plan is not None and job.get('text'):
        # Gate 1 item 1: 'skipped_empty' plan parts (widget-only
        # parts sanitized away) are terminal like 'sent': they will
        # never be sent, so they must not keep a job non-terminal.
        _part_term = all(isinstance(p, dict) and p.get('status') in ('sent', 'failed', 'needs_check', 'blocked_by_prior', 'skipped_empty') for p in _plan)
        _atts = _terminal_attachments(job)
        _att_term = all(isinstance(a, dict) and a.get('status') in ('sent', 'failed', 'needs_check', 'blocked_by_prior') for a in _atts)
        _has_bad = any(isinstance(p, dict) and p.get('status') in ('failed', 'needs_check') for p in _plan) or any(
            isinstance(a, dict) and a.get('status') in ('failed', 'needs_check') for a in _atts)
        return bool(_part_term and _att_term and _has_bad)
    text = job.get('text')
    atts = _terminal_attachments(job)
    if not text and not atts:
        return False
    # All items are in a terminal state (sent/failed/needs_check) AND
    # at least one item is failed/needs_check. An all-sent job is not
    # terminal here: drain skips its items and moves it to sent/.
    text_term = (not text) or job.get('text_status') in ('sent', 'failed', 'needs_check')
    atts_term = all(isinstance(a, dict) and a.get('status') in ('sent', 'failed', 'needs_check') for a in atts)
    has_bad = (bool(text) and job.get('text_status') in ('failed', 'needs_check')) or any(
        isinstance(a, dict) and a.get('status') in ('failed', 'needs_check') for a in atts)
    return bool(text_term and atts_term and has_bad)
_SEND_COOLDOWN = {}

DEFAULT_BASE = "https://ilinkai.weixin.qq.com"
# channel_version is supplied by the Phase 1 instruction as 2.4.10.
# protocol.md itself uses 2.4.8 only as an example and says the value
# comes from package metadata, so 2.4.10 is instruction-supplied, not
# protocol-verified. bot_agent defaults to OpenClaw in protocol.md.
CHANNEL_VERSION = "2.4.10"
BOT_AGENT = "OpenClaw"
# iLink-App-ClientVersion: 0x00MMNNPP as a decimal string, for 2.4.10:
# 2*65536 + 4*256 + 10 = 132106
CLIENT_VERSION = str(2 * 65536 + 4 * 256 + 10)


class TokenExpired(Exception):
    pass


class ApiError(Exception):
    pass


class RetryableError(ApiError):
    """Pre-send failure: getuploadurl/upload/network before final
    sendmessage, so definitely not delivered via sendmessage. May retry."""
    pass


class UnknownDeliveryError(ApiError):
    """Final sendmessage request timed out / connection broke / response
    lost after request may have reached the server. May already be
    delivered. Must NOT be auto-retried; move to needs_check immediately."""
    pass


class PermanentError(ApiError):
    """Local validation or server explicit rejection (ret/errcode != 0).
    Definitely not delivered, retrying the same payload will not help."""
    pass


def ensure_dirs():
    # Gate 3 step 1: creation itself lives in bridge_storage, but the
    # argument list is read from this module's globals at call time,
    # so tests that rebind bridge.STATE etc. still redirect creation.
    bridge_storage.ensure_dirs(STATE, INBOX, RUNNING, DONE, OUTBOX_PENDING, OUTBOX_SENT,
                               OUTBOX_FAILED, MEDIA_INBOUND, MEDIA_OUTBOUND, FAILURES_DIR, ANALYSIS_DIR, LOCAL_VAULT_DIR)


def load_image_event():
    try:
        with open(IMAGE_EVENT_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_image_event(event):
    ensure_dirs()
    atomic_write_json(IMAGE_EVENT_FILE, event, mode=0o600)


def _content_class(data):
    """Classify download bytes by magic only, never print the body."""
    if not data:
        return "empty"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if data.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if data.startswith(b"GIF8"):
        return "GIF"
    if data.startswith(b"RIFF") and b"WEBP" in data[:12]:
        return "WEBP"
    stripped = data.lstrip()[:20]
    if stripped.startswith(b"<"):
        return "HTML"
    if stripped.startswith(b"{") or stripped.startswith(b"["):
        return "JSON"
    return "unknown"


def _sanitize_error(e):
    """Short error for logs/records. Never include a -K key, a full
    URL (which may carry encrypt_query_param), or a token.
    R3 fix: TimeoutExpired str() contains the whole argv list, e.g.
    "'-K', '<hex>'", which the old "-K " check missed. For subprocess
    timeouts return only the exception type; for all other errors
    redact -K arguments (both "-K <hex>" and "'-K', '<hex>'" forms)
    and any 32-char hex token, and drop URL queries entirely.
    """
    import re as _re
    if isinstance(e, subprocess.TimeoutExpired):
        return "TimeoutExpired"
    s = str(e)[:300]
    s = _re.sub(r"'-K'\s*,\s*'[^']*'", "'-K' [redacted]", s)
    s = _re.sub(r"-K\s+\S+", "-K [redacted]", s)
    # R3 narrow (Gemini): do NOT globally delete all 32-char hex,
    # that would also hide MD5/UUID/media IDs needed for debugging.
    # Only redact -K forms and explicit credential query fields.
    s = _re.sub(r"(aeskey|authkey|bot_token|context_token)=[^&\s\]\)]+", r"\1=[redacted]", s)
    # v1.0.2: Bearer token in a curl -H argument, and JSON credential
    # fields that can appear inside a -d request body in an exception.
    s = _re.sub(r"Bearer\s+[^\s'\"]+", "Bearer [redacted]", s)
    s = _re.sub(r'"(aeskey|authkey|bot_token|context_token)"\s*:\s*"[^"]*"', r'"\1": "[redacted]"', s)
    s = _re.sub(r"'(aeskey|authkey|bot_token|context_token)'\s*:\s*'[^']*'", r"'\1': '[redacted]'", s)
    if "?" in s and "http" in s:
        s = s.split("http")[0] + "http [redacted]"
    return s


# Gate 3 step 1: _atomic_tmp_path, _open_private, atomic_write_json
# and atomic_write_text now live in bridge_storage.py (imported at the
# top of this file). bridge.atomic_write_json / bridge.atomic_write_text
# remain available as module attributes via that import, so callers
# and tests that reference them see no change.


def _now_iso():
    """v1.0.4: local ISO timestamp with timezone, for serve.log lines
    and channel_status.json, so tick -> Worker check -> process start
    -> request result can be correlated. No secrets are ever in it."""
    import datetime as _dt
    return _dt.datetime.now().astimezone().isoformat(timespec="seconds")


def _log_ts(msg):
    """v1.0.4: timestamped serve log line. Existing substrings
    (e.g. "lock acquired", "getupdates error", "queued inbox/") are
    preserved after the timestamp, so old log grep patterns still work."""
    print(f"{_now_iso()} {msg}", flush=True)


CHANNEL_STATUS_FILE = _PATHS.channel_status_file
SERVE_RUN_FILE = _PATHS.serve_run_file
# v1.0.4 review fix (Finding 1): instance identity for the current
# serve process. A new run_id is generated after flock succeeds, so
# even a reused PID cannot make an old instance's success look like
# the current instance's success. The Worker must require
# channel_status.run_id == serve_run.run_id, not just ok/0.
_RUN_ID = None
_PROCESS_STARTED_TS = None


def _boot_id():
    """GPT ruling: system boot identity, from /proc/sys/kernel/random/boot_id."""
    try:
        with open("/proc/sys/kernel/random/boot_id", encoding="utf-8") as f:
            v = f.read().strip()
            return v or "unknown"
    except Exception:
        return "unknown"


def _proc_starttime(pid=None):
    """GPT ruling: kernel process starttime, field 22 of /proc/<pid>/stat
    (jiffies since boot). Parses after the LAST ')' so a comm containing
    spaces or parentheses cannot shift the field count. Returns None
    when the process cannot be read (dead / different namespace)."""
    try:
        path = f"/proc/{pid or os.getpid()}/stat"
        with open(path, encoding="utf-8") as f:
            data = f.read()
        rparen = data.rfind(")")
        if rparen < 0:
            return None
        rest = data[rparen + 2:].split()
        # rest[0] is field 3 (state); field 22 is rest[19].
        if len(rest) <= 19:
            return None
        return int(rest[19])
    except Exception:
        return None


def _init_channel_status():
    """GPT ruling (blocking): startup identity establishment, STRONG
    constraint. Called immediately after flock succeeds, BEFORE
    serve.pid is written. Raises on ANY necessary write failure;
    serve_loop must catch it, return non-zero, and NEVER write
    serve.pid. The previous docstring ("diagnostic write failure
    must not stop serve") is superseded: that policy applies ONLY
    to runtime _record_channel_status updates, never to init.
    Writes serve_run.json (run_id, pid, boot_id, proc_starttime)
    and pending channel_status.json, each atomically. A partial
    state (one file new, one old) is a normal unconfirmed state:
    the Worker must treat identity mismatch / missing file as
    channel unconfirmed, never as recovery."""
    global _RUN_ID, _PROCESS_STARTED_TS
    import uuid as _uuid
    _RUN_ID = _uuid.uuid4().hex
    _PROCESS_STARTED_TS = _now_iso()
    boot = _boot_id()
    starttime = _proc_starttime()
    run_obj = {"run_id": _RUN_ID, "pid": os.getpid(),
               "started_ts": _PROCESS_STARTED_TS,
               "boot_id": boot, "proc_starttime": starttime}
    obj = {
        "status": "pending",
        "last_attempt_ts": _PROCESS_STARTED_TS,
        "last_success_ts": None,
        "consecutive_failures": 0,
        "last_error": None,
        "process_pid": os.getpid(),
        "run_id": _RUN_ID,
        "process_started_ts": _PROCESS_STARTED_TS,
        "boot_id": boot,
        "proc_starttime": starttime,
    }
    # No try/except swallow here: atomic_write_json raising means
    # init failed, and serve_loop must abort startup.
    atomic_write_json(SERVE_RUN_FILE, run_obj, mode=0o600)
    atomic_write_json(CHANNEL_STATUS_FILE, obj, mode=0o600)
    return obj


def _safe_warn(msg):
    """Review fix (diagnostic escape): warning output with its OWN
    try/except Exception guard. BrokenPipeError and OSError are
    already Exception subclasses, so except Exception is sufficient.
    NEVER use BaseException here: that would swallow SystemExit
    and KeyboardInterrupt. If the warning itself cannot be printed,
    it is silently dropped, and the caller still returns normally."""
    try:
        print(msg, flush=True)
    except Exception:
        pass


def _record_channel_status(ok, error_text=None):
    """v1.0.4: separate channel status from process liveness.
    Written atomically after every getupdates attempt (success or
    failure). Fields: status ok/error/pending, last_success_ts,
    last_attempt_ts, consecutive_failures, last_error (desensitized,
    truncated, never a URL/token), process_pid, run_id,
    process_started_ts. A successful EMPTY getupdates response
    counts as success: it proves the API answered, even when there
    are no new messages. No new message is NOT channel failure.
    Callers must NOT use this file as a login-validity signal:
    login validity is NEEDS_RELOGIN / ret -14 only.
    GPT ruling: runtime updates are the WEAK constraint (warn and
    continue on write failure), unlike _init_channel_status, which
    is the STRONG constraint (fail startup).
    Review fix (Finding 4): this function NEVER raises. A prev file
    that is valid JSON but has a wrong-typed field (e.g.
    consecutive_failures as a string) is reset to a safe default
    with a visible warning, instead of turning a retryable network
    failure into a serve_loop exit. A write failure is also a
    visible warning, not a silent swallow and not an exit."""
    try:
        prev = {}
        if os.path.exists(CHANNEL_STATUS_FILE):
            with open(CHANNEL_STATUS_FILE, encoding="utf-8") as f:
                prev = json.load(f) or {}
        if not isinstance(prev, dict):
            _safe_warn("serve: warning, channel_status not an object, resetting to defaults")
            prev = {}
        raw_failures = prev.get("consecutive_failures", 0)
        if isinstance(raw_failures, bool) or not isinstance(raw_failures, int) or raw_failures < 0:
            _safe_warn(f"serve: warning, channel_status consecutive_failures invalid ({raw_failures!r}), resetting to 0")
            raw_failures = 0
        prev_success = prev.get("last_success_ts")
        if prev_success is not None and not isinstance(prev_success, str):
            prev_success = None
        prev_run = prev.get("run_id")
        if prev_run is not None and not isinstance(prev_run, str):
            prev_run = None
        prev_started = prev.get("process_started_ts")
        if prev_started is not None and not isinstance(prev_started, str):
            prev_started = None
        failures = 0 if ok else raw_failures + 1
        obj = {
            "status": "ok" if ok else "error",
            "last_attempt_ts": _now_iso(),
            "last_success_ts": _now_iso() if ok else prev_success,
            "consecutive_failures": failures,
            "last_error": None if ok else (_sanitize_error(error_text) if error_text else "unknown"),
            "process_pid": os.getpid(),
            "run_id": _RUN_ID if _RUN_ID is not None else prev_run,
            "process_started_ts": _PROCESS_STARTED_TS if _PROCESS_STARTED_TS is not None else prev_started,
            "boot_id": _boot_id(),
            "proc_starttime": _proc_starttime(),
        }
        try:
            atomic_write_json(CHANNEL_STATUS_FILE, obj, mode=0o600)
        except Exception as e:
            _safe_warn(f"serve: warning, channel_status write failed: {e}")
        return obj
    except Exception as e:
        # Last-resort guard: diagnostics must never kill serve.
        # The warning itself uses _safe_warn, so a broken stdout
        # pipe cannot turn this guard into a second escape.
        _safe_warn(f"serve: warning, channel_status update failed (diagnostics only): {e}")
        return {
            "status": "ok" if ok else "error",
            "last_attempt_ts": _now_iso(),
            "last_success_ts": _now_iso() if ok else None,
            "consecutive_failures": 0 if ok else 1,
            "last_error": None if ok else "diagnostics_failed",
            "process_pid": os.getpid(),
            "run_id": _RUN_ID,
            "process_started_ts": _PROCESS_STARTED_TS,
            "boot_id": _boot_id(),
            "proc_starttime": _proc_starttime(),
        }


# Gate 2 -14 anomaly counting (篇三 Gate 2 item 3, 2026-11-01).
# getupdates failures are bucketed into four fixed categories, with
# counts + last-seen timestamps only. The file NEVER stores tokens,
# URLs, user identifiers, message text, or raw error text: the
# classifier below returns fixed enum strings only, and the record
# function stores nothing else. The -14 behaviour is untouched:
# check_ret still writes NEEDS_RELOGIN and serve_loop still exits
# (return 14) without retrying. No probe experiment exists in this
# code: a 300-second auto loop, a fixed five-minute self loop, and
# any silent token swap are all explicitly out of scope (see
# docs/getupdates-anomalies-manual.md for the gate that would have
# to be met before a probe could even be considered).
GETUPDATES_ANOMALY_CATEGORIES = ("login_expired", "network_error", "rate_limited", "result_unknown")
GETUPDATES_ANOMALIES_SCHEMA = "getupdates-anomalies-0.1"
_GETUPDATES_ANOMALY_DETAILS = {
    "login_expired": ("ret_minus_14",),
    "network_error": ("transport",),
    "rate_limited": ("http_429", "rate_signal"),
    "result_unknown": ("empty_or_nonjson", "http_error", "server_rejection", "unclassified"),
}


def _getupdates_anomalies_path():
    # Computed at call time (not a module constant) so tests that
    # rebind STATE, and any future STATE move, are honoured.
    return os.path.join(STATE, "getupdates_anomalies.json")


def classify_getupdates_error(exc):
    """Classify a getupdates failure into (category, detail).

    Both values are fixed enums. The exception text is only matched
    against fixed patterns; it is never stored or returned. Order
    matters: TokenExpired (-14) first; explicit throttling signals
    before transport markers, because a transport wrapper message can
    carry a stale empty-body note as its curl_err value; transport
    markers before empty/non-JSON, because post_json wraps a failed
    transport as "curl failed: ...; urllib failed: ..." even when the
    curl half failed on an empty body. An explicit non-zero ret that
    carries no rate-limit signal is NOT counted as rate limiting
    (no numeric rate-limit code is assumed from upstream): it lands
    in result_unknown with detail server_rejection, and the manual
    tells the maintainer how to split it from the desensitised
    channel_status last_error.
    """
    if isinstance(exc, TokenExpired):
        return ("login_expired", "ret_minus_14")
    s = str(exc)
    low = s.lower()
    if "http 429" in low:
        return ("rate_limited", "http_429")
    if any(p in low for p in ("rate limit", "rate_limit", "ratelimit", "too many requests", "too frequent")):
        return ("rate_limited", "rate_signal")
    if "curl failed:" in low:
        return ("network_error", "transport")
    if "empty response body" in low or "non-json response" in low or "empty or non-json" in low:
        return ("result_unknown", "empty_or_nonjson")
    if re.search(r"http \d{3}", low):
        return ("result_unknown", "http_error")
    if "ret=" in s:
        return ("result_unknown", "server_rejection")
    return ("result_unknown", "unclassified")


def record_getupdates_anomaly(category, detail=None):
    """Increment one getupdates anomaly bucket. Never raises.

    Returns True when the counting file was written, False otherwise.
    A write failure only leaves a desensitised failures marker
    (kind + sanitised reason, no counts payload) and never blocks the
    poll loop: callers ignore the return value.
    """
    if category not in GETUPDATES_ANOMALY_CATEGORIES:
        category = "result_unknown"
    allowed = _GETUPDATES_ANOMALY_DETAILS[category]
    if detail not in allowed:
        detail = "unclassified" if category == "result_unknown" else allowed[0]
    try:
        path = _getupdates_anomalies_path()
        try:
            os.makedirs(STATE, exist_ok=True)
        except Exception:
            pass
        prev = {}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    prev = json.load(f) or {}
            except Exception:
                prev = {}
        if not isinstance(prev, dict):
            prev = {}
        counts = prev.get("counts")
        if not isinstance(counts, dict):
            counts = {}
        last_seen = prev.get("last_seen")
        if not isinstance(last_seen, dict):
            last_seen = {}
        last_detail = prev.get("last_detail")
        if not isinstance(last_detail, dict):
            last_detail = {}
        clean_counts = {}
        for c in GETUPDATES_ANOMALY_CATEGORIES:
            v = counts.get(c)
            clean_counts[c] = v if (isinstance(v, int) and not isinstance(v, bool) and v >= 0) else 0
        clean_counts[category] += 1
        clean_seen = {c: (last_seen.get(c) if isinstance(last_seen.get(c), str) else None) for c in GETUPDATES_ANOMALY_CATEGORIES}
        clean_seen[category] = _now_iso()
        clean_detail = {c: last_detail.get(c) for c in GETUPDATES_ANOMALY_CATEGORIES if last_detail.get(c) in _GETUPDATES_ANOMALY_DETAILS[c]}
        clean_detail[category] = detail
        atomic_write_json(path, {
            "schema": GETUPDATES_ANOMALIES_SCHEMA,
            "counts": clean_counts,
            "last_seen": clean_seen,
            "last_detail": clean_detail,
        }, mode=0o600)
        return True
    except Exception as e:
        try:
            os.makedirs(FAILURES_DIR, exist_ok=True)
            atomic_write_json(os.path.join(FAILURES_DIR, "getupdates_anomalies_write_failed.json"), {
                "kind": "getupdates_anomalies_write_failed",
                "reason": _sanitize_error(e),
                "ts": _now_iso(),
            }, mode=0o600)
        except Exception:
            pass
        _safe_warn("serve: warning, getupdates anomaly count write failed (poll loop unaffected)")
        return False


def read_getupdates_anomalies():
    """Read-only view of the anomaly counts. Never raises, never creates the file."""
    zeros = {c: 0 for c in GETUPDATES_ANOMALY_CATEGORIES}
    nones = {c: None for c in GETUPDATES_ANOMALY_CATEGORIES}
    path = _getupdates_anomalies_path()
    if not os.path.exists(path):
        return {"schema": GETUPDATES_ANOMALIES_SCHEMA, "present": False, "readable": False, "counts": zeros, "last_seen": nones, "last_detail": {}}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f) or {}
        if not isinstance(data, dict):
            raise ValueError("anomalies file is not an object")
        counts = data.get("counts")
        if not isinstance(counts, dict):
            raise ValueError("anomalies counts missing")
        clean = {}
        for c in GETUPDATES_ANOMALY_CATEGORIES:
            v = counts.get(c)
            clean[c] = v if (isinstance(v, int) and not isinstance(v, bool) and v >= 0) else 0
        last_seen = data.get("last_seen")
        clean_seen = {c: (last_seen.get(c) if isinstance(last_seen, dict) and isinstance(last_seen.get(c), str) else None) for c in GETUPDATES_ANOMALY_CATEGORIES}
        last_detail = data.get("last_detail")
        clean_detail = {c: last_detail.get(c) for c in GETUPDATES_ANOMALY_CATEGORIES if isinstance(last_detail, dict) and last_detail.get(c) in _GETUPDATES_ANOMALY_DETAILS[c]}
        return {"schema": GETUPDATES_ANOMALIES_SCHEMA, "present": True, "readable": True, "counts": clean, "last_seen": clean_seen, "last_detail": clean_detail}
    except Exception:
        return {"schema": GETUPDATES_ANOMALIES_SCHEMA, "present": True, "readable": False, "counts": zeros, "last_seen": nones, "last_detail": {}}


def cmd_getupdates_anomalies(_args):
    print(json.dumps(read_getupdates_anomalies(), ensure_ascii=False, indent=2))
    return 0


def load_session():
    with _SESSION_LOCK:
        if not os.path.exists(SESSION_FILE):
            return None
        with open(SESSION_FILE, encoding="utf-8") as f:
            return json.load(f)


def save_session(sess):
    with _SESSION_LOCK:
        atomic_write_json(SESSION_FILE, sess, mode=0o600)


def load_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    try:
        with open(PROCESSED_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return set(str(x) for x in data)
    except (OSError, ValueError):
        return set()


def save_processed(processed):
    atomic_write_json(PROCESSED_FILE, sorted(processed), mode=0o600)


def load_cursor():
    if not os.path.exists(CURSOR_FILE):
        return ""
    with open(CURSOR_FILE, encoding="utf-8") as f:
        return f.read().strip()


def save_cursor(cursor):
    atomic_write_text(CURSOR_FILE, (cursor or "") + "\n", mode=0o600)


def write_needs_relogin(op, detail):
    with _SESSION_LOCK:
        atomic_write_json(
            NEEDS_RELOGIN_FILE,
            {"op": op, "detail": str(detail), "ts": time.time()},
            mode=0o600,
        )


def check_ret(resp, op):
    """Validate a parsed API response. Raises TokenExpired / ApiError."""
    if not isinstance(resp, dict):
        raise ApiError(f"{op}: empty or non-JSON response, not treated as success")
    ret = resp.get("ret")
    errcode = resp.get("errcode")
    if ret == -14 or errcode == -14:
        write_needs_relogin(op, f"ret={ret} errcode={errcode}")
        raise TokenExpired(f"{op}: token expired (ret/errcode -14), wrote NEEDS_RELOGIN")
    if ret is not None and ret != 0:
        raise ApiError(f"{op}: ret={ret} errcode={errcode} errmsg={resp.get('errmsg')}")
    if errcode is not None and errcode != 0:
        raise ApiError(f"{op}: errcode={errcode} ret={ret} errmsg={resp.get('errmsg')}")
    return resp


def make_headers(session=None, authenticated=False):
    # X-WECHAT-UIN: base64 of the decimal string of a random uint32.
    uin = base64.b64encode(str(random.getrandbits(32)).encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "X-WECHAT-UIN": uin,
        "iLink-App-Id": "bot",
        "iLink-App-ClientVersion": CLIENT_VERSION,
    }
    if authenticated and session and session.get("bot_token"):
        headers["Authorization"] = "Bearer " + session["bot_token"]
    return headers


def base_info():
    return {"channel_version": CHANNEL_VERSION, "bot_agent": BOT_AGENT}


def _post_via_curl(url, headers, data, timeout):
    """POST via system curl (preferred, 2026-10-27 Step 1).

    Cloud Python urllib sometimes drops the connection
    (RemoteDisconnected) on getupdates/sendmessage while the same
    request via curl --http1.1 succeeds, so curl is tried first.
    Returns raw body bytes, or raises ApiError. Never logs headers
    (they carry the Authorization token).
    """
    if not shutil.which("curl"):
        raise ApiError("curl not available")
    cmd = ["curl", "-s", "--http1.1", "--max-time", str(int(timeout)), "-X", "POST", url]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd += ["-d", data.decode("utf-8")]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=int(timeout) + 10)
    except subprocess.TimeoutExpired:
        # v1.0.2: TimeoutExpired str() contains the full argv, including
        # '-H', 'Authorization: Bearer <bot_token>' and the -d body.
        # Never embed the exception text; the type is the whole message.
        raise ApiError("curl POST timeout") from None
    except Exception as e:
        raise ApiError(f"curl POST failed: {type(e).__name__}") from None
    if r.returncode != 0:
        raise ApiError(f"curl POST exit {r.returncode}")
    return r.stdout


def _get_via_curl(url, headers, timeout):
    if not shutil.which("curl"):
        raise ApiError("curl not available")
    cmd = ["curl", "-s", "--http1.1", "--max-time", str(int(timeout)), url]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=int(timeout) + 10)
    except subprocess.TimeoutExpired:
        # v1.0.2: same rule as _post_via_curl; never embed argv text.
        raise ApiError("curl GET timeout") from None
    except Exception as e:
        raise ApiError(f"curl GET failed: {type(e).__name__}") from None
    if r.returncode != 0:
        raise ApiError(f"curl GET exit {r.returncode}")
    return r.stdout


def _desensitized_host_port(value):
    """Return 'host:port' for a URL/proxy string, never userinfo or
    password. Returns 'none' when absent or unparseable. v1.0.2."""
    if not value:
        return "none"
    try:
        parsed = urllib.parse.urlparse(value)
        host = parsed.hostname
        if not host:
            return "none"
        port = parsed.port
        if port:
            return f"{host}:{port}"
        return f"{host}:{'443' if parsed.scheme == 'https' else '80'}"
    except Exception:
        return "none"


def _effective_proxy_host_port(target_url=None):
    """Proxy actually in effect for this process, host:port only.
    Checks https_proxy first (iLink is https), then http_proxy/all_proxy.
    v1.0.2: never print the proxy URL itself (it may embed credentials).
    v1.0.3: if target_url is given and urllib.request.proxy_bypass(host)
    is True, return "none (no_proxy bypass)" -- this matches urllib's
    own bypass behaviour exactly (no_proxy/NO_PROXY, suffix, '*' and IP
    matching), instead of misreporting a proxy that will not be used.
    v1.0.3-final (P2): find a proxy FIRST, then apply the bypass check.
    The old order returned "none (no_proxy bypass)" even when no proxy
    was configured at all (e.g. NO_PROXY=* alone), which mislabels
    "no proxy configured" as "bypassed". Now: no proxy -> "none"."""
    proxy_hp = "none"
    for key in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"):
        value = os.environ.get(key)
        if value:
            proxy_hp = _desensitized_host_port(value)
            break
    if proxy_hp == "none":
        return "none"
    if target_url:
        try:
            host = urllib.parse.urlparse(target_url).hostname
            if host and urllib.request.proxy_bypass(host):
                return "none (no_proxy bypass)"
        except Exception:
            pass
    return proxy_hp


def _describe_urllib_error(e):
    """v1.0.3: distinguish URLError causes without leaking the URL.
    URLError's concrete cause is in e.reason, e.g. ConnectionRefusedError
    (errno 111) vs TimeoutError. Recording only type(e).__name__ made
    both look like "URLError". The reason string is sanitized, and the
    request URL is never included."""
    if isinstance(e, urllib.error.URLError):
        reason = getattr(e, "reason", None)
        if reason is not None:
            if isinstance(reason, Exception):
                return (f"URLError({type(reason).__name__}: "
                        f"errno={getattr(reason, 'errno', None)}: "
                        f"{_sanitize_error(reason)})")
            return f"URLError(reason={_sanitize_error(reason)})"
    return type(e).__name__


def post_json(base, path, payload, session=None, authenticated=False, timeout=15, allow_fallback=True):
    """POST JSON. R1 fix: when allow_fallback=False (final sendmessage),
    a curl failure, empty body, or non-JSON body raises ApiError WITHOUT
    a second urllib POST, because the first POST may already have reached
    the server and its receipt was lost. allow_fallback=True remains only
    for idempotent reads (getupdates).
    """
    url = base.rstrip("/") + path
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = make_headers(session, authenticated)
    # v1.0.2: keep the curl error, do NOT swallow it. If urllib also
    # fails, the raised error must carry both paths, the desensitized
    # target host:port, and the desensitized effective proxy host:port.
    curl_err = None
    try:
        body = _post_via_curl(url, headers, data, timeout)
        if body:
            try:
                return json.loads(body.decode("utf-8"))
            except ValueError:
                if not allow_fallback:
                    raise ApiError(f"{path}: non-JSON response, not treated as success")
                curl_err = "non-JSON response"
        elif not allow_fallback:
            raise ApiError(f"{path}: empty response body, not treated as success")
        else:
            curl_err = "empty response body"
    except ApiError as e:
        if not allow_fallback:
            raise
        curl_err = str(e)
    req = urllib.request.Request(
        url, data=data, headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        # Read the error body: it may still carry ret/errcode -14.
        body = e.read() if hasattr(e, "read") else b""
        try:
            resp = json.loads(body.decode("utf-8")) if body else {}
        except ValueError:
            resp = {}
        if isinstance(resp, dict) and (resp.get("ret") == -14 or resp.get("errcode") == -14):
            write_needs_relogin("http", f"HTTP {e.code} ret/errcode -14")
            raise TokenExpired(f"HTTP {e.code} with ret/errcode -14")
        raise ApiError(f"HTTP {e.code} for {path}: {body[:500]!r} (curl_err={curl_err}; target={_desensitized_host_port(url)}; proxy={_effective_proxy_host_port(url)})") from None
    except Exception as e:
        raise ApiError(f"{path}: curl failed: {curl_err}; urllib failed: {_describe_urllib_error(e)}; target={_desensitized_host_port(url)}; proxy={_effective_proxy_host_port(url)}") from None
    if not body:
        raise ApiError(f"{path}: empty response body, not treated as success (curl_err={curl_err})")
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError:
        raise ApiError(f"{path}: non-JSON response, not treated as success")


def get_json(base, path, session=None, timeout=15):
    url = base.rstrip("/") + path
    headers = {
        "User-Agent": "Mozilla/5.0",
        "iLink-App-Id": "bot",
        "iLink-App-ClientVersion": CLIENT_VERSION,
    }
    # Step 1: prefer curl, fall back to urllib once on curl failure.
    # v1.0.2: same dual-error rule as post_json.
    curl_err = None
    try:
        body = _get_via_curl(url, headers, timeout)
        if body:
            try:
                return json.loads(body.decode("utf-8"))
            except ValueError:
                curl_err = "non-JSON response"
        else:
            curl_err = "empty response body"
    except ApiError as e:
        curl_err = str(e)
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        raise ApiError(f"HTTP {e.code} for {path} (curl_err={curl_err}; target={_desensitized_host_port(url)}; proxy={_effective_proxy_host_port(url)})") from None
    except Exception as e:
        raise ApiError(f"{path}: curl failed: {curl_err}; urllib failed: {_describe_urllib_error(e)}; target={_desensitized_host_port(url)}; proxy={_effective_proxy_host_port(url)}") from None
    if not body:
        raise ApiError(f"{path}: empty response body (curl_err={curl_err})")
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError:
        raise ApiError(f"{path}: non-JSON response")


def extract_text(msg):
    parts = []
    for item in msg.get("item_list") or []:
        if item.get("type") == 1:
            text_item = item.get("text_item") or {}
            if text_item.get("text"):
                parts.append(text_item["text"])
    return "".join(parts)


# Phase 2 step 2: voice. Core function drafted by DeepSeek (split prompt,
# 2026-10-28, 359 tokens for this function); Muse added the
# has-voice / missing-text distinction so a voice message with no
# transcription is never silently dropped and never fabricated.
def _quoted_text_from_message_item(qitem):
    """Text inside a ref_msg.message_item, either a MessageItem with its
    own item_list or a single item carrying type/text_item."""
    parts = []
    if not isinstance(qitem, dict):
        return ""
    items = qitem.get("item_list") or []
    if items:
        for it in items:
            if isinstance(it, dict) and it.get("type") == 1:
                t = (it.get("text_item") or {}).get("text")
                if t:
                    parts.append(t)
    elif qitem.get("type") == 1:
        t = (qitem.get("text_item") or {}).get("text")
        if t:
            parts.append(t)
    return "".join(parts)


def extract_quote(msg):
    """Raw quote info from an upstream ref_msg, or None.

    Mirrors upstream inbound.findReferenceItem/applyInlineQuoteContext
    (openclaw-weixin 874637a): the first item_list entry carrying ref_msg;
    id = svr_id, else message_item.msg_id; inline body = title + quoted
    message_item text. No media is copied here; resolution against bridge
    records (with the local/deleted guard) happens in resolve_quote()."""
    for item in msg.get("item_list") or []:
        if not isinstance(item, dict):
            continue
        ref = item.get("ref_msg")
        if not isinstance(ref, dict):
            continue
        qitem = ref.get("message_item") or {}
        qid = ""
        if isinstance(ref.get("svr_id"), str):
            qid = ref["svr_id"].strip()
        if not qid and isinstance(qitem, dict) and isinstance(qitem.get("msg_id"), str):
            qid = qitem["msg_id"].strip()
        title = ref["title"].strip() if isinstance(ref.get("title"), str) else ""
        # Type 0 is upstream NONE, not a real kind: the 2026-10-31 live
        # quote test recorded quoted_type 0 for a plain text message, so
        # treat 0 as unknown and let resolve_quote derive it from the
        # bridge record (or from inline text) instead.
        qtype = None
        if isinstance(qitem, dict):
            if isinstance(qitem.get("type"), int) and qitem.get("type") != 0:
                qtype = qitem.get("type")
            else:
                for it in qitem.get("item_list") or []:
                    if isinstance(it, dict) and it.get("type") in (2, 3, 4, 5):
                        qtype = it.get("type")
                        break
        return {
            "quoted_message_id": qid or None,
            "quoted_title": title or None,
            "quoted_inline_text": _quoted_text_from_message_item(qitem) or None,
            "quoted_type": qtype,
        }
    return None


def resolve_quote(quote):
    """Resolve a raw quote against bridge records, with the guard first.

    NEVER returns quoted text, title, or media info for a local-only or
    tombstoned target: the quoted target's content must not be resurrected
    through a quote. Unknown targets are marked unresolved, never guessed.
    Explicit tradeoff (user decision 2026-10-31, GPT review 2026-10-31):
    R2 proposed banning the inline fallback for unknown ids outright,
    but that would break tested quote-v1 cases 5/10 (a pre-bridge
    message quoted with platform inline text, where there is no bridge
    record at all). The fallback is therefore KEPT, and its boundary is
    stated, not hidden: a known local-only/deleted id is guarded by TWO
    layers, the permanent tombstone set (sweep_local_ttl never purges)
    AND the done record's local_only flag, and either layer alone
    blocks the inline text. The residual risk is explicit: if BOTH
    layers are lost for some id (record deleted or lost in a migration
    with no tombstone backfilled), that id reads as unknown and its
    platform-supplied inline text/title WILL be shown to the Worker.
    That is an accepted tradeoff, not a guarantee that the risk is
    fully closed; the backfill for pre-tombstone deletions (2026-10-30
    batch) is tracked separately, and a future 'thorough deletion' mode
    that removes ids entirely must remove this fallback first."""
    if not quote:
        return None
    qid = quote.get("quoted_message_id")
    tombs = load_tombstones()
    target = None
    if qid:
        for d in (DONE, INBOX):
            p = os.path.join(d, "%s.json" % qid)
            if os.path.exists(p):
                try:
                    with open(p, encoding="utf-8") as f:
                        target = json.load(f)
                    break
                except Exception:
                    target = None
        if qid in tombs or (isinstance(target, dict) and target.get("local_only")):
            return {
                "quoted_message_id": qid,
                "quoted_local_only": True,
                "quoted_deleted": bool(qid in tombs),
                "quoted_note": "引用目标为仅本地或已删除内容，不可见",
            }
    out = {"quoted_message_id": qid}
    if quote.get("quoted_title"):
        out["quoted_title"] = quote["quoted_title"][:200]
    if isinstance(target, dict):
        out["quoted_resolved"] = True
        # Derive the kind from the bridge record, never from the
        # unreliable ref.message_item type: text 1, image 2, voice 3,
        # video 5 (upstream MessageItemType).
        if target.get("images"):
            out["quoted_type"] = 2
            out["quoted_image_count"] = len(target["images"])
            # Quote-image re-attachment v1: copy the quoted image entries
            # (same shape as a record's own "images") so the Worker can
            # read the quoted picture from the inbox record. Local-only
            # targets never reach here: the guard above returns first,
            # and local-only applies to videos only, never to images.
            out["quoted_images"] = [dict(e) for e in target["images"]]
        elif target.get("videos"):
            out["quoted_type"] = 5
            out["quoted_video_count"] = len(target["videos"])
        elif target.get("source") == "voice":
            out["quoted_type"] = 3
        elif target.get("text"):
            out["quoted_type"] = 1
        elif quote.get("quoted_type") is not None:
            out["quoted_type"] = quote["quoted_type"]
        if target.get("text"):
            out["quoted_text"] = target["text"][:1000]
        elif quote.get("quoted_inline_text"):
            out["quoted_text"] = quote["quoted_inline_text"][:1000]
    elif quote.get("quoted_inline_text"):
        out["quoted_resolved"] = True
        out["quoted_text"] = quote["quoted_inline_text"][:1000]
        out["quoted_type"] = 1
    else:
        out["quoted_unresolved"] = True
        if quote.get("quoted_type") is not None:
            out["quoted_type"] = quote["quoted_type"]
    return out


def has_voice_item(msg):
    for item in msg.get("item_list") or []:
        if item.get("type") == 3:
            return True
    return False


def extract_voice(msg):
    for item in msg.get("item_list") or []:
        if item.get("type") == 3:
            voice_item = item.get("voice_item") or {}
            text = voice_item.get("text")
            if isinstance(text, str):
                text = text.strip()
                if text:
                    return text
    return None


def count_json_files(directory):
    try:
        return len([n for n in os.listdir(directory) if n.endswith(".json")])
    except OSError:
        return 0


def fast_command_reply(text):
    """Phase 2 step 3: /ping and /status, handled by the bridge directly.
    Returns reply text, or None if not a fast command."""
    cmd = (text or "").strip()
    if cmd == "/ping":
        return "桥接在线，serve 正常"
    if cmd == "/status":
        inbox_n = count_json_files(INBOX)
        running_n = count_json_files(RUNNING)
        done_n = count_json_files(DONE)
        pending_n = count_json_files(OUTBOX_PENDING)
        # Oldest pending file age, to avoid calling a normal <1 min
        # queue a delivery failure.
        pending_age = None
        try:
            ages = []
            for n in os.listdir(OUTBOX_PENDING):
                if n.endswith(".json"):
                    ages.append(time.time() - os.path.getmtime(os.path.join(OUTBOX_PENDING, n)))
            if ages:
                pending_age = max(ages)
        except OSError:
            pending_age = None
        if inbox_n > 0:
            state = "任务排队中"
        elif running_n > 0:
            state = "Muse 执行中"
        elif pending_n > 0 and pending_age is not None and pending_age > 600:
            state = "投递失败/待核对"
        elif pending_n > 0:
            state = "投递中"
        else:
            state = "空闲"
        return (
            f"桥接状态：{state}\n"
            f"inbox: {inbox_n}，running: {running_n}，done: {done_n}，outbox/pending: {pending_n}"
        )
    return None


# ---- Tier-1 bridge-side chat command table ----
# Same interception road as the M3 local follow-up precedent in
# serve_loop: AFTER the group/whitelist filter, BEFORE any
# inbox write or merge staging, reply via send_direct_or_outbox
# (queue-only, the sender loop sends), mark processed, and never
# let the message reach the Worker. No new send path is invented.
# Timeline, same as local/fast replies: the command message
# itself gets NO "received" event (that event is only recorded
# on inbox write); the reply job records "queued" via
# send_direct_or_outbox and counts as a genuine sent reply
# (only *_progress/_offline/_queue suffixes are excluded).
# The legacy fast path (/ping, old /status) in
# fast_command_reply is left untouched. This table is checked
# FIRST in serve_loop, so /ping falls through to the legacy
# reply (byte-identical) while /status is answered by the new
# spec below; the old /status branch is therefore shadowed in
# serve flow but still covered by its old unit tests.
# Replies carry counts and states only: never a token, a user
# id, a URL, or the original message text. Trailing arguments
# after a known command are ignored, never echoed.

_CMD_TOKEN_RE = re.compile(r"^/[a-z_]+$")

_CHAT_COMMAND_UNKNOWN = "没有这个指令，输入 /help 查看可用指令。"

_CHAT_COMMAND_FALLBACK = "指令处理失败，请稍后重试。"


def parse_command_attempt(text):
    """Return the leading /command token, or None.

    A command attempt is: stripped text starts with "/", and the
    first whitespace-separated token matches ^/[a-z_]+$.
    Anything else ("/今天天气", "/", "/123", "/STATUS", plain
    text) is NOT an attempt and flows through the normal path
    untouched.
    """
    if not text:
        return None
    stripped = text.strip()
    if not stripped.startswith("/"):
        return None
    token = stripped.split(None, 1)[0]
    if _CMD_TOKEN_RE.match(token):
        return token
    return None


def _bucket(summary, key):
    """One observability bucket as int, or "未知" when absent."""
    try:
        v = (summary or {}).get(key)
    except Exception:
        return "未知"
    return v if isinstance(v, int) else "未知"


def _channel_line(recovered, reason):
    if recovered:
        return "通道：已恢复"
    if reason:
        return "通道：未恢复（%s）" % reason
    return "通道：未恢复"


def build_status_reply(summary, recovered, reason=None):
    """Deterministic /status text: counts + channel state only."""
    return (
        "桥接状态：serve 运行中\n"
        "%s\n"
        "待发送：%s，待核对：%s，已发送：%s，失败：%s，"
        "跳过空：%s，不可读：%s"
        % (_channel_line(recovered, reason),
           _bucket(summary, "pending"), _bucket(summary, "needs_check"),
           _bucket(summary, "sent"), _bucket(summary, "failed"),
           _bucket(summary, "skipped_empty"), _bucket(summary, "unreadable"))
    )


def build_health_reply(recovered, reason, hb_present, hb_age, free_mb, media_mb):
    """Deterministic /health text: serve, channel, heartbeat, disk."""
    if hb_present:
        hb = "Worker 心跳：有（%s 秒前）" % (hb_age if isinstance(hb_age, int) else "未知")
    else:
        hb = "Worker 心跳：无"
    free = "%s MB" % free_mb if isinstance(free_mb, int) else "未知"
    media = "%s MB" % media_mb if isinstance(media_mb, int) else "未知"
    return (
        "桥接健康：serve 运行中\n"
        "%s\n"
        "%s\n"
        "磁盘剩余：%s，媒体目录：%s"
        % (_channel_line(recovered, reason), hb, free, media)
    )


def build_queue_reply(summary, oldest_age):
    """Deterministic /queue text: buckets + oldest pending age."""
    if isinstance(oldest_age, int):
        age = "%s 秒前" % oldest_age
    else:
        age = "无（待发送为 0）"
    return (
        "队列：待发送 %s 条，待核对 %s 条，已发送 %s 条，失败 %s 条\n"
        "最老待发送：%s"
        % (_bucket(summary, "pending"), _bucket(summary, "needs_check"),
           _bucket(summary, "sent"), _bucket(summary, "failed"), age)
    )


def build_help_reply():
    """Deterministic /help text: the four commands + the red line."""
    return (
        "可用指令：\n"
        "/status 桥接状态\n"
        "/health 桥接健康\n"
        "/queue 队列情况\n"
        "/help 显示本帮助\n"
        "踢人、动钱、改设置等敏感操作不在指令表内，请回 Muse App 确认。"
    )


def _oldest_pending_age_seconds():
    """Oldest retryable pending job age in seconds, or None.

    Auxiliary suffixes (_progress/_offline/_queue) are excluded,
    matching the observability summary buckets. Never raises.
    """
    try:
        ages = []
        for name in os.listdir(OUTBOX_PENDING):
            if not name.endswith(".json"):
                continue
            if name.endswith(("_progress.json", "_offline.json", "_queue.json")):
                continue
            try:
                ages.append(time.time() - os.path.getmtime(os.path.join(OUTBOX_PENDING, name)))
            except OSError:
                continue
        return int(max(ages)) if ages else None
    except Exception:
        return None


def live_command_context():
    """Assemble read-only live values for a command reply.

    Every read is guarded: a failed read degrades to None/False
    instead of raising, so a command reply never kills serve.
    Channel recovered uses verify_channel_recovery (read-only:
    it never writes, sends, or restarts anything).
    """
    ctx = {"summary": None, "recovered": False, "reason": None,
           "hb_present": False, "hb_age": None,
           "free_mb": None, "media_mb": None, "oldest_age": None}
    try:
        ctx["summary"] = get_observability_summary()
    except Exception:
        ctx["summary"] = None
    try:
        _rec = verify_channel_recovery()
        ctx["recovered"] = bool(_rec.get("recovered")) if isinstance(_rec, dict) else False
        if not ctx["recovered"] and isinstance(_rec, dict):
            ctx["reason"] = _rec.get("reason")
    except Exception:
        ctx["recovered"] = False
        ctx["reason"] = "check_failed"
    try:
        _ts, _src = read_worker_heartbeat()
        ctx["hb_present"] = _ts is not None
        if _ts is not None:
            ctx["hb_age"] = int(time.time() - _ts)
    except Exception:
        ctx["hb_present"] = False
        ctx["hb_age"] = None
    try:
        ctx["free_mb"], ctx["media_mb"] = _disk_watermark()
    except Exception:
        ctx["free_mb"], ctx["media_mb"] = None, None
    ctx["oldest_age"] = _oldest_pending_age_seconds()
    return ctx


def chat_command_reply(text, ctx=None):
    """Answer a tier-1 chat command, or None.

    None means "not handled here": either not a command attempt
    (flows through the normal path) or /ping (the legacy fast
    path below answers it byte-identically). Every other matched
    /token is answered here and must never reach the Worker.
    Builders never raise: on any unexpected error the fixed
    fallback sentence is returned, preserving interception.
    """
    token = parse_command_attempt(text)
    if token is None or token == "/ping":
        return None
    try:
        if ctx is None:
            ctx = live_command_context()
        if token == "/status":
            return build_status_reply(ctx.get("summary"), ctx.get("recovered"), ctx.get("reason"))
        if token == "/health":
            return build_health_reply(ctx.get("recovered"), ctx.get("reason"),
                                      ctx.get("hb_present"), ctx.get("hb_age"),
                                      ctx.get("free_mb"), ctx.get("media_mb"))
        if token == "/queue":
            return build_queue_reply(ctx.get("summary"), ctx.get("oldest_age"))
        if token == "/help":
            return build_help_reply()
        return _CHAT_COMMAND_UNKNOWN
    except Exception:
        return _CHAT_COMMAND_FALLBACK


# Typing state, process-local only. Never printed, never logged.
_typing_ticket_cache = {"ticket": None, "ts": 0}
_typing_last_send = {"ts": 0}


def get_typing_ticket(session):
    """Phase 2 step 3: POST getconfig, cache typing_ticket. Timeout 3s.
    Returns ticket string or None. Never blocks the main loop on failure
    (except TokenExpired, which must stop serve like other -14 cases)."""
    now = time.time()
    if _typing_ticket_cache["ticket"] and now - _typing_ticket_cache["ts"] < 600:
        return _typing_ticket_cache["ticket"]
    base = session.get("baseurl") or DEFAULT_BASE
    try:
        # Server error observed 2026-10-28: getconfig ret=-2
        # "ilink_user_id required" when payload was base_info only,
        # so include ilink_user_id (and to_user_id alias) explicitly.
        resp = post_json(base, "/ilink/bot/getconfig",
                         {"base_info": base_info(),
                          "ilink_user_id": session.get("ilink_user_id"),
                          "to_user_id": session.get("ilink_user_id")},
                         session=session, authenticated=True, timeout=3)
        check_ret(resp, "getconfig")
    except TokenExpired:
        raise
    except Exception as e:
        print(f"typing: getconfig failed (ignored, not blocking): {e}", flush=True)
        return None
    ticket = None
    if isinstance(resp, dict):
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        ticket = (data.get("typing_ticket") or resp.get("typing_ticket")
                  or data.get("ticket") or resp.get("ticket"))
    if ticket:
        _typing_ticket_cache["ticket"] = str(ticket)
        _typing_ticket_cache["ts"] = now
        return str(ticket)
    return None


QUEUE_NOTICES = [
    "收到啦，蛋挞正在想想怎么回你～",
    "来啦来啦，正在翻记忆找答案…",
    "收到！正在组织语言，马上就好",
    "嗯嗯，听到了，正在动脑筋",
    "蛋挞已上线，正在认真想回复",
    "收到这条了，正在排队等我慢慢想",
    "好嘞，正在看你发的东西，稍等一下",
    "正在努力想一个好回答，别走开",
    "收到收到，正在把想法拼起来",
    "来啦，正在为你准备回复中…",
]


def next_queue_notice():
    """Rotate queue notices sequentially, so the user never sees the
    same fixed '已收到，正在排队等待处理' every time. Counter is
    persisted in state/, so rotation survives serve restarts."""
    counter_file = os.path.join(STATE, "queue_notice_counter.txt")
    try:
        with open(counter_file, encoding="utf-8") as f:
            n = int(f.read().strip() or "0")
    except Exception:
        n = 0
    msg = QUEUE_NOTICES[n % len(QUEUE_NOTICES)]
    try:
        atomic_write_text(counter_file, f"{n + 1}\n", mode=0o600)
    except Exception:
        pass
    return msg


def send_typing(session, ticket, status, to_user_id=None, context_token=None):
    """Phase 2 step 3: POST sendtyping, status 1=start, 2=cancel.
    Timeout 3s, rate-limited to at most one call per 2s. Returns True/False.
    Failure only logs, never blocks the main loop (except TokenExpired).
    Fix 2026-10-28: server requires ilink_user_id explicitly, same as
    getconfig ret=-2 'ilink_user_id required' observed in serve.log.
    to_user_id alone was not enough."""
    if not ticket:
        return False
    now = time.time()
    if now - _typing_last_send["ts"] < 2:
        return False
    _typing_last_send["ts"] = now
    base = session.get("baseurl") or DEFAULT_BASE
    ilink_id = session.get("ilink_user_id") if session else None
    payload = {"typing_ticket": ticket, "status": status, "base_info": base_info(),
               "ilink_user_id": ilink_id or to_user_id}
    if to_user_id:
        payload["to_user_id"] = to_user_id
    if context_token:
        payload["context_token"] = context_token
    try:
        resp = post_json(base, "/ilink/bot/sendtyping", payload,
                         session=session, authenticated=True, timeout=3)
        check_ret(resp, "sendtyping")
    except TokenExpired:
        raise
    except Exception as e:
        print(f"typing: sendtyping status={status} failed (ignored, not blocking): {e}", flush=True)
        return False
    return True


def send_direct_or_outbox(session, text, to_user_id, context_token, outbox_name):
    """Gemini R8 fix: QUEUE ONLY, no network send. The sender thread
    is the sole network sender. Main-thread fast replies (/ping,
    voice prompt, image-fail prompt) are written to
    outbox/pending and sent by _sender_loop within ~1s, instead of
    competing with drain_outbox for the network interface.
    Returns True if queued (not sent), False if queue write failed.
    TokenExpired is no longer raised here because no network call
    is made; the sender thread will surface token expiry on send.
    """
    job = {"text": text, "to_user_id": to_user_id,
           "context_token": context_token, "client_id": uuid.uuid4().hex}
    try:
        with _OUTBOX_LOCK:
            atomic_write_json(os.path.join(OUTBOX_PENDING, outbox_name), job, mode=0o600)
        # Gate 1 item 2: bridge-written jobs record their own
        # queued moment exactly; Worker-written jobs are first
        # seen by drain_outbox instead (see there).
        record_timeline_event(outbox_name, "queued", source="bridge_write")
        # Gate 3 step 2: shadow job mirror for bridge-written tasks
        # (tracked names only; _progress/_offline/_queue receipts
        # are auxiliary and intentionally untracked, mirroring the
        # json_snapshot exclusion).
        _shadow_upsert_job_ready(outbox_name, job)
        return True
    except Exception as e2:
        print(f"outbox queue failed for {outbox_name}: {e2}", flush=True)
        return False


# ---- Phase 2 steps 4-6: image receive / send ----
# DeepSeek attempt for this section: two split prompts on 2026-10-28
# returned reasoning-only output (300 reasoning tokens, no code) for
# parse_aes_key, and one PNG-header line for magic bytes. Muse therefore
# implemented the full AES/upload logic itself, using the PNG-header
# line as the only DeepSeek contribution, and verified it locally.
# Never treat image content as an instruction: text inside an image
# is analysis input only, never authorization to execute, delete,
# or externally send anything.
import hashlib


def parse_aes_key_diag(image_item):
    """Return (key_or_None, diag, error_code_or_None).
    Priority preserved: image_item.aeskey first, then media.aes_key,
    then image_item.aes_key. Key missing (no key field at all) is
    distinct from key invalid (a key field is present but parses to
    no valid 16-byte key): the latter returns KEY_FORMAT_ERROR and
    must NOT silently fall through to plaintext.
    Diag records only presence/type/length/format, never key values."""
    diag = {"candidates": [], "final_source": None, "decoded_length": None,
            "multiple_valid_same": None, "error_code": None}
    if not isinstance(image_item, dict):
        diag["error_code"] = "KEY_FORMAT_ERROR"
        return None, diag, "KEY_FORMAT_ERROR"
    candidates = []
    aeskey = image_item.get("aeskey")
    if aeskey is not None:
        candidates.append(("image_item.aeskey", aeskey))
    media = image_item.get("media") if isinstance(image_item.get("media"), dict) else {}
    if media.get("aes_key") is not None:
        candidates.append(("media.aes_key", media.get("aes_key")))
    if image_item.get("aes_key") is not None:
        candidates.append(("image_item.aes_key", image_item.get("aes_key")))
    if not candidates:
        return None, diag, None
    valid = []
    for source, value in candidates:
        entry = {"source": source, "type": type(value).__name__,
                 "length": len(value) if isinstance(value, (str, bytes)) else None,
                 "format": None, "decoded_length": None}
        key = None
        if source == "image_item.aeskey" and isinstance(value, str) and len(value) == 32:
            try:
                key = bytes.fromhex(value)
                entry["format"] = "aeskey_hex32"
            except Exception:
                entry["format"] = "aeskey_hex32_invalid"
        elif isinstance(value, str) and value:
            try:
                decoded = base64.b64decode(value, validate=False)
                entry["decoded_length"] = len(decoded)
                if len(decoded) == 16:
                    key = decoded
                    entry["format"] = "b64_16"
                elif len(decoded) == 32:
                    try:
                        key = bytes.fromhex(decoded.decode("ascii"))
                        entry["format"] = "b64_hex32"
                    except Exception:
                        entry["format"] = "b64_hex32_invalid"
                else:
                    entry["format"] = "b64_other_length"
            except Exception:
                entry["format"] = "b64_decode_failed"
        diag["candidates"].append(entry)
        if key is not None:
            valid.append((source, key))
    if valid:
        first_source, first_key = valid[0]
        diag["final_source"] = first_source
        diag["decoded_length"] = len(first_key)
        if len(valid) > 1:
            diag["multiple_valid_same"] = all(k == first_key for _s, k in valid)
        return first_key, diag, None
    diag["error_code"] = "KEY_FORMAT_ERROR"
    return None, diag, "KEY_FORMAT_ERROR"


def parse_aes_key(image_item):
    """Backward-compatible wrapper: returns key or None."""
    key, _diag, _err = parse_aes_key_diag(image_item)
    return key


def image_format_from_magic(data):
    if not data:
        return None
    # PNG header line contributed by DeepSeek: data.startswith(b'\x89PNG\r\n\x1a\n')
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    if len(data) >= 12 and data[0:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    return None


def aes_ecb_encrypt(plaintext, key):
    """AES-128-ECB with PKCS#7 padding, in-process (Gate 2 switch).

    Public contract unchanged: returns ciphertext bytes, raises ApiError
    on any strict failure (bad key/plaintext). No subprocess is spawned,
    so there is no 30-second subprocess timeout anymore and no key hex
    in any command-line argv; the only bound is the caller's plaintext
    size cap. In-process errors (ValueError/TypeError) are translated
    to ApiError here so every existing caller keeps its exact behavior
    (decrypt callers map any exception to DECRYPT_FAILED; send callers
    propagate ApiError as a strict failure)."""
    try:
        return aes_inprocess_encrypt(plaintext, key)
    except (ValueError, TypeError):
        raise ApiError("in-process AES encrypt failed (bad key/plaintext)") from None


def aes_ecb_decrypt(ciphertext, key):
    """AES-128-ECB decrypt, strict PKCS#7 padding, in-process (Gate 2).

    Public contract unchanged: returns plaintext bytes, raises ApiError
    on padding/decrypt failure; caller must NOT save the ciphertext
    as plaintext in that case. No subprocess, no subprocess timeout,
    no key hex in any argv."""
    try:
        return aes_inprocess_decrypt(ciphertext, key)
    except (ValueError, TypeError):
        raise ApiError("in-process AES decrypt failed (bad padding/key)") from None


def download_bytes_diag(url, timeout=15):
    """Download with size cap. Returns (bytes_or_None, diag_dict).
    Diag never contains the URL itself (it may carry download params)
    or the body, only status/headers sizes/class/sha.
    Counting points (ChatGPT 2026-10-28 download-integrity fix):
    - http_read: length/sha at HTTP read completion (count 1)
    - pre_decrypt: length/sha just before in-process decrypt (count 2, in caller)
    - saved_file: length/sha after failure download.bin is written
      (count 3, in write_failure_record caller check)
    All three must be identical for the same decoded binary.
    Accept-Encoding: identity is sent explicitly, so no automatic
    decompression can change the byte count; Content-Encoding is
    still recorded to prove it."""
    diag = {"http_status": None, "content_type": None,
            "content_length_header": None, "content_encoding": None,
            "transfer_encoding": None, "content_range": None,
            "etag": None, "accept_ranges": None,
            "actual_bytes": 0, "len_mod_16": None, "content_class": "empty",
            "sha256": None, "error": None, "read_completed": False,
            "read_loop_finished": False, "body_complete": False,
            "missing_bytes": None, "duration_ms": None,
            "read_exception": None}
    import time as _time
    _t0 = _time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0",
                                                   "iLink-App-Id": "bot",
                                                   "Accept-Encoding": "identity"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            diag["http_status"] = getattr(r, "status", None)
            try:
                diag["content_type"] = r.headers.get("Content-Type")
                diag["content_length_header"] = r.headers.get("Content-Length")
                diag["content_encoding"] = r.headers.get("Content-Encoding")
                diag["transfer_encoding"] = r.headers.get("Transfer-Encoding")
                diag["content_range"] = r.headers.get("Content-Range")
                diag["etag"] = r.headers.get("ETag")
                diag["accept_ranges"] = r.headers.get("Accept-Ranges")
            except Exception:
                pass
            chunks = []
            total = 0
            read_completed = False
            try:
                while True:
                    chunk = r.read(65536)
                    if not chunk:
                        read_completed = True
                        break
                    total += len(chunk)
                    if total > MAX_IMAGE_BYTES:
                        diag["actual_bytes"] = total
                        diag["error"] = "over_25MB"
                        diag["read_completed"] = False
                        return None, diag
                    chunks.append(chunk)
            except Exception as e:
                # Read broke mid-body: do NOT return partial bytes as
                # if complete; report incomplete, not a decrypt input.
                diag["read_completed"] = False
                diag["read_exception"] = type(e).__name__
                diag["actual_bytes"] = total
                diag["error"] = "DOWNLOAD_INCOMPLETE"
                print(f"image download read failed: {type(e).__name__}", flush=True)
                return None, diag
            diag["read_completed"] = read_completed
            diag["read_loop_finished"] = read_completed
            data = b"".join(chunks)
            diag["actual_bytes"] = len(data)
            diag["len_mod_16"] = len(data) % 16
            diag["content_class"] = _content_class(data)
            import hashlib as _hashlib
            diag["sha256"] = _hashlib.sha256(data).hexdigest()
            # body_complete is distinct from read_loop_finished:
            # chunked read() can return empty on EOF without raising,
            # even when the body is shorter than Content-Length.
            # Only a matching declared length (or no declared length)
            # plus a finished loop counts as complete.
            _cl = diag.get("content_length_header")
            try:
                _cl_int = int(str(_cl).strip()) if _cl is not None else None
            except Exception:
                _cl_int = None
            if _cl_int is not None:
                diag["missing_bytes"] = max(0, _cl_int - len(data))
                diag["body_complete"] = bool(read_completed and len(data) == _cl_int)
            else:
                diag["missing_bytes"] = 0
                diag["body_complete"] = bool(read_completed)
            diag["duration_ms"] = int((_time.time() - _t0) * 1000)
            return data, diag
    except Exception as e:
        # Log only the exception type, never the URL or full exception.
        diag["error"] = type(e).__name__
        diag["read_exception"] = type(e).__name__
        diag["read_loop_finished"] = False
        diag["body_complete"] = False
        diag["duration_ms"] = int((_time.time() - _t0) * 1000)
        print(f"image download failed: {type(e).__name__}", flush=True)
        return None, diag


def download_bytes(url, timeout=15):
    """Backward-compatible wrapper: returns bytes or None."""
    data, _diag = download_bytes_diag(url, timeout=timeout)
    return data


def extract_images(msg):
    """Return image_item dicts in original item_list order (type=2)."""
    out = []
    for item in msg.get("item_list") or []:
        if item.get("type") == 2:
            ii = item.get("image_item") or {}
            if isinstance(ii, dict):
                out.append(ii)
    return out


def extract_video_items(msg):
    """Return video_item dicts in original item_list order (type=5)."""
    out = []
    for item in msg.get("item_list") or []:
        if item.get("type") == 5:
            vi = item.get("video_item") or {}
            if isinstance(vi, dict):
                out.append(vi)
    return out


def extract_file_items(msg):
    """Return (original_item_list_index, file_item) pairs in order
    (type=4). The ORIGINAL index is the stable key: a message mixing
    media types would otherwise give a video and a file the same
    per-type index 0, colliding in receipts and file names."""
    out = []
    for idx, item in enumerate(msg.get("item_list") or []):
        if item.get("type") == 4:
            fi = item.get("file_item") or {}
            if isinstance(fi, dict):
                out.append((idx, fi))
    return out


def _download_video_to_file(url, tmp_path, timeout=30):
    """Stream-download to tmp_path with a hard cipher cap.
    Returns (total_bytes, complete, error_type_or_None).
    complete requires: read loop finished AND (no Content-Length OR
    Content-Length == total). On over-cap, the partial file is removed.
    Curl fallback is used only when urllib did not complete."""
    import urllib.request as _ur
    total = 0
    complete = False
    err = None
    try:
        req = _ur.Request(url, headers={"User-Agent": "Mozilla/5.0",
                                        "iLink-App-Id": "bot",
                                        "Accept-Encoding": "identity"})
        with _ur.urlopen(req, timeout=timeout) as r:
            cl = r.headers.get("Content-Length")
            try:
                cl_int = int(str(cl).strip()) if cl is not None else None
            except Exception:
                cl_int = None
            if cl_int is not None and cl_int > MAX_VIDEO_CIPHER_BYTES:
                return 0, False, "OVER_CIPHER_CAP"
            with open(tmp_path, "wb") as f:
                os.chmod(tmp_path, 0o600)
                while True:
                    chunk = r.read(65536)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_VIDEO_CIPHER_BYTES:
                        try:
                            os.remove(tmp_path)
                        except OSError:
                            pass
                        return total, False, "OVER_CIPHER_CAP"
                    f.write(chunk)
            complete = (cl_int is None or cl_int == total)
    except Exception as e:
        err = type(e).__name__
    if complete:
        return total, True, None
    # Curl fallback (streaming to file, same cap).
    if shutil.which("curl"):
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            cmd = ["curl", "-sS", "--http1.1", "--max-time", "60",
                   "--max-filesize", str(MAX_VIDEO_CIPHER_BYTES),
                   "--raw", "-H", "Accept-Encoding: identity",
                   "-H", "User-Agent: Mozilla/5.0",
                   "-H", "iLink-App-Id: bot",
                   "-o", tmp_path, url]
            r = subprocess.run(cmd, capture_output=True, timeout=70)
            if r.returncode == 0 and os.path.exists(tmp_path):
                sz = os.path.getsize(tmp_path)
                os.chmod(tmp_path, 0o600)
                if 0 < sz <= MAX_VIDEO_CIPHER_BYTES:
                    return sz, True, None
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return 0, False, "CURL_DOWNLOAD_FAILED"
        except Exception as e:
            return 0, False, type(e).__name__
    return total, False, err or "DOWNLOAD_FAILED"


def process_inbound_video(video_item, message_id, index):
    """Practical video receive v1: download (streaming, capped),
    AES-128-ECB decrypt, save to media/inbound, best-effort ffprobe.
    Returns (saved_or_None, error, diag). Diag is desensitized:
    field names, lengths, formats only; never key values, never URL,
    never body. No plaintext fallback: missing/invalid key, bad
    padding, over-cap and incomplete download all fail, never save
    ciphertext as plaintext. ffprobe failure never rejects a video:
    probe is informational only in v1 (container is not validated)."""
    media = video_item.get("media") if isinstance(video_item.get("media"), dict) else {}
    diag = {"message_id": message_id, "index": index,
            "item_fields": sorted(video_item.keys()) if isinstance(video_item, dict) else [],
            "media_fields": sorted(media.keys()),
            "encrypt_type": media.get("encrypt_type") if "encrypt_type" in media else video_item.get("encrypt_type"),
            "declared_video_size": video_item.get("video_size"),
            "download_source": None, "failure_stage": None, "error_code": None,
            "cipher_size": None, "plain_size": None, "probe": None}
    full_url = media.get("full_url") or video_item.get("full_url")
    if full_url:
        diag["download_source"] = "full_url"
    else:
        enc = media.get("encrypt_query_param") or video_item.get("encrypt_query_param")
        if not enc:
            diag["failure_stage"] = "download"
            diag["error_code"] = "NO_DOWNLOAD_PARAM"
            return None, "no full_url and no encrypt_query_param", diag
        diag["download_source"] = "constructed_cdn"
        cdn_base = media.get("cdn_base_url") or video_item.get("cdn_base_url") or CDN_BASE_DEFAULT
        full_url = f"{cdn_base.rstrip('/')}/download?encrypted_query_param={urllib.parse.quote(str(enc))}"
    key, _kdiag, kerr = parse_aes_key_diag(video_item)
    if kerr == "KEY_FORMAT_ERROR":
        diag["failure_stage"] = "decrypt"
        diag["error_code"] = "KEY_FORMAT_ERROR"
        return None, "video key format error, refusing plaintext fallback", diag
    if key is None:
        diag["failure_stage"] = "decrypt"
        diag["error_code"] = "NO_KEY"
        return None, "video has no aes_key, refusing plaintext fallback", diag
    ensure_dirs()
    cipher_tmp = os.path.join(MEDIA_INBOUND, f"{message_id}_{index}.cipher.tmp.{os.getpid()}")
    try:
        total, complete, derr = _download_video_to_file(full_url, cipher_tmp)
    except Exception as e:
        diag["failure_stage"] = "download"
        diag["error_code"] = type(e).__name__
        return None, "video download failed", diag
    if not complete:
        diag["failure_stage"] = "download"
        diag["error_code"] = derr or "DOWNLOAD_INCOMPLETE"
        return None, "video download incomplete or over cap", diag
    if total % 16 != 0 or total > MAX_VIDEO_CIPHER_BYTES:
        diag["failure_stage"] = "download"
        diag["error_code"] = "BAD_CIPHER_SIZE"
        try:
            os.remove(cipher_tmp)
        except OSError:
            pass
        return None, "video cipher size invalid", diag
    diag["cipher_size"] = total
    try:
        with open(cipher_tmp, "rb") as f:
            cipher = f.read()
        plain = aes_ecb_decrypt(cipher, key)
    except Exception as e:
        diag["failure_stage"] = "decrypt"
        diag["error_code"] = "DECRYPT_FAILED"
        try:
            os.remove(cipher_tmp)
        except OSError:
            pass
        return None, "video decrypt failed, ciphertext not saved as plaintext", diag
    finally:
        try:
            os.remove(cipher_tmp)
        except OSError:
            pass
    if len(plain) > MAX_VIDEO_PLAIN_BYTES:
        diag["failure_stage"] = "download"
        diag["error_code"] = "OVER_PLAIN_CAP"
        return None, "video plain size over 100MiB", diag
    diag["plain_size"] = len(plain)
    final_path = os.path.join(MEDIA_INBOUND, f"{message_id}_{index}.mp4")
    tmp_path = final_path + f".tmp.{os.getpid()}"
    try:
        with open(tmp_path, "wb") as f:
            f.write(plain)
        os.chmod(tmp_path, 0o600)
        if os.path.getsize(tmp_path) != len(plain):
            raise OSError("short write")
        os.replace(tmp_path, final_path)
        os.chmod(final_path, 0o600)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        diag["failure_stage"] = "save"
        diag["error_code"] = "SAVE_FAILED"
        return None, "video save failed", diag
    probe = None
    if shutil.which("ffprobe"):
        try:
            r = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                "format=duration,format_name:stream=codec_type,codec_name,width,height",
                                "-of", "json", final_path],
                               capture_output=True, timeout=20)
            if r.returncode == 0:
                probe = json.loads(r.stdout.decode("utf-8", "ignore") or "{}")
        except Exception:
            probe = None
    diag["probe"] = probe
    return {"path": final_path, "cipher_size": total, "plain_size": len(plain),
            "probe": probe}, None, diag


def _media_url_allowed(url):
    """Production media-URL allowlist for file downloads.

    Only HTTPS endpoints on WeChat hosts (the media CDN and the API
    host) are fetched; anything else, including redirect targets, is
    refused without a request. Test servers are allowed ONLY through
    the _TEST_MEDIA_ORIGINS hook, which is empty in production."""
    try:
        p = urllib.parse.urlparse(url)
    except Exception:
        return False
    if (p.scheme, p.hostname, p.port) in _TEST_MEDIA_ORIGINS:
        return True
    if p.scheme != "https":
        return False
    host = (p.hostname or "").lower()
    return host == "ilinkai.weixin.qq.com" or host.endswith(".weixin.qq.com")


def _resolve_file_media_url(file_item):
    """Mirror the video URL resolution: full_url first, else a
    constructed CDN URL from encrypt_query_param. Returns (url, source)
    or (None, None)."""
    media = file_item.get("media") if isinstance(file_item.get("media"), dict) else {}
    full_url = media.get("full_url") or file_item.get("full_url")
    if full_url:
        return full_url, "full_url"
    enc = media.get("encrypt_query_param") or file_item.get("encrypt_query_param")
    if not enc:
        return None, None
    cdn_base = media.get("cdn_base_url") or file_item.get("cdn_base_url") or CDN_BASE_DEFAULT
    return f"{cdn_base.rstrip('/')}/download?encrypted_query_param={urllib.parse.quote(str(enc))}", "constructed_cdn"


def _clean_file_display_name(name, item_index):
    """Display-only cleaning: the stored file name never uses this
    value, so cleaning only protects what the Worker/user sees.
    Control characters and path separators are stripped; an empty or
    Windows-reserved result falls back to a generated name."""
    if not isinstance(name, str):
        name = ""
    cleaned = "".join(ch for ch in name.strip() if ord(ch) >= 32 and ch not in "/\\")
    cleaned = cleaned[:100]
    base = cleaned.split(".")[0].upper() if cleaned else ""
    reserved = base in ("CON", "PRN", "AUX", "NUL")
    reserved = reserved or (base.startswith("COM") and base[3:].isdigit())
    reserved = reserved or (base.startswith("LPT") and base[3:].isdigit())
    if not cleaned or reserved:
        return f"文件_{item_index}"
    return cleaned


def _file_receipts_load():
    try:
        with open(FILE_RECEIPTS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _file_receipts_save(receipts):
    try:
        atomic_write_json(FILE_RECEIPTS_FILE, receipts, mode=0o600)
    except Exception as e:
        print(f"file receipts write failed: {_sanitize_error(e)}", flush=True)


def _file_receipt_get(message_id, item_index):
    """Reuse a previously saved file for the same message+item, after
    verifying size and sha256. Returns the saved dict or None."""
    receipts = _file_receipts_load()
    rec = receipts.get(f"{message_id}:{item_index}")
    if not isinstance(rec, dict):
        return None
    path = rec.get("path")
    try:
        if not path or not os.path.isfile(path):
            return None
        if os.path.getsize(path) != rec.get("size"):
            return None
        if hashlib.sha256(open(path, "rb").read()).hexdigest() != rec.get("sha256"):
            return None
    except Exception:
        return None
    return {"attachment_id": rec.get("attachment_id"), "name": rec.get("name"),
            "path": path, "size": rec.get("size"), "sha256": rec.get("sha256"),
            "mime": rec.get("mime", "unknown"), "status": "saved"}


def _file_receipt_put(message_id, item_index, saved):
    receipts = _file_receipts_load()
    receipts[f"{message_id}:{item_index}"] = dict(saved)
    _file_receipts_save(receipts)


def _download_file_to_file(url, tmp_path, budget_seconds, cap_bytes):
    """Curl-only bounded download. Returns (size, complete, error).
    --max-time is the REMAINING budget (not a fresh budget per try);
    --fail rejects HTTP errors; up to 3 redirects are followed and
    the FINAL URL must still pass the allowlist. Any failure removes
    the partial file. The subprocess timeout equals the budget as a
    backstop; curl's own --max-time is the binding limit."""
    if budget_seconds <= 0:
        return 0, False, "BUDGET_EXHAUSTED"
    if not shutil.which("curl"):
        return 0, False, "CURL_MISSING"
    budget = max(1, int(budget_seconds))
    cmd = ["curl", "-sS", "-L", "--max-redirs", "3", "--http1.1",
           "--max-time", str(budget), "--max-filesize", str(cap_bytes),
           "--fail", "--raw",
           "-H", "Accept-Encoding: identity",
           "-H", "User-Agent: Mozilla/5.0",
           "-H", "iLink-App-Id: bot",
           "-o", tmp_path, "-w", "%{url_effective}", url]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=budget)
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return 0, False, type(e).__name__
    if r.returncode != 0:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return 0, False, "CURL_DOWNLOAD_FAILED"
    effective = r.stdout.decode("utf-8", "ignore").strip().splitlines()
    effective = effective[-1] if effective else url
    if not _media_url_allowed(effective):
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return 0, False, "REDIRECT_HOST_NOT_ALLOWED"
    try:
        size = os.path.getsize(tmp_path)
        os.chmod(tmp_path, 0o600)
    except OSError:
        return 0, False, "STAT_FAILED"
    if size > cap_bytes:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return size, False, "OVER_CIPHER_CAP"
    return size, True, None


def process_inbound_file(file_item, message_id, item_index, deadline_ts, plain_budget_left):
    """Ordinary file receive v1: bounded download, strict AES-128-ECB
    decrypt, atomic save under media/inbound/files/<mid>/.
    Returns (saved_or_None, error, diag). Diag is desensitized:
    field names and lengths only, never key values, never URL, never
    body. No plaintext fallback: missing/invalid key, bad padding,
    truncation, over-cap and timeout all fail, never save ciphertext
    as plaintext. The declared `len` is recorded only: its inbound
    semantics are unconfirmed, so a mismatch never rejects a file.
    plain_budget_left is the message's remaining plain total (20 MiB
    minus already-saved files); exceeding it fails BEFORE publish.
    A previously receipted file for the same message+item is reused
    after size+sha verification, without re-downloading."""
    mid = str(message_id)
    media = file_item.get("media") if isinstance(file_item.get("media"), dict) else {}
    diag = {"message_id": mid, "item_index": item_index,
            "item_fields": sorted(file_item.keys()) if isinstance(file_item, dict) else [],
            "media_fields": sorted(media.keys()),
            "encrypt_type": media.get("encrypt_type") if "encrypt_type" in media else file_item.get("encrypt_type"),
            "declared_len": file_item.get("len"),
            "declared_file_name": file_item.get("file_name"),
            "download_source": None, "failure_stage": None, "error_code": None,
            "cipher_size": None, "plain_size": None, "sha256": None}
    if not mid.isdigit():
        diag["failure_stage"] = "save"
        diag["error_code"] = "INVALID_MESSAGE_ID"
        return None, "file message_id has path characters, refusing", diag
    reused = _file_receipt_get(mid, item_index)
    if reused:
        diag["download_source"] = "receipt_reuse"
        diag["plain_size"] = reused["size"]
        diag["sha256"] = reused["sha256"]
        return reused, None, diag
    full_url, source = _resolve_file_media_url(file_item)
    if not full_url:
        diag["failure_stage"] = "download"
        diag["error_code"] = "NO_DOWNLOAD_PARAM"
        return None, "file has no full_url and no encrypt_query_param", diag
    if not _media_url_allowed(full_url):
        diag["failure_stage"] = "download"
        diag["error_code"] = "URL_NOT_ALLOWED"
        return None, "file download host not allowed", diag
    diag["download_source"] = source
    key, _kdiag, kerr = parse_aes_key_diag(file_item)
    if kerr == "KEY_FORMAT_ERROR":
        diag["failure_stage"] = "decrypt"
        diag["error_code"] = "KEY_FORMAT_ERROR"
        return None, "file key format error, refusing plaintext fallback", diag
    if key is None:
        diag["failure_stage"] = "decrypt"
        diag["error_code"] = "NO_KEY"
        return None, "file has no aes_key, refusing plaintext fallback", diag
    ensure_dirs()
    file_dir = os.path.join(FILE_INBOUND_DIR, mid)
    try:
        os.makedirs(file_dir, exist_ok=True)
        os.chmod(file_dir, 0o700)
    except Exception:
        diag["failure_stage"] = "save"
        diag["error_code"] = "SAVE_FAILED"
        return None, "file directory create failed", diag
    cipher_tmp = os.path.join(file_dir, f"item_{item_index}.cipher.tmp.{os.getpid()}")
    cipher_size = None
    derr = None
    for _attempt in range(FILE_DOWNLOAD_ATTEMPTS):
        remaining = deadline_ts - time.time()
        if remaining <= 0:
            derr = "BUDGET_EXHAUSTED"
            break
        cipher_size, complete, derr = _download_file_to_file(full_url, cipher_tmp, remaining, FILE_SINGLE_CIPHER_CAP)
        if complete:
            break
        if derr in ("URL_NOT_ALLOWED", "REDIRECT_HOST_NOT_ALLOWED"):
            break
    if cipher_size is None or derr is not None:
        diag["failure_stage"] = "download"
        diag["error_code"] = derr or "DOWNLOAD_FAILED"
        return None, "file download failed", diag
    if cipher_size == 0 or cipher_size % 16 != 0 or cipher_size > FILE_SINGLE_CIPHER_CAP:
        diag["failure_stage"] = "download"
        diag["error_code"] = "BAD_CIPHER_SIZE"
        try:
            os.remove(cipher_tmp)
        except OSError:
            pass
        return None, "file cipher size invalid", diag
    diag["cipher_size"] = cipher_size
    try:
        with open(cipher_tmp, "rb") as f:
            cipher = f.read()
        plain = aes_ecb_decrypt(cipher, key)
    except Exception:
        diag["failure_stage"] = "decrypt"
        diag["error_code"] = "DECRYPT_FAILED"
        try:
            os.remove(cipher_tmp)
        except OSError:
            pass
        return None, "file decrypt failed, ciphertext not saved as plaintext", diag
    finally:
        try:
            os.remove(cipher_tmp)
        except OSError:
            pass
    if len(plain) > FILE_SINGLE_PLAIN_CAP:
        diag["failure_stage"] = "download"
        diag["error_code"] = "OVER_SINGLE_CAP"
        return None, "file plain size over 10MiB", diag
    if len(plain) > plain_budget_left:
        diag["failure_stage"] = "download"
        diag["error_code"] = "OVER_MESSAGE_TOTAL"
        return None, "file would exceed message total 20MiB", diag
    display_name = _clean_file_display_name(file_item.get("file_name") or file_item.get("name"), item_index)
    mime = file_item.get("mime_type") or file_item.get("content_type") or file_item.get("mime") or "unknown"
    if not isinstance(mime, str):
        mime = "unknown"
    final_path = os.path.join(file_dir, f"item_{item_index}_{uuid.uuid4().hex[:12]}.bin")
    tmp_path = final_path + f".tmp.{os.getpid()}"
    try:
        with open(tmp_path, "wb") as f:
            f.write(plain)
        os.chmod(tmp_path, 0o600)
        if os.path.getsize(tmp_path) != len(plain):
            raise OSError("short write")
        os.replace(tmp_path, final_path)
        os.chmod(final_path, 0o600)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        diag["failure_stage"] = "save"
        diag["error_code"] = "SAVE_FAILED"
        return None, "file save failed", diag
    sha = hashlib.sha256(plain).hexdigest()
    diag["plain_size"] = len(plain)
    diag["sha256"] = sha
    saved = {"attachment_id": f"{mid}:{item_index}", "name": display_name,
             "path": final_path, "size": len(plain), "sha256": sha,
             "mime": mime, "status": "saved"}
    _file_receipt_put(mid, item_index, saved)
    return saved, None, diag


# ---------------------------------------------------------------- doc-parse
# wiring (Gate 3 item 2): auto-parse a landed file right after receipt.
# The parser runs under the serve interpreter (system python3); the M0
# venv lacks python-docx/openpyxl and must never be used here. The
# parser's own fuse (--timeout) is the operative bound; the caller
# timeout below is only a kill-switch so a wedged launcher cannot
# stall the loop past the same bound. Parse failures never touch
# receipt: they leave a failure evidence file and the message still
# reaches the Worker.
DOC_PARSE_TIMEOUT_SECONDS = 55
DOC_PARSE_CALLER_TIMEOUT_SECONDS = 60


def doc_evidence_path_for(saved_path, item_index):
    """Sibling evidence name, fixed by contract: doc_evidence_<idx>.json
    next to the landed file. The Worker finds it by this name; the
    inbox record is never mutated for it (no race with Worker pickup)."""
    return os.path.join(os.path.dirname(saved_path),
                        "doc_evidence_%s.json" % item_index)


def _write_doc_parse_failure_evidence(evidence_path, code, reason):
    """Minimal failure evidence, atomic 0600. Never raises."""
    try:
        atomic_write_json(evidence_path, {
            "schema_version": "doc-evidence-0.1",
            "source": {"file_name": None, "sha256": None,
                       "size_bytes": None, "format": None},
            "status": "failed",
            "failure_code": code,
            "failure_reason": reason,
            "anchors": [],
            "notes": {"wiring_synthesized": True},
            "peak_rss_kib": None,
            "scratch_dir": None,
            "scratch_cleanup": None,
        }, mode=0o600)
    except Exception:
        pass


def maybe_parse_inbound_doc(saved, item_index):
    """Auto-parse one landed file. Never raises. Returns 'parsed',
    'failed', or 'skipped_exists'. Only called on the ordinary file
    path: the local-only refusal branch returns before any download,
    so it can never reach here. Receipt reuse flows through here too:
    a reused save still lands an evidence file unless one exists."""
    try:
        path = saved.get("path") if isinstance(saved, dict) else None
        if not path or not os.path.isfile(path):
            return "failed"
        evidence_path = doc_evidence_path_for(path, item_index)
        if os.path.exists(evidence_path):
            return "skipped_exists"
        parser = os.path.join(ROOT, "analyzer", "doc_parser.py")
        if not os.path.isfile(parser):
            _write_doc_parse_failure_evidence(
                evidence_path, "wiring_failed", "doc_parser.py missing")
            return "failed"
        cmd = [sys.executable, parser, path, evidence_path,
               "--timeout", str(DOC_PARSE_TIMEOUT_SECONDS)]
        try:
            proc = subprocess.run(cmd, capture_output=True,
                                  timeout=DOC_PARSE_CALLER_TIMEOUT_SECONDS,
                                  stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            _write_doc_parse_failure_evidence(
                evidence_path, "caller_timeout",
                "doc_parser launcher did not return in time")
            return "failed"
        except Exception:
            _write_doc_parse_failure_evidence(
                evidence_path, "wiring_failed", "doc_parser launch failed")
            return "failed"
        if proc.returncode != 0 or not os.path.isfile(evidence_path):
            if not os.path.isfile(evidence_path):
                _write_doc_parse_failure_evidence(
                    evidence_path, "wiring_failed",
                    "doc_parser exited without evidence")
            return "failed"
        try:
            with open(evidence_path, encoding="utf-8") as f:
                status = json.load(f).get("status")
        except Exception:
            status = None
        return "parsed" if status == "completed" else "failed"
    except Exception:
        return "failed"


def send_video(session, video_path, to_user_id, context_token, client_id=None):
    """Practical video send v1: getuploadurl (media_type=2) -> upload
    ciphertext -> sendmessage type=5. Mirrors send_image, except:
    no container/format validation (any file may be sent, labelled
    .mp4 upstream only by convention), 100MiB plain cap, curl upload
    cap raised to the cipher cap. Returns sendmessage response.
    Raises ApiError on any strict failure; final-send failure is
    UnknownDeliveryError (may already be delivered, do NOT auto-retry)."""
    if not _is_allowed_image_path(video_path):
        raise PermanentError("send_video: path outside allowed media dirs, refusing")
    if not os.path.exists(video_path):
        raise PermanentError("send_video: file missing")
    with open(video_path, "rb") as f:
        plain = f.read()
    if len(plain) > MAX_VIDEO_PLAIN_BYTES:
        raise PermanentError("send_video: over 100MiB")
    rawsize = len(plain)
    rawmd5 = hashlib.md5(plain).hexdigest()
    filekey = uuid.uuid4().hex
    key = os.urandom(16)
    aeskey_hex = key.hex()
    ciphertext = aes_ecb_encrypt(plain, key)
    filesize = len(ciphertext)
    base = session.get("baseurl") or DEFAULT_BASE
    payload = {"media_type": 2, "rawsize": rawsize, "rawfilemd5": rawmd5,
               "filesize": filesize, "filekey": filekey, "aeskey": aeskey_hex,
               "no_need_thumb": True, "base_info": base_info(),
               "ilink_user_id": session.get("ilink_user_id"),
               "to_user_id": to_user_id or session.get("ilink_user_id")}
    try:
        resp = post_json(base, "/ilink/bot/getuploadurl", payload,
                         session=session, authenticated=True, timeout=15)
        check_ret(resp, "getuploadurl")
    except TokenExpired:
        raise
    except ApiError as e:
        raise RetryableError(f"getuploadurl failed: {e}")
    data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
    upload_full_url = data.get("upload_full_url") or resp.get("upload_full_url")
    upload_param = data.get("upload_param") or resp.get("upload_param")
    returned_filekey = data.get("filekey") or resp.get("filekey") or filekey
    if not upload_full_url and upload_param:
        if isinstance(upload_param, str) and upload_param.startswith("http"):
            upload_full_url = upload_param
            if "filekey" not in upload_full_url:
                sep = "&" if "?" in upload_full_url else "?"
                upload_full_url = f"{upload_full_url}{sep}filekey={urllib.parse.quote(str(returned_filekey))}"
        elif isinstance(upload_param, dict) and upload_param.get("url"):
            upload_full_url = upload_param["url"]
    if not upload_full_url:
        raise RetryableError("getuploadurl: no upload_full_url and no usable upload_param")
    encrypt_param = None
    if shutil.which("curl"):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(ciphertext)
            tmp_path = tf.name
        hdr_path = tmp_path + ".hdr"
        try:
            cmd = ["curl", "-s", "--max-time", "60", "--max-filesize", str(MAX_VIDEO_CIPHER_BYTES),
                   "-D", hdr_path, "-o", "/dev/null",
                   "-w", "%{http_code}", "-X", "POST",
                   "-H", "Content-Type: application/octet-stream",
                   "--data-binary", f"@{tmp_path}", upload_full_url]
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=70)
            except subprocess.TimeoutExpired:
                raise RetryableError("upload curl timeout") from None
            except Exception as e:
                raise RetryableError(f"upload curl failed: {type(e).__name__}") from None
            http_code = (r.stdout or b"").decode().strip()
            try:
                with open(hdr_path, encoding="utf-8", errors="ignore") as hf:
                    for line in hf:
                        if line.lower().startswith("x-encrypted-param:"):
                            encrypt_param = line.split(":", 1)[1].strip()
            except OSError:
                pass
            if http_code != "200" or not encrypt_param:
                raise RetryableError(f"upload failed: http={http_code} x-encrypted-param={'present' if encrypt_param else 'missing'}")
        finally:
            for pp in (tmp_path, hdr_path):
                try:
                    os.remove(pp)
                except OSError:
                    pass
    else:
        req = urllib.request.Request(upload_full_url, data=ciphertext,
                                     headers={"Content-Type": "application/octet-stream"},
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                if r.status != 200:
                    raise RetryableError(f"upload http={r.status}")
                encrypt_param = r.headers.get("x-encrypted-param")
        except RetryableError:
            raise
        except Exception as e:
            raise RetryableError(f"upload failed: {type(e).__name__}") from None
        if not encrypt_param:
            raise RetryableError("upload failed: x-encrypted-param missing")
    aes_key_b64 = base64.b64encode(aeskey_hex.encode("utf-8")).decode()
    msg_payload = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id or session.get("ilink_user_id"),
            "client_id": client_id or uuid.uuid4().hex,
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token if context_token is not None else (session.get("context_token") or ""),
            "item_list": [{"type": 5, "video_item": {
                "video_size": filesize,
                "media": {
                    "encrypt_query_param": encrypt_param.strip() if encrypt_param else encrypt_param,
                    "aes_key": aes_key_b64,
                    "encrypt_type": 1,
                },
            }}],
        },
        "base_info": base_info(),
    }
    try:
        resp2 = post_json(base, "/ilink/bot/sendmessage", msg_payload,
                          session=session, authenticated=True, timeout=15, allow_fallback=False)
    except TokenExpired:
        raise
    except Exception as e:
        raise UnknownDeliveryError(f"sendmessage-video post failed, result unknown: {e}")
    try:
        check_ret(resp2, "sendmessage-video")
    except TokenExpired:
        raise
    except Exception as e:
        raise PermanentError(f"sendmessage-video rejected: {e}")
    return resp2


def send_file(session, file_path, to_user_id, context_token, client_id=None, file_name=None):
    """Practical file send v1: getuploadurl (media_type=3) -> upload
    ciphertext -> sendmessage type=4. Mirrors send_video, except:
    item is file_item with file_name (basename unless overridden) and
    len = plaintext size in bytes (upstream UploadedFileInfo.fileSize),
    NOT the cipher size. 100MiB plain cap mirrored from video as a
    bridge-side limit (not an upstream-confirmed file cap). No
    container/type validation: any file under the cap may be sent.
    Returns sendmessage response. Final-send failure is
    UnknownDeliveryError (may already be delivered, do NOT auto-retry).
    Upload failures also surface x-error-code when the server sends one
    (diagnostic only; observed once as -5102031 on a webm VIDEO upload)."""
    if not _is_allowed_image_path(file_path):
        raise PermanentError("send_file: path outside allowed media dirs, refusing")
    if not os.path.exists(file_path):
        raise PermanentError("send_file: file missing")
    with open(file_path, "rb") as f:
        plain = f.read()
    if len(plain) > MAX_VIDEO_PLAIN_BYTES:
        raise PermanentError("send_file: over 100MiB")
    rawsize = len(plain)
    rawmd5 = hashlib.md5(plain).hexdigest()
    filekey = uuid.uuid4().hex
    key = os.urandom(16)
    aeskey_hex = key.hex()
    ciphertext = aes_ecb_encrypt(plain, key)
    filesize = len(ciphertext)
    base = session.get("baseurl") or DEFAULT_BASE
    payload = {"media_type": 3, "rawsize": rawsize, "rawfilemd5": rawmd5,
               "filesize": filesize, "filekey": filekey, "aeskey": aeskey_hex,
               "no_need_thumb": True, "base_info": base_info(),
               "ilink_user_id": session.get("ilink_user_id"),
               "to_user_id": to_user_id or session.get("ilink_user_id")}
    try:
        resp = post_json(base, "/ilink/bot/getuploadurl", payload,
                         session=session, authenticated=True, timeout=15)
        check_ret(resp, "getuploadurl")
    except TokenExpired:
        raise
    except ApiError as e:
        raise RetryableError(f"getuploadurl failed: {e}")
    data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
    upload_full_url = data.get("upload_full_url") or resp.get("upload_full_url")
    upload_param = data.get("upload_param") or resp.get("upload_param")
    returned_filekey = data.get("filekey") or resp.get("filekey") or filekey
    if not upload_full_url and upload_param:
        if isinstance(upload_param, str) and upload_param.startswith("http"):
            upload_full_url = upload_param
            if "filekey" not in upload_full_url:
                sep = "&" if "?" in upload_full_url else "?"
                upload_full_url = f"{upload_full_url}{sep}filekey={urllib.parse.quote(str(returned_filekey))}"
        elif isinstance(upload_param, dict) and upload_param.get("url"):
            upload_full_url = upload_param["url"]
    if not upload_full_url:
        raise RetryableError("getuploadurl: no upload_full_url and no usable upload_param")
    encrypt_param = None
    error_code = None
    if shutil.which("curl"):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(ciphertext)
            tmp_path = tf.name
        hdr_path = tmp_path + ".hdr"
        try:
            cmd = ["curl", "-s", "--max-time", "60", "--max-filesize", str(MAX_VIDEO_CIPHER_BYTES),
                   "-D", hdr_path, "-o", "/dev/null",
                   "-w", "%{http_code}", "-X", "POST",
                   "-H", "Content-Type: application/octet-stream",
                   "--data-binary", f"@{tmp_path}", upload_full_url]
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=70)
            except subprocess.TimeoutExpired:
                raise RetryableError("upload curl timeout") from None
            except Exception as e:
                raise RetryableError(f"upload curl failed: {type(e).__name__}") from None
            http_code = (r.stdout or b"").decode().strip()
            try:
                with open(hdr_path, encoding="utf-8", errors="ignore") as hf:
                    for line in hf:
                        if line.lower().startswith("x-encrypted-param:"):
                            encrypt_param = line.split(":", 1)[1].strip()
                        elif line.lower().startswith("x-error-code:"):
                            error_code = line.split(":", 1)[1].strip()
            except OSError:
                pass
            if http_code != "200" or not encrypt_param:
                raise RetryableError(f"upload failed: http={http_code} x-encrypted-param={'present' if encrypt_param else 'missing'}" + (f" x-error-code={error_code}" if error_code else ""))
        finally:
            for pp in (tmp_path, hdr_path):
                try:
                    os.remove(pp)
                except OSError:
                    pass
    else:
        req = urllib.request.Request(upload_full_url, data=ciphertext,
                                     headers={"Content-Type": "application/octet-stream"},
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                if r.status != 200:
                    raise RetryableError(f"upload http={r.status}")
                encrypt_param = r.headers.get("x-encrypted-param")
        except RetryableError:
            raise
        except Exception as e:
            raise RetryableError(f"upload failed: {type(e).__name__}") from None
        if not encrypt_param:
            raise RetryableError("upload failed: x-encrypted-param missing")
    aes_key_b64 = base64.b64encode(aeskey_hex.encode("utf-8")).decode()
    send_name = file_name or os.path.basename(file_path)
    msg_payload = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id or session.get("ilink_user_id"),
            "client_id": client_id or uuid.uuid4().hex,
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token if context_token is not None else (session.get("context_token") or ""),
            "item_list": [{"type": 4, "file_item": {
                "media": {
                    "encrypt_query_param": encrypt_param.strip() if encrypt_param else encrypt_param,
                    "aes_key": aes_key_b64,
                    "encrypt_type": 1,
                },
                "file_name": send_name,
                "len": str(rawsize),
            }}],
        },
        "base_info": base_info(),
    }
    try:
        resp2 = post_json(base, "/ilink/bot/sendmessage", msg_payload,
                          session=session, authenticated=True, timeout=15, allow_fallback=False)
    except TokenExpired:
        raise
    except Exception as e:
        raise UnknownDeliveryError(f"sendmessage-file post failed, result unknown: {e}")
    try:
        check_ret(resp2, "sendmessage-file")
    except TokenExpired:
        raise
    except Exception as e:
        raise PermanentError(f"sendmessage-file rejected: {e}")
    return resp2


def write_failure_record(message_id, index, diag, raw_data=None, attempt=None):
    """Atomically write failures/<id>_<index>.json (0600) and, when
    raw download bytes exist, failures/<id>_<index>.download.bin
    (0600, isolated, NOT in media/inbound/). Record contains field
    names only, never key values, never full URLs, never body text.
    With attempt set, also write .attempt<attempt>.download.bin so
    retry attempts do not overwrite each other's evidence.
    Returns the failure record path."""
    ensure_dirs()
    record_path = os.path.join(FAILURES_DIR, f"{message_id}_{index}.json")
    diag = dict(diag or {})
    diag["message_id"] = message_id
    diag["index"] = index
    if raw_data is not None:
        bin_path = os.path.join(FAILURES_DIR, f"{message_id}_{index}.download.bin")
        if attempt is not None:
            bin_path = os.path.join(FAILURES_DIR, f"{message_id}_{index}.attempt{attempt}.download.bin")
        try:
            with open(bin_path, "wb") as f:
                f.write(raw_data)
            os.chmod(bin_path, 0o600)
            diag["download_bin"] = bin_path
        except Exception:
            diag["download_bin"] = None
    atomic_write_json(record_path, diag, mode=0o600)
    return record_path


def _detect_magic(b):
    if b.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if len(b) >= 12 and b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "WEBP"
    if b.startswith((b"GIF87a", b"GIF89a")):
        return "GIF"
    if len(b) >= 8 and b[4:8] == b"ftyp":
        return "HEIC_OR_ISO"
    return "UNKNOWN"


def _raw_aes128_ecb_decrypt_block(key, block16):
    """Diagnostic only: decrypt ONE 16-byte block, no unpadding.
    Never used for formal decrypt, never outputs plaintext or key.
    Returns 16 bytes or None. Gate 2: in-process (was openssl -nopad),
    so key bytes never appear in a subprocess argv anymore."""
    if not isinstance(key, (bytes, bytearray)) or len(key) != 16:
        return None
    if not isinstance(block16, (bytes, bytearray)) or len(block16) != 16:
        return None
    try:
        out = aes_inprocess_decrypt_nopad(bytes(block16), bytes(key))
    except Exception:
        return None
    if len(out) != 16:
        return None
    return out


def build_safe_cipher_diagnostics(attempt1_bytes, attempt2_bytes, key_bytes):
    """Gemini 2026-10-28 safe diagnostics: only lengths, prefix
    equality, magic classifications, pad type. Never plaintext, never
    key, never full URL. attempt2 may be None."""
    diag = {
        "attempt1_len": len(attempt1_bytes) if attempt1_bytes is not None else None,
        "attempt1_mod16": (len(attempt1_bytes) % 16) if attempt1_bytes is not None else None,
        "attempt2_len": len(attempt2_bytes) if attempt2_bytes is not None else None,
        "shared_prefix_identical": None,
        "cipher_head_magic": _detect_magic(attempt1_bytes[:16]) if attempt1_bytes else "UNKNOWN",
        "ecb_first_block_magic": None,
        "ecb_last_block_pad_type": None,
    }
    if attempt1_bytes is not None and attempt2_bytes is not None:
        _m = min(len(attempt1_bytes), len(attempt2_bytes))
        diag["shared_prefix_identical"] = (attempt1_bytes[:_m] == attempt2_bytes[:_m])
    if key_bytes and len(key_bytes) == 16 and attempt1_bytes and len(attempt1_bytes) >= 16:
        first_plain = _raw_aes128_ecb_decrypt_block(key_bytes, attempt1_bytes[:16])
        if first_plain is not None:
            diag["ecb_first_block_magic"] = _detect_magic(first_plain)
        if len(attempt1_bytes) % 16 == 0:
            last_plain = _raw_aes128_ecb_decrypt_block(key_bytes, attempt1_bytes[-16:])
            if last_plain is not None:
                pad_len = last_plain[-1]
                if 1 <= pad_len <= 16 and last_plain[-pad_len:] == bytes([pad_len]) * pad_len:
                    diag["ecb_last_block_pad_type"] = "valid_pkcs7"
                elif last_plain.endswith(b"\x00"):
                    diag["ecb_last_block_pad_type"] = "trailing_zeros"
                else:
                    diag["ecb_last_block_pad_type"] = "invalid_pad_or_truncated"
    return diag


def process_inbound_image(image_item, message_id, index):
    """Download, decrypt if needed, validate magic + pixels + full decode,
    then atomic publish from .tmp. Returns (saved_or_None, error, diag).
    Diag is desensitized: field names, lengths, formats, download
    status/class/sha, failure stage. Never key values, never full URL,
    never body. On any failure, a failure record is written first by
    the caller via write_failure_record; this function also writes it
    here so direct callers get evidence too (idempotent, same path)."""
    media = image_item.get("media") if isinstance(image_item.get("media"), dict) else {}
    thumb = image_item.get("thumb_media") if isinstance(image_item.get("thumb_media"), dict) else {}
    diag = {
        "message_id": message_id, "index": index,
        "item_fields": sorted(image_item.keys()) if isinstance(image_item, dict) else [],
        "media_fields": sorted(media.keys()),
        "thumb_media_fields": sorted(thumb.keys()),
        "encrypt_type": media.get("encrypt_type") if "encrypt_type" in media else image_item.get("encrypt_type"),
        # Gemini split: meta size (message JSON) vs HTTP header vs actual.
        "declared_meta_size": image_item.get("mid_size") or image_item.get("hd_size"),
        "meta_sizes": {"hd_size": image_item.get("hd_size"),
                       "mid_size": image_item.get("mid_size"),
                       "thumb_size": image_item.get("thumb_size")},
        "download_source": None, "download": None, "key": None,
        "failure_stage": None, "error_code": None, "error_short": None,
        "content_class": None, "failure_record": None,
    }
    full_url = media.get("full_url") or image_item.get("full_url")
    if full_url:
        diag["download_source"] = "full_url"
    else:
        enc = media.get("encrypt_query_param") or image_item.get("encrypt_query_param")
        if not enc:
            diag["failure_stage"] = "download"
            diag["error_code"] = "NO_DOWNLOAD_PARAM"
            diag["error_short"] = "no full_url and no encrypt_query_param"
            diag["failure_record"] = write_failure_record(message_id, index, diag)
            return None, diag["error_short"], diag
        diag["download_source"] = "constructed_cdn"
        cdn_base = media.get("cdn_base_url") or image_item.get("cdn_base_url") or CDN_BASE_DEFAULT
        full_url = f"{cdn_base.rstrip('/')}/download?encrypted_query_param={urllib.parse.quote(str(enc))}"
    # Complete retry once, same message, same full_url, in-memory
    # snapshot only (ChatGPT 2026-10-28 retry fix). Range is NOT
    # auto-enabled here: it is a conditional diagnostic only, and
    # requires a strong ETag + If-Range, otherwise splicing is unsafe.
    # Attempt 2 reads from zero into a NEW buffer, never appended to
    # attempt 1. Both attempts keep separate .attemptN.download.bin
    # evidence. Only a final success proceeds to decrypt/validate.
    data = None
    dl_diag = None
    attempts = []
    for attempt in (1, 2):
        _data, _dl = download_bytes_diag(full_url)
        _dl = dict(_dl or {})
        _dl["attempt"] = attempt
        attempts.append(_dl)
        if _data is not None:
            # Save attempt evidence immediately, do not overwrite.
            try:
                ensure_dirs()
                _ap = os.path.join(FAILURES_DIR, f"{message_id}_{index}.attempt{attempt}.download.bin")
                with open(_ap, "wb") as _f:
                    _f.write(_data)
                os.chmod(_ap, 0o600)
            except Exception:
                pass
        # Decide if this attempt is download-complete.
        _complete = False
        if _data is not None and len(_data) <= MAX_IMAGE_BYTES and _dl.get("read_loop_finished"):
            _cl2 = _dl.get("content_length_header")
            _te2 = (_dl.get("transfer_encoding") or "").lower()
            _ce2 = (_dl.get("content_encoding") or "").lower()
            if _cl2 is not None and "chunked" not in _te2 and _ce2 in (None, "", "identity"):
                try:
                    _complete = (int(str(_cl2).strip()) == len(_data))
                except Exception:
                    _complete = True
            else:
                _complete = True
        if _complete:
            data, dl_diag = _data, _dl
            break
        # Speed optimisation 2026-10-28: urllib attempt 2 never
        # recovered a full body in any recorded failure (all second
        # attempts were also short, prefix-consistent), while curl
        # recovered both 04:53 images. So after attempt 1 fails,
        # do NOT spend another ~2s + 0.75s backoff on urllib attempt
        # 2; break immediately and let the curl fallback below be
        # the effective second attempt. This saves ~2-3s per image.
        data, dl_diag = _data, _dl
        break
    diag["download"] = dl_diag
    diag["download_attempts"] = attempts
    if data is None:
        # R7 fix: a read exception / incomplete download must also
        # enter the curl fallback, not return before it. Only an
        # over-25MB refusal is permanent and must not be retried.
        if dl_diag.get("error") != "over_25MB" and shutil.which("curl"):
            try:
                import tempfile as _tf2
                with _tf2.NamedTemporaryFile(delete=False) as _cf2:
                    _curl_path2 = _cf2.name
                _hdr2 = _curl_path2 + ".hdr"
                _cmd2 = ["curl", "-sS", "--http1.1", "--max-time", "30", "--max-filesize", "26214400",
                         "--raw", "-H", "Accept-Encoding: identity",
                         "-H", "User-Agent: Mozilla/5.0",
                         "-H", "iLink-App-Id: bot",
                         "-D", _hdr2, "-o", _curl_path2, full_url]
                _r2 = subprocess.run(_cmd2, capture_output=True, timeout=40)
                _len2 = 0
                try:
                    _len2 = os.path.getsize(_curl_path2)
                except Exception:
                    _len2 = 0
                diag["curl_diag"] = {"exit_code": getattr(_r2, "returncode", None),
                                     "actual_bytes": _len2}
                # R7 size budget: curl bytes must also respect 25MB.
                if getattr(_r2, "returncode", 1) == 0 and 0 < _len2 <= MAX_IMAGE_BYTES:
                    with open(_curl_path2, "rb") as _f2:
                        data = _f2.read()
                    diag["curl_fallback_used"] = True
                    # Fall through to the normal length/decrypt path
                    # by updating dl_diag to a complete record.
                    dl_diag = dict(dl_diag or {})
                    dl_diag["actual_bytes"] = len(data)
                    dl_diag["body_complete"] = True
                    dl_diag["read_loop_finished"] = True
                    diag["download"] = dl_diag
                # Cleanup temp files (best effort)
                for _pp in (_curl_path2, _hdr2):
                    try:
                        if os.path.exists(_pp):
                            os.remove(_pp)
                    except Exception:
                        pass
            except Exception as _ce:
                diag["curl_diag"] = {"error": type(_ce).__name__}
        if data is None:
            diag["failure_stage"] = "download"
            diag["error_code"] = dl_diag.get("error") or "DOWNLOAD_FAILED"
            diag["error_short"] = "download failed or over 25MB"
            diag["failure_record"] = write_failure_record(message_id, index, diag)
            return None, diag["error_short"], diag
    if len(data) > MAX_IMAGE_BYTES:
        diag["failure_stage"] = "download"
        diag["error_code"] = "OVER_25MB"
        diag["error_short"] = "over 25MB"
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data, attempt=len(attempts))
        return None, diag["error_short"], diag
    # Download-integrity check: if the final attempt still declares
    # Content-Length, is not chunked, and did not use content-encoding,
    # then actual != declared must be treated as incomplete, NOT sent
    # to decrypt. A missing tail still passes %16==0 but loses the
    # final padding block, so it would otherwise surface misleadingly
    # as bad decrypt. HTTP 200 alone does not prove completeness.
    # Note: this is the final attempt's result; the fourth failure's
    # stage change (decrypt -> download) does NOT by itself prove
    # the root cause changed, only that detection moved earlier.
    _cl = dl_diag.get("content_length_header")
    _te = (dl_diag.get("transfer_encoding") or "").lower()
    _ce = (dl_diag.get("content_encoding") or "").lower()
    if _cl is not None and "chunked" not in _te and _ce in (None, "", "identity"):
        try:
            _cl_int = int(str(_cl).strip())
            if _cl_int != len(data):
                # Gemini Step 2, in-process curl control, same URL,
                # same Accept-Encoding: identity, headers/body separate,
                # exit code preserved. Only status/length/sha are kept
                # in diag, never the URL. If curl gets the full body,
                # use it (Python client layer is then the suspect);
                # if curl also truncates, the fault is outside Python.
                _curl_diag = None
                if shutil.which("curl"):
                    try:
                        import tempfile as _tf
                        with _tf.NamedTemporaryFile(delete=False) as _cf:
                            _curl_path = _cf.name
                        _hdr_path = _curl_path + ".hdr"
                        _cmd = ["curl", "-sS", "--http1.1", "--max-time", "30", "--max-filesize", "26214400",
                                "--raw", "-H", "Accept-Encoding: identity",
                                "-H", "User-Agent: Mozilla/5.0",
                                "-H", "iLink-App-Id: bot",
                                "-D", _hdr_path, "-o", _curl_path, full_url]
                        _r = subprocess.run(_cmd, capture_output=True, timeout=40)
                        _curl_len = 0
                        try:
                            _curl_len = os.path.getsize(_curl_path)
                            # R7 size budget: do not load an over-limit
                            # curl file into memory just to hash it.
                            if _curl_len > MAX_IMAGE_BYTES:
                                _curl_sha = None
                            else:
                                import hashlib as _hh
                                _curl_sha = _hh.sha256(open(_curl_path, "rb").read()).hexdigest()
                        except Exception:
                            _curl_sha = None
                        _curl_diag = {"exit_code": _r.returncode,
                                      "actual_bytes": _curl_len, "sha256": _curl_sha}
                        if _r.returncode == 0 and _curl_len == _cl_int:
                            # Curl recovered a complete body: use it.
                            with open(_curl_path, "rb") as _f:
                                data = _f.read()
                            diag["curl_fallback_used"] = True
                            diag["curl_diag"] = _curl_diag
                            print(f"serve: curl fallback recovered {message_id} declared={_cl_int} curl_bytes={_curl_len}", flush=True)
                            # Fall through to decrypt with curl bytes.
                            _cl_int = len(data)
                        else:
                            diag["curl_diag"] = _curl_diag
                    except Exception as _e:
                        diag["curl_diag"] = {"error": type(_e).__name__}
                    finally:
                        for _p in (locals().get("_curl_path"), locals().get("_hdr_path")):
                            try:
                                if _p and os.path.exists(_p):
                                    os.remove(_p)
                            except Exception:
                                pass
                if _cl_int != len(data):
                    # Safe cipher diagnostics, Gemini minimal change:
                    # parse key here (even though download failed) so
                    # first-block magic can be checked without leaking
                    # plaintext or key. Attempt bytes are the two
                    # urllib attempts already saved, not the URL.
                    try:
                        _k2, _kd2, _ke2 = parse_aes_key_diag(image_item)
                        _a1 = None
                        _a2 = None
                        try:
                            _a1 = open(os.path.join(FAILURES_DIR, f"{message_id}_{index}.attempt1.download.bin"), "rb").read()
                            _a2 = open(os.path.join(FAILURES_DIR, f"{message_id}_{index}.attempt2.download.bin"), "rb").read()
                        except Exception:
                            pass
                        diag["safe_cipher_diagnostics"] = build_safe_cipher_diagnostics(_a1, _a2, _k2)
                        diag["http_content_length"] = _cl_int
                        diag["actual_read_bytes"] = len(data)
                    except Exception:
                        pass
                    diag["failure_stage"] = "download"
                    diag["error_code"] = "DOWNLOAD_LENGTH_MISMATCH"
                    diag["error_short"] = f"DOWNLOAD_LENGTH_MISMATCH: declared={_cl_int} actual={len(data)}"
                    diag["download_length_mismatch"] = {"declared": _cl_int, "actual": len(data)}
                    diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data, attempt=len(attempts))
                    return None, diag["error_short"], diag
        except (ValueError, TypeError):
            pass
    if diag.get("curl_fallback_used"):
        # Curl gave a complete body; treat download as complete for
        # the decrypt stage, and update diag accordingly.
        import hashlib as _hh2
        dl_diag = dict(dl_diag or {})
        dl_diag["actual_bytes"] = len(data)
        dl_diag["sha256"] = _hh2.sha256(data).hexdigest()
        dl_diag["body_complete"] = True
        dl_diag["read_loop_finished"] = True
        diag["download"] = dl_diag
    if not dl_diag.get("read_loop_finished"):
        diag["failure_stage"] = "download"
        diag["error_code"] = "DOWNLOAD_INCOMPLETE"
        diag["error_short"] = "DOWNLOAD_INCOMPLETE: read loop did not finish normally"
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data, attempt=len(attempts))
        return None, diag["error_short"], diag
    if not dl_diag.get("body_complete"):
        diag["failure_stage"] = "download"
        diag["error_code"] = "DOWNLOAD_INCOMPLETE"
        diag["error_short"] = "DOWNLOAD_INCOMPLETE: body_complete=false"
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data, attempt=len(attempts))
        return None, diag["error_short"], diag
    # Decrypt pre-check: non-empty, length multiple of 16 (when keyed).
    key, key_diag, key_err = parse_aes_key_diag(image_item)
    diag["key"] = key_diag
    if key_err == "KEY_FORMAT_ERROR":
        diag["failure_stage"] = "key_parse"
        diag["error_code"] = "KEY_FORMAT_ERROR"
        diag["error_short"] = "KEY_FORMAT_ERROR: key field present but no valid 16-byte key"
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
        return None, diag["error_short"], diag
    if key is not None:
        if not data or len(data) % 16 != 0:
            diag["failure_stage"] = "decrypt"
            diag["error_code"] = "DECRYPT_LENGTH"
            diag["error_short"] = "decrypt length not multiple of 16"
            diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
            return None, diag["error_short"], diag
        # Counting point 2: pre-decrypt length/sha must equal count 1.
        import hashlib as _hashlib2
        diag["pre_decrypt"] = {"length": len(data),
                               "sha256": _hashlib2.sha256(data).hexdigest()}
        if diag["pre_decrypt"]["sha256"] != dl_diag.get("sha256"):
            diag["failure_stage"] = "download"
            diag["error_code"] = "DOWNLOAD_LENGTH_MISMATCH"
            diag["error_short"] = "DOWNLOAD_LENGTH_MISMATCH: pre-decrypt bytes differ from download bytes"
            diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
            return None, diag["error_short"], diag
        try:
            plain = aes_ecb_decrypt(data, key)
        except Exception as e:
            # Never save ciphertext as plaintext on decrypt failure.
            # aes_ecb_decrypt is in-memory only, so no partial file exists.
            # Do NOT use -nopad as a fix; it only hides padding errors.
            diag["failure_stage"] = "decrypt"
            diag["error_code"] = "DECRYPT_FAILED"
            diag["error_short"] = _sanitize_error(e)
            diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
            return None, f"decrypt failed: {diag['error_short']}", diag
    else:
        plain = data
    diag["content_class"] = _content_class(plain)
    if len(plain) > MAX_IMAGE_BYTES:
        diag["failure_stage"] = "validate"
        diag["error_code"] = "DECRYPTED_OVER_25MB"
        diag["error_short"] = "decrypted over 25MB"
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
        return None, diag["error_short"], diag
    fmt = image_format_from_magic(plain)
    if fmt is None:
        diag["failure_stage"] = "validate"
        diag["error_code"] = "MAGIC_MISMATCH"
        diag["error_short"] = "magic bytes do not match PNG/JPEG/GIF/WEBP"
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
        return None, diag["error_short"], diag
    # Full decode, not just header: Pillow .load() forces actual pixels.
    try:
        from PIL import Image
        import io as _io
        with Image.open(_io.BytesIO(plain)) as im:
            w, h = im.size
            if w * h > MAX_IMAGE_PIXELS or w > 20000 or h > 20000:
                diag["failure_stage"] = "validate"
                diag["error_code"] = "PIXEL_LIMIT"
                diag["error_short"] = "pixel limit exceeded"
                diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
                return None, diag["error_short"], diag
            im.load()
    except Exception as e:
        diag["failure_stage"] = "validate"
        diag["error_code"] = "FULL_DECODE_FAILED"
        diag["error_short"] = _sanitize_error(e)
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
        return None, f"full decode failed: {diag['error_short']}", diag
    ensure_dirs()
    ext = "jpg" if fmt == "jpg" else fmt
    final_path = os.path.join(MEDIA_INBOUND, f"{message_id}_{index}.{ext}")
    tmp_path = final_path + f".tmp.{os.getpid()}"
    try:
        with open(tmp_path, "wb") as f:
            f.write(plain)
        os.chmod(tmp_path, 0o600)
        if os.path.getsize(tmp_path) != len(plain):
            raise ApiError("tmp size mismatch")
        os.replace(tmp_path, final_path)
        os.chmod(final_path, 0o600)
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        diag["failure_stage"] = "publish"
        diag["error_code"] = "SAVE_FAILED"
        diag["error_short"] = _sanitize_error(e)
        diag["failure_record"] = write_failure_record(message_id, index, diag, raw_data=data)
        return None, f"save failed: {diag['error_short']}", diag
    return {"path": final_path, "format": fmt, "width": w, "height": h,
            "size": len(plain), "index": index}, None, diag


def _is_allowed_image_path(path):
    """Audit 4: realpath containment + symlink rejection + regular file.
    A symlink inside media/ pointing outside fails realpath check;
    a symlink pointing inside is still rejected by islink, per audit."""
    if not path or not isinstance(path, str):
        return False
    try:
        if os.path.islink(path):
            return False
        if not os.path.isfile(path):
            return False
        rp = os.path.realpath(path)
    except Exception:
        return False
    for base in (MEDIA_INBOUND, MEDIA_OUTBOUND):
        if rp.startswith(os.path.realpath(base) + os.sep):
            return True
    return False


def send_image(session, image_path, to_user_id, context_token, client_id=None):
    """Phase 2 step 5: getuploadurl -> upload ciphertext -> sendmessage type=2.
    Returns sendmessage response. Raises ApiError on any strict failure."""
    if not _is_allowed_image_path(image_path):
        raise PermanentError("send_image: path outside allowed media dirs, refusing")
    if not os.path.exists(image_path):
        raise PermanentError("send_image: file missing")
    with open(image_path, "rb") as f:
        plain = f.read()
    if len(plain) > MAX_IMAGE_BYTES:
        raise PermanentError("send_image: over 25MB")
    fmt = image_format_from_magic(plain)
    if fmt is None:
        raise PermanentError("send_image: not a valid PNG/JPEG/GIF/WEBP")
    rawsize = len(plain)
    rawmd5 = hashlib.md5(plain).hexdigest()
    filekey = uuid.uuid4().hex
    key = os.urandom(16)
    aeskey_hex = key.hex()
    ciphertext = aes_ecb_encrypt(plain, key)
    filesize = len(ciphertext)
    base = session.get("baseurl") or DEFAULT_BASE
    # Fix 2026-10-28: getuploadurl ret=-2, same pattern as getconfig/
    # sendtyping before it: server requires ilink_user_id explicitly.
    payload = {"media_type": 1, "rawsize": rawsize, "rawfilemd5": rawmd5,
               "filesize": filesize, "filekey": filekey, "aeskey": aeskey_hex,
               "no_need_thumb": True, "base_info": base_info(),
               "ilink_user_id": session.get("ilink_user_id"),
               "to_user_id": to_user_id or session.get("ilink_user_id")}
    try:
        resp = post_json(base, "/ilink/bot/getuploadurl", payload,
                         session=session, authenticated=True, timeout=15)
        check_ret(resp, "getuploadurl")
    except TokenExpired:
        raise
    except ApiError as e:
        # Pre-send step: definitely not delivered via sendmessage.
        raise RetryableError(f"getuploadurl failed: {e}")
    data = resp.get("data") if isinstance(resp.get("data"), dict) else resp
    upload_full_url = data.get("upload_full_url") or resp.get("upload_full_url")
    upload_param = data.get("upload_param") or resp.get("upload_param")
    returned_filekey = data.get("filekey") or resp.get("filekey") or filekey
    if not upload_full_url and upload_param:
        # Best-effort construction per spec; if upload_param is not a URL,
        # fail strictly rather than claiming success.
        if isinstance(upload_param, str) and upload_param.startswith("http"):
            upload_full_url = upload_param
            if "filekey" not in upload_full_url:
                sep = "&" if "?" in upload_full_url else "?"
                upload_full_url = f"{upload_full_url}{sep}filekey={urllib.parse.quote(str(returned_filekey))}"
        elif isinstance(upload_param, dict) and upload_param.get("url"):
            upload_full_url = upload_param["url"]
    if not upload_full_url:
        raise RetryableError("getuploadurl: no upload_full_url and no usable upload_param")
    # Upload ciphertext, strict: HTTP 200 AND x-encrypted-param non-empty.
    encrypt_param = None
    if shutil.which("curl"):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(ciphertext)
            tmp_path = tf.name
        hdr_path = tmp_path + ".hdr"
        try:
            cmd = ["curl", "-s", "--max-time", "30", "--max-filesize", "26214400", "-D", hdr_path, "-o", "/dev/null",
                   "-w", "%{http_code}", "-X", "POST",
                   "-H", "Content-Type: application/octet-stream",
                   "--data-binary", f"@{tmp_path}", upload_full_url]
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=40)
            except subprocess.TimeoutExpired:
                # v1.0.2: TimeoutExpired str() contains the full
                # upload_full_url. Convert at the call site to a
                # parameter-free RetryableError; upload is pre-send,
                # so a retry (up to the drain limit) is safe.
                raise RetryableError("upload curl timeout") from None
            except Exception as e:
                raise RetryableError(f"upload curl failed: {type(e).__name__}") from None
            http_code = (r.stdout or b"").decode().strip()
            try:
                with open(hdr_path, encoding="utf-8", errors="ignore") as hf:
                    for line in hf:
                        if line.lower().startswith("x-encrypted-param:"):
                            encrypt_param = line.split(":", 1)[1].strip()
            except OSError:
                pass
            if http_code != "200" or not encrypt_param:
                raise RetryableError(f"upload failed: http={http_code} x-encrypted-param={'present' if encrypt_param else 'missing'}")
        finally:
            for p in (tmp_path, hdr_path):
                try:
                    os.remove(p)
                except OSError:
                    pass
    else:
        req = urllib.request.Request(upload_full_url, data=ciphertext,
                                     headers={"Content-Type": "application/octet-stream"},
                                     method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                if r.status != 200:
                    raise RetryableError(f"upload http={r.status}")
                encrypt_param = r.headers.get("x-encrypted-param")
        except RetryableError:
            raise
        except Exception as e:
            # v1.0.2: urllib error text can embed the upload URL.
            raise RetryableError(f"upload failed: {type(e).__name__}") from None
        if not encrypt_param:
            raise RetryableError("upload failed: x-encrypted-param missing")
    aes_key_b64 = base64.b64encode(aeskey_hex.encode("utf-8")).decode()
    # Fix 2026-10-28 per ChatGPT diagnosis: encrypt_query_param and
    # encrypt_type belong inside image_item.media, not at image_item
    # level. Gateway only checks the outer envelope to return
    # message_id, then drops top-level fields when mapping to Protobuf,
    # so the client got media with only aes_key, no download params,
    # and silently dropped the image. mid_size stays at image_item level.
    msg_payload = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id or session.get("ilink_user_id"),
            "client_id": client_id or uuid.uuid4().hex,
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token if context_token is not None else (session.get("context_token") or ""),
            "item_list": [{"type": 2, "image_item": {
                "mid_size": filesize,
                "media": {
                    "encrypt_query_param": encrypt_param.strip() if encrypt_param else encrypt_param,
                    "aes_key": aes_key_b64,
                    "encrypt_type": 1,
                },
            }}],
        },
        "base_info": base_info(),
    }
    try:
        resp2 = post_json(base, "/ilink/bot/sendmessage", msg_payload,
                          session=session, authenticated=True, timeout=15, allow_fallback=False)
    except TokenExpired:
        raise
    except Exception as e:
        # Final send: request may already have reached the server.
        raise UnknownDeliveryError(f"sendmessage-image post failed, result unknown: {e}")
    try:
        check_ret(resp2, "sendmessage-image")
    except TokenExpired:
        raise
    except Exception as e:
        # Server explicitly responded with an error: definitely not delivered.
        raise PermanentError(f"sendmessage-image rejected: {e}")
    return resp2


import re as _re_local_query

_LOCAL_QUERY_RE = _re_local_query.compile(r"^本地问\s+(\d+)(?:\s*[:：]\s*(.*))?$")
_LOCAL_DELETE_RE = _re_local_query.compile(r"^本地删\s+(\d+)\s*$")
_LOCAL_KEEP_RE = _re_local_query.compile(r"^本地留\s+(\d+)\s+(\d+)\s*天?\s*$")


def parse_local_query(text):
    """M3 local follow-up v1: parse '本地问 <message_id>[:问题]'.
    Returns (video_message_id, question) or None. The question text is
    NOT used for free-form answering in v1: v1 always returns a
    deterministic evidence summary, because there is no local LLM and
    a Worker answer would send the content to Muse, breaking
    local_only isolation. The question is kept only for logging."""
    if not text:
        return None
    m = _LOCAL_QUERY_RE.match(text.strip())
    if not m:
        return None
    return m.group(1), (m.group(2) or "").strip()


def find_local_video(video_message_id):
    """Return the vault video path for a local video, or None.
    Only digits are accepted as an id, and the path must stay inside
    local_vault/<id>/ and be a video_*.mp4 file. No other directory
    is ever read by a local query."""
    if not video_message_id or not str(video_message_id).isdigit():
        return None
    if is_local_deleted(video_message_id):
        return None
    vault = os.path.join(LOCAL_VAULT_DIR, str(video_message_id))
    if not os.path.isdir(vault):
        return None
    for name in sorted(os.listdir(vault)):
        if name.startswith("video_") and name.endswith(".mp4"):
            path = os.path.join(vault, name)
            if os.path.isfile(path):
                return path
    return None


def build_local_evidence_summary(evidence_path, video_message_id):
    """Deterministic evidence summary from an m2 evidence.json.
    No narrative, no speaker identity, no invented content: only
    duration, completed/uninspected tracks, OCR lines, ASR segments,
    high Speech/Music windows, and conflicts, capped for WeChat."""
    try:
        ev = json.load(open(evidence_path, encoding="utf-8"))
    except Exception:
        return None
    lines = [f"本地视频 {video_message_id} 的证据摘要："]
    if ev.get("duration_s") is not None:
        lines.append(f"时长约 {float(ev['duration_s']):.1f} 秒。")
    comp = ev.get("completed_tracks") or []
    lines.append("已完成轨道：" + ("、".join(comp) if comp else "无") + "。")
    un = ev.get("uninspected_ranges") or []
    if un:
        lines.append("未检查区间：" + "；".join(f"{u.get('track')}:{u.get('reason')}" for u in un[:5]) + "。")
    conf = ev.get("conflicts") or []
    if conf:
        lines.append(f"冲突 {len(conf)} 条，见证据文件。")
    ocr = (ev.get("stages") or {}).get("ocr") or {}
    ocr_lines = []
    for fr in ocr.get("ocr_frames") or []:
        for ln in fr.get("lines") or []:
            ocr_lines.append(f"{fr.get('t_s')}s:{ln}")
    if ocr_lines:
        lines.append("画面文字（OCR）：" + "；".join(ocr_lines[:20]) + (f"（共 {len(ocr_lines)} 条）" if len(ocr_lines) > 20 else ""))
    else:
        lines.append("画面文字（OCR）：无。")
    asr = (ev.get("stages") or {}).get("asr") or {}
    segs = asr.get("segments") or []
    if segs:
        lines.append("转写：" + "；".join(f"{seg.get('start_s')}-{seg.get('end_s')}s:{seg.get('text')}" for seg in segs[:20]))
    else:
        lines.append("转写：无。")
    sound = (ev.get("stages") or {}).get("sound") or {}
    hi = []
    for row in sound.get("rows") or []:
        if (row.get("Speech") or 0) >= 0.5 or (row.get("Music") or 0) >= 0.5:
            hi.append(f"{row.get('start_s')}-{row.get('end_s')}s:{row.get('top1')}")
    if hi:
        lines.append("声音高分窗口：" + "；".join(hi[:20]) + "。")
    text = "\n".join(lines)
    return text[:1500]


def load_pending_local_queries():
    try:
        data = json.load(open(PENDING_LOCAL_QUERIES_FILE, encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_pending_local_queries(items):
    try:
        os.makedirs(STATE, exist_ok=True)
        tmp = PENDING_LOCAL_QUERIES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)
        os.chmod(tmp, 0o600)
        os.replace(tmp, PENDING_LOCAL_QUERIES_FILE)
        return True
    except Exception:
        return False


DEFAULT_ANALYZER_PY = "/home/hatch/workspace/goals/muse-wechat-bridge/hidden_files/m0-component/venv/bin/python"
ANALYZER_RUNS_FILE = _PATHS.analyzer_runs_file


def _resolve_candidate(candidate):
    """Resolve one interpreter candidate to an executable path, or None.

    A value containing a separator is a path (relative ones resolve
    under ROOT); a bare name goes through shutil.which. Existence alone
    is NOT enough: the candidate must actually execute `python -c`
    successfully within 5 s. This is the item-1 fix for the old bug
    where a merely-existing path was trusted and a bad value was then
    re-read by the analyzer child through the inherited environment."""
    import shutil
    if not candidate:
        return None
    if os.sep in candidate:
        path = candidate if os.path.isabs(candidate) else os.path.join(ROOT, candidate)
    else:
        path = shutil.which(candidate)
    if not path:
        return None
    try:
        r = subprocess.run([path, "-c", "import sys; print(sys.executable)"],
                           capture_output=True, timeout=5)
        return path if r.returncode == 0 else None
    except Exception:
        return None


def _resolve_analyzer_runtime():
    """Validated analyzer interpreter selection (item 1, 2026-10-31).

    Order: non-empty explicit config -> default venv -> bridge's own
    interpreter. Each candidate must actually execute; the first usable
    one wins. Returns (path, reason); path is None when nothing runs.
    The selected path is passed to the analyzer child via the
    environment, so the child can never re-read a stale invalid value,
    and stages run under the analyzer's own sys.executable."""
    candidates = []
    explicit = os.environ.get("WECHAT_BRIDGE_ANALYZER_PY") or ""
    if explicit:
        candidates.append((explicit, "explicit"))
    candidates.append((DEFAULT_ANALYZER_PY, "default_venv"))
    candidates.append((sys.executable, "bridge_python"))
    seen = set()
    for cand, reason in candidates:
        if cand in seen:
            continue
        seen.add(cand)
        resolved = _resolve_candidate(cand)
        if resolved:
            return resolved, reason
    return None, "no_usable_runtime"


def _resolve_analyzer_py():
    """Backward-compatible wrapper: validated selected path, or None."""
    path, _reason = _resolve_analyzer_runtime()
    return path


def _load_analyzer_runs():
    try:
        with open(ANALYZER_RUNS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_analyzer_runs(runs):
    try:
        atomic_write_json(ANALYZER_RUNS_FILE, runs, mode=0o600)
    except Exception as e:
        print(f"analyzer runs registry write failed: {_sanitize_error(e)}", flush=True)


def _analyzer_run_alive(entry):
    """Identity-checked liveness: pid alive AND boot_id AND
    proc_starttime match, and not a zombie. A PID reused by an
    unrelated process must never be treated as our run, and a zombie
    (kill(pid, 0) still succeeds on it) must not read as alive."""
    pid = entry.get("pid")
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            stat = f.read()
        if stat.rsplit(")", 1)[1].split()[0] == "Z":
            return False
    except Exception:
        return False
    if entry.get("boot_id") != _boot_id():
        return False
    try:
        if _proc_starttime(pid) != entry.get("proc_starttime"):
            return False
    except Exception:
        return False
    return True


def _find_live_run_by_outdir(outdir):
    """Live registered run for an outdir, or None. Stale entries for
    that outdir are purged so a later launch is not blocked by a dead
    run."""
    runs = _load_analyzer_runs()
    found = None
    changed = False
    for rid, entry in list(runs.items()):
        if entry.get("outdir") == outdir:
            if _analyzer_run_alive(entry):
                found = (rid, entry)
            else:
                del runs[rid]
                changed = True
    if changed:
        _save_analyzer_runs(runs)
    return found


def _register_analyzer_run(run_id, outdir, video_key, pid, python):
    runs = _load_analyzer_runs()
    runs[run_id] = {"outdir": outdir, "video_key": video_key, "pid": pid,
                    "python": python, "boot_id": _boot_id(),
                    "proc_starttime": _proc_starttime(pid),
                    "started_ts": time.time()}
    _save_analyzer_runs(runs)


def _kill_analyzer_runs(video_key=None, outdir=None):
    """Stop analyzer trees for a deleted video. Identity-checked; a PID
    that fails the check is never killed. Returns killed run_ids."""
    import signal
    runs = _load_analyzer_runs()
    killed = []
    changed = False
    for rid, entry in list(runs.items()):
        match = (video_key is not None and entry.get("video_key") == video_key) or \
                (outdir is not None and entry.get("outdir") == outdir)
        if not match:
            continue
        pid = entry.get("pid")
        if pid and _analyzer_run_alive(entry):
            try:
                os.killpg(pid, signal.SIGTERM)
            except Exception:
                pass
            for _ in range(5):
                time.sleep(0.1)
                if not _analyzer_run_alive(entry):
                    break
            else:
                try:
                    os.killpg(pid, signal.SIGKILL)
                except Exception:
                    pass
            killed.append(rid)
        del runs[rid]
        changed = True
    if changed:
        _save_analyzer_runs(runs)
    return killed


def _launch_analyzer(video, outdir, video_key, stdout, stderr):
    """Unified analyzer launcher (item 1). Dedupe by outdir: a live run
    for the same outdir is reused, never duplicated. The selected
    interpreter is validated by execution and passed to the child via
    the environment, so stages (which use the analyzer's own
    sys.executable) can never re-read a stale invalid value.
    Returns {"outdir", "run_id", "python", "reason"} or
    {"status": "runtime_unavailable"/"launch_failed", "reason"}."""
    live = _find_live_run_by_outdir(outdir)
    if live:
        rid, entry = live
        return {"outdir": outdir, "run_id": rid, "python": entry.get("python"),
                "reason": "reused_live_run"}
    selected, reason = _resolve_analyzer_runtime()
    if not selected:
        print("analyzer: no usable runtime, refusing launch", flush=True)
        return {"status": "runtime_unavailable", "reason": reason}
    run_id = "run-%d-%s" % (time.time_ns(), uuid.uuid4().hex[:8])
    env = dict(os.environ)
    env["WECHAT_BRIDGE_ANALYZER_PY"] = selected
    analyzer_script = os.path.join(ROOT, "analyzer", "m2_analyzer.py")
    try:
        proc = subprocess.Popen([selected, analyzer_script, video, outdir,
                                 "--run-id", run_id],
                                stdout=stdout, stderr=stderr,
                                stdin=subprocess.DEVNULL,
                                env=env,
                                start_new_session=True, close_fds=True)
    except Exception as e:
        print(f"analyzer: launch failed: {_sanitize_error(e)}", flush=True)
        return {"status": "launch_failed", "reason": type(e).__name__}
    _register_analyzer_run(run_id, outdir, video_key, proc.pid, selected)
    return {"outdir": outdir, "run_id": run_id, "python": selected, "reason": reason}


def start_local_analysis(video_path, video_message_id):
    """Launch the M2 analyzer in the background, outdir INSIDE the
    vault (never analysis/). Returns {"outdir", "run_id"} or None.
    The caller must NOT block serve polling on this process.
    Item 1 (2026-10-31): unified launcher shared with
    queue_video_analysis; a live run for the same outdir is reused
    instead of spawning a duplicate that would race on evidence.json.
    Returns None when no usable runtime exists or launch fails."""
    outdir = os.path.join(LOCAL_VAULT_DIR, str(video_message_id), "analysis")
    try:
        os.makedirs(outdir, exist_ok=True)
    except Exception:
        return None
    launched = _launch_analyzer(video_path, outdir, str(video_message_id),
                                subprocess.DEVNULL, subprocess.DEVNULL)
    if launched.get("status"):
        return None
    return {"outdir": outdir, "run_id": launched["run_id"]}


def _read_analyzer_state(outdir, expected_run_id):
    """Terminal state of an analyzer run, or None when not terminal.

    Returns "ready" (completed/partial evidence for the expected run),
    "failed" (failed/interrupted for the expected run), or None (still
    running, terminal for a different run, unreadable, or a legacy run
    with no state.json). Legacy runs return None so callers keep the
    old evidence-existence behavior."""
    try:
        with open(os.path.join(outdir, "state.json"), encoding="utf-8") as f:
            st = json.load(f)
    except Exception:
        return None
    if not isinstance(st, dict):
        return None
    if expected_run_id and st.get("run_id") and st.get("run_id") != expected_run_id:
        return None
    if st.get("state") in ("completed", "partial"):
        return "ready"
    if st.get("state") in ("failed", "interrupted"):
        return "failed"
    return None


def move_videos_to_local_vault(message_id, saved_videos):
    """M3 v1 local_only move: move saved videos from media/inbound into
    local_vault/<message_id>/ (dir 0700, files 0600). Returns (moved, error).
    On any failure, the caller must NOT claim local_only: already-moved
    files stay in the vault, unmoved files stay in media/inbound, and the
    caller falls back to an explicit failure prompt instead of queuing
    content. This helper only moves files; it never reads video content
    and never writes any path into an inbox record."""
    vault = os.path.join(LOCAL_VAULT_DIR, str(message_id))
    try:
        os.makedirs(vault, exist_ok=True)
        os.chmod(vault, 0o700)
    except Exception as e:
        return 0, type(e).__name__
    moved = 0
    for idx, saved in enumerate(saved_videos or []):
        path = saved.get("path") if isinstance(saved, dict) else None
        if not path or not _is_allowed_image_path(path):
            return moved, "PATH_NOT_ALLOWED"
        dest = os.path.join(vault, f"video_{idx}.mp4")
        try:
            os.replace(path, dest)
            os.chmod(dest, 0o600)
            moved += 1
        except Exception as e:
            return moved, type(e).__name__
    if moved:
        # TTL registration is best-effort here: if this write fails,
        # the sweep backfills from the vault dir mtime, so the video
        # is never left without an expiry.
        register_local_ttl(message_id)
    return moved, None


def _load_json_dict(path):
    # Gate 3 step 1: safe dict read lives in bridge_storage.py.
    # Observable behavior is unchanged (parsed dict, else {} on any
    # failure); the only difference is the file handle is now closed
    # instead of leaked.
    return bridge_storage.read_json_dict(path)


def _save_json_dict_atomic(path, data):
    try:
        os.makedirs(STATE, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        return True
    except Exception:
        return False


def load_ttl_index():
    return _load_json_dict(TTL_INDEX_FILE)


def save_ttl_index(data):
    return _save_json_dict_atomic(TTL_INDEX_FILE, data)


def load_tombstones():
    return _load_json_dict(TOMBSTONE_FILE)


def save_tombstones(data):
    return _save_json_dict_atomic(TOMBSTONE_FILE, data)


def is_local_deleted(video_message_id):
    """Tombstone check: the single anti-resurrection gate. Every read,
    query, analysis-start and summary-delivery path must call this
    before touching vault files. A tombstoned id is never usable again,
    even if a file with that id somehow reappears on disk."""
    if not video_message_id or not str(video_message_id).isdigit():
        return False
    return str(video_message_id) in load_tombstones()


def register_local_ttl(video_message_id, received_ts=None):
    """Register a local video's 48h expiry. Never resets an existing
    entry (an extension is a separate explicit command), and never
    registers a tombstoned id. Returns True if an entry now exists."""
    if not video_message_id or not str(video_message_id).isdigit():
        return False
    mid = str(video_message_id)
    if mid in load_tombstones():
        return False
    idx = load_ttl_index()
    if mid not in idx:
        now = float(received_ts) if received_ts else time.time()
        idx[mid] = {"received_ts": now, "expires_ts": now + LOCAL_TTL_SECONDS, "status": "active"}
        save_ttl_index(idx)
    return True


def delete_local_video_files(video_message_id):
    """Remove the whole vault dir for one id (video + analysis).
    Returns True if the dir is gone afterwards."""
    vault = os.path.join(LOCAL_VAULT_DIR, str(video_message_id))
    try:
        if os.path.isdir(vault):
            shutil.rmtree(vault)
    except Exception:
        pass
    return not os.path.exists(vault)


def mark_local_deleted(video_message_id, reason):
    """Delete order (anti-resurrection): tombstone FIRST, then files.
    A concurrent reader that checks the tombstone refuses even in the
    brief window before the files are gone. Returns True if the
    tombstone is recorded and the vault dir is gone."""
    if not video_message_id or not str(video_message_id).isdigit():
        return False
    mid = str(video_message_id)
    tombs = load_tombstones()
    tombs[mid] = {"deleted_ts": time.time(), "reason": reason}
    save_tombstones(tombs)
    idx = load_ttl_index()
    if mid in idx:
        idx[mid]["status"] = "deleted"
        save_ttl_index(idx)
    # Item 1: tombstone first (above), then stop the analyzer tree so
    # it cannot write back into the deleted vault dir, then delete.
    _kill_analyzer_runs(video_key=mid)
    return delete_local_video_files(mid)


def extend_local_ttl(video_message_id, days):
    """Explicit extension only: new expiry = now + days. Returns
    (ok, reason) where reason is 'deleted' / 'not_found' / 'range'."""
    if not video_message_id or not str(video_message_id).isdigit():
        return False, "not_found"
    mid = str(video_message_id)
    if mid in load_tombstones():
        return False, "deleted"
    if not (1 <= int(days) <= 30):
        return False, "range"
    if find_local_video(mid) is None:
        return False, "not_found"
    idx = load_ttl_index()
    now = time.time()
    idx[mid] = {"received_ts": idx.get(mid, {}).get("received_ts", now),
                "expires_ts": now + int(days) * 86400, "status": "active"}
    save_ttl_index(idx)
    return True, "ok"


def sweep_local_ttl(now=None):
    """One sweep round, idempotent, called every serve poll loop:
    (1) backfill ttl entries for vault dirs with no entry, using the
    dir mtime as the receipt approximation (covers videos saved
    before this feature, and any register write that failed);
    (2) expire entries past 48h: tombstone first, then delete files;
    (3) re-delete any vault dir that reappeared under a tombstoned
    id (e.g. an analyzer still running when the sweep ran);
    (4) R2 P0: tombstones are NEVER purged. The file holds only
    a minimal id set (deleted_ts + reason, no content), so keeping
    it forever costs nothing and guarantees a deleted id can never
    age back into 'unknown' and regain an inline-text fallback.
    Returns the list of message_ids newly expired this round.
    Pending queries are NOT touched here: the delivery loop sees the
    tombstone and sends the 'deleted' notice itself, so a notice is
    never lost by a sweep that runs between analysis and delivery."""
    now = float(now) if now else time.time()
    changed_idx = False
    idx = load_ttl_index()
    tombs = load_tombstones()
    try:
        vault_ids = [d for d in os.listdir(LOCAL_VAULT_DIR)
                     if d.isdigit() and os.path.isdir(os.path.join(LOCAL_VAULT_DIR, d))]
    except Exception:
        vault_ids = []
    for mid in vault_ids:
        if mid not in idx and mid not in tombs:
            try:
                mtime = os.path.getmtime(os.path.join(LOCAL_VAULT_DIR, mid))
            except Exception:
                mtime = now
            idx[mid] = {"received_ts": mtime, "expires_ts": mtime + LOCAL_TTL_SECONDS, "status": "active"}
            changed_idx = True
    expired = []
    for mid, entry in list(idx.items()):
        if entry.get("status") == "active" and now >= float(entry.get("expires_ts") or 0):
            tombs[mid] = {"deleted_ts": now, "reason": "expired"}
            idx[mid]["status"] = "deleted"
            changed_idx = True
            expired.append(mid)
    if expired or changed_idx:
        save_tombstones(tombs)
        save_ttl_index(idx)
    for mid in list(tombs.keys()):
        if os.path.isdir(os.path.join(LOCAL_VAULT_DIR, mid)):
            _kill_analyzer_runs(video_key=mid)
            delete_local_video_files(mid)
    # R2 P0: no tombstone purge (see docstring step 4).
    return expired


def parse_local_delete(text):
    if not text:
        return None
    m = _LOCAL_DELETE_RE.match(text.strip())
    return m.group(1) if m else None


def parse_local_keep(text):
    if not text:
        return None
    m = _LOCAL_KEEP_RE.match(text.strip())
    return (m.group(1), int(m.group(2))) if m else None


SAFE_VIEW_FILE = _PATHS.safe_view_file
SAFE_VIEW_VERSION = "safe-view-v1"
SAFE_VIEW_MAX_ROUNDS = 10

_SAFE_VIEW_STATUS_SUFFIXES = (
    "_queue.json", "_fast.json", "_local.json", "_local_answer.json",
    "_local_del.json", "_local_keep.json", "_local_fail.json",
    "_local_deleted.json", "_voice_prompt.json",
)


def classify_outbox_name(name):
    if name.endswith("_queue.json"):
        return "queue_receipt"
    if any(name.endswith(s) for s in _SAFE_VIEW_STATUS_SUFFIXES):
        return "status_reply"
    return "formal_answer"


def guard_local_path_for_muse(path):
    if not path:
        return False
    try:
        real = os.path.realpath(path)
        vault = os.path.realpath(LOCAL_VAULT_DIR)
        if real == vault or real.startswith(vault + os.sep):
            return False
    except Exception:
        return False
    return True


def build_safe_history_view(max_rounds=SAFE_VIEW_MAX_ROUNDS):
    tombs = load_tombstones()
    rounds = []
    local_placeholders = 0
    try:
        done_names = sorted(n for n in os.listdir(DONE) if n.endswith(".json"))
    except Exception:
        done_names = []
    for name in done_names:
        mid = name[:-5]
        try:
            rec = json.load(open(os.path.join(DONE, name), encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rec, dict):
            continue
        if rec.get("local_only") or mid in tombs:
            local_placeholders += 1
            rounds.append({
                "message_id": mid,
                "user": {"history_class": "user", "text": "【仅本地视频】" if mid not in tombs else "【本地视频已删除】", "placeholder": True},
                "answer": {"history_class": "formal_answer", "text": "【仅本地，内容不进入历史】", "placeholder": True},
            })
            continue
        user_text = rec.get("text") or ""
        if not user_text:
            if rec.get("images"):
                user_text = "附图片 %d 张" % len(rec["images"])
            elif rec.get("source") == "voice":
                user_text = "【语音】"
            elif rec.get("videos"):
                user_text = "附视频 %d 段" % len(rec["videos"])
            elif rec.get("files"):
                user_text = "附文件 %d 个" % len(rec["files"])
        answer_text = None
        answer_extra = {}
        try:
            sent = json.load(open(os.path.join(OUTBOX_SENT, "%s.json" % mid), encoding="utf-8"))
            if isinstance(sent, dict):
                answer_text = sent.get("text")
                # Item 3: an all-sent plan job really did deliver the
                # full text (in labelled parts); say so explicitly
                # so the Worker does not have to guess.
                if _text_plan(sent) is not None:
                    answer_extra = {
                        "delivery_status": "all_parts_sent",
                        "text_parts": "%d/%d sent" % (
                            sum(1 for p in sent["text_parts"] if p.get("status") == "sent"),
                            len(sent["text_parts"])),
                    }
        except Exception:
            answer_text = None
        if answer_text is None:
            # Item 3 partial delivery: a plan job in failed/ or
            # pending/ with some parts sent and some failed /
            # needs_check / blocked must NEVER be shown as if the
            # user had read the full answer. Show only the delivery
            # status and the part numbers to check, never the full
            # top-level text (which the user has not fully received).
            for _dir in (OUTBOX_FAILED, OUTBOX_PENDING):
                try:
                    _pj = json.load(open(os.path.join(_dir, "%s.json" % mid), encoding="utf-8"))
                except Exception:
                    continue
                _plan = _text_plan(_pj) if isinstance(_pj, dict) else None
                if _plan is None:
                    continue
                _sent_n = [p["index"] + 1 for p in _plan if p.get("status") == "sent"]
                _bad_n = [p["index"] + 1 for p in _plan if p.get("status") in ("failed", "needs_check")]
                _blocked_n = [p["index"] + 1 for p in _plan if p.get("status") == "blocked_by_prior"]
                if _sent_n or _bad_n:
                    answer_text = ("【长文部分送达：第 %s 段已送达，第 %s 段待核对，第 %s 段未发送；"
                                   "请勿视为用户已读完整答案】" % (
                                       "/".join(str(n) for n in _sent_n) if _sent_n else "无",
                                       "/".join(str(n) for n in _bad_n) if _bad_n else "无",
                                       "/".join(str(n) for n in _blocked_n) if _blocked_n else "无"))
                    answer_extra = {
                        "delivery_status": "partial",
                        "text_parts_sent": _sent_n,
                        "text_parts_needs_check": _bad_n,
                        "text_parts_blocked": _blocked_n,
                    }
                break
        _answer = None
        if answer_text:
            _answer = {"history_class": "formal_answer", "text": answer_text}
            _answer.update(answer_extra)
        rounds.append({
            "message_id": mid,
            "user": {"history_class": "user", "text": user_text},
            "answer": _answer,
        })
    rounds = rounds[-int(max_rounds):] if max_rounds else rounds
    queue_count = status_count = 0
    try:
        for n in os.listdir(OUTBOX_SENT):
            if not n.endswith(".json"):
                continue
            cls = classify_outbox_name(n)
            if cls == "queue_receipt":
                queue_count += 1
            elif cls == "status_reply":
                status_count += 1
    except Exception:
        pass
    try:
        failure_ids = sorted(n[:-5] for n in os.listdir(OUTBOX_FAILED) if n.endswith(".json"))
    except Exception:
        failure_ids = []
    return {
        "safe_view_version": SAFE_VIEW_VERSION,
        "max_rounds": int(max_rounds),
        "rounds": rounds,
        "rounds_counted": len(rounds),
        "excluded": {
            "queue_receipts_total": queue_count,
            "status_replies_total": status_count,
            "failure_prompt_ids": failure_ids,
            "local_placeholders_in_rounds": local_placeholders,
        },
        "egress_policy": "safe_view_only",
    }


def refresh_safe_history_view():
    try:
        view = build_safe_history_view()
        try:
            old = json.load(open(SAFE_VIEW_FILE, encoding="utf-8"))
            if (isinstance(old, dict) and old.get("rounds") == view["rounds"]
                    and old.get("excluded") == view["excluded"]):
                return old
        except Exception:
            pass
        view["generated_ts"] = time.time()
        if _save_json_dict_atomic(SAFE_VIEW_FILE, view):
            return view
        return None
    except Exception:
        return None


def read_local_only_mode():
    """M3 v1 toggle: True iff state/local_only_mode.json says enabled.
    Read fresh on every video so a toggle text takes effect for the next
    video without restarting serve. Missing/corrupt file means off."""
    try:
        with open(LOCAL_ONLY_MODE_FILE, "r", encoding="utf-8") as f:
            return bool(json.load(f).get("enabled"))
    except Exception:
        return False


def set_local_only_mode(enabled):
    """Atomic toggle write (tmp + replace). Returns True on success."""
    try:
        os.makedirs(STATE, exist_ok=True)
        tmp = LOCAL_ONLY_MODE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"enabled": bool(enabled)}, f)
        os.replace(tmp, LOCAL_ONLY_MODE_FILE)
        return True
    except Exception:
        return False


def queue_video_analysis(message_id, saved_videos):
    """M2 integration v1: spawn the analyzer in the background, never block serve.
    Only paths already saved by process_inbound_video (under media/) are accepted.
    The analyzer writes only to analysis/<message_id>_<index>/ (evidence.json,
    frames/, stage jsons, analyzer.log); it never writes inbox/done/outbox.
    The inbox record only carries the outdir path + queued status, so the
    Worker can read evidence.json when it exists; a Worker reply may still
    arrive before analysis finishes, which is a known v1 limitation, not a
    guarantee of evidence-before-reply."""
    analyses = []
    analyzer = os.path.join(ROOT, "analyzer", "m2_analyzer.py")
    if not os.path.exists(analyzer):
        return analyses
    for idx, saved in enumerate(saved_videos or []):
        path = saved.get("path") if isinstance(saved, dict) else None
        if not path or not _is_allowed_image_path(path):
            continue
        if not guard_local_path_for_muse(path) or is_local_deleted(message_id):
            continue
        outdir = os.path.join(ANALYSIS_DIR, f"{message_id}_{idx}")
        try:
            os.makedirs(outdir, exist_ok=True)
            log_path = os.path.join(outdir, "analyzer.log")
            logf = open(log_path, "ab")
        except Exception as e:
            analyses.append({"video_index": idx, "outdir": outdir, "status": "queue_failed",
                             "error": type(e).__name__})
            continue
        try:
            # Item 1: unified launcher. A missing runtime is a visible
            # runtime_unavailable entry, never a silent skip.
            launched = _launch_analyzer(path, outdir, f"{message_id}_{idx}",
                                        logf, subprocess.STDOUT)
        finally:
            logf.close()
        if launched.get("status") == "runtime_unavailable":
            analyses.append({"video_index": idx, "outdir": outdir,
                             "status": "runtime_unavailable",
                             "reason": launched.get("reason")})
        elif launched.get("status"):
            analyses.append({"video_index": idx, "outdir": outdir, "status": "queue_failed",
                             "error": launched.get("reason")})
        else:
            analyses.append({"video_index": idx, "outdir": outdir, "status": "queued",
                             "run_id": launched["run_id"],
                             "python": launched["python"]})
    return analyses


# ------------------------------------------------------------------
# Item 3 (2026-10-31): long-text segmentation.
#
# A long answer is NEVER sliced inside send_text(). Instead drain_outbox
# builds a plan first, persists it in the SAME outbox job file, and
# then sends only the plan's display_text parts, in order:
#   text_plan_version = 1
#   text_parts[] = {index, raw_slice, display_text, sha256, client_id,
#                   status, attempts, error_code}
#   full_text_sha256 = sha256 of the whole top-level text
# The top-level 'text' field keeps the full text for history reading,
# but the new sender never sends it directly once a plan exists.
#   display budget = 1800 characters, counting the part label
#   "[i/N]" AND any fence close/reopen lines added for that part.
#   max parts = 20; more than that is rejected BEFORE any send, with
#   a prompt to deliver as a file instead (see drain_outbox).
# Ordering / failure rules (the whole point of the plan):
#   - a part is persisted as 'sending' before its network call, and
#     as 'sent' immediately after success. A post-send persist failure
#     HALTs the job in this process (see drain_outbox); on restart,
#     recover_sending_on_startup turns a residual 'sending' part into
#     'needs_check' (see there).
#   - an unknown or failed part stops everything behind it: later
#     parts and all attachments become 'blocked_by_prior' and the job
#     goes to failed/ for manual check. Sent parts are never resent.
#   - a retryable part keeps the job in pending/ (up to 3 attempts);
#     later parts and attachments wait until it succeeds.
#   - attachments are sent only after EVERY text part is sent.
# Manual resend (no reslicing, sent parts untouched):
#   python3 bridge.py resend-text-part <job-file> <1-based part number>
#   It only resets that one part to 'pending' (and clears the job's
#   terminal flag) and moves the job back to pending/. Parts that
#   were 'blocked_by_prior' stay blocked until every part before
#   them is sent; drain_outbox then resumes them in order. That is
#   the only way a blocked tail restarts: the operator never has to
#   (and must not) rewrite the other parts. The command also sets
#   manual_resend_part, which drain_outbox uses to drop the job
#   from the long-running serve's in-memory terminal set -- without
#   that, the serve that archived the job would re-archive the
#   resend without sending it.
# Rollback warning: an OLD sender (before this item) does not know
# text_parts and will send the top-level full text once. Before any
# rollback, isolate every new-format pending/failed job first; never
# let an old serve drain them.
# ------------------------------------------------------------------
TEXT_DISPLAY_BUDGET = 1800
TEXT_PLAN_VERSION = 1
TEXT_PLAN_MAX_PARTS = 20
# Max label length for N <= 20: "[20/20]\n" is 8 chars. Budgeting
# with this maximum (instead of the actual "[i/N]") guarantees every
# display_text fits the budget without iterating to a stable N.
_TEXT_MAX_LABEL_LEN = len("[20/20]\n")


class LongTextTooManyParts(Exception):
    """Segmentation would need more than TEXT_PLAN_MAX_PARTS parts."""


def _split_keepends_nl(text):
    """Split on '\\n' only, keeping each line break. Joining the
    result reproduces the input exactly (unlike splitlines, which
    also splits on \\v/\\f/\\x85/\\u2028 etc.)."""
    lines = []
    start = 0
    while start < len(text):
        idx = text.find("\n", start)
        if idx == -1:
            lines.append(text[start:])
            break
        lines.append(text[start:idx + 1])
        start = idx + 1
    return lines


_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})[ \t]*(.*?)[ \t]*\r?$")


def _fence_match(line):
    """Return (char, run_len, info) for a fence delimiter line, else
    None. The trailing newline is stripped before matching; a line
    is only a delimiter if it is made of backticks or tildes."""
    stripped = line.rstrip("\r\n")
    m = _FENCE_RE.match(stripped)
    if not m:
        return None
    run = m.group(1)
    return (run[0], len(run), m.group(2).strip())


def _update_fence(fence, line):
    """Advance fence state (None or (char, run_len, info)) past one
    whole line. A closing fence must use the same character, a run
    at least as long as the opening run, and no info string."""
    fm = _fence_match(line)
    if fm is None:
        return fence
    char, run_len, info = fm
    if fence is None:
        return (char, run_len, info)
    if char == fence[0] and run_len >= fence[1] and info == "":
        return None
    return fence


def _fence_line(fence):
    char, run_len, info = fence
    return char * run_len + (info if info else "") + "\n"


def _safe_char_cut(text, max_len):
    """Cut text to at most max_len characters without splitting a
    grapheme cluster: never leave a combining mark, variation
    selector, or ZWJ sequence's tail at the start of the next chunk.
    Falls back to the raw cut if backing off would make no progress
    (e.g. a run of combining marks), so callers always progress."""
    if len(text) <= max_len:
        return text
    cut = max_len
    while cut > 0 and (unicodedata.combining(text[cut]) != 0
                       or text[cut] in ("\u200d", "\ufe0e", "\ufe0f")):
        cut -= 1
    while cut > 0 and text[cut - 1] == "\u200d":
        cut -= 1
    if cut <= 0:
        return text[:max_len]
    return text[:cut]


def _segment_raw_slices(text):
    """Greedy line segmentation into raw slices (no labels yet).

    Cut preference, in order: paragraph boundary, then line
    boundary, then a Unicode-safe character cut. A paragraph
    boundary is a blank line; it is only preferred when cutting
    there still keeps at least 60% of the part's content budget
    (an implementation choice: the scheme names the preference
    order but no threshold, and cutting at a very early blank
    line would produce tiny parts). Overlong single lines are
    character-cut. Returns [(raw_slice, fence_at_start, fence_at_end)].
    Fence state is tracked line by line, so a part that starts or
    ends inside a fenced block knows it for display construction."""
    lines = _split_keepends_nl(text)
    slices = []
    fence = None
    i = 0
    while i < len(lines):
        part_start_fence = fence
        part_lines = []
        part_fence_states = []
        # Accumulate whole lines while the display fits.
        while i < len(lines):
            line = lines[i]
            new_fence = _update_fence(fence, line)
            reopen_len = len(_fence_line(part_start_fence)) if part_start_fence else 0
            tentative_raw = "".join(part_lines) + line
            close_len = 0
            if new_fence is not None:
                # Closing fence: run + newline only, no info string.
                close_len = new_fence[1] + 1
                if not tentative_raw.endswith("\n"):
                    close_len += 1
            display_len = _TEXT_MAX_LABEL_LEN + reopen_len + len(tentative_raw) + close_len
            if display_len > TEXT_DISPLAY_BUDGET and part_lines:
                break
            if display_len > TEXT_DISPLAY_BUDGET and not part_lines:
                # Single overlong line: character-cut it below, do
                # not consume it as a whole line.
                break
            part_lines.append(line)
            part_fence_states.append(new_fence)
            fence = new_fence
            i += 1
        if not part_lines:
            # Overlong single line (and not a fence delimiter line,
            # which is short by construction): character-cut a chunk.
            line = lines[i]
            reopen_len = len(_fence_line(part_start_fence)) if part_start_fence else 0
            close_len = 0
            if fence is not None:
                # Closing fence (run + newline) plus the newline
                # inserted before it, since a mid-line chunk never
                # ends with one.
                close_len = fence[1] + 2
            capacity = TEXT_DISPLAY_BUDGET - _TEXT_MAX_LABEL_LEN - reopen_len - close_len
            if capacity <= 0:
                capacity = 1
            chunk = _safe_char_cut(line, capacity)
            if not chunk:
                chunk = line[:1]
            lines[i] = line[len(chunk):]
            slices.append((chunk, part_start_fence, fence))
            continue
        # Paragraph preference: cut back to the last blank line in
        # the part, if that still keeps >= 60% of the content budget
        # and leaves lines for the next part.
        content_budget = TEXT_DISPLAY_BUDGET - _TEXT_MAX_LABEL_LEN
        kept = 0
        cut_at = None
        for j, ln in enumerate(part_lines):
            kept += len(ln)
            if ln.strip() == "" and j < len(part_lines) - 1:
                if kept >= 0.6 * content_budget:
                    cut_at = j
        if cut_at is not None:
            back = len(part_lines) - cut_at - 1
            i -= back
            fence = part_fence_states[cut_at]
            part_lines = part_lines[:cut_at + 1]
        slices.append(("".join(part_lines), part_start_fence, fence))
    return slices


def _segment_text(text):
    """Build a long-text plan. Returns (parts, full_text_sha256).
    Raises LongTextTooManyParts if more than 20 parts are needed.
    Every display_text is verified to fit TEXT_DISPLAY_BUDGET and
    every raw_slice concatenation is verified to restore the text."""
    raw_slices = _segment_raw_slices(text)
    if len(raw_slices) > TEXT_PLAN_MAX_PARTS:
        raise LongTextTooManyParts(
            "%d parts would exceed the %d-part maximum" % (len(raw_slices), TEXT_PLAN_MAX_PARTS))
    total = len(raw_slices)
    parts = []
    for idx, (raw, fence_start, fence_end) in enumerate(raw_slices):
        label = "[%d/%d]\n" % (idx + 1, total)
        reopen = _fence_line(fence_start) if fence_start else ""
        close = ""
        if fence_end is not None:
            # A closing fence carries NO info string (unlike the
            # reopen line above, which repeats the opening fence
            # including its info string, e.g. ```python).
            close = ("\n" if not raw.endswith("\n") else "") + fence_end[0] * fence_end[1] + "\n"
        display = label + reopen + raw + close
        if len(display) > TEXT_DISPLAY_BUDGET:
            raise ValueError("segment display exceeds budget for part %d" % (idx + 1))
        parts.append({
            "index": idx,
            "raw_slice": raw,
            "display_text": display,
            "sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
            "client_id": uuid.uuid4().hex,
            "status": "pending",
            "attempts": 0,
            "error_code": None,
        })
    if "".join(p["raw_slice"] for p in parts) != text:
        raise ValueError("segment raw slices do not restore the full text")
    return parts, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _text_plan(job):
    if job.get("text_plan_version") != TEXT_PLAN_VERSION:
        return None
    parts = job.get("text_parts")
    if not isinstance(parts, list) or not parts:
        return None
    return parts


def _plan_never_attempted(job):
    """True only if no text/attachment item has ever been attempted:
    the only case where an old long text may be upgraded to a plan
    in drain. Anything already sent/failed/needs_check, or with an
    attempt count, keeps the legacy single-send path."""
    if job.get("text_status") in ("sent", "failed", "needs_check"):
        return False
    if int(job.get("text_attempts") or 0) > 0:
        return False
    for att in _terminal_attachments(job):
        if isinstance(att, dict) and (att.get("status") in ("sent", "failed", "needs_check")
                                      or int(att.get("attempts") or 0) > 0):
            return False
    return True


def send_text(session, text, to_user_id=None, context_token=None, client_id=None):
    """Send one text message. Returns the parsed response on success.
    Audit 3: post failure during final sendmessage = UnknownDeliveryError
    (may already be delivered, do NOT auto-retry); explicit ret/errcode
    rejection = PermanentError (definitely not delivered)."""
    if os.path.exists(NEEDS_RELOGIN_FILE):
        raise TokenExpired("NEEDS_RELOGIN exists, refusing to send")
    base = session.get("baseurl") or DEFAULT_BASE
    to_user_id = to_user_id or session.get("ilink_user_id")
    if not to_user_id:
        raise PermanentError("send: no to_user_id and no ilink_user_id in session")
    payload = {
        "msg": {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id or uuid.uuid4().hex,
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token if context_token is not None else (session.get("context_token") or ""),
            "item_list": [{"type": 1, "text_item": {"text": text}}],
        },
        "base_info": base_info(),
    }
    try:
        resp = post_json(base, "/ilink/bot/sendmessage", payload, session=session, authenticated=True, timeout=15, allow_fallback=False)
    except TokenExpired:
        raise
    except Exception as e:
        raise UnknownDeliveryError(f"sendmessage post failed, result unknown: {e}")
    try:
        check_ret(resp, "sendmessage")
    except TokenExpired:
        raise
    except Exception as e:
        raise PermanentError(f"sendmessage rejected: {e}")
    return resp


def recover_sending_on_startup():
    """Audit 3: on serve start, any pending job left in delivery_state
    'sending' from a previous run (crash/restart during send) is
    result-unknown: it may already have been delivered. Move it to
    failed/ as needs_check, do NOT auto-retry it."""
    try:
        names = sorted(os.listdir(OUTBOX_PENDING))
    except OSError:
        return
    for name in names:
        if not name.endswith(".json"):
            continue
        if name in _IN_MEMORY_HALTED_FILES:
            print(f"outbox: skipping halted (post-send write failed) {name}, manual check required", flush=True)
            continue
        _cd = _SEND_COOLDOWN.get(name, 0)
        if _cd and time.time() - _cd < 10:
            continue
        path = os.path.join(OUTBOX_PENDING, name)
        try:
            with open(path, encoding="utf-8") as f:
                job = json.load(f)
        except Exception:
            continue
        if not isinstance(job, dict):
            # Gate 1 remainder: shape guard lives in drain_outbox;
            # startup recovery only skips a non-object file (drain
            # parks it on the next tick) instead of dying on the
            # first job.get() below.
            print(f"outbox: skipping non-object job {name} in startup recovery (drain will park it)", flush=True)
            continue
        _plan_rec = _text_plan(job)
        _plan_sending = bool(_plan_rec and any(
            isinstance(p, dict) and p.get("status") == "sending" for p in _plan_rec))
        if job.get("delivery_state") == "sending" or _plan_sending:
            if _plan_sending:
                # Item 3: a residual 'sending' text part is
                # result-unknown, so it goes to needs_check (never
                # auto-resent), later pending parts and pending
                # attachments are blocked_by_prior, and the whole job
                # goes to failed/ for manual check. The operator
                # resends only that part via resend-text-part.
                for _p in _plan_rec:
                    if _p.get("status") == "sending":
                        _p["status"] = "needs_check"
                        _p["error_code"] = "restart_unknown"
                    elif _p.get("status") == "pending":
                        _p["status"] = "blocked_by_prior"
                for _att in job.get("attachments") or []:
                    if isinstance(_att, dict) and _att.get("status") not in ("sent", "failed", "needs_check"):
                        _att["status"] = "blocked_by_prior"
                job["text_status"] = "needs_check"
                job["needs_check_reason"] = "serve restarted with text part in sending state"
            else:
                job["needs_check_reason"] = "serve restarted with item in sending state"
            job["delivery_state"] = "unknown_restart"
            # v1.0.2: persist the terminal marker on disk BEFORE the
            # move attempt, so a move failure + process restart still
            # leaves a disk-recognisable terminal job (not just a
            # memory set entry). Its text/attachment statuses stay
            # 'pending' on purpose: the delivery result is unknown.
            # (Item 3 plan jobs are the exception above: their
            # sending part is normalised to needs_check first.)
            job["terminal"] = True
            try:
                atomic_write_json(path, job, mode=0o600)
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: recovered sending {name} -> failed/ needs_check (restart)", flush=True)
                # Gate 3 step 2: shadow sync AFTER the JSON lock
                # (design 1.2 startup row order). Fail-open.
                _shadow_mirror_terminal(name, job, "needs_check", "send_restart_unknown")
            except Exception as e:
                _TERMINAL_TASK_IDS.add(name)
                print(f"outbox: recovery failed for {name}: {_sanitize_error(e)}; marked terminal, will NOT resend", flush=True)
            # Gate 1 item 2: a crash-mid-send job is result-unknown
            # and must be checked by hand, never auto-resent.
            record_timeline_event(name, "needs_check", failure_code="send_restart_unknown",
                                  source="startup_recovery")


def _classify_delivery_error(e):
    """Return (status, is_unknown): Permanent -> failed immediately,
    Retryable -> pending up to 3 attempts, Unknown -> needs_check immediately.
    Any other ApiError is treated as Unknown (conservative, anti-duplicate)."""
    if isinstance(e, UnknownDeliveryError):
        return "needs_check", True
    if isinstance(e, PermanentError):
        return "failed", False
    if isinstance(e, RetryableError):
        return "pending", False
    # Conservative default: unknown, do not auto-retry.
    return "needs_check", True


def _scrub_job_token(path, job):
    """Pop context_token and persist the job without it.

    Returns True if the token-free job was persisted. On write failure
    returns False and records a failures/ note (kind
    token_scrub_failed, field names only, never the token value): the
    caller still archives the job with its correct delivery state, but
    the record must never be read as desensitized. A scrub failure and
    a delivery failure are separate: this never resends anything."""
    if job.pop("context_token", None) is None:
        return True
    try:
        atomic_write_json(path, job, mode=0o600)
        return True
    except Exception as _e:
        print(f"outbox: token scrub write failed for {path}: {_sanitize_error(_e)}", flush=True)
        try:
            _mid = os.path.basename(path)
            if _mid.endswith(".json"):
                _mid = _mid[:-5]
            write_failure_record(
                _mid,
                "scrub",
                {"kind": "token_scrub_failed",
                 "archived_with_token_possible": True,
                 "delivery_state": job.get("delivery_state")},
            )
        except Exception:
            pass
        return False


def _sha256_file(path):
    """Return the hex SHA-256 of a file, or None if it cannot be read.

    Never raises: a missing or unreadable evidence file is an audit
    fact (null hash), not a reason to break sending."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def write_evidence_audit(job_id, reply_text):
    """Append one bridge-side evidence audit record, or nothing.

    Bridge-side observation only, and the docstring must stay honest
    about it: this record proves that, at the moment a reply reached
    the sent terminal state, an analysis evidence file existed (or
    did not), what run state it reported, and what its hash was. It
    does NOT prove that the Worker actually read that evidence:
    Worker reading happens outside this bridge code and is not
    observable from here. A record that says evidence_before_reply
    is true must never be read as "the Worker read the evidence".

    Rules (item: evidence audit, 2026-10-31):
    - Only replies that correspond to a video message are audited:
      the done record has a non-empty videos list and is not
      local_only, or an analysis/<message_id>_* directory exists.
      Anything else returns False without writing.
    - Local-only video replies are never audited: a local-only
      reply is a generic sentence, not an analysis reply, and its
      done record carries local_only=True.
    - Per analysis directory, state.json is read best-effort:
      missing or unparsable state.json gives state_status
      "unknown", never an exception.
    - Only hashes and lengths are recorded, never reply text,
      evidence text, or any token.
    - An audit write failure must never break sending: it is
      recorded in failures/ (field names and error type only) and
      this function returns False.

    Returns True when an audit line was appended, False otherwise
    (not a video reply, local-only, or any failure). Never raises.
    """
    try:
        message_id = str(job_id)
        if message_id.endswith(".json"):
            message_id = message_id[:-5]
        # Local-only replies are generic sentences, not analysis
        # replies: skip them even if an analysis directory were
        # somehow present.
        try:
            with open(os.path.join(DONE, "%s.json" % message_id), encoding="utf-8") as f:
                done = json.load(f)
        except Exception:
            done = None
        if isinstance(done, dict) and done.get("local_only") is True:
            return False
        has_videos = isinstance(done, dict) and bool(done.get("videos"))
        try:
            dirs = sorted(
                d for d in os.listdir(ANALYSIS_DIR)
                if d.startswith("%s_" % message_id)
                and os.path.isdir(os.path.join(ANALYSIS_DIR, d)))
        except Exception:
            dirs = []
        if not has_videos and not dirs:
            return False
        evidence_dirs = []
        evidence_before_reply = False
        for d in dirs:
            outdir = os.path.join(ANALYSIS_DIR, d)
            state_status = "unknown"
            try:
                with open(os.path.join(outdir, "state.json"), encoding="utf-8") as f:
                    state_doc = json.load(f)
                if isinstance(state_doc, dict) and isinstance(state_doc.get("state"), str):
                    state_status = state_doc["state"]
            except Exception:
                state_status = "unknown"
            evidence_path = os.path.join(outdir, "evidence.json")
            evidence_sha = _sha256_file(evidence_path) if os.path.exists(evidence_path) else None
            narrative_sha = _sha256_file(os.path.join(outdir, "narrative.md"))
            if state_status in ("completed", "partial") and evidence_sha is not None:
                evidence_before_reply = True
            evidence_dirs.append({
                "dir": d,
                "state_status": state_status,
                "evidence_sha256": evidence_sha,
                "narrative_sha256": narrative_sha,
            })
        text = reply_text if isinstance(reply_text, str) else ""
        record = {
            "schema": "evidence-audit-0.1",
            "message_id": message_id,
            "sent_at_wall": _now_iso(),
            "evidence_before_reply": evidence_before_reply,
            "evidence_dirs": evidence_dirs,
            "reply_text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "reply_text_len": len(text),
        }
        audit_path = os.path.join(STATE, "evidence_audit.jsonl")
        try:
            os.makedirs(STATE, exist_ok=True)
            with open(audit_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass
            try:
                os.chmod(audit_path, 0o600)
            except OSError:
                pass
        except Exception as e:
            try:
                write_failure_record(
                    message_id, "evidence_audit",
                    {"kind": "evidence_audit_failed",
                     "error_type": type(e).__name__},
                )
            except Exception:
                pass
            return False
        return True
    except Exception:
        # Audit must never break sending, whatever went wrong above.
        return False


# Gate 1 item 1 (2026-10-31): outbound sanitize.
#
# sanitize_outbound_text() removes Muse internal widget tags
# [[hatch_widget:...]] from text that is about to be sent to WeChat,
# while leaving normal Markdown, quotes and code-fence contents
# untouched. Rules, in order:
# - A tag starts with the literal prefix "[[hatch_widget:" and ends
#   at the matching "]]". Bracket depth is tracked, so a single "]"
#   inside a tag does not end it, and a nested "[[" inside a tag
#   is consumed as part of that tag. If no matching "]]" is ever
#   found, the tag is left verbatim: when in doubt, keep text, do
#   not delete user prose (conservative, per the task definition).
# - Fenced code blocks are literal: lines inside a fenced block
#   (tracked with the same _fence_match/_update_fence helpers used
#   by long-text segmentation) are returned unchanged, so a widget
#   tag a user pasted as code text is preserved, not stripped.
# - Nothing else is changed: no whitespace normalisation, no
#   Markdown rewriting, no family-phrasing rewrite (there is no
#   family-mode switch in this bridge, so no such rewrite exists
#   here; inventing a default-on rewrite would change wording
#   the operator never asked for).
# - Emptiness is judged by the caller on the sanitized text with
#   .strip(): a text that is only widget tags, whitespace and
#   blank lines sanitizes to whitespace-only and must never be
#   sent as a blank bubble (see drain_outbox).
_WIDGET_PREFIX = "[[hatch_widget:"


def _strip_widget_tags(s):
    """Remove [[hatch_widget:...]] tags from one plain-text string.

    Depth-tracked: "]]" only closes a tag when bracket depth
    returns to zero, so single "]" characters inside a tag are
    harmless, and nested "[[" inside a tag is consumed with it.
    An unclosed tag is left verbatim (conservative: never delete
    prose on a guess)."""
    out = []
    i = 0
    n = len(s)
    while i < n:
        if s.startswith(_WIDGET_PREFIX, i):
            depth = 1
            j = i + 2
            closed = None
            while j < n:
                if s.startswith("[[", j):
                    depth += 1
                    j += 2
                elif s.startswith("]]", j):
                    depth -= 1
                    j += 2
                    if depth == 0:
                        closed = j
                        break
                else:
                    j += 1
            if closed is not None:
                i = closed
                continue
            out.append(s[i:])
            break
        out.append(s[i])
        i += 1
    return "".join(out)


def sanitize_outbound_text(text):
    """Strip widget tags outside fenced code blocks, nothing else.

    Returns "" for non-string input. Fence state is tracked line
    by line with _fence_match/_update_fence on the original lines,
    so an unclosed fence keeps the rest of the text literal, and
    a fence delimiter line itself is never treated as content."""
    if not isinstance(text, str):
        return ""
    out = []
    fence = None
    for line in _split_keepends_nl(text):
        if fence is None:
            out.append(_strip_widget_tags(line))
        else:
            out.append(line)
        fence = _update_fence(fence, line)
    return "".join(out)


def _sanitized_part_is_empty(display_text):
    """True when a sanitized plan part carries no sendable content.

    The caller passes the already-sanitized display_text. The
    "[i/N]" label line and any fence delimiter lines are not
    content, so they are dropped before the emptiness check; what
    remains (including any widget-like text preserved inside a
    fenced block) decides. A part that is only a label, only
    fence lines, or only whitespace is empty."""
    if not isinstance(display_text, str):
        return True
    lines = display_text.splitlines()
    if lines and re.match(r"^\[\d+/\d+\]$", lines[0].strip()):
        lines = lines[1:]
    while lines and _fence_match(lines[0]) is not None:
        lines = lines[1:]
    while lines and _fence_match(lines[-1]) is not None:
        lines = lines[:-1]
    return "".join(lines).strip() == ""


# Gate 1 item 2 (2026-11-01): observability.
#
# One message's journey (received -> queued -> sent/failed) is made
# checkable from the bridge side, instead of guessable from log
# lines. Rules, in order:
# - Events are appended to state/message_timeline.jsonl (0600), one
#   JSON object per line: schema, job (outbox filename without
#   ".json"), stage, UTC ISO timestamp, source, and, for terminal
#   events, a stable failure code plus a short fixed reason. No
#   reply text, no attachment content, no token, no user id, no
#   recipient id is ever written here: the job filename is the
#   only identifier, and it is already the outbox filename.
# - Timestamps are UTC ISO with seconds
#   (e.g. 2026-11-01T03:00:00+00:00), so they sort
#   lexicographically. The older _now_iso() uses local time and
#   is kept for serve.log lines only; it is NOT used here.
# - A missing stage is missing: the reader returns None for it,
#   never a fabricated time. In particular, "worker_started" is
#   part of the schema but is NEVER emitted by bridge code: the
#   Worker runs outside this bridge, and there is no bridge-side
#   signal for the moment it starts. The queue notice is written
#   by the bridge itself and must never be read as Worker start
#   (serve_loop already warns about exactly this). Filling
#   worker_started needs a Worker-side marker, which does not
#   exist here; until then it stays not_observed.
# - "queued" has two honest sources. Bridge-written jobs
#   (send_direct_or_outbox: fast replies, local replies; queue
#   notices were removed by the 2026-11-01 waiting-prompt merge,
#   old ones on disk keep their bridge_write times) record it
#   at the write, source "bridge_write".
#   Worker-written jobs (<message_id>.json) are written outside
#   this code, so the bridge cannot timestamp that write; drain
#   records the first sighting instead, source
#   "drain_first_seen", which is an upper bound on the queue
#   time, not the write time. The reader takes the earliest
#   event per stage, so a bridge_write time always wins over a
#   later first-sighting.
# - Timeline writes never block sending: record_timeline_event
#   never raises, and every call site treats a False return as
#   "no observability record", never as a send failure.
# - Failure codes are a fixed, enumerable set (see
#   _OBS_FAILURE_REASONS); they are derived from job fields and
#   part error codes, never from exception text or message text.
_OBS_TIMELINE_SCHEMA = "message-timeline-0.1"
_OBS_STAGES = ("received", "queued", "worker_started", "sent",
               "failed", "needs_check", "skipped_empty", "progress",
               "offline_receipt")
_OBS_FAILURE_REASONS = {
    "send_permanent": "permanent delivery failure",
    "send_retryable": "retryable failure, attempts remain",
    "send_retry_limit": "retry limit reached",
    "send_unknown": "delivery result unknown",
    "send_restart_unknown": "serve restarted with item in sending state",
    "recipient_mismatch": "recipient does not match bound account",
    "recipient_missing": "recipient missing and no local receipt record",
    "job_not_object": "pending job is not a JSON object",
    "text_plan_mismatch": "long-text plan does not match text",
    "text_part_failed_or_unknown": "long-text part failed or unknown",
    "text_too_long": "text exceeds segment limit",
    "unknown_attachment_type": "unknown attachment type",
    "skipped_empty": "sanitized text is empty",
    "delivery_failed": "delivery failed",
    "delivery_unknown": "delivery result unknown",
}
_OBS_PART_CODE_MAP = {
    "retry_limit": "send_retry_limit",
    "retryable": "send_retryable",
    "permanent": "send_permanent",
    "unknown_delivery": "send_unknown",
    "restart_unknown": "send_restart_unknown",
}
# Process-lifetime dedup for drain first-sighting of queued jobs.
# The reader takes the earliest event per stage, so a re-record
# after a serve restart cannot corrupt the view.
_OBS_QUEUED_SEEN = set()


def _now_utc_iso():
    """UTC ISO timestamp with seconds, lexicographically sortable."""
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _obs_job_key(name):
    """Outbox filename -> timeline job key, or None if unsafe.

    Only a bare filename is accepted (no separators, no empty,
    no dot-files): a crafted name must never turn into a path
    outside the timeline file."""
    if not isinstance(name, str):
        return None
    if name.endswith(".json"):
        name = name[:-5]
    if not name or name.startswith("."):
        return None
    if "/" in name or "\\" in name:
        return None
    return name


def record_timeline_event(job_name, stage, failure_code=None, source=None):
    """Append one observability event. Never raises, never blocks.

    Returns True when the line was appended, False otherwise
    (bad stage, unsafe job key, or any write failure). The caller
    must never treat False as a send failure."""
    try:
        key = _obs_job_key(job_name)
        if key is None or stage not in _OBS_STAGES:
            return False
        if failure_code is not None and failure_code not in _OBS_FAILURE_REASONS:
            return False
        record = {
            "schema": _OBS_TIMELINE_SCHEMA,
            "job": key,
            "stage": stage,
            "ts": _now_utc_iso(),
            "source": source or "bridge",
        }
        if failure_code is not None:
            record["failure_code"] = failure_code
            record["reason"] = _OBS_FAILURE_REASONS[failure_code]
        os.makedirs(STATE, exist_ok=True)
        path = os.path.join(STATE, "message_timeline.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        return True
    except Exception:
        return False


def _obs_failure_code_for_job(job):
    """Stable failure code for a terminal job, or None.

    Preference order: explicit needs_check_reason, then the first
    bad long-text part's error code, then attachment errors, then
    the legacy text error. Unknown shapes fall back to the generic
    delivery_failed / delivery_unknown codes, never to exception
    text or message text."""
    if not isinstance(job, dict):
        return None
    if job.get("delivery_state") == "unknown_restart":
        return "send_restart_unknown"
    _reason = job.get("needs_check_reason")
    if _reason == "recipient_mismatch":
        return "recipient_mismatch"
    if _reason == "recipient_missing":
        return "recipient_missing"
    if _reason == "job_not_object":
        return "job_not_object"
    if _reason == "text_plan_mismatch":
        return "text_plan_mismatch"
    # Any other needs_check_reason (including the generic
    # text_part_failed_or_unknown and the startup-recovery
    # sentences) falls through to the derivation below, so a
    # precise stored code wins over a generic reason.
    _tcode = job.get("text_error_code")
    if isinstance(_tcode, str) and _tcode in _OBS_FAILURE_REASONS:
        return _tcode
    _plan = _text_plan(job)
    if _plan is not None:
        for _p in _plan:
            if not isinstance(_p, dict):
                continue
            if _p.get("status") in ("failed", "needs_check"):
                _mapped = _OBS_PART_CODE_MAP.get(_p.get("error_code"))
                if _mapped is not None:
                    return _mapped
                return "delivery_failed" if _p.get("status") == "failed" else "delivery_unknown"
    for _att in _terminal_attachments(job):
        if not isinstance(_att, dict):
            continue
        if _att.get("status") in ("failed", "needs_check"):
            _err = _att.get("error") or ""
            if isinstance(_err, str) and "unknown attachment type" in _err:
                return "unknown_attachment_type"
            _acode = _att.get("error_code")
            if isinstance(_acode, str) and _acode in _OBS_FAILURE_REASONS:
                return _acode
            return "delivery_failed" if _att.get("status") == "failed" else "delivery_unknown"
    if job.get("text_status") == "failed":
        _terr = job.get("text_error") or ""
        if isinstance(_terr, str) and "too long" in _terr:
            return "text_too_long"
        return "delivery_failed"
    if job.get("text_status") == "needs_check":
        return "delivery_unknown"
    if job.get("delivery_state") == "failed":
        return "delivery_failed"
    if job.get("delivery_state") == "needs_check":
        return "delivery_unknown"
    return None


def read_message_timeline(job_name):
    """All timeline events for one job, earliest first. Never raises.

    Returns [] when the timeline file is missing, unreadable, or
    the job has no events. Events are returned as stored (schema,
    job, stage, ts, source, and terminal failure_code/reason
    only); no text or token ever appears in them."""
    key = _obs_job_key(job_name)
    if key is None:
        return []
    try:
        path = os.path.join(STATE, "message_timeline.jsonl")
        events = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                if isinstance(ev, dict) and ev.get("job") == key:
                    events.append(ev)
        return events
    except Exception:
        return []


def _obs_job_outcome(job):
    """Outcome bucket for one job dict (no file access).

    needs_check and skipped_empty are separate buckets, never
    folded into failed or sent: a skipped text with sent
    attachments still reports text_skipped, and a needs_check job
    is never counted as failed."""
    if not isinstance(job, dict):
        return "unknown", False
    _skipped = job.get("delivery_state") == "skipped_empty" or job.get("text_status") == "skipped_empty"
    # Bucket order is deliberate and shared by the per-message view
    # and the summary: needs_check first, then failed, then
    # skipped_empty (a job whose text was skipped AND whose
    # attachment failed is a failure first, with text_skipped
    # noted alongside), then sent, then pending.
    if job.get("delivery_state") in ("needs_check", "unknown_restart") or job.get("needs_check_reason"):
        return "needs_check", _skipped
    _plan = _text_plan(job)
    if _plan is not None:
        if any(isinstance(p, dict) and p.get("status") == "needs_check" for p in _plan):
            return "needs_check", _skipped
        if any(isinstance(p, dict) and p.get("status") == "failed" for p in _plan):
            return "failed", _skipped
    if any(isinstance(a, dict) and a.get("status") == "needs_check" for a in _terminal_attachments(job)):
        return "needs_check", _skipped
    if any(isinstance(a, dict) and a.get("status") == "failed" for a in _terminal_attachments(job)):
        return "failed", _skipped
    if job.get("text_status") == "needs_check":
        return "needs_check", _skipped
    if job.get("text_status") == "failed":
        return "failed", _skipped
    if job.get("delivery_state") == "failed":
        return "failed", _skipped
    if _skipped:
        return "skipped_empty", True
    if job.get("delivery_state") == "sent":
        return "sent", _skipped
    return "pending", _skipped


def get_message_observability(job_name):
    """Per-message observability view. Read-only, never raises.

    Stages are the earliest timeline timestamp per stage, or None
    when the stage was never observed (never a fabricated time).
    worker_started is always None here: no bridge-side signal
    exists for it (see the module note above). Outcome comes
    from the job file's location and status, not from the
    timeline, so a missing timeline line cannot fake a sent job.
    failure_codes lists the terminal event's code (if any) plus
    the job-derived code (if any), deduplicated. analysis_states
    lists only directory names and analyzer state strings for a
    bare message-id job, never analysis content."""
    key = _obs_job_key(job_name)
    view = {
        "schema": "message-observability-0.1",
        "job": key,
        "stages": {"received": None, "queued": None,
                   "worker_started": None, "sent": None},
        "stage_notes": {
            "worker_started": "not_observed_by_bridge",
        },
        "outcome": "unknown",
        "text_skipped": False,
        "failure_codes": [],
        "analysis_states": [],
        "event_count": 0,
        # Gate 1 item 3: earliest progress-receipt moment, or None.
        # Kept out of "stages" on purpose: the item-2 view contract
        # (stages is exactly received/queued/worker_started/sent)
        # stays intact, and a progress receipt is not a sent
        # formal reply, so it must never appear as stages.sent.
        "progress_at": None,
        # Gate 1 item 4: earliest offline-receipt moment, or None.
        # Same reason as progress_at: an offline receipt is not
        # a formal reply and must never appear as stages.sent.
        "offline_receipt_at": None,
    }
    if key is None:
        return view
    events = read_message_timeline(key)
    view["event_count"] = len(events)
    _earliest = {}
    _terminal_code = None
    for ev in events:
        _st = ev.get("stage")
        _ts = ev.get("ts")
        if _st in ("received", "queued", "sent") and isinstance(_ts, str):
            if _st not in _earliest or _ts < _earliest[_st]:
                _earliest[_st] = _ts
        if _st == "progress" and isinstance(_ts, str):
            if view["progress_at"] is None or _ts < view["progress_at"]:
                view["progress_at"] = _ts
        if _st == "offline_receipt" and isinstance(_ts, str):
            if view["offline_receipt_at"] is None or _ts < view["offline_receipt_at"]:
                view["offline_receipt_at"] = _ts
        if _st in ("failed", "needs_check", "skipped_empty") and _terminal_code is None:
            if isinstance(ev.get("failure_code"), str):
                _terminal_code = ev["failure_code"]
    view["stages"].update(_earliest)
    job = None
    _found_dir = None
    for _d, _label in ((OUTBOX_SENT, "sent"), (OUTBOX_FAILED, "failed"),
                       (OUTBOX_PENDING, "pending")):
        try:
            _p = os.path.join(_d, "%s.json" % key)
            if os.path.exists(_p):
                with open(_p, encoding="utf-8") as f:
                    job = json.load(f)
                _found_dir = _label
                break
        except Exception:
            job = None
            _found_dir = None
            break
    if job is not None and _found_dir == "sent":
        view["outcome"] = "sent"
        view["text_skipped"] = job.get("text_status") == "skipped_empty"
    elif job is not None:
        if _found_dir == "pending" and not _is_terminal_job(job):
            # A still-retryable job in pending/ is pending,
            # whatever its last item's status says; the terminal
            # derivation only applies once the whole job is
            # terminal (or stuck terminal in pending/).
            view["outcome"] = "pending"
            view["text_skipped"] = job.get("text_status") == "skipped_empty"
        else:
            _outcome, _skipped = _obs_job_outcome(job)
            if _found_dir == "failed" and _outcome == "pending":
                # A job archived in failed/ is a failure even if
                # its item statuses alone look pending (e.g. a
                # terminal flag with no bad item recorded yet):
                # location wins over status derivation here.
                _outcome = "failed"
            view["outcome"] = _outcome
            view["text_skipped"] = _skipped
    _codes = []
    if _terminal_code is not None:
        _codes.append(_terminal_code)
    if job is not None:
        _derived = _obs_failure_code_for_job(job)
        if _derived is not None and _derived not in _codes:
            _codes.append(_derived)
    view["failure_codes"] = _codes
    # Analyzer states, by directory name + state string only.
    # Only for a bare numeric message id; suffix jobs (queue
    # notices, local replies) have no analysis directories.
    if key.isdigit():
        try:
            for d in sorted(os.listdir(ANALYSIS_DIR)):
                if d.startswith("%s_" % key) and os.path.isdir(os.path.join(ANALYSIS_DIR, d)):
                    _st = "unknown"
                    try:
                        with open(os.path.join(ANALYSIS_DIR, d, "state.json"), encoding="utf-8") as f:
                            _doc = json.load(f)
                        if isinstance(_doc, dict) and isinstance(_doc.get("state"), str):
                            _st = _doc["state"]
                    except Exception:
                        _st = "unknown"
                    view["analysis_states"].append({"dir": d, "state": _st})
        except Exception:
            view["analysis_states"] = []
        if any(a.get("state") == "failed" for a in view["analysis_states"]):
            if "analysis_failed" not in view["failure_codes"]:
                # analysis_failed is a view-level code for a failed
                # analyzer run; it is not a timeline failure code
                # (timeline codes stay in _OBS_FAILURE_REASONS).
                view["failure_codes"].append("analysis_failed")
    return view


def _disk_watermark():
    """Read-only disk watermark: (free MB, media dir MB), ints or None.

    Only reports, never deletes, never warns on thresholds: callers
    decide what a low number means. The media dir is the parent of
    MEDIA_INBOUND (i.e. <root>/media); unreadable entries inside it
    are skipped, never raised. Uses MEDIA_INBOUND (not ROOT) so
    tests that rebind module paths still measure the right tree.
    """
    free_mb = None
    try:
        free_mb = int(shutil.disk_usage(MEDIA_INBOUND).free // (1024 * 1024))
    except Exception:
        free_mb = None
    media_mb = None
    try:
        media_dir = os.path.dirname(MEDIA_INBOUND)
        total = 0
        for _dirpath, _dirnames, _filenames in os.walk(media_dir):
            for _fn in _filenames:
                try:
                    _p = os.path.join(_dirpath, _fn)
                    if os.path.islink(_p):
                        continue
                    total += os.path.getsize(_p)
                except OSError:
                    continue
        media_mb = int(total // (1024 * 1024))
    except Exception:
        media_mb = None
    return free_mb, media_mb


def get_observability_summary():
    """Counts across outbox dirs, each bucket separate. Read-only.

    pending counts only jobs still retryable in pending/; a
    terminal job stuck in pending/ (move failed) counts in its
    terminal bucket, never as pending. needs_check, failed and
    skipped_empty are separate buckets; sent is its own bucket.
    Unreadable job files count under "unreadable", never silently
    dropped. Only counts are returned, never job text or ids."""
    summary = {
        "schema": "observability-summary-0.1",
        "generated_at": _now_utc_iso(),
        "pending": 0,
        "sent": 0,
        "failed": 0,
        "needs_check": 0,
        "skipped_empty": 0,
        "unreadable": 0,
    }
    try:
        _sent_names = set(os.listdir(OUTBOX_SENT)) if os.path.isdir(OUTBOX_SENT) else set()
    except Exception:
        _sent_names = set()
    # Gate 1 item 3: a progress receipt is not a formal delivery,
    # so *_progress.json is counted in no bucket at all (sent,
    # failed, pending, needs_check, skipped_empty all exclude it).
    # Gate 1 item 4: same for *_offline.json (an offline receipt
    # is not a formal delivery either).
    # Waiting-prompt merge (2026-11-01): same for *_queue.json.
    # Queue notices were never replies either; they used to count
    # as sent, which overstated formal deliveries. Excluding them
    # corrects the count: historical queue files on disk stop
    # counting, so "sent" may drop by that many without any
    # delivery being lost. Other bridge-written suffix jobs
    # (fast replies, local replies) are genuine replies and keep
    # their pre-existing counting.
    summary["sent"] = len([n for n in _sent_names
                           if n.endswith(".json")
                           and not n.endswith("_progress.json")
                           and not n.endswith("_offline.json")
                           and not n.endswith("_queue.json")])
    for _d in (OUTBOX_FAILED, OUTBOX_PENDING):
        try:
            _names = sorted(os.listdir(_d)) if os.path.isdir(_d) else []
        except Exception:
            continue
        for name in _names:
            if not name.endswith(".json"):
                continue
            if name.endswith("_progress.json"):
                continue
            if name.endswith("_offline.json"):
                continue
            if name.endswith("_queue.json"):
                continue
            try:
                with open(os.path.join(_d, name), encoding="utf-8") as f:
                    job = json.load(f)
            except Exception:
                summary["unreadable"] += 1
                continue
            if _d == OUTBOX_FAILED:
                # Same bucket order as _obs_job_outcome, so the
                # summary and the per-message view can never
                # disagree about one job.
                _outcome, _ = _obs_job_outcome(job)
                if _outcome == "needs_check":
                    summary["needs_check"] += 1
                elif _outcome == "skipped_empty":
                    summary["skipped_empty"] += 1
                else:
                    summary["failed"] += 1
            else:
                if _is_terminal_job(job):
                    _outcome, _ = _obs_job_outcome(job)
                    if _outcome == "needs_check":
                        summary["needs_check"] += 1
                    elif _outcome == "skipped_empty":
                        summary["skipped_empty"] += 1
                    else:
                        summary["failed"] += 1
                else:
                    summary["pending"] += 1
    _free_mb, _media_mb = _disk_watermark()
    summary["disk_free_mb"] = _free_mb
    summary["media_dir_size_mb"] = _media_mb
    return summary


# Gate 1 item 3 (2026-11-01): progress receipt for slow replies.
#
# When a message's formal reply takes longer than 5 seconds, the
# user should hear "still working" instead of silence — but the
# progress note must never disturb the formal reply. Rules:
# - Trigger: a timeline "received" event exists, more than 5
#   seconds have passed since it, and no formal job
#   (<message_id>.json, exact name) exists in pending, sent or
#   failed. A formal job that merely sits unsent still counts
#   as "reply exists": the progress note must not second-guess
#   a reply the Worker already produced. Queue notices
#   (<mid>_queue.json) do NOT count as a formal reply.
# - One attempt only: a persistent marker in
#   state/progress_receipts.json is written BEFORE the queue
#   write, so a crash between marker and send can never cause a
#   second progress note. A failed queue write leaves the marker
#   with ok=False and is never retried: the formal reply is
#   unaffected either way.
# - Independent message: the receipt is its own outbox job
#   (<mid>_progress.json) with its own stable client_id
#   ("progress-<mid>"), never the formal job's key. It does not
#   rewrite, replace or truncate the formal reply, does not mark
#   the formal job sent, and is excluded from every summary
#   bucket. The evidence audit already only audits video replies
#   (done record with videos, or an analysis dir), so a progress
#   job can never produce an audit row.
# - Content: one fixed sentence, no user text, no reply draft,
#   no token. The job still carries the session context_token
#   like every bridge-written job; drain scrubs it before
#   archiving, same as queue notices.
# - Observability: the send is recorded as a "progress" timeline
#   event on the FORMAL job's timeline (source bridge_progress),
#   separate from its sent event, and surfaced as progress_at in
#   the per-message view. A failed queue write records no
#   timeline event: an event must never claim a note was sent
#   when it was not.
# - Caller contract: maybe_send_progress_receipts() takes the
#   outbox lock itself (_OUTBOX_LOCK is reentrant, so calling it
#   from _sender_loop while the lock is held is safe). It never
#   raises and never blocks sending: any failure is contained
#   per candidate message.
PROGRESS_RECEIPT_DELAY_SECONDS = 5.0
PROGRESS_RECEIPT_TEXT = "还在处理中，稍等一下，我会把正式回复发给你。"
PROGRESS_RECEIPT_SCHEMA = "progress-receipt-0.1"


def _progress_client_id(mid):
    """Stable idempotency key for one message's progress receipt.

    Independent from the formal job's client_id by construction:
    the formal job's key is its own uuid, this one is derived
    from the message id with a progress-only prefix."""
    return "progress-%s" % mid


def _parse_utc_iso(ts):
    """UTC ISO timestamp -> epoch seconds, or None if unparsable."""
    if not isinstance(ts, str):
        return None
    try:
        import datetime as _dt
        _d = _dt.datetime.fromisoformat(ts)
        if _d.tzinfo is None:
            return None
        return _d.timestamp()
    except Exception:
        return None


def _progress_receipts_path():
    return os.path.join(STATE, "progress_receipts.json")


def _load_progress_receipts():
    """Marker map mid -> {ts, client_id, ok}. Never raises."""
    try:
        with open(_progress_receipts_path(), encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict) and isinstance(doc.get("receipts"), dict):
            return doc["receipts"]
    except Exception:
        pass
    return {}


def _save_progress_receipts(receipts):
    """Persist the marker map (0600, atomic). Never raises."""
    try:
        os.makedirs(STATE, exist_ok=True)
        atomic_write_json(_progress_receipts_path(),
                          {"schema": PROGRESS_RECEIPT_SCHEMA,
                           "receipts": receipts}, mode=0o600)
        return True
    except Exception:
        return False


def _progress_formal_job_exists(mid):
    """True when the formal reply job exists in any outbox dir."""
    name = "%s.json" % mid
    for _d in (OUTBOX_PENDING, OUTBOX_SENT, OUTBOX_FAILED):
        try:
            if os.path.exists(os.path.join(_d, name)):
                return True
        except Exception:
            continue
    return False


def _progress_job_exists(mid):
    """True when this message's progress job already exists."""
    name = "%s_progress.json" % mid
    for _d in (OUTBOX_PENDING, OUTBOX_SENT, OUTBOX_FAILED):
        try:
            if os.path.exists(os.path.join(_d, name)):
                return True
        except Exception:
            continue
    return False


def _earliest_received_map():
    """Earliest timeline 'received' epoch per bare numeric id.

    One pass over the timeline file, no per-job file reads.
    Returns None when the timeline file cannot be read at all
    (missing, unreadable, any read error): callers must treat
    that as 'wait cannot be computed, send nothing', never as
    'every message is overdue'. Unparsable lines/timestamps are
    skipped, never treated as overdue. Never raises."""
    received = {}
    try:
        with open(os.path.join(STATE, "message_timeline.jsonl"),
                  encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(ev, dict) or ev.get("stage") != "received":
                    continue
                _mid = ev.get("job")
                if not isinstance(_mid, str) or not _mid.isdigit():
                    continue
                _ts = _parse_utc_iso(ev.get("ts"))
                if _ts is None:
                    continue
                if _mid not in received or _ts < received[_mid]:
                    received[_mid] = _ts
    except Exception:
        return None
    return received


def maybe_send_progress_receipts(session, now=None):
    """Queue one progress receipt per overdue message, at most once.

    Returns the list of message ids for which a progress job was
    queued this call (in sorted order). `now` is epoch seconds
    and exists for tests: production passes None for time.time().
    Never raises; a failure for one message never blocks others
    or the formal replies. Must be called with the sender loop
    running (it takes _OUTBOX_LOCK itself, which is reentrant)."""
    queued = []
    try:
        if now is None:
            now = time.time()
        if not isinstance(session, dict) or not session.get("ilink_user_id"):
            return queued
        # Gate 1 item 4 (grace revision, 2026-11-01): progress
        # suppression is PER MESSAGE, never global. A stale
        # heartbeat alone must not silence progress: within the
        # offline grace window the Worker may well be working
        # (per-message wakeup plus ~60-75s pickup latency), so
        # "still working" is a true statement there. Only a
        # message that already got an offline receipt/job is
        # suppressed below (its "will reply on return" promise
        # stands; a progress note on top would stack a third
        # message for one question).
        receipts = _load_progress_receipts()
        _offline_acks = _load_offline_receipts()
        received = _earliest_received_map()
        if received is None:
            return queued
        with _OUTBOX_LOCK:
            for mid in sorted(received):
                if mid in receipts:
                    continue
                # Gate 1 item 4: no stacking after recovery. A
                # message that already got an offline receipt
                # ("will reply on return") must not also get a
                # progress note once the Worker is back: the
                # formal reply is coming, the extra note adds
                # nothing but a third message for one question.
                if mid in _offline_acks:
                    continue
                if _offline_job_exists(mid):
                    continue
                if _progress_job_exists(mid):
                    continue
                if _progress_formal_job_exists(mid):
                    continue
                if now - received[mid] <= PROGRESS_RECEIPT_DELAY_SECONDS:
                    continue
                _cid = _progress_client_id(mid)
                # Marker FIRST: a crash after this point must not
                # cause a second progress note for this message.
                receipts[mid] = {"ts": _now_utc_iso(), "client_id": _cid,
                                 "ok": False}
                _save_progress_receipts(receipts)
                try:
                    job = {"text": PROGRESS_RECEIPT_TEXT,
                           "to_user_id": session.get("ilink_user_id"),
                           "context_token": session.get("context_token") or "",
                           "client_id": _cid}
                    atomic_write_json(
                        os.path.join(OUTBOX_PENDING, "%s_progress.json" % mid),
                        job, mode=0o600)
                except Exception as _e:
                    print(f"serve: progress receipt queue FAILED for {mid} "
                          f"(formal reply unaffected, no retry): "
                          f"{_sanitize_error(_e)}", flush=True)
                    continue
                receipts[mid]["ok"] = True
                _save_progress_receipts(receipts)
                # Only a successfully queued note earns a timeline
                # event, on the formal job's timeline.
                record_timeline_event("%s.json" % mid, "progress",
                                      source="bridge_progress")
                queued.append(mid)
                print(f"serve: progress receipt queued for {mid}", flush=True)
        return queued
    except Exception:
        return queued


# Gate 1 item 4 prerequisite (2026-11-01): Worker heartbeat
# reading + bridge-side offline receipt. This is only the bridge
# half: the Worker-side heartbeat WRITE is not in this code and
# must be added to the production Worker instruction separately
# (see the heartbeat contract below). Rules:
#
# - Heartbeat contract (writer = Worker, reader = bridge):
#   state/worker_heartbeat.json, schema "worker-heartbeat-0.1",
#   fields: ts (UTC ISO with seconds, e.g.
#   2026-11-01T03:00:00+00:00) and source (a short writer tag,
#   e.g. "worker"). The Worker writes it atomically (0600) at
#   startup and then every 30 seconds while alive, and stops
#   writing when it stops, so staleness is the offline signal.
#   The writer's clock must be UTC-synced; a naive timestamp
#   (no timezone) is invalid to the reader.
# - RED LINE: the bridge only READS this file. It must never
#   write a heartbeat itself: a bridge-written heartbeat would
#   fake Worker liveness and hide a real outage. There is no
#   code path in this file that writes worker_heartbeat.json.
# - Offline判定: heartbeat file missing, unreadable, or its ts
#   unparsable -> offline. ts older than 120 seconds (strictly
#   greater; exactly 120s is still online) -> offline. 120s is
#   4x the 30s write interval: it tolerates 2-3 missed beats
#   from scheduling jitter without a false offline, yet still
#   detects a real outage within about 2 minutes — not so
#   small that normal jitter flips it, not so large that an
#   outage goes unnoticed for half a day.
# - Offline receipt (grace revision, 2026-11-01): when the
#   heartbeat is stale AND an inbox message has waited strictly
#   more than OFFLINE_RECEIPT_GRACE_SECONDS (180s) since its
#   earliest timeline 'received' event AND no formal job exists
#   AND no prior offline ack/job exists for it, the bridge
#   queues one fixed receipt for that message:
#   "蛋挞现在不在线，消息已收到，回来后会回复你。"
#   It promises only "received, will be handled on return":
#   never instant reply, never "Worker has read it". The
#   receipt is its own outbox job (<mid>_offline.json) with its
#   own stable client_id ("offline-<mid>"), never the formal
#   job's key. A fresh heartbeat NEVER yields an offline
#   receipt, however long the message has waited: a live Worker
#   that is merely backlogged is not offline, and saying so
#   would be a lie — such messages only ever get the normal
#   formal reply (and progress notes while they wait).
# - Cooling lock (anti-spam): one receipt per original message,
#   forever, keyed by message id in
#   state/offline_receipt_acks.json (0600, atomic, schema
#   "offline-receipt-0.1", id/timestamp/client_id/ok only, no
#   text, no token). Marker BEFORE the queue write (like
#   progress receipts), ok=False never retried, and an
#   already-existing offline job file also suppresses. This is
#   also the global guard: a burst of new messages yields at
#   most one short receipt per distinct message, never a
#   repeated or stacked flood for the same message, across
#   restarts and watchdog relaunches.
# - NEVER consume the original: the receipt leaves the inbox
#   record exactly where it is (not moved to done, not marked
#   formally sent, no formal job created). When the Worker
#   comes back online it picks the message up normally and its
#   formal reply goes through the normal path.
# - Recovery: when the heartbeat turns fresh again, nothing is
#   re-sent, nothing is withdrawn, and no extra action is taken
#   for messages that already got an offline receipt; formal
#   replies just flow normally.
# - Receipt send failure: only recorded (marker ok=False, no
#   timeline event — an event must never claim a receipt was
#   sent when it was not), never blocks the original message,
#   never retried, never affects the post-recovery formal reply.
# - Mutual exclusion with progress receipts (grace revision):
#   suppression is PER MESSAGE, not global. Within the grace
#   window a stale heartbeat does not silence progress: the
#   Worker may be working (per-message wakeup plus pickup
#   latency), so "still working" is true and the progress note
#   fires normally. Only a message that already has an offline
#   ack/job is suppressed from progress, so recovery does not
#   stack a progress note on top of the offline receipt. The
#   reverse order is deliberately asymmetric: a message that
#   got a progress note and then outlives both the heartbeat
#   and the grace window may still get one offline receipt
#   later, because that sequence (was working, then went
#   offline) is a genuine status change, not a duplicate.
# - DEPLOYMENT ORDER NOTE (revised by the grace revision):
#   the pre-grace version read "no heartbeat file" as offline
#   and fired immediately, which would have suppressed all
#   progress receipts and spammed offline receipts for normal
#   messages merely waiting out Worker pickup latency. The
#   grace revision removes both catastrophic pre-heartbeat
#   behaviors: progress is no longer gated on the global
#   heartbeat at all, and offline receipts are gated by the
#   180s wait plus the no-formal-job rule. The Worker heartbeat
#   writing SHOULD still go live first for accurate
#   online/offline distinction: without any heartbeat, a
#   message whose Worker is alive but takes over 180s to
#   produce a reply would still earn an offline receipt, and
#   a fresh heartbeat is the only thing that prevents that.
#   Once the Worker beats every 30s, bridge restarts are safe:
#   the heartbeat file persists in state/ across restarts.
# - Observability: a successfully queued receipt earns an
#   "offline_receipt" timeline event on the FORMAL job's
#   timeline (source bridge_offline), surfaced as
#   offline_receipt_at in the per-message view, separate from
#   stages.sent. The summary excludes *_offline.json from
#   every bucket. The evidence audit only audits video replies,
#   so an offline job can never produce an audit row.
# - Caller contract: maybe_send_offline_receipts() takes the
#   outbox lock itself (reentrant, same as progress). It never
#   raises and never blocks sending.
WORKER_HEARTBEAT_SCHEMA = "worker-heartbeat-0.1"
WORKER_HEARTBEAT_INTERVAL_SECONDS = 30
WORKER_HEARTBEAT_STALE_SECONDS = 120
# Grace revision (2026-11-01): an offline receipt fires only
# after a message has waited this long with no formal job. 180s
# is more than twice the measured Worker pickup latency
# (~60-75s from inbox write to formal job), so normal messages
# never trip it, yet a genuine outage is still noticed after
# about 3 minutes of waiting. It must never be set below the
# pickup latency (the idle-false-alarm defect returns) nor in
# the tens of minutes (a real outage would go unnoticed).
OFFLINE_RECEIPT_GRACE_SECONDS = 180
OFFLINE_RECEIPT_TEXT = "蛋挞现在不在线，消息已收到，回来后会回复你。"
OFFLINE_RECEIPT_SCHEMA = "offline-receipt-0.1"


def _worker_heartbeat_path():
    return os.path.join(STATE, "worker_heartbeat.json")


def read_worker_heartbeat():
    """Read the Worker heartbeat. Returns (epoch_or_None, source).

    Bridge-side READ ONLY (see the red line in the block above):
    this function never writes, and no other function in this file
    writes worker_heartbeat.json. A missing file, unreadable JSON,
    a non-dict document, or an unparsable/naive ts all yield
    (None, None), which the offline check treats as offline.
    Never raises."""
    try:
        with open(_worker_heartbeat_path(), encoding="utf-8") as f:
            doc = json.load(f)
        if not isinstance(doc, dict):
            return None, None
        _ts = _parse_utc_iso(doc.get("ts"))
        _src = doc.get("source")
        if not isinstance(_src, str):
            _src = None
        return _ts, _src
    except Exception:
        return None, None


def is_worker_offline(now=None):
    """True when the Worker must be treated as offline.

    Missing/unparsable heartbeat -> offline. ts strictly older
    than WORKER_HEARTBEAT_STALE_SECONDS -> offline; exactly at
    the threshold is still online. `now` is epoch seconds and
    exists for tests; production passes None for time.time().
    Never raises."""
    try:
        if now is None:
            now = time.time()
        _ts, _ = read_worker_heartbeat()
        if _ts is None:
            return True
        return now - _ts > WORKER_HEARTBEAT_STALE_SECONDS
    except Exception:
        return True


def _offline_client_id(mid):
    """Stable idempotency key for one message's offline receipt.

    Independent from the formal job's client_id by construction:
    the formal job's key is its own uuid, this one is derived
    from the message id with an offline-only prefix."""
    return "offline-%s" % mid


def _offline_receipts_path():
    return os.path.join(STATE, "offline_receipts.json")


def _load_offline_receipts():
    """Marker map mid -> {ts, client_id, ok}. Never raises."""
    try:
        with open(_offline_receipts_path(), encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict) and isinstance(doc.get("receipts"), dict):
            return doc["receipts"]
    except Exception:
        pass
    return {}


def _save_offline_receipts(receipts):
    """Persist the marker map (0600, atomic). Never raises."""
    try:
        os.makedirs(STATE, exist_ok=True)
        atomic_write_json(_offline_receipts_path(),
                          {"schema": OFFLINE_RECEIPT_SCHEMA,
                           "receipts": receipts}, mode=0o600)
        return True
    except Exception:
        return False


def _offline_job_exists(mid):
    """True when this message's offline job already exists."""
    name = "%s_offline.json" % mid
    for _d in (OUTBOX_PENDING, OUTBOX_SENT, OUTBOX_FAILED):
        try:
            if os.path.exists(os.path.join(_d, name)):
                return True
        except Exception:
            continue
    return False


def maybe_send_offline_receipts(session, now=None):
    """Queue one offline receipt per unprocessed inbox message.

    Returns the list of message ids for which an offline job was
    queued this call (in sorted order). `now` is epoch seconds
    and exists for tests: production passes None for time.time().
    All four gates must hold before a receipt is queued: (1)
    the heartbeat is stale (missing/unparsable/naive ts, or ts
    strictly more than WORKER_HEARTBEAT_STALE_SECONDS old);
    (2) the message has waited strictly more than
    OFFLINE_RECEIPT_GRACE_SECONDS since its earliest timeline
    'received' event (no received event means the wait cannot
    be computed: never guess, never send); (3) no formal job
    exists in pending/sent/failed; (4) no prior offline ack
    record and no existing offline job file for the message.
    The original inbox record is never touched: no move to done,
    no formal job, no sent marking — the Worker can still pick
    it up normally after recovery. Never raises; a failure for
    one message never blocks others or the formal replies. Must
    be called with the sender loop running (it takes
    _OUTBOX_LOCK itself, which is reentrant)."""
    queued = []
    try:
        if now is None:
            now = time.time()
        if not isinstance(session, dict) or not session.get("ilink_user_id"):
            return queued
        if not is_worker_offline(now):
            return queued
        receipts = _load_offline_receipts()
        received = _earliest_received_map()
        if received is None:
            return queued
        try:
            _names = sorted(os.listdir(INBOX)) if os.path.isdir(INBOX) else []
        except Exception:
            return queued
        with _OUTBOX_LOCK:
            for name in _names:
                if not name.endswith(".json"):
                    continue
                mid = name[:-5]
                if not mid.isdigit():
                    continue
                if mid in receipts:
                    continue
                if _offline_job_exists(mid):
                    continue
                # A formal job already exists: the Worker produced
                # a reply (it just has not moved inbox->done yet),
                # so an "offline" receipt would be misleading even
                # with a stale heartbeat. The formal reply drains
                # normally instead.
                if _progress_formal_job_exists(mid):
                    continue
                # Grace gate (2026-11-01 revision): the Worker
                # is woken per message and pickup takes ~60-75s,
                # so a stale heartbeat right after arrival proves
                # nothing — idle gaps over 2 minutes are normal.
                # Only a message that has waited longer than
                # OFFLINE_RECEIPT_GRACE_SECONDS with no formal
                # job earns an offline receipt. No received
                # event means the wait cannot be computed:
                # never guess, never send.
                _recv = received.get(mid)
                if _recv is None:
                    continue
                if now - _recv <= OFFLINE_RECEIPT_GRACE_SECONDS:
                    continue
                _cid = _offline_client_id(mid)
                # Marker FIRST: a crash after this point must not
                # cause a second offline receipt for this message.
                receipts[mid] = {"ts": _now_utc_iso(), "client_id": _cid,
                                 "ok": False}
                _save_offline_receipts(receipts)
                try:
                    job = {"text": OFFLINE_RECEIPT_TEXT,
                           "to_user_id": session.get("ilink_user_id"),
                           "context_token": session.get("context_token") or "",
                           "client_id": _cid}
                    atomic_write_json(
                        os.path.join(OUTBOX_PENDING, "%s_offline.json" % mid),
                        job, mode=0o600)
                except Exception as _e:
                    print(f"serve: offline receipt queue FAILED for {mid} "
                          f"(original untouched, no retry): "
                          f"{_sanitize_error(_e)}", flush=True)
                    continue
                receipts[mid]["ok"] = True
                _save_offline_receipts(receipts)
                # Only a successfully queued receipt earns a
                # timeline event, on the formal job's timeline.
                record_timeline_event("%s.json" % mid, "offline_receipt",
                                      source="bridge_offline")
                queued.append(mid)
                print(f"serve: offline receipt queued for {mid}", flush=True)
        return queued
    except Exception:
        return queued


# Gate 2 item 4 (2026-11-01): same-source merge.
#
# DISPATCH-CHAIN FINDING (read the code, do not assume): serve polls
# getupdates, downloads/decrypts media, writes inbox/<mid>.json, and
# only then marks the message processed. The bridge NEVER wakes the
# Worker itself: the watchdog hook, outside this file, wakes the
# Worker per inbox file, the Worker reads inbox records and writes
# outbox/pending/<mid>.json formal jobs, and _sender_loop drains
# them every second. There is therefore no bridge function that can
# turn several inbox files into one Worker inference task. Merge
# must live at the intake staging layer instead:
#
# - When the switch is ON, a normal Worker-bound message is NOT
#   written to inbox immediately. It is staged in bridge-private
#   state/merge_staging/ (never in inbox/, so the watchdog never
#   delivers it) with its full record plus its intake context
#   token (staging files are bridge-private, 0600, never
#   Worker-visible).
# - _sender_loop promotes due groups every second. A group is all
#   staged records from one from_user_id inside one FIXED window
#   (MERGE_WINDOW_SECONDS from the FIRST message's received time;
#   fixed, not sliding, so a steady stream cannot merge forever
#   and added latency is bounded by the window).
# - A due group of >= 2 becomes ONE merged inbox record anchored
#   on the FIRST-STAGED message (staging order == serve processing
#   order == typing-ticket holder, so the anchor's formal job
#   always cancels the ticket), plus a bridge-private manifest.
#   A due single is promoted VERBATIM (byte-identical record, no
#   merged fields), so singles behave exactly like the pre-merge
#   path.
# - When the switch is OFF, serve writes inbox directly (old
#   path). Already-staged groups are still promoted verbatim on
#   the next tick/startup, never stranded.
#
# TOKEN TENSION, RESOLVED DELIBERATELY (deviation from the brief's
# literal "send with the last message's token", with reasons):
# inbox/done records carry NO context_token by B2-lite (only the
# session holds the latest token), so the merged inbox record
# likewise carries NO token. Per-message tokens are preserved
# ONLY in the bridge-private manifest, for audit, and are NEVER
# auto-used for sending: formal replies keep the B2-lite
# session-token-first rule, and a token failure goes to
# needs_check without a silent swap. The supplement's "use the
# last token" is therefore recorded as UNVERIFIED and NOT adopted
# as a send rule; it must be validated with a real WeChat case
# before any use. The Worker-cooperation note (parent appends it
# separately, never this file's job) must tell the Worker: reply
# ONCE for the anchor id, cite source message ids when quoting,
# never reply separately to constituents, never open
# state/merge_* files.
#
# COST, STATED PLAINLY: with the switch ON, every message waits
# up to one window before inbox delivery (singles included).
# Against ~60s Worker pickup latency this is negligible, but it
# is real: the switch trades <= 5s of intake latency for merge.
MERGE_WINDOW_SECONDS = 5.0
MERGE_FLAG_NAME = "same_source_merge.json"
MERGE_STAGING_DIRNAME = "merge_staging"
MERGE_MANIFESTS_DIRNAME = "merge_manifests"
MERGE_COMPLETED_DIRNAME = "merge_completed"
MERGE_STAGED_SCHEMA = "merge-staged-0.1"
MERGE_MANIFEST_SCHEMA = "merge-manifest-0.1"
MERGE_COMPLETED_SCHEMA = "merge-completed-0.1"


def _merge_flag_path():
    return os.path.join(STATE, MERGE_FLAG_NAME)


def _merge_staging_dir():
    return os.path.join(STATE, MERGE_STAGING_DIRNAME)


def _merge_manifests_dir():
    return os.path.join(STATE, MERGE_MANIFESTS_DIRNAME)


def _merge_completed_dir():
    return os.path.join(STATE, MERGE_COMPLETED_DIRNAME)


def read_same_source_merge_mode():
    """True iff state/same_source_merge.json says enabled.

    Read fresh on every use (no restart needed to flip). Missing
    or corrupt file means OFF. Default is OFF: deploy merged code
    with the switch OFF until a real WeChat merge test passes.
    """
    try:
        with open(_merge_flag_path(), "r", encoding="utf-8") as f:
            return bool(json.load(f).get("enabled"))
    except Exception:
        return False


def _merge_mid_key(mid):
    try:
        return (0, int(mid))
    except Exception:
        return (1, str(mid))


def _merge_iso(ts_seconds):
    """Format epoch seconds as Asia/Shanghai wall time for merged segments.

    Root-cause note (2026-11-01 live test): the old version referenced
    module-level ``datetime``, which bridge.py never imports at module
    scope (it only imports datetime locally inside functions), so the
    call always raised NameError and the blanket except returned "" —
    every merged segment rendered "[源消息 <mid> @ ]" with an empty
    time. Fixed with a local import, ZoneInfo("Asia/Shanghai") so the
    label matches the wall time the user sees, and a fixed UTC+8
    fallback if zone data is missing. Invalid input still returns "".
    """
    try:
        import datetime as _dt
        ts = float(ts_seconds)
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo("Asia/Shanghai")
        except Exception:
            tz = _dt.timezone(_dt.timedelta(hours=8))
        return _dt.datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _merge_staging_for_sender(from_user_id):
    """Staged dicts for one sender, in STAGING order.

    Order is the staging files' mtime (serve processing order),
    NOT received_ts: the anchor must be the first-staged message,
    because the typing ticket goes to the first-staged message
    and only the anchor's formal job can cancel it. If order
    came from received_ts, a clock jump could split anchor from
    ticket holder and leak a ticket forever. received_ts is used
    only for window math. Corrupt staging files are skipped
    loudly and left in place for inspection, never silently
    dropped and never deleted. The sort key lives only in
    memory (_staging_seq), never on disk or in merged records.
    """
    out = []
    d = _merge_staging_dir()
    try:
        names = sorted(os.listdir(d)) if os.path.isdir(d) else []
    except Exception:
        return out
    for name in names:
        if not name.endswith(".json"):
            continue
        path = os.path.join(d, name)
        try:
            with open(path, encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            print(f"serve: merge staging corrupt, left in place: {name}", flush=True)
            continue
        if not isinstance(doc, dict) or doc.get("schema") != MERGE_STAGED_SCHEMA:
            continue
        if doc.get("from_user_id") != from_user_id:
            continue
        try:
            doc["_staging_seq"] = os.stat(path).st_mtime_ns
        except Exception:
            doc["_staging_seq"] = 0
        out.append(doc)
    out.sort(key=lambda x: (x.get("_staging_seq", 0), _merge_mid_key(x.get("message_id", ""))))
    return out


def _merge_all_staged():
    """All staged dicts grouped by sender, in staging order.

    See _merge_staging_for_sender for why order is mtime, not
    received_ts. Never raises.
    """
    grouped = {}
    d = _merge_staging_dir()
    try:
        names = sorted(os.listdir(d)) if os.path.isdir(d) else []
    except Exception:
        return grouped
    for name in names:
        if not name.endswith(".json"):
            continue
        path = os.path.join(d, name)
        try:
            with open(path, encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            print(f"serve: merge staging corrupt, left in place: {name}", flush=True)
            continue
        if not isinstance(doc, dict) or doc.get("schema") != MERGE_STAGED_SCHEMA:
            continue
        try:
            doc["_staging_seq"] = os.stat(path).st_mtime_ns
        except Exception:
            doc["_staging_seq"] = 0
        grouped.setdefault(doc.get("from_user_id"), []).append(doc)
    for docs in grouped.values():
        docs.sort(key=lambda x: (x.get("_staging_seq", 0), _merge_mid_key(x.get("message_id", ""))))
    return grouped


def stage_merge_message(mid, from_user_id, record, context_token, received_ts=None):
    """Stage one intake record for later merge promotion.

    Returns (ok, is_first): is_first is True when this message
    starts a new group for its sender (no other staged records
    for that sender existed before this write). The caller uses
    is_first for the typing ticket: only the group's first
    message gets a ticket, because only the anchor ever gets a
    formal job that cancels it. On failure returns (False,
    False); the caller must NOT mark the message processed.
    """
    try:
        first = not _merge_staging_for_sender(from_user_id)
        os.makedirs(_merge_staging_dir(), exist_ok=True)
        doc = {
            "schema": MERGE_STAGED_SCHEMA,
            "message_id": str(mid),
            "from_user_id": from_user_id,
            "received_ts": float(received_ts) if received_ts is not None else time.time(),
            # Bridge-private: staging files are never Worker-visible,
            # so holding the intake token here does not violate
            # B2-lite (which bans tokens in Worker-visible records).
            # It is used ONLY for the private manifest, never for
            # sending and never copied into the merged record.
            "context_token": context_token or "",
            "record": record,
        }
        atomic_write_json(os.path.join(_merge_staging_dir(), "%s.json" % mid), doc, mode=0o600)
        return True, first
    except Exception:
        return False, False


def build_merged_record(staged):
    """Build (merged_record, manifest) from sorted staged dicts.

    staged must be non-empty and contain no local_only record
    (local-only placeholders are never merged; the caller must
    promote those verbatim). Raises ValueError otherwise.
    The merged record carries NO context_token (B2-lite).
    """
    if not staged:
        raise ValueError("empty merge group")
    for doc in staged:
        if isinstance(doc.get("record"), dict) and doc["record"].get("local_only"):
            raise ValueError("local_only record must never merge")
    anchor = staged[0]
    anchor_mid = str(anchor.get("message_id"))
    mids = [str(d.get("message_id")) for d in staged]
    sections = []
    images, videos, files = [], [], []
    analyses, video_errors, image_errors, file_errors = [], [], [], []
    parts = []
    merged_quotes = []
    for doc in staged:
        rec = doc.get("record") or {}
        mid = str(doc.get("message_id"))
        ts_ms = rec.get("ts")
        header = "[源消息 %s @ %s]" % (mid, _merge_iso(float(ts_ms) / 1000.0) if ts_ms else _merge_iso(doc.get("received_ts")))
        body = rec.get("text") or ""
        lines = [header]
        if body:
            lines.append(body)
        else:
            lines.append("(无文字内容)")
        for kind, items in (("图片", rec.get("images") or []),
                            ("视频", rec.get("videos") or []),
                            ("文件", rec.get("files") or [])):
            for item in items:
                base = os.path.basename(str(item)) if not isinstance(item, dict) else str(item.get("path") or item)
                lines.append("[附件：%s %s（源消息 %s）]" % (kind, base, mid))
        sections.append("\n".join(lines))
        images.extend(rec.get("images") or [])
        videos.extend(rec.get("videos") or [])
        files.extend(rec.get("files") or [])
        analyses.extend(rec.get("analysis") or [])
        video_errors.extend(rec.get("video_errors") or [])
        image_errors.extend(rec.get("image_errors") or [])
        file_errors.extend(rec.get("file_errors") or [])
        parts.append({
            "message_id": mid,
            "ts_ms": ts_ms,
            "text": body,
            "images": list(rec.get("images") or []),
            "videos": list(rec.get("videos") or []),
            "files": list(rec.get("files") or []),
        })
        if rec.get("quote"):
            merged_quotes.append({"source_message_id": mid, "quote": rec.get("quote")})
    first_rec = anchor.get("record") or {}
    record = {
        "message_id": anchor.get("message_id"),
        "text": "\n\n".join(sections),
        "from_user_id": anchor.get("from_user_id"),
        "ts": first_rec.get("ts") or int(float(anchor.get("received_ts") or 0) * 1000),
        "history_class": "user",
        "egress_policy": "muse_default",
        "policy_version": first_rec.get("policy_version") or SAFE_VIEW_VERSION,
        "merged": True,
        "merged_message_ids": mids,
        "merged_parts": parts,
    }
    # No top-level "quote": with several constituents, a single
    # quote field would be ambiguous. Quotes live ONLY in
    # merged_quotes with their source message id, so a reply
    # quoting one part can cite exactly that part.
    if merged_quotes:
        record["merged_quotes"] = merged_quotes
    if images:
        record["images"] = images
    if videos:
        record["videos"] = videos
    if files:
        record["files"] = files
    if analyses:
        record["analysis"] = analyses
    if video_errors:
        record["video_errors"] = video_errors
    if image_errors:
        record["image_errors"] = image_errors
    if file_errors:
        record["file_errors"] = file_errors
    manifest = {
        "schema": MERGE_MANIFEST_SCHEMA,
        "anchor_message_id": anchor_mid,
        "merged_message_ids": mids,
        "window_seconds": MERGE_WINDOW_SECONDS,
        "first_received_ts": float(staged[0].get("received_ts") or 0),
        "parts": [{
            "message_id": str(d.get("message_id")),
            "received_ts": float(d.get("received_ts") or 0),
            "ts_ms": (d.get("record") or {}).get("ts"),
            "n_images": len((d.get("record") or {}).get("images") or []),
            "n_videos": len((d.get("record") or {}).get("videos") or []),
            "n_files": len((d.get("record") or {}).get("files") or []),
            "has_text": bool((d.get("record") or {}).get("text")),
        } for d in staged],
        # Bridge-private audit only. NEVER auto-used for sending:
        # formal replies keep the B2-lite session-token-first rule.
        # The supplement's "use the last message's token" is
        # UNVERIFIED and NOT adopted as a send rule until a real
        # WeChat case validates it.
        "tokens_by_message": {str(d.get("message_id")): (d.get("context_token") or "") for d in staged},
        "tokens_unverified_for_send": True,
        "send_token_note": "B2-lite: send with the session token; last-message token hypothesis UNVERIFIED, do not use without a real WeChat validation.",
    }
    return record, manifest


def _merge_remove_staged(mids):
    for mid in mids:
        try:
            os.remove(os.path.join(_merge_staging_dir(), "%s.json" % mid))
        except Exception:
            pass


def maybe_promote_merge_groups(now=None):
    """Promote due staged groups to inbox. Returns anchor mids.

    Runs every sender-loop tick and at serve startup. Never
    raises. Semantics:
    - Switch OFF: promote every staged record VERBATIM at once
      (drains staging, never strands a message).
    - Switch ON, fixed window: a group is due when now minus
      its FIRST message's received_ts is >= MERGE_WINDOW_SECONDS.
      Due singles go verbatim; due groups of >= 2 go as one
      merged record (manifest written first, then inbox record).
    - A group containing a local_only record is never merged:
      its members go verbatim.
    - If the inbox record already exists (a previous promotion
      wrote it but staging removal failed), staging is cleaned
      without rewriting, so promotion is idempotent.
    - Timeline "received" is recorded at promotion (the moment
      the Worker can first see the record), matching the old
      inbox-write moment. Constituent timestamps live in the
      merged record and manifest, not in the timeline.
    """
    promoted = []
    try:
        if now is None:
            now = time.time()
        mode_on = read_same_source_merge_mode()
        grouped = _merge_all_staged()
        for sender in sorted(grouped, key=str):
            docs = grouped[sender]
            if not docs:
                continue
            first_ts = float(docs[0].get("received_ts") or 0)
            if mode_on and (now - first_ts) < MERGE_WINDOW_SECONDS:
                continue
            mids = [str(d.get("message_id")) for d in docs]
            has_local_only = any(isinstance(d.get("record"), dict) and d["record"].get("local_only") for d in docs)
            try:
                if len(docs) >= 2 and mode_on and not has_local_only:
                    record, manifest = build_merged_record(docs)
                    anchor = str(docs[0].get("message_id"))
                    inbox_path = os.path.join(INBOX, "%s.json" % anchor)
                    if not os.path.exists(inbox_path):
                        os.makedirs(_merge_manifests_dir(), exist_ok=True)
                        atomic_write_json(os.path.join(_merge_manifests_dir(), "%s.json" % anchor), manifest, mode=0o600)
                        atomic_write_json(inbox_path, record, mode=0o600)
                        record_timeline_event("%s.json" % anchor, "received", source="serve_inbox_write")
                        # Gate 3 step 2: only the anchor row is
                        # mirrored (constituent timestamps live in
                        # the merged record and manifest, not here;
                        # the private manifest itself never enters
                        # the shadow -- it holds a token map).
                        _shadow_upsert_inbound(anchor, record)
                    _merge_remove_staged(mids)
                    promoted.append(anchor)
                    print(f"serve: merge promoted {len(mids)} messages as {anchor}.json", flush=True)
                else:
                    for doc in docs:
                        mid = str(doc.get("message_id"))
                        inbox_path = os.path.join(INBOX, "%s.json" % mid)
                        if not os.path.exists(inbox_path):
                            atomic_write_json(inbox_path, doc.get("record") or {}, mode=0o600)
                            record_timeline_event("%s.json" % mid, "received", source="serve_inbox_write")
                            # Gate 3 step 2: shadow inbound mirror.
                            _shadow_upsert_inbound(mid, doc.get("record") or {})
                        _merge_remove_staged([mid])
                        promoted.append(mid)
            except Exception as _prom_e:
                # Staging is retained: the next tick retries. Never
                # mark anything delivered that was not written.
                print(f"serve: merge promote FAILED for {sender}: {_sanitize_error(_prom_e)}", flush=True)
                continue
        return promoted
    except Exception:
        return promoted


def maybe_record_merge_completions():
    """Write completion records for merged anchors already sent.

    Lazy scanner (sender loop, after drain): for each manifest
    without a completion record, if outbox/sent/<anchor>.json
    exists, the formal reply for the whole group was sent, so
    record every constituent mid as completed. Never raises,
    never touches inbox/done (the Worker owns the done move),
    never deletes manifests (they are the audit trail).
    """
    try:
        mdir = _merge_manifests_dir()
        cdir = _merge_completed_dir()
        if not os.path.isdir(mdir):
            return
        for name in sorted(os.listdir(mdir)):
            if not name.endswith(".json"):
                continue
            anchor = name[:-5]
            try:
                if os.path.exists(os.path.join(cdir, name)):
                    continue
                if not os.path.exists(os.path.join(OUTBOX_SENT, "%s.json" % anchor)):
                    continue
                with open(os.path.join(mdir, name), encoding="utf-8") as f:
                    manifest = json.load(f)
                if not isinstance(manifest, dict):
                    continue
                os.makedirs(cdir, exist_ok=True)
                atomic_write_json(os.path.join(cdir, name), {
                    "schema": MERGE_COMPLETED_SCHEMA,
                    "anchor_message_id": anchor,
                    "merged_message_ids": manifest.get("merged_message_ids") or [anchor],
                    "formal_sent": True,
                    "completed_ts": _now_utc_iso(),
                }, mode=0o600)
            except Exception:
                continue
    except Exception:
        return


# ---- Gate 3 step 2: SQLite authority (design
# docs/sqlite-write-switch-design-DRAFT.md, sections 1-3 and 6).
# Default mode is json: every helper below is a no-op in json mode
# and the serve/drain loops behave exactly as before (zero behavior
# change, and no bridge.db file is ever created). Dual/shadow modes
# are operator actions (mode file), never flipped by this code.
# Two deliberate deviations from the transition table, both forced
# by compare-consistency (see bridge_sqlite.mirror_job_terminal):
# the table has no ready_to_send -> needs_check edge, yet pre-send
# terminal dispositions (recipient gate, sanitize, plan mismatch)
# must land in the same terminal state the JSON snapshot mapping
# reports, so they use a direct, audited update that never moves a
# row OUT of a terminal state. Likewise pending -> ready_to_send
# has no edge (claim is Worker-side), yet a pending-dir JSON file
# IS the production fact, so first-seen converges it directly.
# Two parent-decided amendments (2026-11-01, before any dual soak):
# (1) Retryable reset: a drain pass that keeps a job pending on an
# explicitly-undelivered Retryable failure resets the shadow row
# sending_in_flight -> ready_to_send with its send lease cleared
# (see _shadow_reset_for_retry), so the next drain can begin_send
# again under JSON's immediate 3-attempt retry. Without it, the job
# would be skipped by first-seen until its 120s lease expired, then
# locked to needs_check -- losing the retry entirely. Unknown /
# timeout / interrupt outcomes never reset: they go terminal ->
# needs_check as before, and terminal rows are never downgraded.
# (2) Benign inbound lag: the bridge never observes the Worker
# moving inbox/ -> done/, so a shadow inbound row stays "received"
# while JSON says "processed" (same record hash). Compare now
# reports that exact triple as benign_lag (listed separately, not
# a mismatch) instead of a difference -- so a dual soak CAN be
# compare-clean while the Worker processes messages. No done-sweep
# write point was added (design 3.1 still forbids off-table write
# points) and compare stays read-only.
_SQLITE_AUTHORITY_MODE = None  # cached at serve startup; None = unread
_SHADOW_LAST_LEASE_TICK = 0.0
_SHADOW_SEND_LEASE_SECONDS = 120  # == bridge_sqlite.begin_send default


def _shadow_root():
    return os.path.dirname(STATE)


def _shadow_db_path():
    return os.path.join(STATE, "bridge.db")


def refresh_sqlite_authority():
    """Read the mode file once and cache it. A running mode change
    does NOT take effect until serve restarts (design 2.1). Invalid
    values and a missing shadow module both fall back to json,
    loudly: an invalid value must never silently become shadow.
    Returns the mode. Never raises."""
    global _SQLITE_AUTHORITY_MODE
    if _SHADOW_MOD is None:
        _SQLITE_AUTHORITY_MODE = "json"
        try:
            with open(os.path.join(STATE, "sqlite_authority.json"),
                      encoding="utf-8") as f:
                _doc = json.load(f)
            if isinstance(_doc, dict) and _doc.get("mode") in ("dual", "shadow"):
                print("shadow: module unavailable, falling back to json mode (loud)", flush=True)
                try:
                    write_failure_record("sqlite_authority", "shadow",
                                         {"kind": "shadow_module_missing",
                                          "mode": _doc.get("mode")})
                except Exception:
                    pass
        except Exception:
            pass
        return "json"
    try:
        mode, valid = _SHADOW_MOD.read_authority_mode(_shadow_root())
    except Exception:
        mode, valid = "json", False
    if not valid:
        print("shadow: invalid authority mode value, falling back to json (never silently shadow)", flush=True)
        try:
            write_failure_record("sqlite_authority", "shadow",
                                 {"kind": "shadow_mode_invalid"})
        except Exception:
            pass
        mode = "json"
    _SQLITE_AUTHORITY_MODE = mode
    print(f"shadow: authority mode={mode}", flush=True)
    return mode


def get_sqlite_mode():
    if _SQLITE_AUTHORITY_MODE is None:
        return refresh_sqlite_authority()
    return _SQLITE_AUTHORITY_MODE


def _shadow_tracked(name):
    """Mirror json_snapshot's auxiliary-job exclusion: bridge-written
    receipt jobs (_progress/_offline/_queue) are NOT shadow-tracked
    and get no shadow rows in any mode. Anything else is tracked."""
    if _SHADOW_MOD is None:
        return False
    return _SHADOW_MOD._is_shadow_tracked_job_name(name)


def _shadow_mark(kind, job_key, extra=None):
    diag = {"kind": kind}
    if extra:
        diag.update(extra)
    try:
        write_failure_record(job_key, "shadow", diag)
    except Exception:
        pass


def _shadow_guarded(kind, job_key, fn):
    """Run fn(conn) with a fresh shadow connection: commit on success,
    rollback on error. Returns (True, result) or (False, None).
    Never raises. In json mode (or with no module) it returns
    (False, None) WITHOUT touching the DB or writing a marker: json
    mode means the shadow is not read at all."""
    if _SHADOW_MOD is None or get_sqlite_mode() == "json":
        return False, None
    try:
        conn = _SHADOW_MOD.connect_shadow(_shadow_db_path())
    except Exception as e:
        _shadow_mark("shadow_open_failed", job_key,
                     {"error": type(e).__name__})
        print(f"shadow: open failed for {job_key}: {_sanitize_error(e)}", flush=True)
        return False, None
    try:
        result = fn(conn)
        conn.commit()
        return True, result
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _shadow_mark(kind, job_key, {"error": type(e).__name__})
        print(f"shadow: {kind} failed for {job_key}: {_sanitize_error(e)}", flush=True)
        return False, None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _shadow_upsert_inbound(mid, record):
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None:
        return
    _shadow_guarded(
        "shadow_inbound_failed", mid,
        lambda conn: _SHADOW_MOD.upsert_inbound(
            conn, str(mid), "received",
            _SHADOW_MOD.record_sha256(record)))


def _shadow_upsert_job_ready(name, job):
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return
    key = name[:-5] if name.endswith(".json") else name
    _shadow_guarded(
        "shadow_job_ready_failed", key,
        lambda conn: _SHADOW_MOD.upsert_job_ready(
            conn, key, _SHADOW_MOD.record_sha256(job)))


def _shadow_mirror_segments(name, job):
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return
    key = name[:-5] if name.endswith(".json") else name
    _shadow_guarded(
        "shadow_segments_failed", key,
        lambda conn: _SHADOW_MOD.upsert_job_segments(conn, key, job))


def _shadow_lease_expired(lease_until):
    """Expired means: no lease at all (an in-flight row without a
    lease cannot prove it is still live), or a deadline at/before
    now. Fail-safe toward human review, never toward resend."""
    if lease_until is None:
        return True
    try:
        from datetime import datetime, timezone
        _text = str(lease_until)
        if _text.endswith("Z"):
            _text = _text[:-1] + "+00:00"
        _until = datetime.fromisoformat(_text)
        if _until.tzinfo is None:
            _until = _until.replace(tzinfo=timezone.utc)
        return _until <= datetime.now(timezone.utc)
    except Exception:
        return True


def _shadow_first_seen(name, job):
    """Drain first-seen shadow convergence (dual/shadow only).
    Returns "proceed", "skip", "archived_sent", or "archived_failed".
    Runs on EVERY drain pass for a pending job, not just the first:
    row states are idempotent, so re-evaluation is safe. Terminal
    shadow rows archive the JSON file here (never resend); in-flight
    unexpired rows skip this pass; leased rows skip for manual
    confirmation; a missing row is inserted as ready_to_send. A
    first-seen failure never blocks: dual fails open, and in shadow
    mode the send-start gate below is the fail-closed point."""
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return "proceed"
    key = name[:-5] if name.endswith(".json") else name
    path = os.path.join(OUTBOX_PENDING, name)
    ok, row = _shadow_guarded(
        "shadow_first_seen_failed", key,
        lambda conn: _SHADOW_MOD.get_job(conn, key))
    if not ok:
        return "proceed"
    if row is None:
        _shadow_upsert_job_ready(name, job)
        _shadow_mirror_segments(name, job)
        return "proceed"
    state = row["state"]
    if state in ("sent", "needs_check", "dead"):
        # Startup-reconcile convergence, applied in drain too: the
        # shadow already decided, so move the JSON file, never send.
        job["terminal"] = True
        if state == "sent":
            job["delivery_state"] = "sent"
        elif state == "needs_check":
            job["delivery_state"] = "needs_check"
            job["needs_check_reason"] = row.get("failure_code") or "shadow_terminal"
            job["failure_code"] = row.get("failure_code")
        else:
            job["delivery_state"] = "failed"
        try:
            atomic_write_json(path, job, mode=0o600)
        except Exception:
            pass
        _outcome, _ = _obs_job_outcome(job)
        record_timeline_event(
            name, "sent" if state == "sent" else (
                "needs_check" if _outcome == "needs_check" else "failed"),
            failure_code=_obs_failure_code_for_job(job),
            source="drain_shadow_archive")
        try:
            os.replace(path, os.path.join(
                OUTBOX_SENT if state == "sent" else OUTBOX_FAILED, name))
            print(f"outbox: shadow terminal {state} {name} archived (no resend)", flush=True)
            # The archive above mutated terminal flags, so refresh
            # the row hash to the moved file (compare consistency).
            _shadow_refresh_job_sha(name, job)
        except OSError as e:
            print(f"outbox: shadow archive move FAILED for {name}: {_sanitize_error(e)}; kept pending but terminal, will NOT resend", flush=True)
        return "archived_sent" if state == "sent" else "archived_failed"
    if state == "sending_in_flight":
        if not _shadow_lease_expired(row.get("lease_until")):
            _SEND_COOLDOWN[name] = time.time()
            print(f"outbox: shadow in-flight {name} skipped this pass (lease live)", flush=True)
            return "skip"
        # Lease expired: iron law locks it to needs_check, never
        # back to pending, never auto-resent.
        _shadow_guarded(
            "shadow_lease_lock_failed", key,
            lambda conn: _SHADOW_MOD.transition_job(
                conn, key, "needs_check",
                failure_code="lease_expired_in_flight"))
        job["delivery_state"] = "needs_check"
        job["needs_check_reason"] = "lease_expired_in_flight"
        job["failure_code"] = "lease_expired_in_flight"
        job["terminal"] = True
        try:
            atomic_write_json(path, job, mode=0o600)
        except Exception:
            pass
        record_timeline_event(name, "needs_check",
                              failure_code="delivery_unknown",
                              source="drain_shadow_lease")
        try:
            os.replace(path, os.path.join(OUTBOX_FAILED, name))
            print(f"outbox: shadow lease expired {name} locked to needs_check (no resend)", flush=True)
            _shadow_refresh_job_sha(name, job)
        except OSError as e:
            print(f"outbox: shadow lease move FAILED for {name}: {_sanitize_error(e)}; kept pending but terminal, will NOT resend", flush=True)
        return "archived_failed"
    if state == "leased_inferring":
        # Claim state has no bridge-side disposition (design 4.4):
        # skip for manual confirmation, never auto-move.
        _SEND_COOLDOWN[name] = time.time()
        print(f"outbox: shadow leased_inferring {name} skipped (manual confirmation required)", flush=True)
        return "skip"
    # ready_to_send: proceed (segments mirrored). pending: converged
    # to ready by upsert_job_ready (see the deviation note above).
    _shadow_upsert_job_ready(name, job)
    _shadow_mirror_segments(name, job)
    return "proceed"


def _shadow_begin_send_gate(name, job):
    """Send-start gate (design 1.4). Returns True if this pass may
    call send functions for the job. Json mode and untracked jobs
    always pass. Dual mode attempts the transition but never blocks
    on failure (fail-open). Shadow mode blocks on ANY begin_send
    failure: the job stays pending + 10s cooldown, never terminal,
    never sent."""
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return True
    key = name[:-5] if name.endswith(".json") else name
    ok, _row = _shadow_guarded(
        "shadow_begin_send_failed", key,
        lambda conn: _SHADOW_MOD.begin_send(conn, key))
    if ok:
        return True
    if get_sqlite_mode() == "shadow":
        _SEND_COOLDOWN[name] = time.time()
        print(f"outbox: shadow begin-send failed for {name}, NOT sending (cooldown 10s)", flush=True)
        return False
    return True


def _shadow_reset_for_retry(name):
    """Retryable reset (parent decision 2026-11-01). After a drain
    pass that kept the job pending on an explicitly-undelivered
    Retryable failure, move the shadow row sending_in_flight ->
    ready_to_send and clear its send lease, so the next drain can
    begin_send again (JSON's immediate 3-attempt retry). Fail-open
    in every mode: a reset failure only records a desensitized
    marker (via _shadow_guarded) and never blocks the JSON-side
    retry. Unknown / timeout / interrupt failures never reach here
    (they go terminal -> mirror needs_check); terminal rows are
    never touched (reset_for_retry refuses them). Segment rows are
    descriptive mirrors only and are untouched by the reset; the
    next drain's segment mirror refreshes them on proceed."""
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return
    key = name[:-5] if name.endswith(".json") else name
    _shadow_guarded(
        "shadow_reset_failed", key,
        lambda conn: _SHADOW_MOD.reset_for_retry(conn, key))


def _shadow_mirror_terminal(name, job, target_state, failure_code=None):
    """Mirror a job-level terminal disposition (fail-open in every
    mode: a mirror failure must never block an archive). Target
    states: "sent", "needs_check", "dead". The job's current record
    hash is stored too, so compare keeps matching the archived file
    (the hash is over the token-scrubbed record, never the record
    itself)."""
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return
    key = name[:-5] if name.endswith(".json") else name
    _shadow_guarded(
        "shadow_terminal_failed", key,
        lambda conn: _SHADOW_MOD.mirror_job_terminal(
            conn, key, target_state, failure_code,
            _SHADOW_MOD.record_sha256(job)))


def _shadow_refresh_job_sha(name, job):
    """Refresh one job row's record hash after the bridge mutated
    the JSON file's terminal flags around a move (first-seen
    archive, lease lock, startup reconcile). State untouched,
    fail-open, no-op in json mode or for untracked jobs."""
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None \
            or not _shadow_tracked(name):
        return
    key = name[:-5] if name.endswith(".json") else name
    _shadow_guarded(
        "shadow_sha_failed", key,
        lambda conn: _SHADOW_MOD.update_job_record_sha(
            conn, key, _SHADOW_MOD.record_sha256(job)))


def _shadow_startup_sync():
    """Apply reconcile_on_startup moves (design 2.4). Runs after
    recover_sending_on_startup, before new messages are taken.
    Guarded, never raises. Json mode does nothing."""
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None:
        return
    ok, report = _shadow_guarded(
        "shadow_reconcile_failed", "startup",
        lambda conn: _SHADOW_MOD.reconcile_on_startup(
            conn, _shadow_root()))
    if not ok or not isinstance(report, dict):
        return
    for key in report.get("move_to_sent", []):
        name = key + ".json"
        if name in _IN_MEMORY_HALTED_FILES:
            continue
        path = os.path.join(OUTBOX_PENDING, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                job = json.load(f)
        except Exception:
            continue
        job["terminal"] = True
        job["delivery_state"] = "sent"
        try:
            atomic_write_json(path, job, mode=0o600)
            os.replace(path, os.path.join(OUTBOX_SENT, name))
            record_timeline_event(name, "sent", source="startup_shadow_reconcile")
            print(f"outbox: startup reconcile moved {name} to sent/ (already delivered)", flush=True)
            _shadow_refresh_job_sha(name, job)
        except Exception as e:
            print(f"outbox: startup reconcile move FAILED for {name}: {_sanitize_error(e)}", flush=True)
    for item in report.get("move_to_failed", []):
        key = item.get("job_key") if isinstance(item, dict) else item
        state = item.get("state") if isinstance(item, dict) else "needs_check"
        code = item.get("failure_code") if isinstance(item, dict) else None
        name = key + ".json"
        if name in _IN_MEMORY_HALTED_FILES:
            continue
        path = os.path.join(OUTBOX_PENDING, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                job = json.load(f)
        except Exception:
            continue
        job["terminal"] = True
        if state == "needs_check":
            job["delivery_state"] = "needs_check"
            job["needs_check_reason"] = code or "shadow_terminal"
            job["failure_code"] = code
        else:
            job["delivery_state"] = "failed"
        try:
            atomic_write_json(path, job, mode=0o600)
            os.replace(path, os.path.join(OUTBOX_FAILED, name))
            _outcome, _ = _obs_job_outcome(job)
            record_timeline_event(
                name, "needs_check" if _outcome == "needs_check" else "failed",
                failure_code=_obs_failure_code_for_job(job),
                source="startup_shadow_reconcile")
            print(f"outbox: startup reconcile moved {name} to failed/ (shadow {state})", flush=True)
            _shadow_refresh_job_sha(name, job)
        except Exception as e:
            print(f"outbox: startup reconcile move FAILED for {name}: {_sanitize_error(e)}", flush=True)
    _left = len(report.get("left_inflight", []))
    _missing = len(report.get("missing_row", []))
    if _left or _missing:
        print(f"outbox: startup reconcile left {_left} in-flight (lease live), {_missing} without shadow row (report only)", flush=True)


def _shadow_lease_tick():
    """Lease recovery tick (U3: every 60s, from _sender_loop).
    Only expired leases move, and only in shadow rows -- JSON files
    for locked jobs are archived by drain first-seen. Guarded,
    never raises. Prints only when something actually moved."""
    global _SHADOW_LAST_LEASE_TICK
    if get_sqlite_mode() == "json" or _SHADOW_MOD is None:
        return
    now = time.time()
    if now - _SHADOW_LAST_LEASE_TICK < 60:
        return
    _SHADOW_LAST_LEASE_TICK = now
    ok, report = _shadow_guarded(
        "shadow_lease_failed", "lease_tick",
        lambda conn: _SHADOW_MOD.recover_expired_leases(conn))
    if ok and isinstance(report, dict) and (
            report.get("recovered_to_pending") or report.get("locked_to_needs_check")):
        print(f"outbox: shadow lease tick recovered {report['recovered_to_pending']} to pending, locked {report['locked_to_needs_check']} to needs_check", flush=True)


def drain_outbox(session):
    """Send pending outbox files, each output item independently.
    Audit 3: text and each attachment have their own persisted ID,
    client_id, status, attempts, and delivery_state. Before sending an
    item, delivery_state='sending' is persisted with its client_id, so
    a crash during send can be recovered as unknown on restart.
    Permanent -> failed immediately. Retryable (pre-send only) -> pending
    up to 3 attempts, then failed. Unknown (final send timeout/break) ->
    needs_check immediately, NO auto-retry. Fixed client_id is NOT
    claimed as a verified server dedup guarantee; it is only a
    persisted identifier for manual checking in WeChat.
    Returns (sent_names, failed_names) for typing cancel in serve_loop.
    Queue order: sorted filename (message_id ascending = arrival order).
    /ping and queue notices are NOT blocked behind Worker processing:
    Worker processing happens in the main loop, sender thread drains
    independently every 1s, so a queued /ping is sent within ~1s even
    if a large image is still being processed by Worker."""
    sent_names = []
    failed_names = []
    try:
        names = sorted(os.listdir(OUTBOX_PENDING))
    except OSError:
        return sent_names, failed_names
    for name in names:
        if not name.endswith(".json"):
            continue
        if name in _IN_MEMORY_HALTED_FILES:
            print(f"outbox: skipping halted (post-send write failed) {name}, manual check required", flush=True)
            continue
        _cd = _SEND_COOLDOWN.get(name, 0)
        if _cd and time.time() - _cd < 10:
            continue
        path = os.path.join(OUTBOX_PENDING, name)
        try:
            with open(path, encoding="utf-8") as f:
                _raw = f.read()
            job = json.loads(_raw)
        except (OSError, ValueError) as e:
            print(f"outbox: skipping unreadable {name}: {_sanitize_error(e)}", flush=True)
            continue
        if not isinstance(job, dict):
            # Gate 1 remainder (Worker permission tightening, strict
            # additive check): a pending file that parses as JSON
            # but is not an object can never be a valid task. Before
            # this guard, the first job.get() below raised
            # AttributeError, aborting this drain pass and starving
            # every later-sorted job on every tick until the file
            # was removed by hand. Park it as needs_check instead:
            # never sent, never re-entered. The original bytes are
            # preserved under failures/ for manual inspection; the
            # failed/ record is a dict-shaped terminal record so
            # the summary and the per-message view agree (needs_check).
            _shape = type(job).__name__
            _key = name[:-5] if name.endswith(".json") else name
            _parked = {
                "delivery_state": "needs_check",
                "needs_check_reason": "job_not_object",
                "terminal": True,
                "original_job_type": _shape,
            }
            try:
                atomic_write_text(
                    os.path.join(FAILURES_DIR, f"{_key}_job_not_object.orig"),
                    _raw, mode=0o600)
            except Exception as _jo_e:
                print(f"outbox: job-not-object evidence write failed for {name}: {_sanitize_error(_jo_e)}", flush=True)
            try:
                write_failure_record(
                    _key, "job_not_object",
                    {"kind": "job_not_object", "original_job_type": _shape})
            except Exception as _jo_e2:
                print(f"outbox: job-not-object marker write failed for {name}: {_sanitize_error(_jo_e2)}", flush=True)
            _TERMINAL_TASK_IDS.add(name)
            record_timeline_event(name, "needs_check", failure_code="job_not_object",
                                  source="drain_job_shape")
            try:
                atomic_write_json(os.path.join(OUTBOX_FAILED, name), _parked, mode=0o600)
                os.remove(path)
                print(f"outbox: non-object job {name} parked to failed/ as needs_check (no send)", flush=True)
            except OSError as _jo_e3:
                print(f"outbox: non-object job {name} park FAILED: {_sanitize_error(_jo_e3)}; kept in pending, will re-park next drain, never sent", flush=True)
            _shadow_mirror_terminal(name, _parked, "needs_check", "job_not_object")
            failed_names.append(name)
            continue
        # Gate 1 item 2: Worker-written jobs (<message_id>.json)
        # are written outside this code, so the bridge cannot
        # timestamp that write; the first drain sighting is
        # recorded instead (an upper bound on the queue time,
        # source drain_first_seen). Bridge-written suffix jobs
        # already recorded "queued" at their own write, so they
        # are not re-recorded here. The reader takes the earliest
        # event per stage, so a later sighting never overwrites
        # an exact bridge_write time.
        if name not in _OBS_QUEUED_SEEN:
            _OBS_QUEUED_SEEN.add(name)
            _qk = _obs_job_key(name)
            if _qk is not None and _qk.isdigit():
                record_timeline_event(name, "queued", source="drain_first_seen")
        # v1.0.1 + v1.0.2: a job already at a terminal status must
        # never be sent again, even if its earlier move to failed/
        # failed and it is still sitting in pending/. A terminal job
        # may only retry archiving, never re-enter sending.
        # Item 3 exception: a job the operator explicitly put back
        # via resend-text-part (manual_resend_part set, terminal
        # flag cleared on disk) is NOT terminal, even though this
        # long-running serve process still has its name in the
        # in-memory terminal set from when it archived the job.
        # Without this discard, a manual resend would be re-archived
        # without sending by the very serve that archived it.
        if job.get("manual_resend_part") is not None and not job.get("terminal"):
            _TERMINAL_TASK_IDS.discard(name)
        if name in _TERMINAL_TASK_IDS or _is_terminal_job(job):
            _TERMINAL_TASK_IDS.add(name)
            failed_names.append(name)
            # Gate 1 item 2: a job already terminal on disk is
            # archived, never resent; record its terminal outcome
            # (the original failure was recorded, if at all, when
            # it first became terminal; this event marks the
            # archive moment with the same derived code).
            _tb_outcome, _ = _obs_job_outcome(job)
            # Gate 3 step 2: mirror the already-terminal disposition
            # (kept_terminal when the row is already terminal).
            _shadow_mirror_terminal(
                name, job, "needs_check" if _tb_outcome == "needs_check" else "dead",
                _obs_failure_code_for_job(job))
            record_timeline_event(
                name, "needs_check" if _tb_outcome == "needs_check" else "failed",
                failure_code=_obs_failure_code_for_job(job), source="drain_terminal_archive")
            try:
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: terminal {name} archived to failed/ on retry (no resend)", flush=True)
            except OSError as e:
                print(f"outbox: skipping terminal {name}, archive retry failed: {_sanitize_error(e)}; will NOT resend", flush=True)
            continue
        # Gate 3 step 2: shadow first-seen convergence (dual/shadow
        # only; json mode returns "proceed" untouched). Terminal
        # shadow rows archive the JSON file here, never resend;
        # in-flight unexpired rows skip this pass; leased rows
        # skip for manual confirmation.
        _shadow_seen = _shadow_first_seen(name, job)
        if _shadow_seen == "skip":
            continue
        if _shadow_seen == "archived_sent":
            sent_names.append(name)
            continue
        if _shadow_seen == "archived_failed":
            failed_names.append(name)
            continue
        attachments = []
        if isinstance(job.get("attachments"), list):
            attachments = job["attachments"]
        elif job.get("image_path"):
            attachments = [{"path": job["image_path"], "task_id": job.get("task_id"),
                            "type": "image", "status": job.get("image_status", "pending")}]
        text = job.get("text")
        if not text and not attachments:
            continue
        changed = False
        # Retryable-reset flag (parent decision 2026-11-01): set by
        # any send branch below when a failure classifies Retryable
        # AND still has attempts left (status still "pending").
        # The keep-pending branch at the end of this pass resets
        # the shadow row only when this flag is set; Unknown /
        # timeout failures go terminal and never set it.
        _retryable_this_pass = False
        # Fix 2026-10-28: Worker writes pending with to_user_id from
        # session.json, but that file's key is ilink_user_id, not
        # to_user_id, so pending can arrive with to_user_id null.
        # R1 gate (2026-11-01, cross-root miswrite protection):
        # a recipient-less task is NO LONGER filled unconditionally.
        # Fill it only when this root holds a receipt record for
        # the task's message id (inbox/ or done/): that proves the
        # task belongs to a message this root actually received,
        # so legacy compat is fully preserved. A recipient-less
        # task with NO local receipt record has the exact shape of
        # a task miswritten here from another root: never send it
        # under this root's account, park it for manual check
        # instead (mirrors the recipient_mismatch park below, and
        # re-fires on every drain). This is deliberately NOT a
        # blanket rejection of recipient-less tasks: whether the
        # current Worker writes to_user_id on every task is
        # unproven (sent archives may be scrub-rewritten versions,
        # not Worker originals), so a blanket rejection could
        # stall normal replies in needs_check.
        if not job.get("to_user_id"):
            _gate_mid = name[:-5] if name.endswith(".json") else name
            for _gate_suffix in ("_local_deleted", "_local_answer",
                                 "_voice_prompt", "_local_del",
                                 "_local_keep", "_local_fail",
                                 "_progress", "_offline", "_queue",
                                 "_fast", "_local"):
                if _gate_mid.endswith(_gate_suffix):
                    _gate_mid = _gate_mid[: -len(_gate_suffix)]
                    break
            _gate_local = (
                os.path.isfile(os.path.join(INBOX, _gate_mid + ".json"))
                or os.path.isfile(os.path.join(DONE, _gate_mid + ".json"))
            )
            if _gate_local and session.get("ilink_user_id"):
                job["to_user_id"] = session.get("ilink_user_id")
                changed = True
            elif not _gate_local:
                try:
                    write_failure_record(
                        _gate_mid,
                        "recipient_missing",
                        {"kind": "recipient_missing",
                         "reason": "recipient_missing"})
                except Exception as _rm_e:
                    print(f"outbox: recipient-missing marker write failed for {name}: {_sanitize_error(_rm_e)}", flush=True)
                job["delivery_state"] = "needs_check"
                job["needs_check_reason"] = "recipient_missing"
                job["terminal"] = True
                _TERMINAL_TASK_IDS.add(name)
                try:
                    atomic_write_json(path, job, mode=0o600)
                except Exception as _rm_e2:
                    print(f"outbox: recipient-missing flag write failed for {name}: {_sanitize_error(_rm_e2)}", flush=True)
                # Gate 1 item 2: parked for manual check, never sent.
                record_timeline_event(name, "needs_check", failure_code="recipient_missing",
                                      source="drain_recipient_gate")
                try:
                    os.replace(path, os.path.join(OUTBOX_FAILED, name))
                    print(f"outbox: recipient missing {name} parked to failed/ (no send)", flush=True)
                except OSError as _rm_e3:
                    print(f"outbox: recipient-missing move FAILED for {name}: {_sanitize_error(_rm_e3)}; kept in pending but marked terminal, will NOT resend", flush=True)
                # Gate 3 step 2: mirror the pre-send terminal
                # disposition (never downgrades a terminal row).
                _shadow_mirror_terminal(name, job, "needs_check", "recipient_missing")
                failed_names.append(name)
                continue
            # else: local record exists but the session has no user
            # id; leave unfilled. The send functions fail closed
            # below exactly as before (no behavior change here).
        # GPT review 2026-10-31 (user decision: keep fallback, document
        # tradeoff): a pending job may carry a to_user_id from an older
        # login. Never send it under a different bound account: park it
        # for manual check instead. The check re-fires on every drain,
        # so even a kept-in-pending job can never slip through a send.
        _session_user = session.get("ilink_user_id")
        if job.get("to_user_id") and _session_user and job.get("to_user_id") != _session_user:
            job["delivery_state"] = "needs_check"
            job["needs_check_reason"] = "recipient_mismatch"
            job["terminal"] = True
            _TERMINAL_TASK_IDS.add(name)
            try:
                atomic_write_json(path, job, mode=0o600)
            except Exception as _mm_e:
                print(f"outbox: recipient-mismatch flag write failed for {name}: {_sanitize_error(_mm_e)}", flush=True)
            # Gate 1 item 2: parked for manual check, never sent.
            record_timeline_event(name, "needs_check", failure_code="recipient_mismatch",
                                  source="drain_recipient_check")
            try:
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: recipient mismatch {name} parked to failed/ (no send)", flush=True)
            except OSError as _mm_e2:
                print(f"outbox: recipient-mismatch move FAILED for {name}: {_sanitize_error(_mm_e2)}; kept in pending but marked terminal, will NOT resend", flush=True)
            # Gate 3 step 2: mirror the pre-send terminal disposition.
            _shadow_mirror_terminal(name, job, "needs_check", "recipient_mismatch")
            failed_names.append(name)
            continue
        # Gate 1 item 1 (outbound sanitize, 2026-10-31): strip
        # widget tags BEFORE any send, and never send a blank
        # bubble. This runs after the recipient check (which must
        # stay first) and before plan building / single-send, so:
        # - a fresh long text is sanitized first, then segmented:
        #   segment count and splice restore hold for the
        #   sanitized text, and no widget tag can survive in a part;
        # - an existing plan is NOT resliced and its top-level
        #   text is NOT rewritten (the top level is history, and
        #   reslicing a plan with sent parts would go needs_check):
        #   only the display_text of still-pending/blocked parts
        #   is sanitized, every drain, so a manually resent part
        #   is re-checked too. A pending/blocked part whose
        #   sanitized display carries no content becomes
        #   'skipped_empty' (never sent, never renumbered: labels
        #   stay as built, so a skipped part leaves a visible
        #   label gap such as [1/3] then [3/3] rather than a
        #   silently renumbered plan that would break index
        #   mapping; resend-text-part refuses skipped_empty parts,
        #   so they cannot be sent back by hand either).
        # - a job whose text sanitizes to empty is handled here:
        #   with no attachments it is archived to failed/ as a
        #   terminal skipped_empty job (never sent); with
        #   attachments it proceeds with text skipped and only
        #   the attachments sent. The failures/ marker carries
        #   lengths and reason only, never text or any token.
        _sanitize_existing_plan = _text_plan(job)
        _sanitize_orig_len = len(text) if isinstance(text, str) else 0
        if _sanitize_existing_plan is not None:
            for _sp in _sanitize_existing_plan:
                if not isinstance(_sp, dict):
                    continue
                if _sp.get("status") not in ("pending", "blocked_by_prior"):
                    continue
                _old_disp = _sp.get("display_text")
                if not isinstance(_old_disp, str):
                    continue
                _new_disp = sanitize_outbound_text(_old_disp)
                if _new_disp != _old_disp:
                    _sp["display_text"] = _new_disp
                    changed = True
                if _sanitized_part_is_empty(_new_disp):
                    _sp["status"] = "skipped_empty"
                    _sp["error_code"] = None
                    changed = True
            _sanitize_all_parts_skipped = bool(_sanitize_existing_plan) and all(
                isinstance(_sp, dict) and _sp.get("status") == "skipped_empty"
                for _sp in _sanitize_existing_plan)
            if _sanitize_all_parts_skipped and not attachments:
                _sanitized_top = sanitize_outbound_text(text) if isinstance(text, str) else ""
                try:
                    write_failure_record(
                        name[:-5] if name.endswith(".json") else name,
                        "outbound_sanitize",
                        {"kind": "skipped_empty", "reason": "skipped_empty",
                         "original_len": _sanitize_orig_len,
                         "sanitized_len": len(_sanitized_top),
                         "sanitized_stripped_len": len(_sanitized_top.strip()),
                         "parts_skipped": len(_sanitize_existing_plan)})
                except Exception as _sk_e:
                    print(f"outbox: skipped-empty marker write failed for {name}: {_sanitize_error(_sk_e)}", flush=True)
                job["text_status"] = "skipped_empty"
                job["delivery_state"] = "skipped_empty"
                job["terminal"] = True
                _TERMINAL_TASK_IDS.add(name)
                try:
                    atomic_write_json(path, job, mode=0o600)
                except Exception as _sk_e2:
                    print(f"outbox: skipped-empty flag write failed for {name}: {_sanitize_error(_sk_e2)}", flush=True)
                # Gate 1 item 2: sanitized-empty, never sent.
                record_timeline_event(name, "skipped_empty", failure_code="skipped_empty",
                                      source="drain_sanitize")
                try:
                    os.replace(path, os.path.join(OUTBOX_FAILED, name))
                    print(f"outbox: skipped empty text (plan) {name} archived to failed/ (no send)", flush=True)
                except OSError as _sk_e3:
                    print(f"outbox: skipped-empty move FAILED for {name}: {_sanitize_error(_sk_e3)}; kept in pending but marked terminal, will NOT resend", flush=True)
                # Gate 3 step 2: skipped_empty is a permanent
                # non-send (dead in the shadow mapping).
                _shadow_mirror_terminal(name, job, "dead", "skipped_empty")
                failed_names.append(name)
                continue
        elif isinstance(text, str) and _sanitize_orig_len > 0:
            _sanitized_text = sanitize_outbound_text(text)
            if _sanitized_text != text:
                text = _sanitized_text
                job["text"] = _sanitized_text
                changed = True
            if text.strip() == "":
                if not attachments:
                    try:
                        write_failure_record(
                            name[:-5] if name.endswith(".json") else name,
                            "outbound_sanitize",
                            {"kind": "skipped_empty", "reason": "skipped_empty",
                             "original_len": _sanitize_orig_len,
                             "sanitized_len": len(_sanitized_text),
                             "sanitized_stripped_len": len(_sanitized_text.strip())})
                    except Exception as _sk_e:
                        print(f"outbox: skipped-empty marker write failed for {name}: {_sanitize_error(_sk_e)}", flush=True)
                    job["text_status"] = "skipped_empty"
                    job["delivery_state"] = "skipped_empty"
                    job["terminal"] = True
                    _TERMINAL_TASK_IDS.add(name)
                    try:
                        atomic_write_json(path, job, mode=0o600)
                    except Exception as _sk_e2:
                        print(f"outbox: skipped-empty flag write failed for {name}: {_sanitize_error(_sk_e2)}", flush=True)
                    # Gate 1 item 2: sanitized-empty, never sent.
                    record_timeline_event(name, "skipped_empty", failure_code="skipped_empty",
                                          source="drain_sanitize")
                    try:
                        os.replace(path, os.path.join(OUTBOX_FAILED, name))
                        print(f"outbox: skipped empty text {name} archived to failed/ (no send)", flush=True)
                    except OSError as _sk_e3:
                        print(f"outbox: skipped-empty move FAILED for {name}: {_sanitize_error(_sk_e3)}; kept in pending but marked terminal, will NOT resend", flush=True)
                    # Gate 3 step 2: mirror the permanent non-send.
                    _shadow_mirror_terminal(name, job, "dead", "skipped_empty")
                    failed_names.append(name)
                    continue
                # Empty text with attachments: text is already ""
                # here, so the legacy single-send branch (which
                # requires a truthy text) is skipped, _text_sent
                # treats empty text as resolved, and only the
                # attachments are sent below. Record the skip.
                job["text_status"] = "skipped_empty"
                # Gate 1 item 2: text skipped, attachments proceed.
                record_timeline_event(name, "skipped_empty", failure_code="skipped_empty",
                                      source="drain_sanitize")
                try:
                    write_failure_record(
                        name[:-5] if name.endswith(".json") else name,
                        "outbound_sanitize",
                        {"kind": "skipped_empty", "reason": "skipped_empty",
                         "original_len": _sanitize_orig_len,
                         "sanitized_len": len(_sanitized_text),
                         "sanitized_stripped_len": len(_sanitized_text.strip())})
                except Exception as _sk_e:
                    print(f"outbox: skipped-empty marker write failed for {name}: {_sanitize_error(_sk_e)}", flush=True)
        # R2 P0 (B2-lite): the bridge session owns context_token.
        # A pending job may still carry a Worker-copied token from
        # the old contract; prefer the live session token and fall
        # back to the job's only during transition, so the Worker
        # never needs to handle a platform token at all.
        _job_token = session.get("context_token") or job.get("context_token")
        # Ensure persisted client_ids before any send.
        if text and not job.get("client_id"):
            job["client_id"] = uuid.uuid4().hex
        for att in attachments:
            if isinstance(att, dict) and not att.get("client_id"):
                att["client_id"] = uuid.uuid4().hex
        # Item 3 long-text plan handling. Short texts and old jobs
        # that were already attempted keep the legacy single-send
        # path below (_plan is None). A plan job sends ONLY its
        # display_text parts, in order, never the top-level text.
        _plan = None
        _plan_blocked = False
        _plan_halted = False
        if text:
            _existing_plan = _text_plan(job)
            if _existing_plan is not None:
                _plan = _existing_plan
                if job.get("full_text_sha256") != hashlib.sha256(text.encode("utf-8")).hexdigest():
                    if any(isinstance(p, dict) and p.get("status") in ("sent", "failed", "needs_check") for p in _plan):
                        # Stale plan with sent parts: never reslice.
                        job["delivery_state"] = "needs_check"
                        job["needs_check_reason"] = "text_plan_mismatch"
                        job["terminal"] = True
                        _plan_blocked = True
                        changed = True
                    else:
                        try:
                            _parts, _sha = _segment_text(text)
                        except LongTextTooManyParts as _e:
                            job["text_status"] = "failed"
                            job["text_error"] = "text too long: %s, deliver as file instead" % _e
                            job["terminal"] = True
                            _plan_blocked = True
                            changed = True
                        except Exception as _e:
                            _SEND_COOLDOWN[name] = time.time()
                            print(f"outbox: plan rebuild failed, skipping send (cooldown 10s): {_sanitize_error(_e)}", flush=True)
                            continue
                        else:
                            job["text_plan_version"] = TEXT_PLAN_VERSION
                            job["text_parts"] = _parts
                            job["full_text_sha256"] = _sha
                            _plan = _parts
                            try:
                                atomic_write_json(path, job, mode=0o600)
                            except Exception as _e:
                                _SEND_COOLDOWN[name] = time.time()
                                print(f"outbox: plan write failed, zero sends (cooldown 10s): {_sanitize_error(_e)}", flush=True)
                                continue
            elif len(text) > TEXT_DISPLAY_BUDGET and _plan_never_attempted(job):
                try:
                    _parts, _sha = _segment_text(text)
                except LongTextTooManyParts as _e:
                    job["text_status"] = "failed"
                    job["text_error"] = "text too long: %s, deliver as file instead" % _e
                    job["terminal"] = True
                    _plan_blocked = True
                    changed = True
                except Exception as _e:
                    _SEND_COOLDOWN[name] = time.time()
                    print(f"outbox: plan build failed, zero sends (cooldown 10s): {_sanitize_error(_e)}", flush=True)
                    continue
                else:
                    job["text_plan_version"] = TEXT_PLAN_VERSION
                    job["text_parts"] = _parts
                    job["full_text_sha256"] = _sha
                    _plan = _parts
                    try:
                        atomic_write_json(path, job, mode=0o600)
                    except Exception as _e:
                        _SEND_COOLDOWN[name] = time.time()
                        print(f"outbox: plan write failed, zero sends (cooldown 10s): {_sanitize_error(_e)}", flush=True)
                        continue
        if _plan is not None and not _plan_blocked:
            _part_bad = False
            _part_retry = False
            for _part in _plan:
                if not isinstance(_part, dict):
                    continue
                _pstatus = _part.get("status")
                if _pstatus == "blocked_by_prior":
                    # Tail resumes only once every earlier part is
                    # sent (or sanitizer-skipped); otherwise it stays
                    # blocked.
                    if all(isinstance(p, dict) and p.get("status") in ("sent", "skipped_empty") for p in _plan[:_part.get("index", 0)]):
                        _part["status"] = "pending"
                        _pstatus = "pending"
                        changed = True
                    else:
                        continue
                if _pstatus in ("sent", "skipped_empty"):
                    # Gate 1 item 1: a sanitizer-skipped part is
                    # resolved like a sent part: never sent, never
                    # blocking later parts or attachments.
                    continue
                if _pstatus in ("failed", "needs_check"):
                    _part_bad = True
                    continue
                if _pstatus == "sending":
                    # Result unknown at drain start (e.g. a halted
                    # job re-entering drain): never auto-resend it.
                    _part["status"] = "needs_check"
                    _part["error_code"] = "restart_unknown"
                    _part_bad = True
                    changed = True
                    continue
                # Pending part: persist 'sending' BEFORE the call.
                _part["status"] = "sending"
                job["delivery_state"] = "sending"
                changed = True
                try:
                    atomic_write_json(path, job, mode=0o600)
                except Exception as _e:
                    _part["status"] = "pending"
                    job["delivery_state"] = "pending"
                    _SEND_COOLDOWN[name] = time.time()
                    print(f"outbox: part sending-state write failed, skipping send (cooldown 10s): {_sanitize_error(_e)}", flush=True)
                    _part_retry = True
                    break
                try:
                    _part["attempts"] = int(_part.get("attempts", 0)) + 1
                    send_text(session, _part["display_text"], to_user_id=job.get("to_user_id"),
                              context_token=_job_token,
                              client_id=_part.get("client_id"))
                except TokenExpired:
                    raise
                except Exception as _e:
                    _status, _unk = _classify_delivery_error(_e)
                    if _status == "pending":
                        if _part["attempts"] >= 3:
                            _part["status"] = "failed"
                            _part["error_code"] = "retry_limit"
                            _part_bad = True
                        else:
                            _part["status"] = "pending"
                            _part["error_code"] = "retryable"
                            _part_retry = True
                            _retryable_this_pass = True
                    else:
                        _part["status"] = _status
                        _part["error_code"] = "permanent" if _status == "failed" else "unknown_delivery"
                        _part_bad = True
                    job["delivery_state"] = _part["status"]
                    changed = True
                    print(f"outbox: text part {_part.get('index')} {_part['status']} for {name}: {_sanitize_error(_e)}", flush=True)
                    try:
                        atomic_write_json(path, job, mode=0o600)
                        changed = False
                    except Exception as _we:
                        _IN_MEMORY_HALTED_FILES.add(name)
                        print(f"outbox: HALTED {name} after part state write failure, will not auto-resend", flush=True)
                        _plan_halted = True
                    break
                else:
                    _part["status"] = "sent"
                    _part["error_code"] = None
                    job["delivery_state"] = "pending"
                    changed = True
                    try:
                        atomic_write_json(path, job, mode=0o600)
                        changed = False
                    except Exception as _we:
                        _IN_MEMORY_HALTED_FILES.add(name)
                        print(f"outbox: HALTED {name} after part state write failure, will not auto-resend", flush=True)
                        _plan_halted = True
                    if _plan_halted:
                        break
                    # Otherwise the loop continues to the next
                    # part in order; do NOT break on success.
            if _plan_halted:
                continue
            if all(isinstance(p, dict) and p.get("status") in ("sent", "skipped_empty") for p in _plan):
                # Gate 1 item 1: "sent" here means every sendable
                # part was sent; skipped_empty parts are excluded
                # from that claim. If no part was actually sent
                # (all skipped), the text itself was skipped, not
                # sent, and attachments below still proceed.
                if all(isinstance(p, dict) and p.get("status") == "skipped_empty" for p in _plan):
                    job["text_status"] = "skipped_empty"
                else:
                    job["text_status"] = "sent"
                changed = True
            elif _part_bad:
                for _p in _plan:
                    if isinstance(_p, dict) and _p.get("status") == "pending":
                        _p["status"] = "blocked_by_prior"
                        changed = True
                for _att in attachments:
                    if isinstance(_att, dict) and _att.get("status") not in ("sent", "failed", "needs_check"):
                        _att["status"] = "blocked_by_prior"
                        changed = True
                job["text_status"] = "needs_check" if any(
                    isinstance(p, dict) and p.get("status") == "needs_check" for p in _plan) else "failed"
                job["delivery_state"] = "needs_check"
                job["needs_check_reason"] = "text_part_failed_or_unknown"
                _plan_blocked = True
            else:
                # A retryable part (or a pre-send write failure) is
                # still pending: later parts and attachments wait.
                _plan_blocked = True
        if _plan_halted:
            continue
        # Gate 3 step 2: send-start gate. Only when this pass will
        # actually attempt a send: in shadow mode the
        # ready_to_send -> sending_in_flight transition must succeed
        # first, or nothing is sent this pass (job stays pending +
        # 10s cooldown, never terminal); in dual mode a failed
        # transition is fail-open (JSON decision stands). Blocked
        # jobs (plan blocked, nothing sendable) skip the gate and
        # reach the terminal/keep logic below untouched.
        _will_send = False
        if _plan is not None and not _plan_blocked:
            for _p in _plan:
                if not isinstance(_p, dict):
                    continue
                if _p.get("status") == "pending":
                    _will_send = True
                    break
                if _p.get("status") == "blocked_by_prior" and all(
                        isinstance(q, dict) and q.get("status") in ("sent", "skipped_empty")
                        for q in _plan[:_p.get("index", 0)]):
                    _will_send = True
                    break
        if _plan is None and not _plan_blocked and text and job.get("text_status") not in ("sent", "failed", "needs_check"):
            _will_send = True
        if not _plan_blocked and any(isinstance(a, dict) and a.get("status") == "pending" for a in attachments):
            _will_send = True
        if _will_send and not _shadow_begin_send_gate(name, job):
            continue
        # Text item. v1.0.2 sub-item interception: a failed/needs_check
        # text item is terminal on its own, even when other items in
        # the same job are still pending.
        if _plan is None and not _plan_blocked and text and job.get("text_status") not in ("sent", "failed", "needs_check"):
            job["delivery_state"] = "sending"
            _state_ok = True
            try:
                atomic_write_json(path, job, mode=0o600)
            except Exception as _state_e:
                _state_ok = False
                _SEND_COOLDOWN[name] = time.time()
                print(f"outbox: sending-state write failed, skipping send (cooldown 10s): {_state_e}", flush=True)
            try:
                if not _state_ok:
                    raise RuntimeError("sending-state write failed")
                send_text(session, text, to_user_id=job.get("to_user_id"),
                          context_token=_job_token,
                          client_id=job.get("client_id"))
                job["text_status"] = "sent"
                job["delivery_state"] = "sent"
                changed = True
            except TokenExpired:
                raise
            except Exception as e:
                status, _unknown = _classify_delivery_error(e)
                job["text_status"] = status
                job["delivery_state"] = status
                job["text_error"] = _sanitize_error(e)
                if status == "pending":
                    job["text_attempts"] = int(job.get("text_attempts", 0)) + 1
                    if job["text_attempts"] >= 3:
                        job["text_status"] = "failed"
                        job["delivery_state"] = "failed"
                # Gate 1 item 2: keep the delivery classification
                # drain already computed, as a stable code on the
                # job. Without this, a legacy text failure would
                # only leave text_status + free-form error text,
                # and the observability code would have to guess
                # between permanent / retryable / unknown.
                _tcode = {"failed": "send_permanent", "pending": "send_retryable",
                          "needs_check": "send_unknown"}.get(status)
                if status == "pending" and job.get("text_attempts", 0) >= 3:
                    _tcode = "send_retry_limit"
                if _tcode is not None:
                    job["text_error_code"] = _tcode
                if job.get("text_status") == "pending":
                    _retryable_this_pass = True
                changed = True
                print(f"outbox: text {status} for {name}: {_sanitize_error(e)}", flush=True)
        # Attachment items, each with its persisted client_id.
        # Item 3: attachments wait for the whole text (plan or
        # legacy) to be sent; a blocked plan job sends none here.
        for att in attachments:
            if _plan_blocked:
                break
            if not isinstance(att, dict):
                continue
            if att.get("status") == "blocked_by_prior" and _plan is not None and all(
                    isinstance(p, dict) and p.get("status") in ("sent", "skipped_empty") for p in _plan):
                att["status"] = "pending"
                changed = True
            # v1.0.2 sub-item interception, same rule as text above.
            if att.get("status") in ("sent", "failed", "needs_check", "blocked_by_prior"):
                continue
            att_path = att.get("path") or att.get("image_path")
            job["delivery_state"] = "sending"
            _state_ok = True
            try:
                atomic_write_json(path, job, mode=0o600)
            except Exception as _state_e:
                _state_ok = False
                _SEND_COOLDOWN[name] = time.time()
                print(f"outbox: sending-state write failed, skipping send (cooldown 10s): {_state_e}", flush=True)
            try:
                if not _state_ok:
                    raise RuntimeError("sending-state write failed")
                # R2 P0 F1: explicit dispatch. A file attachment
                # must NEVER fall through to send_image, and an
                # unknown type must NEVER be guessed: legacy jobs
                # without a type are images (the pre-type schema),
                # anything else unknown is a permanent failure.
                _att_type = att.get("type")
                if _att_type == "video":
                    send_video(session, att_path, job.get("to_user_id"), _job_token,
                               client_id=att.get("client_id"))
                elif _att_type == "file":
                    send_file(session, att_path, job.get("to_user_id"), _job_token,
                              client_id=att.get("client_id"))
                elif _att_type in (None, "image"):
                    send_image(session, att_path, job.get("to_user_id"), _job_token,
                               client_id=att.get("client_id"))
                else:
                    raise PermanentError(f"unknown attachment type: {_att_type!r}")
                att["status"] = "sent"
                att.pop("error", None)
                changed = True
            except TokenExpired:
                raise
            except Exception as e:
                status, _unknown = _classify_delivery_error(e)
                att["status"] = status
                att["error"] = _sanitize_error(e)
                if status == "pending":
                    att["attempts"] = int(att.get("attempts", 0)) + 1
                    if att["attempts"] >= 3:
                        att["status"] = "failed"
                # Gate 1 item 2: same stable-code rule as the
                # text branch above (see there for why).
                _acode = {"failed": "send_permanent", "pending": "send_retryable",
                          "needs_check": "send_unknown"}.get(status)
                if status == "pending" and att.get("attempts", 0) >= 3:
                    _acode = "send_retry_limit"
                if _acode is not None:
                    att["error_code"] = _acode
                if att.get("status") == "pending":
                    _retryable_this_pass = True
                changed = True
                print(f"outbox: {att.get('type') or 'image'} {status} for {name}: {_sanitize_error(e)}", flush=True)
        if attachments:
            job["attachments"] = attachments
        # Item 3: for a plan job, text is sent only when every part
        # is sent; the legacy text_status field is set to 'sent'
        # by the part loop in that case, but check the parts
        # directly so a stale text_status cannot fake all_sent.
        if _plan is not None:
            # Gate 1 item 1: skipped_empty parts count as resolved
            # here (their all-skipped, no-attachment case was
            # already archived before the plan loop, so reaching
            # here with all skipped means attachments decided the
            # outcome, and the text_status set above stays
            # 'skipped_empty', never rewritten to 'sent').
            _text_sent = all(isinstance(p, dict) and p.get("status") in ("sent", "skipped_empty") for p in _plan)
        else:
            _text_sent = (not text or job.get("text_status") == "sent")
        all_sent = _text_sent and all(
            a.get("status") == "sent" for a in attachments)
        # Fix 2026-10-28: attachment loop sets delivery_state="sending"
        # before each send but never sets it back to "sent" on success,
        # so sent files were left with delivery_state="sending".
        if all_sent:
            job["delivery_state"] = "sent"
            changed = True
        # v1.0.2 boundary fix (user review): do NOT archive on
        # any_terminal alone. If one attachment is RetryableError
        # (status pending, attempts < 3) and another is PermanentError
        # (status failed), any_terminal is True, but archiving now
        # would set terminal=True and move the whole job to failed/,
        # losing the retryable item's remaining retries. Archive only
        # when _is_terminal_job(job) is True, i.e. every sub-item is
        # already sent/failed/needs_check and at least one is
        # failed/needs_check. A still-pending sub-item keeps the job
        # in pending/; the next drain skips the terminal sub-items
        # (sub-item interception) and retries only the pending one.
        # If "one permanent failure = whole job to manual handling"
        # were the intended design, any_terminal would be enough;
        # the chosen design is "retryable sub-items run out their
        # 3 attempts first", which is also what drain_outbox's
        # retryable-pending logic promises.
        _final_write_ok = True
        if changed:
            try:
                atomic_write_json(path, job, mode=0o600)
            except Exception as e:
                _final_write_ok = False
                print(f"outbox: final state write failed for {name}, keeping pending and aborting this job: {e}", flush=True)
        if not _final_write_ok:
            # Gemini R5 post-send: if the job may already have been
            # sent but its final state cannot be persisted, do NOT
            # leave it as a normal pending file (sender loop would
            # resend it every second). Halt it in memory for this
            # process lifetime and require manual check.
            _IN_MEMORY_HALTED_FILES.add(name)
            print(f"outbox: HALTED {name} after post-send write failure, will not auto-resend", flush=True)
            continue
        # R2 P0 (B2-lite): never archive a platform token into
        # sent/ or failed/, where the Worker could read it back.
        # GPT review 2026-10-31: a scrub-write failure must not block
        # delivery of an already-sent job, but it must also never be
        # reported as desensitized. The pending file on disk still
        # carries the token, and the archive move below carries it
        # into sent/failed/; _scrub_job_token records that durably in
        # failures/ (field names only, never the token value) so the
        # archived record is never mistaken for clean.
        _scrub_job_token(path, job)
        if all_sent:
            # Item: evidence audit. Only a job that reached the
            # sent terminal state is audited, once, here: for a
            # segmented long-text job all_sent is true only after
            # every part and every attachment is sent, so a plan
            # job is audited once, never once per part. Audit
            # failure never blocks archiving: write_evidence_audit
            # never raises, and this guard swallows even that.
            try:
                write_evidence_audit(name, job.get("text"))
            except Exception as _audit_e:
                print(f"outbox: evidence audit skipped for {name}: {_sanitize_error(_audit_e)}", flush=True)
            # Gate 1 item 2: terminal sent moment, once per job
            # (plan jobs reach here only after every part and
            # every attachment is sent, like the audit above).
            # Gate 3 step 2: shadow terminal FIRST (fail-open in
            # every mode: a mirror failure must never block an
            # already-sent job's archive), then the JSON move.
            _shadow_mirror_terminal(name, job, "sent")
            _shadow_mirror_segments(name, job)
            record_timeline_event(name, "sent", source="drain_archive")
            os.replace(path, os.path.join(OUTBOX_SENT, name))
            sent_names.append(name)
            print(f"outbox: sent {name}", flush=True)
        elif _is_terminal_job(job):
            # v1.0.1 + v1.0.2: mark terminal in-place first (text/att
            # status are already failed/needs_check in the persisted
            # job), persist job['terminal']=True on disk, then try to
            # move. If the move itself fails, the next drain (even in
            # a new process, after the memory set is cleared) still
            # recognises the job as terminal from disk and never resends.
            _TERMINAL_TASK_IDS.add(name)
            job["terminal"] = True
            try:
                atomic_write_json(path, job, mode=0o600)
            except Exception as e:
                print(f"outbox: terminal flag write failed for {name}: {_sanitize_error(e)}", flush=True)
            # Gate 1 item 2: terminal failed/needs_check moment
            # with the stable code derived from the job itself.
            _tf_outcome, _ = _obs_job_outcome(job)
            # Gate 3 step 2: shadow terminal before the JSON move.
            _shadow_mirror_terminal(
                name, job, "needs_check" if _tf_outcome == "needs_check" else "dead",
                _obs_failure_code_for_job(job))
            _shadow_mirror_segments(name, job)
            record_timeline_event(
                name, "needs_check" if _tf_outcome == "needs_check" else "failed",
                failure_code=_obs_failure_code_for_job(job), source="drain_terminal_archive")
            try:
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: needs_check/failed {name} (moved to failed/, no auto-retry for unknown)", flush=True)
            except OSError as e:
                print(f"outbox: failed move FAILED for {name}: {_sanitize_error(e)}; kept in pending but marked terminal, will NOT resend", flush=True)
            failed_names.append(name)
        else:
            # Retryable reset (parent decision 2026-11-01): this
            # pass kept the job pending on an explicitly-undelivered
            # Retryable failure, so reset the shadow row to
            # ready_to_send (lease cleared) for the next drain.
            # Unknown / timeout failures never reach here (they
            # went terminal above); a reset failure only records a
            # desensitized marker and never blocks this retry.
            if _retryable_this_pass:
                _shadow_reset_for_retry(name)
            failed_names.append(name)
            print(f"outbox: keeping {name} in pending (retryable, retry next loop)", flush=True)
    return sent_names, failed_names


import threading as _threading_global
_OUTBOX_LOCK = _threading_global.RLock()
_TYPING_LOCK = _threading_global.Lock()
_TYPING_MAP = {}

def _sender_loop(session, stop_event, sent_queue):
    """R8 fix: independent sender thread. The main loop's getupdates
    long poll (45s) must not block Worker replies that are already in
    outbox/pending. This thread drains outbox every 1s and reports
    sent/failed names via sent_queue for typing cancellation.
    drain_outbox remains the only sender; the thread does not
    generate replies, it only sends already-queued files.
    """
    import queue as _q
    import threading as _th
    while not stop_event.is_set():
        try:
            with _OUTBOX_LOCK:
                sent_names, failed_names = drain_outbox(session)
                # Gate 1 item 3: after draining, a formal job that
                # only just arrived has been sent and suppresses
                # its progress note; a message still without a
                # formal job past 5s gets exactly one note. The
                # call never raises and never blocks sending.
                # Gate 1 item 4 (grace revision): offline
                # receipts run first so a message that just
                # earned one is suppressed from progress in the
                # same tick; progress self-guards per message on
                # offline ack/job too, so the order is defense
                # in depth, not the only guard.
                # Gate 2 same-source merge: promote due staged
                # groups BEFORE offline/progress evaluate, so a
                # just-promoted record gets a fresh received event
                # and cannot earn a spurious receipt in this tick.
                # Completions run after drain, so a formal job
                # archived in this tick can complete its group.
                # Both helpers never raise; the guards are belts
                # and braces, like the receipt calls.
                try:
                    maybe_promote_merge_groups()
                except Exception:
                    pass
                try:
                    maybe_send_offline_receipts(session)
                except Exception:
                    pass
                try:
                    maybe_send_progress_receipts(session)
                except Exception:
                    pass
                try:
                    maybe_record_merge_completions()
                except Exception:
                    pass
                # Gate 3 step 2: lease recovery tick (U3: every
                # 60s, guarded; only expired leases move, and only
                # in shadow rows -- JSON files for locked jobs are
                # archived by drain first-seen).
                try:
                    _shadow_lease_tick()
                except Exception:
                    pass
            for _n in sent_names + failed_names:
                try:
                    sent_queue.put(_n, timeout=0.1)
                except Exception:
                    pass
                # Gemini R8: typing cancel must NOT wait for main loop's
                # 45s getupdates to return. Sender thread (sole network
                # sender) cancels typing itself using the shared map.
                _mid = _n[:-5] if _n.endswith(".json") else _n
                # Only cancel for message_id jobs, not _fast/_queue/_voice/_image_fail suffix jobs
                # (those suffix jobs share the same base? Actually fast jobs are <mid>_fast.json,
                #  their base is not a typing entry, so lookup will miss safely.)
                with _TYPING_LOCK:
                    _ticket = _TYPING_MAP.pop(_mid, None)
                if _ticket:
                    try:
                        send_typing(session, _ticket, 2,
                                    to_user_id=session.get("ilink_user_id"),
                                    context_token=session.get("context_token") or "")
                    except Exception:
                        pass
        except TokenExpired as _e:
            # Gemini session lock: sender thread 401/-14 must stop
            # itself and let main loop exit, not keep draining.
            try:
                write_needs_relogin("sender", str(_e))
            except Exception:
                pass
            stop_event.set()
            print(f"serve: sender thread token expired, stopping sender: {_e}", flush=True)
            break
        except Exception as _e:
            print(f"serve: sender thread drain error (continuing): {_e}", flush=True)
        stop_event.wait(1.0)


# v1.5.0 item 2 (backoff tiers, GPT conditional pass 2026-11-01):
# Tiered getupdates backoff, getupdates ONLY (never sendmessage).
# Sequence is fixed: consecutive rate_limited polls wait 5, 10, 20,
# 30, then 30 s capped. 30 s is a LOCAL backoff cap only: it covers
# neither request/transport fallback, nor later polls, nor Worker or
# send waits, and must never be described as an end-to-end delay bound.
# MINIMAL TIER ONLY (GPT boundary, documented, not silently complete):
# this tiers only rate_limited already identified by
# classify_getupdates_error. The curl helper does not preserve full
# HTTP status or Retry-After, and post_json may fall back to urllib
# immediately after a curl failure, so a raw HTTP 429 / Retry-After
# that the classifier does not recognise is NOT covered here. The
# full tier (structured status on both transports, honour Retry-After
# even above 30 s, no immediate transport switch after a confirmed
# limit) is a separate follow-up, not claimed by this change.
# -14 (TokenExpired) never enters backoff: its branch returns 14
# before this code runs, and classification priority keeps it first.
_RATE_LIMIT_DELAYS = (5, 10, 20, 30, 30)


def _rate_limit_delay(streak):
    """Backoff seconds for the streak-th consecutive rate_limited poll
    (1-based). Zero or negative streaks mean 'not in a streak' -> 5 s."""
    if streak <= 0:
        return _RATE_LIMIT_DELAYS[0]
    return _RATE_LIMIT_DELAYS[min(streak, len(_RATE_LIMIT_DELAYS)) - 1]


def _interruptible_sleep(seconds, stop_predicate):
    """Sleep in 0.5 s slices. Returns True as soon as stop_predicate
    fires (the poll loop's own stop signal), else False after the
    full duration. Never a single uninterruptible sleep(30)."""
    deadline = time.monotonic() + seconds
    while True:
        if stop_predicate():
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.5, remaining))


def serve_loop():

    ensure_dirs()
    if os.path.exists(NEEDS_RELOGIN_FILE):
        print("serve: NEEDS_RELOGIN exists, refusing to start. Re-login required.", flush=True)
        return 3
    session = load_session()
    if not session or not session.get("bot_token") or not session.get("ilink_user_id"):
        print("serve: no session.json, run `bridge.py login` first.", flush=True)
        return 3
    # flock: hold for the whole process life. On Windows fcntl does not
    # exist, so use msvcrt locking if available, else proceed with a
    # warning (local login use only runs one serve at a time anyway).
    lock_f = open(LOCK_FILE, "a+")
    if fcntl is not None:
        try:
            fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print("serve: another serve process holds serve.lock, exiting (no double polling).", flush=True)
            return 2
    else:
        try:
            import msvcrt
            msvcrt.locking(lock_f.fileno(), msvcrt.LK_NBLCK, 1)
        except Exception:
            print("serve: warning, no file lock on Windows, make sure only one serve runs.", flush=True)
    # GPT ruling (blocking): instance identity BEFORE the PID file
    # points at this process. If init fails, abort startup with a
    # non-zero code and NEVER write serve.pid. Do NOT treat this as
    # token expiry (-14 / NEEDS_RELOGIN).
    try:
        _init_channel_status()
    except Exception as _init_e:
        _safe_warn(f"serve: FATAL, channel identity init failed, aborting startup (no serve.pid written): {_init_e}")
        return 1
    _log_ts(f"serve: lock acquired, polling. Ctrl-C to stop. pid={os.getpid()}")
    # Step 1: PID file so the watchdog hook can check liveness without
    # matching its own shell command line. A stale PID file alone is
    # NOT proof of life; the hook must also kill -0 the PID.
    try:
        atomic_write_text(os.path.join(STATE, "serve.pid"), f"{os.getpid()}\n", mode=0o600)
    except Exception:
        pass
    # Gate 3 step 2: read the authority mode BEFORE any recovery or
    # promotion touches shadow state; a running mode change needs a
    # restart (design 2.1). Never raises (refresh falls back to json).
    try:
        refresh_sqlite_authority()
    except Exception:
        pass
    # Audit 3: recover any sending-state left by a crashed previous serve.
    recover_sending_on_startup()
    # Gate 3 step 2: startup reconciliation (design 2.4): shadow
    # decisions already made are applied to JSON files here, before
    # new messages are taken. Guarded, never raises.
    try:
        _shadow_startup_sync()
    except Exception:
        pass
    # Gate 2 same-source merge: promote staged groups left by a
    # crashed previous serve (expired groups merge, unexpired
    # ones wait for the sender thread's 1s tick). With the switch
    # OFF, staged records are promoted verbatim, never stranded.
    # Never raises.
    try:
        maybe_promote_merge_groups()
    except Exception:
        pass
    # R8: start independent sender thread before the poll loop.
    import queue as _queue_mod
    import threading as _thread_mod
    _sender_stop = _thread_mod.Event()
    _sender_queue = _queue_mod.Queue()
    _sender_thread = _thread_mod.Thread(target=_sender_loop, args=(session, _sender_stop, _sender_queue), daemon=True)
    _sender_thread.start()
    processed = load_processed()
    # Phase 2 step 3: message_id -> typing ticket, for cancel-on-finish.
    typing_by_msg = {}
    # v1.5.0 item 2: consecutive rate_limited streak for tiered backoff.
    # Reset on any valid success (incl. empty msgs); any other error
    # breaks the streak (it is not counted as a new limit).
    _rate_limited_streak = 0
    while True:
        if os.path.exists(NEEDS_RELOGIN_FILE):
            _sender_stop.set()
            print("serve: NEEDS_RELOGIN appeared, stopping.", flush=True)
            return 14
        try:
            # R8: sender thread already drains outbox; here only
            # collect its sent/failed names for typing cancellation.
            sent_names, failed_names = [], []
            try:
                while True:
                    _n = _sender_queue.get_nowait()
                    # Names are message_id.json; typing map keys are message_id
                    sent_names.append(_n)
                    failed_names.append(_n)
            except Exception:
                pass
            # Typing cancel below uses sent+failed names; to avoid
            # double-counting, use a separate list for cancel.
            _cancel_names = list(sent_names)
            sent_names, failed_names = _cancel_names, []
            # A Worker reply is a sent/failed outbox file whose name is
            # exactly <message_id>.json (queue notices use _queue.json).
            # Cancel typing for those, do not re-run Worker.
            for _name in sent_names + failed_names:
                _mid = _name[:-5] if _name.endswith(".json") else _name
                if _mid in typing_by_msg:
                    _ticket = typing_by_msg.pop(_mid)
                    try:
                        send_typing(session, _ticket, 2,
                                    to_user_id=session.get("ilink_user_id"),
                                    context_token=session.get("context_token"))
                    except TokenExpired:
                        raise
                    except Exception:
                        pass
        except TokenExpired:
            _sender_stop.set()
            print("serve: token expired during outbox drain, stopping.", flush=True)
            return 14
        except Exception as e:
            print(f"serve: outbox drain error (continuing): {e}", flush=True)
        # TTL sweep: every poll loop, before any local delivery, so an
        # expired video can never be summarized after its deadline.
        try:
            sweep_local_ttl()
        except Exception as _sweep_e:
            print(f"serve: ttl sweep error (continuing): {_sweep_e}", flush=True)
        try:
            refresh_safe_history_view()
        except Exception as _sv_e:
            print(f"serve: safe view refresh error (continuing): {_sv_e}", flush=True)
        # M3 local follow-up v1: deliver completed local analyses.
        # This runs every poll loop, never blocks on analysis, and never
        # routes local content through the Worker.
        _pend = load_pending_local_queries()
        if _pend:
            _remain = []
            _pend_changed = False
            for _q in _pend:
                if is_local_deleted(_q.get("video_mid")):
                    # Anti-resurrection: a deleted video's summary is
                    # never delivered, even if an evidence file reappears.
                    try:
                        _ok = send_direct_or_outbox(session, "该本地视频已删除，证据摘要不再可用。", _q.get("to_user_id") or session.get("ilink_user_id"), _q.get("context_token") or "", f"{_q.get('query_mid')}_local_deleted.json")
                    except TokenExpired:
                        _sender_stop.set()
                        print("serve: token expired during local deleted notice, stopping.", flush=True)
                        return 14
                    if _ok:
                        _pend_changed = True
                        continue
                    _remain.append(_q)
                    continue
                # Item 1: a terminal failed/interrupted state is
                # reported promptly instead of waiting out the 1800 s
                # timeout; a ready state without evidence (vanished
                # between publish and delivery) is a failure too.
                _evp = os.path.join(_q.get("outdir", ""), "evidence.json")
                _astate = _read_analyzer_state(_q.get("outdir", ""), _q.get("run_id"))
                if _astate == "failed" or (_astate == "ready" and not os.path.exists(_evp)):
                    try:
                        _ok = send_direct_or_outbox(session, "本地分析失败，请重发本地问。", _q.get("to_user_id") or session.get("ilink_user_id"), _q.get("context_token") or "", f"{_q.get('query_mid')}_local_fail.json")
                    except TokenExpired:
                        _sender_stop.set()
                        print("serve: token expired during local fail notice, stopping.", flush=True)
                        return 14
                    if _ok:
                        _pend_changed = True
                        continue
                    _remain.append(_q)
                    continue
                if os.path.exists(_evp):
                    _summ = build_local_evidence_summary(_evp, _q.get("video_mid"))
                    if _summ:
                        try:
                            _ok = send_direct_or_outbox(session, _summ, _q.get("to_user_id") or session.get("ilink_user_id"), _q.get("context_token") or "", f"{_q.get('query_mid')}_local_answer.json")
                        except TokenExpired:
                            _sender_stop.set()
                            print("serve: token expired during local answer, stopping.", flush=True)
                            return 14
                        if _ok:
                            _pend_changed = True
                            continue
                    _remain.append(_q)
                elif time.time() - float(_q.get("started_ts") or 0) > 1800:
                    try:
                        send_direct_or_outbox(session, "本地分析超时或失败，请重发本地问。", _q.get("to_user_id") or session.get("ilink_user_id"), _q.get("context_token") or "", f"{_q.get('query_mid')}_local_fail.json")
                    except TokenExpired:
                        _sender_stop.set()
                        print("serve: token expired during local fail, stopping.", flush=True)
                        return 14
                    _pend_changed = True
                else:
                    _remain.append(_q)
            if _pend_changed:
                save_pending_local_queries(_remain)
        cursor = load_cursor()
        base = session.get("baseurl") or DEFAULT_BASE
        try:
            resp = post_json(
                base,
                "/ilink/bot/getupdates",
                {"get_updates_buf": cursor, "base_info": base_info()},
                session=session,
                authenticated=True,
                timeout=45,
            )
            check_ret(resp, "getupdates")
            _record_channel_status(True)
            # v1.5.0 item 2: any valid success response (incl. msgs=[])
            # resets the consecutive rate-limit streak.
            _rate_limited_streak = 0
        except TokenExpired as e:
            # Gate 2 -14 counting: the -14 stop itself is unchanged
            # (NEEDS_RELOGIN already written by check_ret/post_json,
            # no retry); only a desensitised category count is added.
            _cat, _det = classify_getupdates_error(e)
            record_getupdates_anomaly(_cat, _det)
            print("serve: token expired (-14), wrote NEEDS_RELOGIN, stopping.", flush=True)
            return 14
        except Exception as e:
            # v1.5.0 item 2: tiered backoff (replaces the flat 5 s sleep).
            # Gate 2 counting is unchanged; only the wait is tiered.
            # -14 never reaches here (TokenExpired branch returns 14).
            _cat, _det = classify_getupdates_error(e)
            record_getupdates_anomaly(_cat, _det)
            # v1.0.2: post_json now carries curl_err + urllib_err +
            # desensitized target/proxy; sanitise again as defence in depth.
            _record_channel_status(False, _sanitize_error(e))
            if _cat == "rate_limited":
                _rate_limited_streak += 1
                _delay = _rate_limit_delay(_rate_limited_streak)
            else:
                # Any other error breaks the consecutive rate-limit
                # sequence; it is not counted as a new limit.
                _rate_limited_streak = 0
                _delay = 5
            _log_ts(f"serve: getupdates error ({_cat}), backing off {_delay}s: {_sanitize_error(e)}")
            # Interruptible wait on the poll loop's own stop signal
            # (NEEDS_RELOGIN appearance, also checked at loop top).
            # The sender thread is independent (own 1 s tick) and is
            # never paused by this wait.
            if _interruptible_sleep(_delay, lambda: os.path.exists(NEEDS_RELOGIN_FILE)):
                _sender_stop.set()
                print("serve: stop requested during backoff, stopping.", flush=True)
                return 14
            continue
        new_cursor = resp.get("get_updates_buf")
        msgs = resp.get("msgs") or []
        _batch_enqueue_failed = False
        for msg in msgs:
            # Private-chat + whitelist filter, exact case-sensitive compare.
            if msg.get("group_id"):
                continue
            if msg.get("from_user_id") != session.get("ilink_user_id"):
                continue
            # Phase 2: text, voice transcription, or images, in order.
            # A message with both text and images keeps both, in order.
            text = extract_text(msg)
            voice_text = extract_voice(msg)
            is_voice_only = bool(has_voice_item(msg) and not text and not voice_text)
            if voice_text:
                text = (text + voice_text) if text else voice_text
            image_items = extract_images(msg)
            video_items = extract_video_items(msg)
            file_items = extract_file_items(msg)
            # M3 v1 toggle (WeChat videos carry no caption, so a caption
            # trigger alone cannot work): a text message "仅本地" or
            # "开启仅本地" turns local-only mode ON for all following videos;
            # "关闭仅本地" turns it OFF. The toggle message itself is a
            # normal text message (its reply confirms the mode); only videos
            # arriving while the mode is on are diverted to local_vault.
            _toggle = (text or "").strip()
            if _toggle in ("仅本地", "开启仅本地"):
                set_local_only_mode(True)
                print("serve: local-only mode ON", flush=True)
            elif _toggle == "关闭仅本地":
                set_local_only_mode(False)
                print("serve: local-only mode OFF", flush=True)
            if not text and not is_voice_only and not image_items and not video_items and not file_items:
                continue
            mid = str(msg.get("message_id"))
            if mid in processed:
                continue
            # R2 fix: durability order reversed from Phase 1. Do NOT
            # save processed/cursor before this message is durable.
            # The batch cursor (new_cursor) is only saved after the
            # whole batch loop, and processed is only saved after this
            # message's inbox file or direct-reply file is durable.
            # A crash mid-batch therefore re-fetches the same batch
            # and deduplicates already-processed messages, instead of
            # losing unprocessed messages in the batch.
            ctx = msg.get("context_token") or ""
            if ctx:
                session["context_token"] = ctx
            # M3 local follow-up v1: '本地问 <message_id>' is intercepted
            # here, never queued to inbox, never seen by the Worker.
            # v1 returns a deterministic evidence summary only.
            # TTL v1 commands, same interception as local queries: never
            # queued to inbox, never seen by the Worker.
            _local_del = parse_local_delete(text) if text else None
            if _local_del is not None:
                if is_local_deleted(_local_del):
                    _reply = "该本地视频已删除，无需重复删除。"
                elif find_local_video(_local_del) is None:
                    _reply = "本地视频不存在或已删除，请确认消息号是否正确。"
                else:
                    mark_local_deleted(_local_del, "manual")
                    _reply = f"本地视频 {_local_del} 已删除，证据与分析文件已一并清除。"
                try:
                    _queued = send_direct_or_outbox(session, _reply, session.get("ilink_user_id"), ctx, f"{mid}_local_del.json")
                except TokenExpired:
                    print("serve: token expired during local delete, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: local delete reply queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: local delete for {mid} -> video {_local_del}", flush=True)
                continue
            _local_keep = parse_local_keep(text) if text else None
            if _local_keep is not None:
                _keep_mid, _keep_days = _local_keep
                _ok, _reason = extend_local_ttl(_keep_mid, _keep_days)
                if _reason == "deleted":
                    _reply = "该本地视频已删除，无法延长。"
                elif _reason == "range":
                    _reply = "延长天数须在 1 到 30 天之间。"
                elif not _ok:
                    _reply = "本地视频不存在或已删除，请确认消息号是否正确。"
                else:
                    _reply = f"本地视频 {_keep_mid} 已延长 {_keep_days} 天（从现在起算）。"
                try:
                    _queued = send_direct_or_outbox(session, _reply, session.get("ilink_user_id"), ctx, f"{mid}_local_keep.json")
                except TokenExpired:
                    print("serve: token expired during local keep, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: local keep reply queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: local keep for {mid} -> video {_keep_mid} +{_keep_days}d", flush=True)
                continue
            _local_q = parse_local_query(text) if text else None
            if _local_q is not None:
                _video_mid, _q_text = _local_q
                if is_local_deleted(_video_mid):
                    _reply = "该本地视频已删除，证据摘要不再可用。"
                    try:
                        _queued = send_direct_or_outbox(session, _reply, session.get("ilink_user_id"), ctx, f"{mid}_local.json")
                    except TokenExpired:
                        print("serve: token expired during local query, stopping.", flush=True)
                        return 14
                    if not _queued:
                        _batch_enqueue_failed = True
                        print(f"serve: local query reply queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                        continue
                    processed.add(mid); save_processed(processed)
                    print(f"serve: local query for {mid} -> deleted video {_video_mid}", flush=True)
                    continue
                _vpath = find_local_video(_video_mid)
                if _vpath is None:
                    _reply = "本地视频不存在或已删除，请确认消息号是否正确。"
                else:
                    _evp = os.path.join(LOCAL_VAULT_DIR, _video_mid, "analysis", "evidence.json")
                    if os.path.exists(_evp):
                        _reply = build_local_evidence_summary(_evp, _video_mid) or "本地证据读取失败，请重发本地问。"
                    else:
                        _launched = start_local_analysis(_vpath, _video_mid)
                        if _launched is None:
                            _reply = "本地分析启动失败，请重发本地问。"
                        else:
                            _pend = [q for q in load_pending_local_queries() if q.get("video_mid") != _video_mid]
                            _pend.append({"query_mid": mid, "video_mid": _video_mid, "outdir": _launched["outdir"], "run_id": _launched["run_id"], "to_user_id": session.get("ilink_user_id"), "context_token": ctx, "started_ts": time.time()})
                            save_pending_local_queries(_pend)
                            _reply = "本地分析正在跑，跑完我把证据摘要发你，不用重发。"
                try:
                    _queued = send_direct_or_outbox(session, _reply, session.get("ilink_user_id"), ctx, f"{mid}_local.json")
                except TokenExpired:
                    print("serve: token expired during local query, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: local query reply queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: local query for {mid} -> video {_video_mid}", flush=True)
                continue
            # Tier-1 chat command table: same interception road as
            # the M3 local follow-up above (after group/whitelist
            # filter, before inbox/merge staging). /ping falls
            # through to the legacy fast path below; every other
            # matched /token is answered here and never reaches
            # the Worker.
            _cmd_reply = chat_command_reply(text) if text else None
            if _cmd_reply is not None:
                try:
                    _queued = send_direct_or_outbox(session, _cmd_reply, session.get("ilink_user_id"),
                                          ctx, f"{mid}_cmd.json")
                except TokenExpired:
                    print("serve: token expired during chat command, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: chat command reply queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: chat command for {mid}", flush=True)
                continue
            # Phase 2 step 3: fast commands, direct reply, no inbox, no Worker.
            fast = fast_command_reply(text) if text else None
            if fast is not None:
                # v1.0.1: send_direct_or_outbox returns False on queue
                # write failure. Only then is it safe to mark processed.
                # On failure, do NOT mark processed and do NOT advance
                # the batch cursor (see batch end), so the next fetch
                # retries this message instead of silently dropping it.
                try:
                    _queued = send_direct_or_outbox(session, fast, session.get("ilink_user_id"),
                                          ctx, f"{mid}_fast.json")
                except TokenExpired:
                    print("serve: token expired during fast reply, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: fast reply queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: fast reply for {mid}.json", flush=True)
                continue
            # Phase 2 step 2: voice with no transcription from WeChat.
            # Explicit prompt, recorded as processed, never fabricated.
            if is_voice_only:
                try:
                    _queued = send_direct_or_outbox(
                        session, "这条语音未收到微信端转写文字，请改发文字",
                        session.get("ilink_user_id"), ctx, f"{mid}_voice_prompt.json")
                except TokenExpired:
                    print("serve: token expired during voice prompt, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: voice prompt queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: voice prompt for {mid}.json", flush=True)
                continue
            # Phase 2 step 4: download / decrypt / save images, in order.
            # Download failure never blocks the main loop; decrypt failure
            # never saves ciphertext as plaintext; partial success proceeds.
            # Image event: record pending BEFORE processing, so a later
            # text arriving before download finishes cannot associate
            # to an older successful image. Event persists in state/,
            # so it survives restarts.
            if image_items:
                save_image_event({"message_id": mid, "status": "pending",
                                  "ts": int(time.time() * 1000)})
            saved_images = []
            image_errors = []
            image_diags = []
            for idx, ii in enumerate(image_items):
                try:
                    saved, err, diag = process_inbound_image(ii, mid, idx)
                except Exception as e:
                    saved, err, diag = None, _sanitize_error(e), None
                if diag:
                    image_diags.append(diag)
                if saved:
                    saved_images.append(saved)
                else:
                    image_errors.append({"index": idx, "error": err,
                                         "failure_record": (diag or {}).get("failure_record"),
                                         "failure_stage": (diag or {}).get("failure_stage"),
                                         "error_code": (diag or {}).get("error_code")})
            if image_items:
                if saved_images and not image_errors:
                    save_image_event({"message_id": mid, "status": "ok",
                                      "paths": [s.get("path") for s in saved_images],
                                      "ts": int(time.time() * 1000)})
                elif saved_images and image_errors:
                    save_image_event({"message_id": mid, "status": "partial",
                                      "paths": [s.get("path") for s in saved_images],
                                      "errors": image_errors,
                                      "ts": int(time.time() * 1000)})
                elif image_errors:
                    save_image_event({"message_id": mid, "status": "failed",
                                      "errors": image_errors,
                                      "reason": image_errors[0].get("error_code") or "failed",
                                      "ts": int(time.time() * 1000)})
            if image_items and not saved_images and not text and not file_items:
                # All images failed and no text: explicit prompt, no inbox.
                # Item 2: files present means the message is not done:
                # fall through to file processing instead of swallowing
                # the files with the image prompt.
                # Fix 2026-10-28: log the error strings (not the image
                # item itself, which may contain download/decrypt params)
                # so a failed image can be diagnosed after the fact.
                print(f"serve: image fail for {mid}.json errors={image_errors}", flush=True)
                try:
                    _queued = send_direct_or_outbox(session, "图片接收失败，请重发",
                                          session.get("ilink_user_id"), ctx,
                                          f"{mid}_image_fail.json")
                except TokenExpired:
                    print("serve: token expired during image fail prompt, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: image fail prompt queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: image fail prompt for {mid}.json", flush=True)
                continue
            # Practical video receive v1: process videos like images.
            # Download/decrypt failure never blocks the main loop;
            # decrypt failure never saves ciphertext as plaintext.
            saved_videos = []
            video_errors = []
            for idx, vi in enumerate(video_items):
                try:
                    saved, err, diag = process_inbound_video(vi, mid, idx)
                except Exception as e:
                    saved, err, diag = None, _sanitize_error(e), None
                if saved:
                    saved_videos.append(saved)
                else:
                    video_errors.append({"index": idx, "error": err,
                                         "error_code": (diag or {}).get("error_code")})
            if video_items and not saved_videos and not text and not file_items:
                # All videos failed and no text: explicit prompt, no inbox.
                # Item 2: same guard as images; files still to process.
                print(f"serve: video fail for {mid}.json errors={video_errors}", flush=True)
                try:
                    _queued = send_direct_or_outbox(session, "视频接收失败，请重发",
                                          session.get("ilink_user_id"), ctx,
                                          f"{mid}_video_fail.json")
                except TokenExpired:
                    print("serve: video fail prompt, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: video fail prompt queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: video fail prompt for {mid}.json", flush=True)
                continue
            # Item 2: local-only mode protects videos only. Files never
            # inherit it: when the mode is on, refuse files here (no
            # download, nothing to the Worker) and tell the user to
            # turn the mode off and resend. This is the fixed choice
            # (plan 2.4); the message is dropped whole, like every
            # other direct-prompt path in this loop.
            if file_items and read_local_only_mode():
                print(f"serve: file refused under local-only mode for {mid}.json", flush=True)
                try:
                    _queued = send_direct_or_outbox(session, "仅本地模式只保护视频，文件不会只存本地；已暂不接收这份文件，请关闭仅本地模式后重发。",
                                          session.get("ilink_user_id"), ctx, f"{mid}_file_local.json")
                except TokenExpired:
                    print("serve: token expired during file local-mode prompt, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: file local-mode prompt queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: file local-mode prompt for {mid}.json", flush=True)
                continue
            # Item 2: ordinary file receive v1. Files are processed
            # like images/videos but with their own caps, budget, and
            # stable original-index keys. Download/decrypt failure
            # never blocks the main loop; decrypt failure never saves
            # ciphertext as plaintext; partial success proceeds.
            saved_files = []
            saved_file_indices = []
            file_errors = []
            file_deadline = time.time() + FILE_MESSAGE_BUDGET_SECONDS
            file_plain_total = 0
            for orig_idx, fi in file_items:
                if len(saved_files) >= FILE_MESSAGE_MAX_COUNT:
                    file_errors.append({"index": orig_idx, "error": "message file count over 5",
                                        "error_code": "OVER_MESSAGE_COUNT"})
                    continue
                try:
                    saved, err, diag = process_inbound_file(fi, mid, orig_idx, file_deadline,
                                                            FILE_MESSAGE_TOTAL_CAP - file_plain_total)
                except Exception as e:
                    saved, err, diag = None, _sanitize_error(e), None
                if saved:
                    saved_files.append(saved)
                    saved_file_indices.append(orig_idx)
                    file_plain_total += saved["size"]
                else:
                    file_errors.append({"index": orig_idx, "error": err,
                                        "error_code": (diag or {}).get("error_code")})
            # Gate 3 item 2 wiring: auto-parse each landed file. Parse
            # never blocks receipt: failures leave failure evidence and
            # the message still reaches the Worker below. The local-only
            # refusal branch returns above, so it never reaches here.
            for _saved, _idx in zip(saved_files, saved_file_indices):
                try:
                    maybe_parse_inbound_doc(_saved, _idx)
                except Exception:
                    pass
            if file_items and not saved_files and not text and not saved_images and not saved_videos:
                # All files failed and nothing else to queue: explicit
                # prompt, no inbox. (If images/videos were also saved,
                # the record below carries file_errors instead.)
                print(f"serve: file fail for {mid}.json errors={file_errors}", flush=True)
                try:
                    _queued = send_direct_or_outbox(session, "文件接收失败，请重发",
                                          session.get("ilink_user_id"), ctx, f"{mid}_file_fail.json")
                except TokenExpired:
                    print("serve: token expired during file fail prompt, stopping.", flush=True)
                    return 14
                if not _queued:
                    _batch_enqueue_failed = True
                    print(f"serve: file fail prompt queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                print(f"serve: file fail prompt for {mid}.json", flush=True)
                continue
            # M3 defect fix: decide local_only intent BEFORE queueing analysis.
            # A local_only video must never be queued into analysis/ at all:
            # previously the queue ran first and the analyzer failed only
            # because the file had already been moved to local_vault, leaving
            # a stray analysis dir. Intent uses caption OR toggle mode, same
            # rule as the move step below, and does not depend on saved files.
            _local_only_intent = bool(video_items and ("仅本地" in (text or "") or read_local_only_mode()))
            # M2 integration v1: queue background analysis (non-blocking),
            # skipped entirely for local_only videos.
            video_analyses = queue_video_analysis(mid, saved_videos) if (saved_videos and not _local_only_intent) else []
            # M3 v1 local_only: caption contains exactly "仅本地" and at least one
            # video was saved. Move files to local_vault, do NOT queue analysis
            # into analysis/, and do NOT put any video path, analysis path or
            # original text into the inbox record (see record construction below).
            # Fail-safe: any move failure -> explicit failure prompt, no content queued.
            is_local_only = bool(_local_only_intent and saved_videos)
            local_video_count = 0
            if is_local_only:
                _moved, _merr = move_videos_to_local_vault(mid, saved_videos)
                if _merr is not None or _moved != len(saved_videos):
                    print(f"serve: local vault move FAILED for {mid}, moved={_moved} err={_merr}", flush=True)
                    try:
                        _queued = send_direct_or_outbox(session, "仅本地保存失败，请重发",
                                              session.get("ilink_user_id"), ctx,
                                              f"{mid}_local_fail.json")
                    except TokenExpired:
                        print("serve: local fail prompt, stopping.", flush=True)
                        return 14
                    if not _queued:
                        _batch_enqueue_failed = True
                        print(f"serve: local fail prompt queue FAILED for {mid}, NOT marking processed, will retry", flush=True)
                        continue
                    processed.add(mid); save_processed(processed)
                    print(f"serve: local fail prompt for {mid}.json", flush=True)
                    continue
                local_video_count = _moved
                saved_videos = []
                video_analyses = []
            # Normal queued task (text, voice with transcription, and/or images).
            # Quote v1: resolve any upstream ref_msg here so the Worker sees
            # the quoted target instead of guessing from history. Local or
            # tombstoned targets resolve to a guard only, never content.
            quote = resolve_quote(extract_quote(msg))
            # R2 P0 (B2-lite): the inbox/done record is Worker-
            # visible, so it must NOT carry context_token. The
            # bridge session already holds ctx (updated above from
            # the inbound message), and drain_outbox sends with the
            # session token, so nothing needs the token from here.
            record = {
                "message_id": msg.get("message_id"),
                "text": text,
                "from_user_id": msg.get("from_user_id"),
                "ts": msg.get("create_time_ms") or int(time.time() * 1000),
                "history_class": "user",
                "egress_policy": "muse_default",
                "policy_version": SAFE_VIEW_VERSION,
            }
            if quote:
                record["quote"] = quote
            if voice_text:
                record["source"] = "voice"
            if saved_images:
                record["images"] = saved_images
            if is_local_only:
                # Placeholder only: no video path, no analysis path, no original text.
                record["egress_policy"] = "local_only"
                record["local_only"] = True
                record["local_video_count"] = local_video_count
                record["local_only_note"] = "仅本地保存"
                record["text"] = ""
            else:
                if saved_videos:
                    record["videos"] = saved_videos
                if video_analyses:
                    record["analysis"] = video_analyses
                if video_errors:
                    record["video_errors"] = video_errors
            if image_errors:
                record["image_errors"] = image_errors
            if saved_files:
                record["files"] = saved_files
            if file_errors:
                record["file_errors"] = file_errors
            # Associator blocking: if this text is a follow-up about
            # an image ("这张图", "刚才那张", "图里", "写了什么",
            # "解释一下", etc.) and the latest image event is
            # pending/failed, attach image_reference unavailable and
            # do NOT let the Worker fall back to an older success.
            if not image_items and text:
                _ev = load_image_event()
                if _ev and _ev.get("status") in ("pending", "failed"):
                    _t = text
                    if any(k in _t for k in ("这张图", "刚才那张", "那张图", "图里",
                                             "写了什么", "解释一下", "这张", "刚才")):
                        record["image_reference"] = {
                            "message_id": str(_ev.get("message_id")),
                            "status": "unavailable",
                            "reason": _ev.get("reason") or _ev.get("status"),
                        }
            # Gate 2 same-source merge: with the switch ON, a normal
            # Worker-bound message is staged bridge-privately instead
            # of written to inbox (local-only placeholders bypass
            # staging and keep the old path exactly). The timeline
            # "received" event is recorded at promotion, when the
            # Worker can first see the record.
            _use_staging = (not is_local_only) and read_same_source_merge_mode()
            _staged_first = False
            if _use_staging:
                _staged_ok, _staged_first = stage_merge_message(
                    mid, msg.get("from_user_id"), record, ctx)
                if not _staged_ok:
                    _batch_enqueue_failed = True
                    print(f"serve: merge staging FAILED for {mid}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                _log_ts(f"serve: staged merge/{mid}.json")
            else:
                try:
                    atomic_write_json(os.path.join(INBOX, f"{mid}.json"), record, mode=0o600)
                except Exception as _inbox_e:
                    _batch_enqueue_failed = True
                    print(f"serve: inbox write FAILED for {mid}: {_sanitize_error(_inbox_e)}, NOT marking processed, will retry", flush=True)
                    continue
                processed.add(mid); save_processed(processed)
                # Gate 1 item 2: the inbox write above is durable, so
                # this is the exact received moment for this message.
                record_timeline_event(f"{mid}.json", "received", source="serve_inbox_write")
                # Gate 3 step 2: shadow inbound mirror (dual/shadow
                # only; json mode is a no-op). Hash only, never the
                # record itself.
                _shadow_upsert_inbound(mid, record)
                _log_ts(f"serve: queued inbox/{mid}.json")
            # Phase 2 step 3: typing only. The queue notice was
            # removed by the waiting-prompt merge (2026-11-01):
            # the progress receipt is the single waiting prompt.
            # Do NOT claim Muse has started executing before Worker runs.
            # Merge typing rule: only the group's first message gets
            # a ticket (it becomes the anchor, whose formal job
            # cancels it); followers get none, so no ticket leaks
            # for messages that never get their own formal job.
            if (not _use_staging) or _staged_first:
                try:
                    ticket = get_typing_ticket(session)
                    if ticket:
                        with _TYPING_LOCK:
                            _TYPING_MAP[mid] = ticket
                        send_typing(session, ticket, 1,
                                    to_user_id=session.get("ilink_user_id"),
                                    context_token=ctx)
                except TokenExpired:
                    print("serve: token expired during typing, stopping.", flush=True)
                    return 14
                except Exception:
                    pass
            # Waiting-prompt merge (2026-11-01): no queue notice
            # is written here anymore. The progress receipt (Gate 1
            # item 3) is the single waiting prompt: fast messages
            # get only the formal reply, slow messages get
            # progress + formal. The queue-notice helper is kept,
            # unused, so restoring the old notice is a one-call
            # revert. Note: removing this call also removes one
            # early token-expiry exit; expiry still surfaces on
            # the next real send (progress, formal, local).
        # R2: only now, after the whole batch, advance the cursor
        # and persist the last context_token. A crash before this
        # point re-fetches the batch; processed messages deduplicate.
        # v1.0.1: if any direct-reply/inbox enqueue in this batch
        # failed, do NOT advance the cursor, so the failed message is
        # re-fetched and retried instead of being silently dropped.
        if _batch_enqueue_failed:
            print("serve: batch enqueue failed, cursor NOT advanced, will re-fetch batch", flush=True)
        elif new_cursor and new_cursor != cursor:
            save_cursor(new_cursor)
        try:
            save_session(session)
        except Exception:
            pass


def render_qr(qrcode_value, out_png):
    """Render QR PNG with the venv qrcode package if available."""
    try:
        import qrcode
        img = qrcode.make(qrcode_value)
        img.save(out_png)
    except Exception:
        return False
    return True


def cmd_login(args):
    ensure_dirs()
    base = DEFAULT_BASE
    # If a previous login is pending and a verify code was supplied,
    # resume polling that qrcode instead of requesting a new one.
    pending = None
    if os.path.exists(LOGIN_PENDING_FILE):
        try:
            with open(LOGIN_PENDING_FILE, encoding="utf-8") as f:
                pending = json.load(f)
        except (OSError, ValueError):
            pending = None
    if args.resume and pending:
        qrcode_value = pending["qrcode"]
        base = pending.get("baseurl") or DEFAULT_BASE
        print("login: resuming pending QR code.", flush=True)
    else:
        # QR via GET (2026-10-27 Gemini probe A): POST get_bot_qrcode hangs
        # from this VM (TLS completes, body sent, 0 bytes received), while
        # GET with http/1.1 + Mozilla UA returns 200 in ~2.9s. Protocol.md
        # documents POST, but older iLink clients used GET, so login now
        # uses GET. ClientVersion 132105 (=2.4.9) matches the successful
        # probe; server does not strictly require 2.4.10.
        url = base.rstrip("/") + "/ilink/bot/get_bot_qrcode?bot_type=3"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "iLink-App-Id": "bot",
                "iLink-App-ClientVersion": "132105",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                resp = json.loads(r.read().decode("utf-8"))
        except Exception as e:
            print(f"login: GET get_bot_qrcode failed: {e}", flush=True)
            return 1
        qrcode_value = resp.get("qrcode")
        qrcode_img = resp.get("qrcode_img_content")
        if not qrcode_value:
            print(f"login: get_bot_qrcode returned no qrcode: {str(resp)[:500]}", flush=True)
            return 1
        print("login: GET get_bot_qrcode ok, ret=%s" % resp.get("ret"), flush=True)
        # Do NOT print qrcode_img_content in full here: it contains the
        # login QR payload. It is saved to login_pending.json (0600) and
        # rendered to login_qr.png.
        atomic_write_json(
            LOGIN_PENDING_FILE,
            {"qrcode": qrcode_value, "qrcode_img_content": qrcode_img, "baseurl": base, "ts": time.time()},
            mode=0o600,
        )
        out_png = os.path.join(ROOT, "login_qr.png")
        # Protocol.md: qrcode_img_content is "content to display as a QR
        # code", so encode that when present, not the raw qrcode hex.
        qr_payload = qrcode_img or qrcode_value
        if render_qr(qr_payload, out_png):
            print(f"login: QR PNG written to {out_png} (open this file to scan).", flush=True)
            # Local login: auto-open the PNG so the user does not lose
            # time finding/opening it by hand (QR expires in ~2 minutes).
            if sys.platform == "win32":
                try:
                    os.startfile(out_png)
                    print("login: QR image opened automatically, scan it now.", flush=True)
                except Exception as e:
                    print(f"login: auto-open failed ({e}), open the PNG by hand.", flush=True)
        else:
            print("login: qrcode package not available in system python; "
                  "use the venv python to render.", flush=True)
    # Poll status.
    deadline = time.time() + (args.timeout or 180)
    while time.time() < deadline:
        query = {"qrcode": qrcode_value}
        if args.verify_code:
            query["verify_code"] = args.verify_code
        qs = urllib.parse.urlencode(query)
        try:
            # Status endpoint is long-poll: it held ~31s before returning
            # {"ret":0,"status":"wait"} when unscanned (tested 2026-10-27).
            # A 15s timeout aborts before the server answers, so use 45s.
            resp = get_json(base, f"/ilink/bot/get_qrcode_status?{qs}", timeout=45)
        except Exception as e:
            print(f"login: status poll error, retrying: {e}", flush=True)
            time.sleep(3)
            continue
        status = resp.get("status")
        print(f"login: status={status}", flush=True)
        if status == "confirmed":
            bot_token = resp.get("bot_token")
            ilink_user_id = resp.get("ilink_user_id")
            if not bot_token or not ilink_user_id:
                print("login: confirmed but missing bot_token/ilink_user_id.", flush=True)
                return 1
            sess = {
                "bot_token": bot_token,
                "ilink_bot_id": resp.get("ilink_bot_id"),
                "baseurl": resp.get("baseurl") or base,
                "ilink_user_id": ilink_user_id,
                "context_token": "",
            }
            save_session(sess)
            # R6 fix: only after the new session is saved, clear the
            # old expired marker, otherwise serve/send will keep
            # refusing with the stale NEEDS_RELOGIN file.
            try:
                os.remove(NEEDS_RELOGIN_FILE)
            except OSError:
                pass
            try:
                os.remove(LOGIN_PENDING_FILE)
            except OSError:
                pass
            print("login: confirmed, session.json saved (0600). Token is NOT printed.", flush=True)
            return 0
        if status == "expired":
            print("login: QR expired, run login again for a fresh code.", flush=True)
            return 1
        if status == "need_verifycode":
            print("login: server asks for a verification code. Re-run with: "
                  "python3 bridge.py login --resume --verify-code <code shown in WeChat>", flush=True)
            return 2
        if status == "verify_code_blocked":
            print("login: verify code blocked, refresh the QR code.", flush=True)
            return 1
        if status in ("scaned_but_redirect", "binded_redirect") and resp.get("redirect_host"):
            base = "https://" + resp["redirect_host"]
            print(f"login: redirecting polls to {base}", flush=True)
        # wait / scaned / other: keep polling.
        time.sleep(3)
    print("login: timed out waiting for scan. QR may still be valid; "
          "re-run with --resume to keep polling the same code.", flush=True)
    return 2


def verify_channel_recovery():
    """Production Worker verification (GPT ruling, blocking).
    This is the ONLY channel-recovery check the Worker should use:
    `python3 bridge.py verify-channel` prints this dict as JSON.
    Read-only: it never writes, never sends, never restarts serve.
    It NEVER raises and NEVER compares ISO timestamp strings.
    Recovery is confirmed only when ALL hold at check time:
      - serve.pid exists, parses, and the process is alive with a
        python bridge.py serve cmdline;
      - serve_run.json and channel_status.json exist and are objects;
      - channel_status.status == "ok", consecutive_failures == 0
        (int, not bool), last_success_ts is not None;
      - process_pid (channel) == serve.pid == serve_run.pid;
      - run_id matches between the two files;
      - boot_id matches the current boot_id, and proc_starttime
        matches the current /proc/<pid>/stat field 22, in BOTH files
        (old files without these fields are unconfirmed, not ok);
      - serve.pid and proc_starttime do not change during the check.
    Any missing file, mismatch, unreadable identity, dead process,
    or checker error -> recovered False, with a reason. Time
    strings are returned for human readability only, never used
    as a logic switch."""
    def _unconfirmed(reason, extra=None):
        out = {"recovered": False, "reason": reason}
        if extra:
            out.update(extra)
        return out
    try:
        pid_path = os.path.join(STATE, "serve.pid")
        if not os.path.exists(pid_path):
            return _unconfirmed("pid_missing")
        try:
            with open(pid_path, encoding="utf-8") as f:
                pid_text = f.read().strip()
        except Exception:
            return _unconfirmed("pid_unreadable")
        if not pid_text:
            return _unconfirmed("pid_empty")
        try:
            pid = int(pid_text)
        except Exception:
            return _unconfirmed("pid_invalid")
        # Process alive + cmdline, read-only.
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().replace(b"\x00", b" ").decode(errors="ignore")
        except Exception:
            return _unconfirmed("process_not_running", {"pid": pid})
        if "bridge.py" not in cmdline or "serve" not in cmdline:
            return _unconfirmed("process_cmdline_mismatch", {"pid": pid})
        try:
            os.kill(pid, 0)
        except Exception:
            return _unconfirmed("process_not_running", {"pid": pid})
        cur_boot = _boot_id()
        cur_start = _proc_starttime(pid)
        if cur_start is None:
            return _unconfirmed("kernel_identity_unavailable", {"pid": pid})
        try:
            with open(SERVE_RUN_FILE, encoding="utf-8") as f:
                run = json.load(f)
            with open(CHANNEL_STATUS_FILE, encoding="utf-8") as f:
                chan = json.load(f)
        except Exception:
            return _unconfirmed("identity_file_missing_or_invalid", {"pid": pid})
        if not isinstance(run, dict) or not isinstance(chan, dict):
            return _unconfirmed("identity_file_not_object", {"pid": pid})
        run_id = run.get("run_id")
        if not run_id or chan.get("run_id") != run_id:
            return _unconfirmed("run_id_mismatch", {"pid": pid})
        if run.get("pid") != pid or chan.get("process_pid") != pid:
            return _unconfirmed("pid_mismatch", {"pid": pid})
        if run.get("boot_id") != cur_boot or chan.get("boot_id") != cur_boot:
            return _unconfirmed("boot_id_mismatch", {"pid": pid})
        if run.get("proc_starttime") != cur_start or chan.get("proc_starttime") != cur_start:
            return _unconfirmed("proc_starttime_mismatch", {"pid": pid})
        if chan.get("status") != "ok":
            return _unconfirmed("channel_not_ok", {"pid": pid, "status": chan.get("status")})
        failures = chan.get("consecutive_failures")
        if isinstance(failures, bool) or not isinstance(failures, int) or failures != 0:
            return _unconfirmed("channel_failures_not_zero", {"pid": pid})
        if chan.get("last_success_ts") is None:
            return _unconfirmed("channel_pending", {"pid": pid})
        # Identity must not change during the check.
        try:
            with open(pid_path, encoding="utf-8") as f:
                pid_text2 = f.read().strip()
            start2 = _proc_starttime(pid)
        except Exception:
            return _unconfirmed("identity_changed_during_check", {"pid": pid})
        if pid_text2 != pid_text or start2 != cur_start:
            return _unconfirmed("identity_changed_during_check", {"pid": pid})
        return {"recovered": True, "reason": "ok", "pid": pid, "run_id": run_id,
                "status": chan.get("status"), "last_success_ts": chan.get("last_success_ts")}
    except Exception as e:
        # Checker errors are unconfirmed, never a crash and never recovery.
        return _unconfirmed("checker_error", {"error_type": type(e).__name__})


def cmd_verify_channel(_args):
    result = verify_channel_recovery()
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("recovered") else 1


def cmd_status(_args):
    ensure_dirs()
    session = load_session()
    print(f"session: {'present' if session else 'missing'}")
    print(f"needs_relogin: {os.path.exists(NEEDS_RELOGIN_FILE)}")
    print(f"inbox files: {len([n for n in os.listdir(INBOX) if n.endswith('.json')]) if os.path.exists(INBOX) else 0}")
    print(f"cursor: {'present' if os.path.exists(CURSOR_FILE) else 'missing'}")
    return 0


def _serve_alive():
    """Read-only serve liveness: (pid_or_None, alive_bool). Never raises.

    Same checks as the watchdog (serve.pid + /proc cmdline), but only
    for reporting: this never restarts, never writes, never sends.
    """
    try:
        with open(os.path.join(STATE, "serve.pid"), encoding="utf-8") as f:
            pid = int(f.read().strip())
    except Exception:
        return None, False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmdline = f.read().replace(b"\x00", b" ").decode(errors="ignore")
        if "bridge.py" not in cmdline or "serve" not in cmdline:
            return pid, False
        os.kill(pid, 0)
        return pid, True
    except Exception:
        return pid, False


def get_bridge_health():
    """Desensitised bridge health snapshot. Read-only, never raises.

    Only booleans, counts and fixed short strings are returned: never
    job text, never tokens, never URLs, never user identifiers, never
    run ids. Exit code of the `health` command is always 0: a dead
    serve is a finding in the output, not a CLI failure, so callers
    must read serve_alive / channel_recovered instead of the code.
    """
    out = {
        "schema": "bridge-health-0.1",
        "serve_alive": False,
        "serve_pid": None,
        "channel_recovered": False,
        "channel_reason": "unknown",
        "channel_status": None,
        "channel_last_success_ts": None,
        "worker_heartbeat_present": False,
        "worker_online": False,
        "worker_heartbeat_age_seconds": None,
        "queue": {"pending": 0, "sent": 0, "failed": 0,
                  "needs_check": 0, "skipped_empty": 0, "unreadable": 0},
        "anomalies_present": False,
        "anomalies_readable": False,
        "anomalies_counts": {},
    }
    try:
        pid, alive = _serve_alive()
        out["serve_pid"] = pid
        out["serve_alive"] = alive
    except Exception:
        pass
    try:
        ch = verify_channel_recovery()
        out["channel_recovered"] = bool(ch.get("recovered"))
        if isinstance(ch.get("reason"), str):
            out["channel_reason"] = ch["reason"]
        if isinstance(ch.get("status"), str):
            out["channel_status"] = ch["status"]
        if isinstance(ch.get("last_success_ts"), str):
            out["channel_last_success_ts"] = ch["last_success_ts"]
    except Exception:
        pass
    try:
        ts, _src = read_worker_heartbeat()
        out["worker_heartbeat_present"] = ts is not None
        if ts is not None:
            out["worker_heartbeat_age_seconds"] = int(time.time() - ts)
        out["worker_online"] = not is_worker_offline()
    except Exception:
        pass
    try:
        s = get_observability_summary()
        for k in out["queue"]:
            v = s.get(k)
            out["queue"][k] = v if (isinstance(v, int) and not isinstance(v, bool)) else 0
    except Exception:
        pass
    try:
        a = read_getupdates_anomalies()
        out["anomalies_present"] = bool(a.get("present"))
        out["anomalies_readable"] = bool(a.get("readable"))
        counts = a.get("counts") or {}
        out["anomalies_counts"] = {c: counts.get(c, 0) for c in GETUPDATES_ANOMALY_CATEGORIES}
    except Exception:
        pass
    try:
        _free_mb, _media_mb = _disk_watermark()
        out["disk_free_mb"] = _free_mb
        out["media_dir_size_mb"] = _media_mb
    except Exception:
        pass
    return out


def cmd_health(_args):
    """Gate 3 CLI: print the health snapshot (read-only JSON)."""
    ensure_dirs()
    try:
        print(json.dumps(get_bridge_health(), ensure_ascii=False, indent=2))
    except Exception as e:
        print(json.dumps({"schema": "bridge-health-0.1", "ok": False,
                          "error_type": type(e).__name__}, ensure_ascii=False))
    return 0


def cmd_send_media(args, kind):
    """Gate 3 CLI: send-video / send-file.

    Direct send through the EXISTING send_video / send_file functions
    (same upload + encrypt + sendmessage path the bridge already
    uses), exactly like the text `send` command uses send_text. This
    is NOT a new send path and NOT an outbox job: the sender loop is
    not involved, no queue file is written, no queue format changes.
    Exit codes mirror `send`: 3 no session, 14 token expired /
    NEEDS_RELOGIN, 2 delivery unknown (may already be delivered, do
    NOT auto-retry), 1 strict failure, 0 ok. No new delivery
    semantics are invented: an unknown result stays unknown.
    An optional --caption is sent AFTER the media succeeds, as a
    separate text bubble via the existing send_text; a failed media
    send never sends the caption, and a failed caption never retries
    the media.
    """
    ensure_dirs()
    name = "send-video" if kind == "video" else "send-file"
    session = load_session()
    if not session:
        print(f"{name}: no session.json, run login first.", flush=True)
        return 3
    if os.path.exists(NEEDS_RELOGIN_FILE):
        print(f"{name}: NEEDS_RELOGIN exists, refusing to send (re-login required).", flush=True)
        return 14
    path = args.path
    if not os.path.isfile(path):
        print(f"{name}: file missing: {path}", flush=True)
        return 1
    if not _is_allowed_image_path(path):
        print(f"{name}: path outside allowed media dirs, refusing", flush=True)
        return 1
    try:
        size = os.path.getsize(path)
    except OSError:
        print(f"{name}: cannot stat file", flush=True)
        return 1
    if size > MAX_VIDEO_PLAIN_BYTES:
        print(f"{name}: over 100MiB", flush=True)
        return 1
    fn = send_video if kind == "video" else send_file
    kwargs = {}
    if kind == "file" and getattr(args, "file_name", None):
        kwargs["file_name"] = args.file_name
    try:
        resp = fn(session, path, args.to, args.context_token,
                  client_id=args.client_id, **kwargs)
    except TokenExpired:
        print(f"{name}: token expired (ret/errcode -14), wrote NEEDS_RELOGIN", flush=True)
        return 14
    except UnknownDeliveryError:
        print(f"{name}: delivery unknown (may already be delivered, NOT auto-retried)", flush=True)
        return 2
    except Exception as e:
        print(f"{name}: failed: {_sanitize_error(e)}", flush=True)
        return 1
    mid = resp.get("message_id") if isinstance(resp, dict) else None
    mid_txt = f" message_id={mid}" if mid else ""
    caption = getattr(args, "caption", None)
    if caption:
        try:
            send_text(session, caption, to_user_id=args.to,
                      context_token=args.context_token, client_id=None)
        except TokenExpired:
            print(f"{name}: ok (media sent, caption failed: token expired){mid_txt}", flush=True)
            return 14
        except UnknownDeliveryError:
            print(f"{name}: ok (media sent, caption delivery unknown, NOT auto-retried){mid_txt}", flush=True)
            return 2
        except Exception as e:
            print(f"{name}: ok (media sent, caption failed: {_sanitize_error(e)}){mid_txt}", flush=True)
            return 1
        print(f"{name}: ok (caption sent){mid_txt}", flush=True)
        return 0
    print(f"{name}: ok{mid_txt}", flush=True)
    return 0


def cmd_refresh_qr(args):
    """Gate 3 CLI: resume a pending login QR code, nothing more.

    Safety boundary (checked before anything else): this NEVER
    requests a new QR code and NEVER touches session.json except
    through the normal confirmed path inside cmd_login, which saves
    a new session and clears NEEDS_RELOGIN only after the user scans
    and confirms the pending code. With no pending QR code it
    refuses and tells the user to run `login` for a fresh code; a
    valid session is left untouched, and NEEDS_RELOGIN is never
    cleared or bypassed by this command itself.
    """
    ensure_dirs()
    pending = None
    try:
        with open(LOGIN_PENDING_FILE, encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict) and doc.get("qrcode"):
            pending = doc
    except Exception:
        pending = None
    if not pending:
        print("refresh-qr: no pending QR code (expired or never requested); "
              "run `login` for a fresh code. Session untouched.", flush=True)
        return 1
    ns = argparse.Namespace(resume=True, verify_code=args.verify_code,
                            timeout=args.timeout)
    return cmd_login(ns)


def cmd_resend_text_part(args):
    """Item 3 manual resend: reset ONE text part to 'pending'.

    Usage: python3 bridge.py resend-text-part <job-file> <part>
    <job-file> is a filename in outbox/failed/ (or outbox/pending/,
    or a full path); <part> is the 1-based part number shown as
    [i/N] in the sent display text. Only that part is reset:
    sent parts are never resent, the plan is never resliced, and
    other parts (including blocked_by_prior ones) are untouched
    here -- a blocked tail resumes in drain_outbox only once every
    earlier part is sent. Refuses to run if the job has no plan,
    the part number is out of range, the part is already sent /
    pending / sending, or the plan's full_text_sha256 no longer
    matches the top-level text. Prints only part/job metadata,
    never tokens or user identifiers."""
    job_arg = args.job
    if os.path.sep in job_arg or (os.path.altsep and os.path.altsep in job_arg):
        candidates = [job_arg]
    else:
        candidates = [os.path.join(OUTBOX_FAILED, job_arg),
                      os.path.join(OUTBOX_PENDING, job_arg)]
    path = None
    for cand in candidates:
        if os.path.exists(cand):
            path = cand
            break
    if path is None:
        print("resend-text-part: job not found in outbox/failed/ or outbox/pending/", flush=True)
        return 1
    try:
        with open(path, encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        print(f"resend-text-part: unreadable job: {_sanitize_error(e)}", flush=True)
        return 1
    plan = _text_plan(job)
    if plan is None:
        print("resend-text-part: job has no long-text plan, refusing", flush=True)
        return 1
    text = job.get("text") or ""
    if job.get("full_text_sha256") != hashlib.sha256(text.encode("utf-8")).hexdigest():
        print("resend-text-part: plan does not match top-level text, refusing (no reslicing)", flush=True)
        return 1
    part_no = args.part
    if part_no < 1 or part_no > len(plan):
        print(f"resend-text-part: part {part_no} out of range (1..{len(plan)})", flush=True)
        return 1
    part = plan[part_no - 1]
    if part.get("status") in ("sent", "pending", "sending"):
        print(f"resend-text-part: part {part_no} is already {part.get('status')}, refusing", flush=True)
        return 1
    if part.get("status") not in ("failed", "needs_check", "blocked_by_prior"):
        print(f"resend-text-part: part {part_no} has unexpected status {part.get('status')!r}, refusing", flush=True)
        return 1
    part["status"] = "pending"
    part["error_code"] = None
    part["attempts"] = 0
    job.pop("terminal", None)
    job["delivery_state"] = "pending"
    job.pop("needs_check_reason", None)
    job["manual_resend_part"] = part_no - 1
    job["manual_resend_ts"] = time.time()
    dest = os.path.join(OUTBOX_PENDING, os.path.basename(path))
    try:
        atomic_write_json(path, job, mode=0o600)
        if os.path.abspath(path) != os.path.abspath(dest):
            os.replace(path, dest)
    except Exception as e:
        print(f"resend-text-part: write/move failed: {_sanitize_error(e)}", flush=True)
        return 1
    print(json.dumps({"job": os.path.basename(dest), "part": part_no,
                      "parts_total": len(plan), "status": "pending"}, ensure_ascii=False), flush=True)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="WeChat iLink bridge (text only, Phase 1)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_login = sub.add_parser("login", help="Get QR code and poll until confirmed")
    p_login.add_argument("--resume", action="store_true", help="Resume a pending QR code")
    p_login.add_argument("--verify-code", default=None, help="Verification code shown in WeChat")
    p_login.add_argument("--timeout", type=int, default=180, help="Seconds to poll for scan")
    sub.add_parser("serve", help="Long-poll getUpdates and drain outbox (holds flock)")
    p_send = sub.add_parser("send", help="Send one text message")
    p_send.add_argument("--text", required=True)
    p_send.add_argument("--to", default=None)
    p_send.add_argument("--context-token", default=None)
    p_send.add_argument("--client-id", default=None)
    sub.add_parser("status", help="Show bridge state without printing secrets")
    p_resend = sub.add_parser("resend-text-part", help="Item 3: reset one long-text part to pending for manual resend (never reslices, never resends sent parts)")
    p_resend.add_argument("job", help="Job filename in outbox/failed/ or outbox/pending/, or a full path")
    p_resend.add_argument("part", type=int, help="1-based part number, as shown in [i/N]")
    sub.add_parser("verify-channel", help="Production Worker channel-recovery check (read-only JSON, exit 0 only if recovered)")
    sub.add_parser("safe-view", help="Print the deterministic safe history view (read-only JSON)")
    p_obs = sub.add_parser("observability", help="Gate 1 item 2: print one job's stage view (read-only JSON, no text or tokens)")
    p_obs.add_argument("job", help="Job filename in outbox/sent/, outbox/failed/ or outbox/pending/, with or without .json")
    sub.add_parser("observability-summary", help="Gate 1 item 2: print outbox counts by bucket (read-only JSON, counts only)")
    sub.add_parser("getupdates-anomalies", help="Gate 2: print desensitised getupdates anomaly counts (read-only JSON, no tokens or text)")
    p_sv = sub.add_parser("send-video", help="Gate 3: send a video file directly (existing send path, not a queue job)")
    p_sv.add_argument("path", help="Local file under media/inbound/ or media/outbound/")
    p_sv.add_argument("--to", default=None)
    p_sv.add_argument("--context-token", default=None)
    p_sv.add_argument("--client-id", default=None)
    p_sv.add_argument("--caption", default=None, help="Optional text sent as a separate bubble after the media succeeds")
    p_sf = sub.add_parser("send-file", help="Gate 3: send a file directly (existing send path, not a queue job)")
    p_sf.add_argument("path", help="Local file under media/inbound/ or media/outbound/")
    p_sf.add_argument("--to", default=None)
    p_sf.add_argument("--context-token", default=None)
    p_sf.add_argument("--client-id", default=None)
    p_sf.add_argument("--caption", default=None, help="Optional text sent as a separate bubble after the media succeeds")
    p_sf.add_argument("--file-name", default=None, help="Display name; default is the file basename")
    sub.add_parser("health", help="Gate 3: print desensitised bridge health (read-only JSON, no tokens, URLs, user ids or text)")
    p_rq = sub.add_parser("refresh-qr", help="Gate 3: resume a pending login QR code (never requests a new code, never touches a valid session)")
    p_rq.add_argument("--verify-code", default=None, help="Verification code shown in WeChat")
    p_rq.add_argument("--timeout", type=int, default=180, help="Seconds to poll for scan")
    args = parser.parse_args(argv)
    ensure_dirs()
    if args.cmd == "login":
        return cmd_login(args)
    if args.cmd == "serve":
        return serve_loop()
    if args.cmd == "status":
        return cmd_status(args)
    if args.cmd == "resend-text-part":
        return cmd_resend_text_part(args)
    if args.cmd == "safe-view":
        view = build_safe_history_view()
        print(json.dumps(view, ensure_ascii=False, indent=2))
        return 0
    if args.cmd == "observability":
        print(json.dumps(get_message_observability(args.job), ensure_ascii=False, indent=2))
        return 0
    if args.cmd == "observability-summary":
        print(json.dumps(get_observability_summary(), ensure_ascii=False, indent=2))
        return 0
    if args.cmd == "getupdates-anomalies":
        return cmd_getupdates_anomalies(args)
    if args.cmd == "verify-channel":
        return cmd_verify_channel(args)
    if args.cmd == "send":
        session = load_session()
        if not session:
            print("send: no session.json, run login first.", flush=True)
            return 3
        try:
            send_text(session, args.text, to_user_id=args.to, context_token=args.context_token, client_id=args.client_id)
        except TokenExpired as e:
            print(f"send: {e}", flush=True)
            return 14
        except Exception as e:
            print(f"send: failed: {e}", flush=True)
            return 1
        print("send: ok", flush=True)
        return 0
    if args.cmd == "send-video":
        return cmd_send_media(args, "video")
    if args.cmd == "send-file":
        return cmd_send_media(args, "file")
    if args.cmd == "health":
        return cmd_health(args)
    if args.cmd == "refresh-qr":
        return cmd_refresh_qr(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
