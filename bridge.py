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
Pillow (image full decode), system openssl (AES-128-ECB), system curl
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

ROOT = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(ROOT, "state")
INBOX = os.path.join(ROOT, "inbox")
RUNNING = os.path.join(ROOT, "running")
DONE = os.path.join(ROOT, "done")
OUTBOX_PENDING = os.path.join(ROOT, "outbox", "pending")
OUTBOX_SENT = os.path.join(ROOT, "outbox", "sent")
OUTBOX_FAILED = os.path.join(ROOT, "outbox", "failed")
MEDIA_INBOUND = os.path.join(ROOT, "media", "inbound")
ANALYSIS_DIR = os.path.join(ROOT, "analysis")
LOCAL_VAULT_DIR = os.path.join(ROOT, "local_vault")
LOCAL_ONLY_MODE_FILE = os.path.join(STATE, "local_only_mode.json")
PENDING_LOCAL_QUERIES_FILE = os.path.join(STATE, "pending_local_queries.json")
TTL_INDEX_FILE = os.path.join(STATE, "local_ttl_index.json")
TOMBSTONE_FILE = os.path.join(STATE, "local_tombstones.json")
LOCAL_TTL_SECONDS = 48 * 3600
TOMBSTONE_RETENTION_SECONDS = 90 * 86400
MEDIA_OUTBOUND = os.path.join(ROOT, "media", "outbound")
FAILURES_DIR = os.path.join(ROOT, "failures")
IMAGE_EVENT_FILE = os.path.join(STATE, "last_image_event.json")
MAX_IMAGE_BYTES = 25 * 1024 * 1024
MAX_VIDEO_PLAIN_BYTES = 100 * 1024 * 1024
MAX_VIDEO_CIPHER_BYTES = MAX_VIDEO_PLAIN_BYTES + 16
MAX_IMAGE_PIXELS = 50_000_000
CDN_BASE_DEFAULT = "https://novac2c.cdn.weixin.qq.com/c2c"
SESSION_FILE = os.path.join(STATE, "session.json")
CURSOR_FILE = os.path.join(STATE, "cursor.txt")
PROCESSED_FILE = os.path.join(STATE, "processed_ids.json")
NEEDS_RELOGIN_FILE = os.path.join(STATE, "NEEDS_RELOGIN")
LOGIN_PENDING_FILE = os.path.join(STATE, "login_pending.json")
LOCK_FILE = os.path.join(STATE, "serve.lock")
import threading as _threading_top
_SESSION_LOCK = _threading_top.RLock()
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
    for d in (STATE, INBOX, RUNNING, DONE, OUTBOX_PENDING, OUTBOX_SENT,
              OUTBOX_FAILED, MEDIA_INBOUND, MEDIA_OUTBOUND, FAILURES_DIR, ANALYSIS_DIR, LOCAL_VAULT_DIR):
        os.makedirs(d, exist_ok=True)


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


def _atomic_tmp_path(path):
    # Unique tmp name, so concurrent writers do not share one .tmp file.
    return f"{path}.{os.getpid()}.tmp"


def _open_private(path, mode):
    # R4 fix: create the tmp file with the final mode from the first
    # instant, instead of creating 0644 under umask 022 and chmodding
    # only after replace. Use os.open with mode, then wrap in a file.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    return os.fdopen(fd, "w", encoding="utf-8")


def atomic_write_json(path, obj, mode=0o600):
    tmp = _atomic_tmp_path(path)
    with _open_private(tmp, mode) as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass
    os.replace(tmp, path)
    # Gemini R2: fsync parent dir so the rename itself is durable
    try:
        _dfd = os.open(os.path.dirname(os.path.abspath(path)) or ".", os.O_DIRECTORY)
        try:
            os.fsync(_dfd)
        finally:
            os.close(_dfd)
    except Exception:
        pass
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def atomic_write_text(path, text, mode=0o600):
    tmp = _atomic_tmp_path(path)
    with _open_private(tmp, mode) as f:
        f.write(text)
    os.replace(tmp, path)
    try:
        os.chmod(path, mode)
    except OSError:
        pass


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


CHANNEL_STATUS_FILE = os.path.join(STATE, "channel_status.json")
SERVE_RUN_FILE = os.path.join(STATE, "serve_run.json")
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
    through a quote. Unknown targets are marked unresolved, never guessed."""
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
    queue notice, voice prompt, image-fail prompt) are written to
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
    """AES-128-ECB with PKCS#7 padding via system openssl (no salt)."""
    if not shutil.which("openssl"):
        raise ApiError("openssl not available for AES-ECB")
    cmd = ["openssl", "enc", "-aes-128-ecb", "-e", "-K", key.hex(), "-nosalt"]
    try:
        r = subprocess.run(cmd, input=plaintext, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        # v1.0.1: TimeoutExpired str() contains the full argv incl. -K hex.
        # Never let that exception object escape; raise a key-free ApiError.
        # v1.0.3: from None cuts the __context__ chain, so a traceback
        # can never print the original TimeoutExpired argv.
        raise ApiError("openssl encrypt timeout") from None
    if r.returncode != 0:
        raise ApiError(f"openssl encrypt failed: {r.stderr[:200]!r}")
    return r.stdout


def aes_ecb_decrypt(ciphertext, key):
    """AES-128-ECB decrypt, strict PKCS#7 padding via openssl.
    Non-zero exit = padding/decrypt failure; caller must NOT save
    the ciphertext as plaintext in that case."""
    if not shutil.which("openssl"):
        raise ApiError("openssl not available for AES-ECB")
    cmd = ["openssl", "enc", "-aes-128-ecb", "-d", "-K", key.hex(), "-nosalt"]
    try:
        r = subprocess.run(cmd, input=ciphertext, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        raise ApiError("openssl decrypt timeout") from None
    if r.returncode != 0:
        raise ApiError(f"openssl decrypt failed (bad padding/key): {r.stderr[:200]!r}")
    return r.stdout


def download_bytes_diag(url, timeout=15):
    """Download with size cap. Returns (bytes_or_None, diag_dict).
    Diag never contains the URL itself (it may carry download params)
    or the body, only status/headers sizes/class/sha.
    Counting points (ChatGPT 2026-10-28 download-integrity fix):
    - http_read: length/sha at HTTP read completion (count 1)
    - pre_decrypt: length/sha just before OpenSSL (count 2, in caller)
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
    """Diagnostic only: decrypt ONE 16-byte block with -nopad.
    Never used for formal decrypt, never outputs plaintext or key.
    Returns 16 bytes or None."""
    if not shutil.which("openssl") or len(key) != 16 or len(block16) != 16:
        return None
    try:
        r = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-d",
                            "-K", key.hex(), "-nosalt", "-nopad"],
                           input=block16, capture_output=True, timeout=10)
        if r.returncode != 0 or len(r.stdout) != 16:
            return None
        return r.stdout
    except Exception:
        return None


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


def start_local_analysis(video_path, video_message_id):
    """Launch the M2 analyzer in the background, outdir INSIDE the
    vault (never analysis/). Returns the outdir, or None on failure.
    The caller must NOT block serve polling on this process."""
    outdir = os.path.join(LOCAL_VAULT_DIR, str(video_message_id), "analysis")
    try:
        os.makedirs(outdir, exist_ok=True)
        subprocess.Popen([sys.executable, os.path.join(ROOT, "analyzer", "m2_analyzer.py"),
                          video_path, outdir],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
        return outdir
    except Exception:
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
    try:
        data = json.load(open(path, encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


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
    (4) purge tombstones older than 90 days (a purged id still
    answers 'not found or deleted', because its files are gone and
    WeChat message_ids are never reused).
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
            delete_local_video_files(mid)
    purged = [m for m, t in list(tombs.items())
              if now - float(t.get("deleted_ts") or 0) > TOMBSTONE_RETENTION_SECONDS]
    if purged:
        for m in purged:
            tombs.pop(m, None)
            idx.pop(m, None)
        save_tombstones(tombs)
        save_ttl_index(idx)
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


SAFE_VIEW_FILE = os.path.join(STATE, "safe_history_view.json")
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
        answer_text = None
        try:
            sent = json.load(open(os.path.join(OUTBOX_SENT, "%s.json" % mid), encoding="utf-8"))
            if isinstance(sent, dict):
                answer_text = sent.get("text")
        except Exception:
            answer_text = None
        rounds.append({
            "message_id": mid,
            "user": {"history_class": "user", "text": user_text},
            "answer": ({"history_class": "formal_answer", "text": answer_text} if answer_text else None),
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
    venv_py = "/home/hatch/workspace/goals/muse-wechat-bridge/hidden_files/m0-component/venv/bin/python"
    if not os.path.exists(analyzer) or not os.path.exists(venv_py):
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
            try:
                subprocess.Popen([venv_py, analyzer, path, outdir],
                                 stdout=logf, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL,
                                 start_new_session=True, close_fds=True)
            finally:
                logf.close()
            analyses.append({"video_index": idx, "outdir": outdir, "status": "queued"})
        except Exception as e:
            analyses.append({"video_index": idx, "outdir": outdir, "status": "queue_failed",
                             "error": type(e).__name__})
    return analyses


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
        if job.get("delivery_state") == "sending":
            job["delivery_state"] = "unknown_restart"
            # v1.0.2: persist the terminal marker on disk BEFORE the
            # move attempt, so a move failure + process restart still
            # leaves a disk-recognisable terminal job (not just a
            # memory set entry). Its text/attachment statuses stay
            # 'pending' on purpose: the delivery result is unknown.
            job["terminal"] = True
            job["needs_check_reason"] = "serve restarted with item in sending state"
            try:
                atomic_write_json(path, job, mode=0o600)
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: recovered sending {name} -> failed/ needs_check (restart)", flush=True)
            except Exception as e:
                _TERMINAL_TASK_IDS.add(name)
                print(f"outbox: recovery failed for {name}: {_sanitize_error(e)}; marked terminal, will NOT resend", flush=True)


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
                job = json.load(f)
        except (OSError, ValueError) as e:
            print(f"outbox: skipping unreadable {name}: {_sanitize_error(e)}", flush=True)
            continue
        # v1.0.1 + v1.0.2: a job already at a terminal status must
        # never be sent again, even if its earlier move to failed/
        # failed and it is still sitting in pending/. A terminal job
        # may only retry archiving, never re-enter sending.
        if name in _TERMINAL_TASK_IDS or _is_terminal_job(job):
            _TERMINAL_TASK_IDS.add(name)
            failed_names.append(name)
            try:
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: terminal {name} archived to failed/ on retry (no resend)", flush=True)
            except OSError as e:
                print(f"outbox: skipping terminal {name}, archive retry failed: {_sanitize_error(e)}; will NOT resend", flush=True)
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
        # Fix 2026-10-28: Worker writes pending with to_user_id from
        # session.json, but that file's key is ilink_user_id, not
        # to_user_id, so pending can arrive with to_user_id null.
        # Fill it here so the sent file is not left with null recipient.
        if not job.get("to_user_id") and session.get("ilink_user_id"):
            job["to_user_id"] = session.get("ilink_user_id")
            changed = True
        # Ensure persisted client_ids before any send.
        if text and not job.get("client_id"):
            job["client_id"] = uuid.uuid4().hex
        for att in attachments:
            if isinstance(att, dict) and not att.get("client_id"):
                att["client_id"] = uuid.uuid4().hex
        # Text item. v1.0.2 sub-item interception: a failed/needs_check
        # text item is terminal on its own, even when other items in
        # the same job are still pending.
        if text and job.get("text_status") not in ("sent", "failed", "needs_check"):
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
                          context_token=job.get("context_token"),
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
                changed = True
                print(f"outbox: text {status} for {name}: {_sanitize_error(e)}", flush=True)
        # Attachment items, each with its persisted client_id.
        for att in attachments:
            if not isinstance(att, dict):
                continue
            # v1.0.2 sub-item interception, same rule as text above.
            if att.get("status") in ("sent", "failed", "needs_check"):
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
                if att.get("type") == "video":
                    send_video(session, att_path, job.get("to_user_id"), job.get("context_token"),
                               client_id=att.get("client_id"))
                else:
                    send_image(session, att_path, job.get("to_user_id"), job.get("context_token"),
                               client_id=att.get("client_id"))
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
                changed = True
                print(f"outbox: {att.get('type') or 'image'} {status} for {name}: {_sanitize_error(e)}", flush=True)
        if attachments:
            job["attachments"] = attachments
        all_sent = (not text or job.get("text_status") == "sent") and all(
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
        if all_sent:
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
            try:
                os.replace(path, os.path.join(OUTBOX_FAILED, name))
                print(f"outbox: needs_check/failed {name} (moved to failed/, no auto-retry for unknown)", flush=True)
            except OSError as e:
                print(f"outbox: failed move FAILED for {name}: {_sanitize_error(e)}; kept in pending but marked terminal, will NOT resend", flush=True)
            failed_names.append(name)
        else:
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
    # Audit 3: recover any sending-state left by a crashed previous serve.
    recover_sending_on_startup()
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
                _evp = os.path.join(_q.get("outdir", ""), "evidence.json")
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
        except TokenExpired:
            print("serve: token expired (-14), wrote NEEDS_RELOGIN, stopping.", flush=True)
            return 14
        except Exception as e:
            # v1.0.2: post_json now carries curl_err + urllib_err +
            # desensitized target/proxy; sanitise again as defence in depth.
            _record_channel_status(False, _sanitize_error(e))
            _log_ts(f"serve: getupdates error, backing off 5s: {_sanitize_error(e)}")
            time.sleep(5)
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
            if not text and not is_voice_only and not image_items and not video_items:
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
                        _outdir = start_local_analysis(_vpath, _video_mid)
                        if _outdir is None:
                            _reply = "本地分析启动失败，请重发本地问。"
                        else:
                            _pend = [q for q in load_pending_local_queries() if q.get("video_mid") != _video_mid]
                            _pend.append({"query_mid": mid, "video_mid": _video_mid, "outdir": _outdir, "to_user_id": session.get("ilink_user_id"), "context_token": ctx, "started_ts": time.time()})
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
            if image_items and not saved_images and not text:
                # All images failed and no text: explicit prompt, no inbox.
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
            if video_items and not saved_videos and not text:
                # All videos failed and no text: explicit prompt, no inbox.
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
            record = {
                "message_id": msg.get("message_id"),
                "text": text,
                "from_user_id": msg.get("from_user_id"),
                "context_token": ctx,
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
            try:
                atomic_write_json(os.path.join(INBOX, f"{mid}.json"), record, mode=0o600)
            except Exception as _inbox_e:
                _batch_enqueue_failed = True
                print(f"serve: inbox write FAILED for {mid}: {_sanitize_error(_inbox_e)}, NOT marking processed, will retry", flush=True)
                continue
            processed.add(mid); save_processed(processed)
            _log_ts(f"serve: queued inbox/{mid}.json")
            # Phase 2 step 3: typing + one queue notice only.
            # Do NOT claim Muse has started executing before Worker runs.
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
            try:
                send_direct_or_outbox(session, next_queue_notice(),
                                      session.get("ilink_user_id"), ctx,
                                      f"{mid}_queue.json")
            except TokenExpired:
                print("serve: token expired during queue notice, stopping.", flush=True)
                return 14
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
    sub.add_parser("verify-channel", help="Production Worker channel-recovery check (read-only JSON, exit 0 only if recovered)")
    sub.add_parser("safe-view", help="Print the deterministic safe history view (read-only JSON)")
    args = parser.parse_args(argv)
    ensure_dirs()
    if args.cmd == "login":
        return cmd_login(args)
    if args.cmd == "serve":
        return serve_loop()
    if args.cmd == "status":
        return cmd_status(args)
    if args.cmd == "safe-view":
        view = build_safe_history_view()
        print(json.dumps(view, ensure_ascii=False, indent=2))
        return 0
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
    return 2


if __name__ == "__main__":
    sys.exit(main())
