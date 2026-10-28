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
MEDIA_OUTBOUND = os.path.join(ROOT, "media", "outbound")
FAILURES_DIR = os.path.join(ROOT, "failures")
IMAGE_EVENT_FILE = os.path.join(STATE, "last_image_event.json")
MAX_IMAGE_BYTES = 25 * 1024 * 1024
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
              OUTBOX_FAILED, MEDIA_INBOUND, MEDIA_OUTBOUND, FAILURES_DIR):
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
                print(f"outbox: image {status} for {name}: {_sanitize_error(e)}", flush=True)
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
    print("serve: lock acquired, polling. Ctrl-C to stop.", flush=True)
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
        except TokenExpired:
            print("serve: token expired (-14), wrote NEEDS_RELOGIN, stopping.", flush=True)
            return 14
        except Exception as e:
            # v1.0.2: post_json now carries curl_err + urllib_err +
            # desensitized target/proxy; sanitise again as defence in depth.
            print(f"serve: getupdates error, backing off 5s: {_sanitize_error(e)}", flush=True)
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
            if not text and not is_voice_only and not image_items:
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
            # Normal queued task (text, voice with transcription, and/or images).
            record = {
                "message_id": msg.get("message_id"),
                "text": text,
                "from_user_id": msg.get("from_user_id"),
                "context_token": ctx,
                "ts": msg.get("create_time_ms") or int(time.time() * 1000),
            }
            if voice_text:
                record["source"] = "voice"
            if saved_images:
                record["images"] = saved_images
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
            print(f"serve: queued inbox/{mid}.json", flush=True)
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
    args = parser.parse_args(argv)
    ensure_dirs()
    if args.cmd == "login":
        return cmd_login(args)
    if args.cmd == "serve":
        return serve_loop()
    if args.cmd == "status":
        return cmd_status(args)
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
