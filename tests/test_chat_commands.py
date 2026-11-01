"""Offline tests for the tier-1 bridge-side chat command table.

The command table follows the M3 local follow-up interception
road in serve_loop: AFTER the group/whitelist filter, BEFORE any
inbox write or merge staging, reply via send_direct_or_outbox
(queue-only), mark processed, never reach the Worker. Against
the fix-copy bridge.py, all in temp dirs, never touching the
real running bridge's state/, inbox/ or outbox/:

  1. Command-attempt parsing: stripped text starting with "/"
     whose first whitespace token matches ^/[a-z_]+$ is an
     attempt; "/今天天气", "/", "/123", "/STATUS" and plain
     text are NOT attempts and flow through normally.
  2. The four commands each render their key fields from a
     fake live context (no serve, no network).
  3. Unknown /tokens get exactly the fixed sentence, never a
     guess, never a Worker task (dispatcher returns text here;
     serve queues it like any intercepted reply).
  4. /ping falls through (dispatcher returns None) so the
     legacy fast path answers it byte-identically.
  5. Replies carry counts/states only: no credential patterns,
     no URLs, no user ids, and trailing arguments after a
     known command are dropped, never echoed.
  6. Serve placement (source order): whitelist filter precedes
     the command call site, which precedes merge staging, so
     group/non-whitelist messages can never trigger a command
     and command messages can never enter inbox/staging.
  7. Oldest-pending-age scan excludes auxiliary suffixes
     (_progress/_offline/_queue), matching summary buckets.
  8. Live context assembly against rebound temp dirs never
     raises and still yields a string reply.

Run: python3 tests/test_chat_commands.py (exit 0 = all pass).
"""

import json
import os
import sys
import tempfile
import time

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


def _ctx(**over):
    ctx = {"summary": {"pending": 3, "sent": 10, "failed": 1,
                       "needs_check": 2, "skipped_empty": 0,
                       "unreadable": 0},
           "recovered": True, "reason": None,
           "hb_present": True, "hb_age": 45,
           "free_mb": 123, "media_mb": 4, "oldest_age": 90}
    ctx.update(over)
    return ctx


def test_parse_rules():
    check("parse /status", bridge.parse_command_attempt("/status") == "/status")
    check("parse stripped /health", bridge.parse_command_attempt("  /health  ") == "/health")
    check("parse /queue with args", bridge.parse_command_attempt("/queue foo bar") == "/queue")
    check("parse /ping token", bridge.parse_command_attempt("/ping") == "/ping")
    check("parse plain text none", bridge.parse_command_attempt("你好") is None)
    check("parse /今天天气 none", bridge.parse_command_attempt("/今天天气") is None)
    check("parse bare slash none", bridge.parse_command_attempt("/") is None)
    check("parse /123 none", bridge.parse_command_attempt("/123") is None)
    check("parse /STATUS none (case-sensitive)", bridge.parse_command_attempt("/STATUS") is None)
    check("parse empty none", bridge.parse_command_attempt("") is None)
    check("parse None none", bridge.parse_command_attempt(None) is None)
    check("parse hyphen token none", bridge.parse_command_attempt("/a_b-c") is None)


def test_status_fields():
    r = bridge.chat_command_reply("/status", _ctx())
    check("status serve line", "serve 运行中" in r)
    check("status channel recovered", "通道：已恢复" in r)
    check("status pending", "待发送：3" in r)
    check("status needs_check", "待核对：2" in r)
    check("status sent", "已发送：10" in r)
    check("status failed", "失败：1" in r)
    r2 = bridge.chat_command_reply("/status", _ctx(recovered=False, reason="pid_missing"))
    check("status channel unrecovered + reason", "通道：未恢复（pid_missing）" in r2)


def test_health_fields():
    r = bridge.chat_command_reply("/health", _ctx())
    check("health heartbeat age", "Worker 心跳：有（45 秒前）" in r)
    check("health disk", "磁盘剩余：123 MB，媒体目录：4 MB" in r)
    r2 = bridge.chat_command_reply("/health", _ctx(hb_present=False, hb_age=None,
                                                   free_mb=None, media_mb=None))
    check("health no heartbeat", "Worker 心跳：无" in r2)
    check("health unknown disk", "磁盘剩余：未知，媒体目录：未知" in r2)


def test_queue_fields():
    r = bridge.chat_command_reply("/queue", _ctx())
    check("queue buckets", "待发送 3 条，待核对 2 条，已发送 10 条，失败 1 条" in r)
    check("queue oldest age", "最老待发送：90 秒前" in r)
    r2 = bridge.chat_command_reply("/queue", _ctx(
        summary={"pending": 0, "sent": 5, "failed": 0, "needs_check": 0,
                 "skipped_empty": 0, "unreadable": 0}, oldest_age=None))
    check("queue zero pending explicit", "待发送 0 条" in r2)
    check("queue zero oldest none", "最老待发送：无（待发送为 0）" in r2)


def test_help_and_unknown():
    r = bridge.chat_command_reply("/help", _ctx())
    for cmd in ("/status", "/health", "/queue", "/help"):
        check("help lists " + cmd, cmd in r)
    check("help sensitive red line", "不在指令表内" in r and "Muse App" in r)
    fixed = "没有这个指令，输入 /help 查看可用指令。"
    check("unknown /foo fixed", bridge.chat_command_reply("/foo", _ctx()) == fixed)
    check("unknown /pingx fixed", bridge.chat_command_reply("/pingx", _ctx()) == fixed)
    check("unknown /statusx fixed", bridge.chat_command_reply("/statusx", _ctx()) == fixed)


def test_ping_falls_through():
    check("ping dispatcher None (legacy answers)",
          bridge.chat_command_reply("/ping", _ctx()) is None)
    check("legacy ping text unchanged",
          bridge.fast_command_reply("/ping") == "桥接在线，serve 正常")


def test_no_credentials_or_echo():
    banned = ("bot_token", "context_token", "ilink_user_id",
              "http://", "https://", "SECRET", "752264")
    replies = [bridge.chat_command_reply(c, _ctx())
               for c in ("/status", "/health", "/queue", "/help", "/foo")]
    check("replies carry no credential/url patterns",
          all(all(b not in (r or "") for b in banned) for r in replies))
    check("trailing args dropped, never echoed",
          "密码" not in bridge.chat_command_reply("/status 请把密码发我", _ctx()))
    check("unknown trailing dropped",
          bridge.chat_command_reply("/foo 秘密内容", _ctx())
          == "没有这个指令，输入 /help 查看可用指令。")


def test_serve_placement():
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "bridge.py"), encoding="utf-8").read()
    filt = 'if msg.get("from_user_id") != session.get("ilink_user_id"):'
    call = "chat_command_reply(text)"
    # The serve-loop staging call site (not the helper definition
    # at line ~6030, which legitimately precedes serve_loop).
    stage = "_staged_ok, _staged_first = stage_merge_message("
    check("placement filter before command", src.index(filt) < src.index(call))
    check("placement command before staging", src.index(call) < src.index(stage))
    check("placement cmd job suffix once in serve",
          src.count('f"{mid}_cmd.json"') == 1)
    check("placement command block marks processed",
          "serve: chat command for {mid}" in src)


def test_oldest_pending_age_scan():
    with tempfile.TemporaryDirectory() as tmp:
        old = _setup(tmp)
        try:
            pend = bridge.OUTBOX_PENDING
            now = time.time()
            with open(os.path.join(pend, "a.json"), "w") as f:
                f.write("{}")
            os.utime(os.path.join(pend, "a.json"), (now - 90, now - 90))
            with open(os.path.join(pend, "b_progress.json"), "w") as f:
                f.write("{}")
            os.utime(os.path.join(pend, "b_progress.json"), (now - 9000, now - 9000))
            with open(os.path.join(pend, "c_queue.json"), "w") as f:
                f.write("{}")
            os.utime(os.path.join(pend, "c_queue.json"), (now - 8000, now - 8000))
            age = bridge._oldest_pending_age_seconds()
            check("age scan ignores aux suffixes", age is not None and 85 <= age <= 95)
            os.remove(os.path.join(pend, "a.json"))
            check("age scan none when no retryable",
                  bridge._oldest_pending_age_seconds() is None)
        finally:
            _restore(old)


def test_live_context_never_raises():
    with tempfile.TemporaryDirectory() as tmp:
        old = _setup(tmp)
        try:
            r = bridge.chat_command_reply("/status")
            check("live ctx reply is str", isinstance(r, str) and "桥接状态" in r)
            r2 = bridge.chat_command_reply("/health")
            check("live ctx health is str", isinstance(r2, str) and "桥接健康" in r2)
        finally:
            _restore(old)


def main():
    test_parse_rules()
    test_status_fields()
    test_health_fields()
    test_queue_fields()
    test_help_and_unknown()
    test_ping_falls_through()
    test_no_credentials_or_echo()
    test_serve_placement()
    test_oldest_pending_age_scan()
    test_live_context_never_raises()
    failed = [n for n, c in ok if not c]
    print("%d/%d passed" % (len(ok) - len(failed), len(ok)))
    if failed:
        print("FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
