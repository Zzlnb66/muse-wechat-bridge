"""Offline tests for the GPT-review follow-ups, user decision 2026-10-31
(keep the unknown-quote fallback, document the tradeoff).

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network:
  1. drain_outbox recipient mismatch -> parked to failed/, never sent.
  2. drain_outbox recipient match -> normal send, no block.
  3. _resolve_analyzer_py: unset and empty env both mean the default.
  4. start_local_analysis honors the env override, and falls back to
     sys.executable when the env value points nowhere.
  5. _scrub_job_token failure -> failures/ note (no token value in it),
     delivery state untouched; success -> token-free file on disk.
  6. Unknown-id quote STILL falls back to platform inline text: this is
     the documented tradeoff (user decision 2026-10-31), not a bug.
  7. A tombstoned id is guarded even with no done/inbox record at all:
     this is the mechanism the pre-tombstone backfill relies on.

Run: python3 tests/test_gpt_followup.py (exit 0 = all pass).
"""

import json
import os
import sys
import tempfile

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


def test_recipient_mismatch():
    tmp = tempfile.mkdtemp(prefix="gpt-recipient-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        job = {"text": "hi", "to_user_id": "user-A", "client_id": "c1"}
        with open(os.path.join(pend, "m1.json"), "w", encoding="utf-8") as f:
            json.dump(job, f)
        calls = []
        orig_send = bridge.send_text
        bridge.send_text = lambda *a, **k: calls.append((a, k))
        try:
            sent_names, failed_names = bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            bridge.send_text = orig_send
        check("recipient mismatch: send_text never called", len(calls) == 0)
        check("recipient mismatch: job in failed_names", "m1.json" in failed_names)
        check("recipient mismatch: job not in sent_names", "m1.json" not in sent_names)
        archived = os.path.join(failed, "m1.json")
        check("recipient mismatch: parked in failed/", os.path.exists(archived))
        if os.path.exists(archived):
            parked = json.load(open(archived, encoding="utf-8"))
            check("recipient mismatch: terminal + reason recorded",
                  parked.get("terminal") is True
                  and parked.get("needs_check_reason") == "recipient_mismatch"
                  and parked.get("delivery_state") == "needs_check")
        bridge._TERMINAL_TASK_IDS.discard("m1.json")
    finally:
        _restore_outbox(old)


def test_recipient_match():
    tmp = tempfile.mkdtemp(prefix="gpt-recipient-ok-")
    old, pend, sent, failed = _setup_outbox(tmp)
    try:
        job = {"text": "hi", "to_user_id": "user-B", "client_id": "c2"}
        with open(os.path.join(pend, "m2.json"), "w", encoding="utf-8") as f:
            json.dump(job, f)
        calls = []
        orig_send = bridge.send_text
        bridge.send_text = lambda *a, **k: calls.append((a, k)) or {}
        try:
            sent_names, failed_names = bridge.drain_outbox({"ilink_user_id": "user-B"})
        finally:
            bridge.send_text = orig_send
        check("recipient match: send_text called once", len(calls) == 1)
        check("recipient match: job sent", "m2.json" in sent_names)
        check("recipient match: archived in sent/",
              os.path.exists(os.path.join(sent, "m2.json")))
    finally:
        _restore_outbox(old)


def test_resolve_analyzer_py():
    saved = os.environ.get("WECHAT_BRIDGE_ANALYZER_PY")
    try:
        os.environ.pop("WECHAT_BRIDGE_ANALYZER_PY", None)
        check("analyzer py: unset env -> default",
              bridge._resolve_analyzer_py() == bridge.DEFAULT_ANALYZER_PY)
        os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = ""
        check("analyzer py: empty env -> default (not empty string)",
              bridge._resolve_analyzer_py() == bridge.DEFAULT_ANALYZER_PY)
        os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = "/tmp/fake-py"
        check("analyzer py: set env wins",
              bridge._resolve_analyzer_py() == "/tmp/fake-py")
    finally:
        if saved is None:
            os.environ.pop("WECHAT_BRIDGE_ANALYZER_PY", None)
        else:
            os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = saved


def test_start_local_analysis_paths():
    tmp = tempfile.mkdtemp(prefix="gpt-local-analysis-")
    old_vault = bridge.LOCAL_VAULT_DIR
    bridge.LOCAL_VAULT_DIR = tmp
    orig_popen = bridge.subprocess.Popen
    seen = []
    bridge.subprocess.Popen = lambda argv, **k: seen.append(argv)
    saved = os.environ.get("WECHAT_BRIDGE_ANALYZER_PY")
    try:
        os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = "/nonexistent/py-xyz"
        out = bridge.start_local_analysis("/tmp/fake.mp4", "mid1")
        check("local analysis: missing env path falls back to sys.executable",
              out is not None and seen and seen[0][0] == sys.executable)
        real_py = os.path.join(tmp, "fake-py")
        with open(real_py, "w", encoding="utf-8") as f:
            f.write("")
        os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = real_py
        seen.clear()
        out = bridge.start_local_analysis("/tmp/fake.mp4", "mid2")
        check("local analysis: existing env path is honored",
              out is not None and seen and seen[0][0] == real_py)
    finally:
        bridge.subprocess.Popen = orig_popen
        bridge.LOCAL_VAULT_DIR = old_vault
        if saved is None:
            os.environ.pop("WECHAT_BRIDGE_ANALYZER_PY", None)
        else:
            os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = saved


def test_scrub_helper():
    tmp = tempfile.mkdtemp(prefix="gpt-scrub-")
    old_fail = bridge.FAILURES_DIR
    bridge.FAILURES_DIR = os.path.join(tmp, "failures")
    os.makedirs(bridge.FAILURES_DIR, exist_ok=True)
    try:
        # Failure path: parent is a file, so the write cannot succeed.
        blocker = os.path.join(tmp, "blocker")
        with open(blocker, "w", encoding="utf-8") as f:
            f.write("x")
        secret = "SECRET-TOKEN-VALUE-NEVER-PRINTED"
        job = {"message_id": "scrub1", "delivery_state": "sent",
               "context_token": secret, "text": "hi"}
        res = bridge._scrub_job_token(os.path.join(blocker, "scrub1.json"), job)
        note_path = os.path.join(bridge.FAILURES_DIR, "scrub1_scrub.json")
        check("scrub failure: helper returns False", res is False)
        check("scrub failure: failures/ note written", os.path.exists(note_path))
        if os.path.exists(note_path):
            note = open(note_path, encoding="utf-8").read()
            check("scrub failure: note has kind, no token value",
                  "token_scrub_failed" in note and secret not in note)
        check("scrub failure: token popped from memory job",
              "context_token" not in job)
        # Success path.
        good_path = os.path.join(tmp, "good.json")
        job2 = {"message_id": "scrub2", "delivery_state": "sent",
                "context_token": secret, "text": "hi"}
        res2 = bridge._scrub_job_token(good_path, job2)
        on_disk = json.load(open(good_path, encoding="utf-8")) if os.path.exists(good_path) else {}
        check("scrub success: returns True, disk file token-free",
              res2 is True and "context_token" not in on_disk)
    finally:
        bridge.FAILURES_DIR = old_fail


def test_quote_tradeoff_and_backfill():
    tmp = tempfile.mkdtemp(prefix="gpt-quote-")
    done = os.path.join(tmp, "done")
    inbox = os.path.join(tmp, "inbox")
    os.makedirs(done, exist_ok=True)
    os.makedirs(inbox, exist_ok=True)
    old_done, old_inbox = bridge.DONE, bridge.INBOX
    old_tombs = bridge.load_tombstones
    bridge.DONE, bridge.INBOX = done, inbox
    try:
        # Unknown id: fallback kept by design (user decision 2026-10-31).
        bridge.load_tombstones = lambda: {}
        msg = {"message_id": "probe", "item_list": [
            {"type": 1, "text_item": {"text": "这是什么"},
             "ref_msg": {"svr_id": "999999", "title": "平台标题",
                         "message_item": {"msg_id": "999999", "type": 1,
                                          "item_list": [{"type": 1, "text_item": {"text": "平台内联原文"}}]}}}]}
        q = bridge.resolve_quote(bridge.extract_quote(msg))
        check("quote tradeoff: unknown id still returns platform inline",
              q.get("quoted_text") == "平台内联原文" and q.get("quoted_resolved") is True)
        # Tombstoned id with NO record at all: guard holds (backfill case).
        bridge.load_tombstones = lambda: {"7521952422146222856": {"reason": "backfill"}}
        msg2 = {"message_id": "probe2", "item_list": [
            {"type": 1, "text_item": {"text": "这是什么"},
             "ref_msg": {"svr_id": "7521952422146222856", "title": "平台标题",
                         "message_item": {"msg_id": "7521952422146222856", "type": 1,
                                          "item_list": [{"type": 1, "text_item": {"text": "平台内联原文"}}]}}}]}
        q2 = bridge.resolve_quote(bridge.extract_quote(msg2))
        check("quote backfill: tombstoned id guarded without any record",
              q2.get("quoted_local_only") is True
              and "quoted_text" not in q2 and "quoted_title" not in q2)
    finally:
        bridge.DONE, bridge.INBOX = old_done, old_inbox
        bridge.load_tombstones = old_tombs


test_recipient_mismatch()
test_recipient_match()
test_resolve_analyzer_py()
test_start_local_analysis_paths()
test_scrub_helper()
test_quote_tradeoff_and_backfill()

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
