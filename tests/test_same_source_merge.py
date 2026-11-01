"""Gate 2 item 4: same-source merge. Temp dirs only, fake senders,
injectable clock. Never touches the live bridge state/inbox/outbox
and never sends a real WeChat message.

Run: python3 tests/test_same_source_merge.py
Exit non-zero on any failure.
"""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

PASS = 0
FAIL = 0
FAILURES = []


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS: %s" % name)
    else:
        FAIL += 1
        FAILURES.append(name)
        print("FAIL: %s" % name)


def _setup():
    tmp = tempfile.mkdtemp(prefix="merge-test-")
    old = {}
    for n in ("STATE", "INBOX", "RUNNING", "DONE", "OUTBOX_PENDING",
              "OUTBOX_SENT", "OUTBOX_FAILED", "MEDIA_INBOUND",
              "MEDIA_OUTBOUND", "FAILURES_DIR", "ANALYSIS_DIR",
              "LOCAL_VAULT_DIR"):
        old[n] = getattr(bridge, n)
        d = os.path.join(tmp, n.lower())
        os.makedirs(d, exist_ok=True)
        setattr(bridge, n, d)
    return tmp, old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _teardown(tmp):
    shutil.rmtree(tmp, ignore_errors=True)


def _set_flag(enabled):
    with open(os.path.join(bridge.STATE, "same_source_merge.json"),
              "w", encoding="utf-8") as f:
        json.dump({"enabled": enabled}, f)


def _rec(mid, text, **kw):
    r = {"message_id": mid, "text": text, "from_user_id": "user-B",
         "ts": 1700000000000, "history_class": "user",
         "egress_policy": "muse_default", "policy_version": "safe-0.1"}
    r.update(kw)
    return r


def _read_inbox(mid):
    with open(os.path.join(bridge.INBOX, "%s.json" % mid), encoding="utf-8") as f:
        return json.load(f)


def _staging_names():
    d = os.path.join(bridge.STATE, "merge_staging")
    return sorted(os.listdir(d)) if os.path.isdir(d) else []


def _manifest(anchor):
    p = os.path.join(bridge.STATE, "merge_manifests", "%s.json" % anchor)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _timeline():
    p = os.path.join(bridge.STATE, "message_timeline.jsonl")
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_flag_default():
    tmp, old = _setup()
    try:
        check("flag: missing file means OFF", bridge.read_same_source_merge_mode() is False)
        with open(os.path.join(bridge.STATE, "same_source_merge.json"),
                  "w", encoding="utf-8") as f:
            f.write("{not json")
        check("flag: corrupt file means OFF", bridge.read_same_source_merge_mode() is False)
        _set_flag(True)
        check("flag: enabled true reads ON", bridge.read_same_source_merge_mode() is True)
        _set_flag(False)
        check("flag: enabled false reads OFF", bridge.read_same_source_merge_mode() is False)
    finally:
        _restore(old)
        _teardown(tmp)


def test_single_not_merged():
    tmp, old = _setup()
    try:
        _set_flag(True)
        rec = _rec("111", "hello alone")
        ok, first = bridge.stage_merge_message("111", "user-B", rec, "CTX-1", received_ts=1000.0)
        check("single: stage ok and first", ok is True and first is True)
        got = bridge.maybe_promote_merge_groups(now=1010.0)
        check("single: promoted as itself", got == ["111"])
        out = _read_inbox("111")
        check("single: record verbatim, no merged fields",
              out == rec and "merged" not in out and "merged_message_ids" not in out)
        check("single: no manifest written", _manifest("111") is None)
        check("single: staging drained", _staging_names() == [])
        tl = [r for r in _timeline() if r["job"] == "111" and r["stage"] == "received"]
        check("single: timeline received at promotion", len(tl) == 1
              and tl[0]["source"] == "serve_inbox_write")
    finally:
        _restore(old)
        _teardown(tmp)


def test_window_holds_and_boundary():
    tmp, old = _setup()
    try:
        _set_flag(True)
        bridge.stage_merge_message("201", "user-B", _rec("201", "first"), "CTX-1", received_ts=1000.0)
        bridge.stage_merge_message("202", "user-B", _rec("202", "second"), "CTX-2", received_ts=1001.0)
        check("window: held at 4.9s after first",
              bridge.maybe_promote_merge_groups(now=1004.9) == []
              and len(_staging_names()) == 2
              and not os.path.exists(os.path.join(bridge.INBOX, "201.json")))
        got = bridge.maybe_promote_merge_groups(now=1005.0)
        check("window: promoted at exactly 5.0s boundary", got == ["201"])
        merged = _read_inbox("201")
        check("window: merged ids in received order",
              merged.get("merged_message_ids") == ["201", "202"] and merged.get("merged") is True)
    finally:
        _restore(old)
        _teardown(tmp)


def test_merge_content_and_tokens():
    tmp, old = _setup()
    try:
        _set_flag(True)
        # Stage order is the group order (staging order == serve
        # processing order), even when received_ts values are
        # injected out of order: the anchor must be the
        # first-STAGED message, because it holds the typing ticket.
        ok2, first2 = bridge.stage_merge_message(
            "302", "user-B", _rec("302", "second text", images=["/tmp/a.jpg"],
                                  quote="quoted words"), "CTX-2", received_ts=1001.0)
        ok1, first1 = bridge.stage_merge_message(
            "301", "user-B", _rec("301", "first text", images=["/tmp/b.jpg", "/tmp/c.jpg"]),
            "CTX-1", received_ts=1000.0)
        ok3, first3 = bridge.stage_merge_message(
            "303", "user-B", _rec("303", "third text", files=["/tmp/f.bin"]),
            "CTX-3", received_ts=1002.0)
        check("content: staging flags first/follower",
              first2 is True and first1 is False and first3 is False
              and ok1 and ok2 and ok3)
        bridge.maybe_promote_merge_groups(now=1010.0)
        merged = _read_inbox("302")
        check("content: anchor is first-STAGED (ticket holder)",
              merged["message_id"] == "302")
        check("content: merged_message_ids complete in staging order",
              merged.get("merged_message_ids") == ["302", "301", "303"])
        text = merged.get("text", "")
        check("content: labeled text keeps every part and source mid",
              "first text" in text and "second text" in text and "third text" in text
              and "[源消息 301" in text and "[源消息 302" in text and "[源消息 303" in text)
        check("content: top images/files concatenated in staging order",
              merged.get("images") == ["/tmp/a.jpg", "/tmp/b.jpg", "/tmp/c.jpg"]
              and merged.get("files") == ["/tmp/f.bin"])
        parts = {p["message_id"]: p for p in merged.get("merged_parts", [])}
        check("content: merged_parts attribution",
              parts.get("302", {}).get("text") == "second text"
              and parts.get("302", {}).get("images") == ["/tmp/a.jpg"]
              and parts.get("303", {}).get("files") == ["/tmp/f.bin"])
        check("content: quotes traceable, no ambiguous top quote",
              merged.get("merged_quotes") == [{"source_message_id": "302", "quote": "quoted words"}]
              and "quote" not in merged)
        raw = json.dumps(merged, ensure_ascii=False)
        check("content: merged record carries NO token (B2-lite)",
              "CTX-1" not in raw and "CTX-2" not in raw and "CTX-3" not in raw
              and "context_token" not in raw)
        man = _manifest("302")
        check("content: manifest keeps every token mapping privately",
              man is not None
              and man.get("tokens_by_message") == {"301": "CTX-1", "302": "CTX-2", "303": "CTX-3"}
              and man.get("tokens_unverified_for_send") is True
              and man.get("merged_message_ids") == ["302", "301", "303"])
    finally:
        _restore(old)
        _teardown(tmp)


def test_cross_window_and_senders():
    tmp, old = _setup()
    try:
        _set_flag(True)
        bridge.stage_merge_message("401", "user-B", _rec("401", "early"), "CTX-1", received_ts=1000.0)
        bridge.maybe_promote_merge_groups(now=1006.0)
        bridge.stage_merge_message("402", "user-B", _rec("402", "late"), "CTX-2", received_ts=1007.0)
        bridge.maybe_promote_merge_groups(now=1013.0)
        check("cross-window: two verbatim singles, no merge",
              _read_inbox("401") == _rec("401", "early")
              and _read_inbox("402") == _rec("402", "late")
              and _manifest("401") is None and _manifest("402") is None)
        # Two senders in the same window must never merge together.
        bridge.stage_merge_message("411", "user-B", _rec("411", "b one"), "CTX-1", received_ts=2000.0)
        bridge.stage_merge_message("412", "user-B", _rec("412", "b two"), "CTX-2", received_ts=2001.0)
        bridge.stage_merge_message("421", "user-C", _rec("421", "c one"), "CTX-9", received_ts=2000.5)
        bridge.stage_merge_message("422", "user-C", _rec("422", "c two"), "CTX-8", received_ts=2001.5)
        got = bridge.maybe_promote_merge_groups(now=2010.0)
        check("senders: two separate groups promoted",
              sorted(got) == ["411", "421"])
        check("senders: no cross-sender contamination",
              _read_inbox("411").get("merged_message_ids") == ["411", "412"]
              and _read_inbox("421").get("merged_message_ids") == ["421", "422"])
    finally:
        _restore(old)
        _teardown(tmp)


def test_switch_off_verbatim():
    tmp, old = _setup()
    try:
        _set_flag(False)
        bridge.stage_merge_message("501", "user-B", _rec("501", "off one"), "CTX-1", received_ts=1000.0)
        bridge.stage_merge_message("502", "user-B", _rec("502", "off two"), "CTX-2", received_ts=1001.0)
        got = bridge.maybe_promote_merge_groups(now=1001.0)
        check("off: staged group drains verbatim immediately",
              sorted(got) == ["501", "502"]
              and _read_inbox("501") == _rec("501", "off one")
              and _read_inbox("502") == _rec("502", "off two")
              and _manifest("501") is None and _staging_names() == [])
    finally:
        _restore(old)
        _teardown(tmp)


def test_local_only_never_merges():
    tmp, old = _setup()
    try:
        _set_flag(True)
        bridge.stage_merge_message("601", "user-B", _rec("601", "normal"), "CTX-1", received_ts=1000.0)
        bridge.stage_merge_message(
            "602", "user-B",
            _rec("602", "", local_only=True, local_video_count=1,
                 local_only_note="仅本地保存"), "CTX-2", received_ts=1001.0)
        got = bridge.maybe_promote_merge_groups(now=1010.0)
        check("local-only: group members promoted verbatim, not merged",
              sorted(got) == ["601", "602"]
              and _read_inbox("602").get("local_only") is True
              and "merged" not in _read_inbox("601")
              and _manifest("601") is None)
        check("local-only: builder refuses a local_only group",
              _raises_valueerror())
    finally:
        _restore(old)
        _teardown(tmp)


def _raises_valueerror():
    try:
        bridge.build_merged_record([
            {"message_id": "601", "from_user_id": "user-B", "received_ts": 1000.0,
             "record": _rec("601", "x")},
            {"message_id": "602", "from_user_id": "user-B", "received_ts": 1001.0,
             "record": _rec("602", "", local_only=True)},
        ])
        return False
    except ValueError:
        return True


def test_promotion_idempotent_and_no_regen():
    tmp, old = _setup()
    try:
        _set_flag(True)
        bridge.stage_merge_message("701", "user-B", _rec("701", "one"), "CTX-1", received_ts=1000.0)
        bridge.stage_merge_message("702", "user-B", _rec("702", "two"), "CTX-2", received_ts=1001.0)
        bridge.maybe_promote_merge_groups(now=1010.0)
        with open(os.path.join(bridge.INBOX, "701.json"), encoding="utf-8") as f:
            before = f.read()
        check("idempotent: second promotion is a no-op",
              bridge.maybe_promote_merge_groups(now=1020.0) == [])
        with open(os.path.join(bridge.INBOX, "701.json"), encoding="utf-8") as f:
            after = f.read()
        check("idempotent: inbox file unchanged", before == after)
        # A fresh message afterwards forms a NEW group, never
        # re-merges into the completed one.
        bridge.stage_merge_message("703", "user-B", _rec("703", "fresh"), "CTX-3", received_ts=1030.0)
        got = bridge.maybe_promote_merge_groups(now=1040.0)
        check("no-regen: fresh message promotes alone",
              got == ["703"] and _read_inbox("703") == _rec("703", "fresh"))
    finally:
        _restore(old)
        _teardown(tmp)


def test_promotion_failure_keeps_staging():
    tmp, old = _setup()
    try:
        _set_flag(True)
        bridge.stage_merge_message("801", "user-B", _rec("801", "one"), "CTX-1", received_ts=1000.0)
        bridge.stage_merge_message("802", "user-B", _rec("802", "two"), "CTX-2", received_ts=1001.0)
        # Break the inbox directory: promotion must fail loudly
        # and keep staging for the next tick.
        inbox_dir = bridge.INBOX
        shutil.rmtree(inbox_dir)
        with open(inbox_dir, "w", encoding="utf-8") as f:
            f.write("not a directory")
        got = bridge.maybe_promote_merge_groups(now=1010.0)
        check("failure: promotion fails without consuming staging",
              got == [] and len(_staging_names()) == 2)
        os.remove(inbox_dir)
        os.makedirs(inbox_dir, exist_ok=True)
        got = bridge.maybe_promote_merge_groups(now=1010.0)
        check("failure: retry after repair succeeds",
              got == ["801"] and _read_inbox("801").get("merged_message_ids") == ["801", "802"])
    finally:
        _restore(old)
        _teardown(tmp)


def test_stage_failure():
    tmp, old = _setup()
    try:
        # Break the staging directory path: staging must report
        # failure so the caller does NOT mark the message processed.
        staging = os.path.join(bridge.STATE, "merge_staging")
        with open(staging, "w", encoding="utf-8") as f:
            f.write("not a directory")
        ok, first = bridge.stage_merge_message("901", "user-B", _rec("901", "x"), "CTX-1", received_ts=1000.0)
        check("stage-failure: returns (False, False)", ok is False and first is False)
    finally:
        _restore(old)
        _teardown(tmp)


def test_completion_records():
    tmp, old = _setup()
    try:
        _set_flag(True)
        bridge.stage_merge_message("1001", "user-B", _rec("1001", "one"), "CTX-1", received_ts=1000.0)
        bridge.stage_merge_message("1002", "user-B", _rec("1002", "two"), "CTX-2", received_ts=1001.0)
        bridge.maybe_promote_merge_groups(now=1010.0)
        bridge.maybe_record_merge_completions()
        comp_path = os.path.join(bridge.STATE, "merge_completed", "1001.json")
        check("completion: none before formal sent", not os.path.exists(comp_path))
        with open(os.path.join(bridge.OUTBOX_SENT, "1001.json"), "w", encoding="utf-8") as f:
            json.dump({"text": "reply"}, f)
        bridge.maybe_record_merge_completions()
        check("completion: written after formal sent", os.path.exists(comp_path))
        with open(comp_path, encoding="utf-8") as f:
            comp = json.load(f)
        check("completion: lists every constituent",
              comp.get("merged_message_ids") == ["1001", "1002"]
              and comp.get("formal_sent") is True
              and comp.get("anchor_message_id") == "1001")
        check("completion: manifest retained as audit trail", _manifest("1001") is not None)
        with open(comp_path, encoding="utf-8") as f:
            before = f.read()
        bridge.maybe_record_merge_completions()
        with open(comp_path, encoding="utf-8") as f:
            after = f.read()
        check("completion: second scan is a no-op", before == after)
    finally:
        _restore(old)
        _teardown(tmp)


def test_merge_segment_timestamps():
    tmp, old = _setup()
    try:
        _set_flag(True)
        # Live case (2026-11-01): the old _merge_iso referenced
        # module-level datetime, which bridge.py never imports at
        # module scope, so it always raised and every segment
        # rendered "[源消息 <mid> @ ]" with an empty time. The fix
        # formats ts_ms as Asia/Shanghai wall time in the label.
        bridge.stage_merge_message(
            "401", "user-B", _rec("401", "first", ts=1793519651019),
            "CTX-1", received_ts=1793519651.019)
        bridge.stage_merge_message(
            "402", "user-B", _rec("402", "second", ts=1793519652879),
            "CTX-2", received_ts=1793519652.879)
        bridge.maybe_promote_merge_groups(now=1793519657.0)
        merged = _read_inbox("401")
        text = merged.get("text", "")
        check("timestamps: live-case segments carry Shanghai wall time",
              "[源消息 401 @ 2026-11-01 15:54:11]" in text
              and "[源消息 402 @ 2026-11-01 15:54:12]" in text)
        check("timestamps: no empty '@ ]' header remains",
              "@ ]" not in text)
        parts = {p["message_id"]: p for p in merged.get("merged_parts", [])}
        check("timestamps: parts ts_ms untouched by the display fix",
              parts.get("401", {}).get("ts_ms") == 1793519651019
              and parts.get("402", {}).get("ts_ms") == 1793519652879)
        # A part without a rec ts falls back to the staging receipt time.
        rec, _man = bridge.build_merged_record([
            {"message_id": "403", "from_user_id": "user-B",
             "received_ts": 1793519651.019,
             "record": {"message_id": "403", "text": "no-ts",
                        "from_user_id": "user-B", "images": [], "videos": [],
                        "files": []}},
        ])
        check("timestamps: missing rec ts falls back to received_ts",
              "[源消息 403 @ 2026-11-01 15:54:11]" in rec.get("text", ""))
        check("timestamps: invalid input still yields empty, not a crash",
              bridge._merge_iso(None) == "" and bridge._merge_iso("bogus") == "")
    finally:
        _restore(old)
        _teardown(tmp)


def main():
    test_flag_default()
    test_single_not_merged()
    test_window_holds_and_boundary()
    test_merge_content_and_tokens()
    test_cross_window_and_senders()
    test_switch_off_verbatim()
    test_local_only_never_merges()
    test_promotion_idempotent_and_no_regen()
    test_promotion_failure_keeps_staging()
    test_stage_failure()
    test_completion_records()
    test_merge_segment_timestamps()
    print("same_source_merge: %d passed, %d failed" % (PASS, FAIL))
    if FAIL:
        print("failures: %s" % ", ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
