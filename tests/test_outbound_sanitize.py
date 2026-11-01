"""Offline tests for Gate 1 item 1: outbound sanitize.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network, and never touching the real running
bridge's state/, inbox/ or outbox/:

Unit-level (sanitize_outbound_text, no drain):
  1. Single and multiple widget tags stripped, prose kept.
  2. A tag containing a single "]" inside is still stripped whole.
  3. A nested "[[" inside a tag is consumed with that tag.
  4. An unclosed tag is left verbatim (conservative: never delete
     prose on a guess).
  5. A widget tag inside a fenced code block is preserved as
     literal code text; the same tag outside a fence is stripped.
  6. A widget tag on a quote line is stripped, quote structure kept.
  7. Family phrasing ("回 Muse App 确认") is NOT rewritten: there
     is no family-mode switch in this bridge, so no default-on
     rewrite exists here (explicitly not done, per the task).

Drain-level (drain_outbox with fake senders):
  8. Prose with a widget tag is sent with the tag removed.
  9. Text that sanitizes to empty with no attachments is never
     sent: job lands in failed/ as terminal skipped_empty, with a
     failures/ marker carrying lengths and reason only (no text).
 10. Text that sanitizes to empty WITH an attachment sends only
     the attachment, no blank text segment.
 11. A fresh long text is sanitized BEFORE segmentation: the
     archived plan's raw slices join back to the sanitized text,
     and no sent part contains a widget tag.
 12. An existing plan is sanitized per part: a widget-only part
     becomes skipped_empty and is never sent, the other part is
     sent with its original label, and the job still archives sent.
 13. Mixed Markdown (heading, bold, fence, quote) survives
     sanitization with its structure intact, fence content kept.
 14. Plain text without any tag is sent unchanged, with no marker.

Run: python3 tests/test_outbound_sanitize.py (exit 0 = all pass).
"""

import hashlib
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


class _Fakes:
    def __init__(self, fail_text=False):
        self.text_calls = []
        self.file_calls = []
        self.fail_text = fail_text

    def send_text(self, session, text, to_user_id=None, context_token=None, client_id=None):
        if self.fail_text:
            raise bridge.PermanentError("simulated permanent")
        self.text_calls.append(text)
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        self.file_calls.append(path)
        return {}


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
    return old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _read_job(d, name):
    with open(os.path.join(d, name), encoding="utf-8") as f:
        return json.load(f)


def _drain(job_name, job, fakes):
    _write_json(os.path.join(bridge.OUTBOX_PENDING, job_name), job)
    orig = (bridge.send_text, bridge.send_file)
    bridge.send_text, bridge.send_file = fakes.send_text, fakes.send_file
    try:
        sent, failed = bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file = orig
    return sent, failed


def _job(text, **extra):
    job = {"text": text, "to_user_id": "user-B", "client_id": "c-san"}
    job.update(extra)
    return job


# ---------------------------------------------------------------- unit
def test_unit():
    s = bridge.sanitize_outbound_text
    check("unit: single tag stripped, prose kept",
          s("hello [[hatch_widget:widget-1]] world") == "hello  world")
    check("unit: multiple tags stripped",
          s("a [[hatch_widget:x]] b [[hatch_widget:y]] c") == "a  b  c")
    check("unit: single ] inside tag still stripped whole",
          s("A [[hatch_widget:widget-1:foo]bar]] B") == "A  B")
    check("unit: nested [[ inside tag consumed with it",
          s("A [[hatch_widget:outer [[inner]] end]] B") == "A  B")
    check("unit: unclosed tag left verbatim",
          s("A [[hatch_widget:never-closed B") == "A [[hatch_widget:never-closed B")
    check("unit: fence content kept, outside stripped",
          s("```\n[[hatch_widget:code]]\n```\nAfter [[hatch_widget:real]] end")
          == "```\n[[hatch_widget:code]]\n```\nAfter  end")
    check("unit: quote line stripped, structure kept",
          s("> quote [[hatch_widget:q]] text") == "> quote  text")
    check("unit: family phrasing NOT rewritten",
          s("请回 Muse App 确认") == "请回 Muse App 确认")
    check("unit: plain text unchanged",
          s("plain text, no tags") == "plain text, no tags")
    check("unit: non-string input gives empty",
          s(None) == "")


# ---------------------------------------------------------------- drain 8
def test_drain_prose():
    tmp = tempfile.mkdtemp(prefix="san-prose-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        sent, _ = _drain("6001.json",
                         _job("你好 [[hatch_widget:widget-9]]，这是正文。"),
                         fakes)
        check("drain-prose: sent, tag removed, prose kept",
              "6001.json" in sent and fakes.text_calls == ["你好 ，这是正文。"])
    finally:
        _restore(old)


# ---------------------------------------------------------------- drain 9
def test_drain_empty_no_attachment():
    tmp = tempfile.mkdtemp(prefix="san-empty-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        sent, failed = _drain("6002.json",
                              _job("[[hatch_widget:secret-tag]]   \n  "),
                              fakes)
        check("drain-empty: nothing sent",
              fakes.text_calls == [] and "6002.json" not in sent)
        check("drain-empty: job in failed/",
              os.path.exists(os.path.join(bridge.OUTBOX_FAILED, "6002.json")))
        if os.path.exists(os.path.join(bridge.OUTBOX_FAILED, "6002.json")):
            archived = _read_job(bridge.OUTBOX_FAILED, "6002.json")
            check("drain-empty: terminal skipped_empty",
                  archived.get("text_status") == "skipped_empty"
                  and archived.get("terminal") is True
                  and archived.get("delivery_state") == "skipped_empty")
        markers = [n for n in os.listdir(bridge.FAILURES_DIR)
                   if "outbound_sanitize" in n]
        check("drain-empty: failures marker exists", len(markers) >= 1)
        if markers:
            marker = _read_job(bridge.FAILURES_DIR, markers[0])
            blob = json.dumps(marker, ensure_ascii=False)
            check("drain-empty: marker lengths + reason, no text",
                  marker.get("kind") == "skipped_empty"
                  and marker.get("reason") == "skipped_empty"
                  and isinstance(marker.get("original_len"), int)
                  and marker.get("original_len") > 0
                  and "secret-tag" not in blob
                  and "hatch_widget" not in blob)
    finally:
        _restore(old)


# ---------------------------------------------------------------- drain 10
def test_drain_empty_with_attachment():
    tmp = tempfile.mkdtemp(prefix="san-att-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        job = _job("[[hatch_widget:only]]",
                   attachments=[{"path": "/tmp/fake-att.bin", "type": "file",
                                 "status": "pending", "client_id": "att-san"}])
        sent, _ = _drain("6003.json", job, fakes)
        check("drain-att: no blank text sent, attachment sent",
              fakes.text_calls == [] and fakes.file_calls == ["/tmp/fake-att.bin"])
        check("drain-att: job archived sent",
              "6003.json" in sent
              and os.path.exists(os.path.join(bridge.OUTBOX_SENT, "6003.json")))
        if os.path.exists(os.path.join(bridge.OUTBOX_SENT, "6003.json")):
            archived = _read_job(bridge.OUTBOX_SENT, "6003.json")
            check("drain-att: text_status skipped_empty, attachment sent",
                  archived.get("text_status") == "skipped_empty"
                  and archived.get("attachments", [{}])[0].get("status") == "sent")
    finally:
        _restore(old)


# ---------------------------------------------------------------- drain 11
def test_drain_fresh_segmented():
    tmp = tempfile.mkdtemp(prefix="san-seg-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        raw = ("段落一内容。[[hatch_widget:w1]] " * 300 + "\n\n"
               + "段落二内容。[[hatch_widget:w2]] " * 300 + "\n")
        assert len(raw) > 1800
        sanitized = bridge.sanitize_outbound_text(raw)
        # The sanitized text itself must still exceed the budget,
        # otherwise no segmentation is expected at all.
        assert len(sanitized) > 1800
        assert "hatch_widget" not in sanitized
        sent, _ = _drain("6004.json", _job(raw), fakes)
        check("drain-seg: job sent", "6004.json" in sent)
        check("drain-seg: no sent part contains a widget tag",
              all("hatch_widget" not in c for c in fakes.text_calls))
        check("drain-seg: more than one part (segmented, not single)",
              len(fakes.text_calls) > 1)
        if os.path.exists(os.path.join(bridge.OUTBOX_SENT, "6004.json")):
            archived = _read_job(bridge.OUTBOX_SENT, "6004.json")
            parts = archived.get("text_parts") or []
            joined = "".join(p.get("raw_slice", "") for p in parts)
            check("drain-seg: raw slices restore sanitized text",
                  joined == sanitized)
            check("drain-seg: all parts sent",
                  parts and all(p.get("status") == "sent" for p in parts))
    finally:
        _restore(old)


# ---------------------------------------------------------------- drain 12
def test_drain_existing_plan():
    tmp = tempfile.mkdtemp(prefix="san-plan-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        top = "[[hatch_widget:only]]\nWorld text here"
        job = {"text": top, "to_user_id": "user-B", "client_id": "c-plan",
               "text_plan_version": 1,
               "full_text_sha256": hashlib.sha256(top.encode("utf-8")).hexdigest(),
               "text_parts": [
                   {"index": 0, "raw_slice": "[[hatch_widget:only]]\n",
                    "display_text": "[1/2]\n[[hatch_widget:only]]\n",
                    "sha256": "x0", "client_id": "p0",
                    "status": "pending", "attempts": 0, "error_code": None},
                   {"index": 1, "raw_slice": "World text here",
                    "display_text": "[2/2]\nWorld text here",
                    "sha256": "x1", "client_id": "p1",
                    "status": "pending", "attempts": 0, "error_code": None},
               ]}
        sent, _ = _drain("6005.json", job, fakes)
        check("drain-plan: only the non-empty part sent, label kept",
              fakes.text_calls == ["[2/2]\nWorld text here"])
        check("drain-plan: job archived sent",
              "6005.json" in sent)
        if os.path.exists(os.path.join(bridge.OUTBOX_SENT, "6005.json")):
            archived = _read_job(bridge.OUTBOX_SENT, "6005.json")
            parts = archived.get("text_parts") or []
            check("drain-plan: widget-only part skipped_empty, other sent",
                  len(parts) == 2
                  and parts[0].get("status") == "skipped_empty"
                  and parts[1].get("status") == "sent")
    finally:
        _restore(old)


# ---------------------------------------------------------------- drain 13, 14
def test_drain_mixed_and_plain():
    tmp = tempfile.mkdtemp(prefix="san-mixed-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        text = ("# 标题\n\n正文 **加粗** [[hatch_widget:mix]] 结束。\n\n"
                "```python\nprint('[[hatch_widget:code]]')\n```\n\n"
                "> 引用 [[hatch_widget:q]] 内容\n")
        expected = ("# 标题\n\n正文 **加粗**  结束。\n\n"
                    "```python\nprint('[[hatch_widget:code]]')\n```\n\n"
                    "> 引用  内容\n")
        sent, _ = _drain("6006.json", _job(text), fakes)
        check("drain-mixed: structure kept, fence kept, outside stripped",
              "6006.json" in sent and fakes.text_calls == [expected])
        fakes2 = _Fakes()
        sent2, _ = _drain("6007.json", _job("plain, no tags at all"), fakes2)
        markers = [n for n in os.listdir(bridge.FAILURES_DIR)
                   if "outbound_sanitize" in n]
        check("drain-plain: unchanged, no marker",
              "6007.json" in sent2
              and fakes2.text_calls == ["plain, no tags at all"]
              and markers == [])
    finally:
        _restore(old)


test_unit()
test_drain_prose()
test_drain_empty_no_attachment()
test_drain_empty_with_attachment()
test_drain_fresh_segmented()
test_drain_existing_plan()
test_drain_mixed_and_plain()

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
