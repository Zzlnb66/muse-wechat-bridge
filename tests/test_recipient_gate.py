"""Offline tests for the R1 recipient gate (2026-11-01).

A recipient-less pending job is no longer filled unconditionally:
- recipient-less + a receipt record for its message id in this
  root (inbox/ or done/) -> filled with the session account and
  sent exactly as before (legacy compat fully preserved);
- recipient-less + NO local receipt record -> the exact shape of
  a task miswritten here from another root: parked as
  needs_check + recipient_missing, moved to failed/, never sent,
  re-checked on every drain, never auto-resent.
Explicit recipient_mismatch behaviour is unchanged and still
fires before send and sanitize.

Run against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network, never touching the real running
bridge's state/, inbox/ or outbox/:

Run: python3 tests/test_recipient_gate.py (exit 0 = all pass).
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


class _Fakes:
    def __init__(self):
        self.text_calls = []
        self.file_calls = []
        self.image_calls = []

    def send_text(self, session, text, to_user_id=None, context_token="", client_id=None):
        self.text_calls.append(text)
        return {}

    def send_file(self, session, path, to_user_id, token, client_id=None):
        self.file_calls.append(path)
        return {}

    def send_image(self, session, path, to_user_id, token, client_id=None):
        self.image_calls.append(path)
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
    bridge._OBS_QUEUED_SEEN.clear()
    bridge._TERMINAL_TASK_IDS.clear()
    bridge._IN_MEMORY_HALTED_FILES.clear()
    try:
        bridge._SEND_COOLDOWN.clear()
    except Exception:
        pass
    return old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def _read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _drain(job_name, job, fakes):
    bridge._OBS_QUEUED_SEEN.clear()
    bridge._TERMINAL_TASK_IDS.clear()
    bridge._IN_MEMORY_HALTED_FILES.clear()
    try:
        bridge._SEND_COOLDOWN.clear()
    except Exception:
        pass
    _write_json(os.path.join(bridge.OUTBOX_PENDING, job_name), job)
    orig = (bridge.send_text, bridge.send_file, bridge.send_image)
    bridge.send_text, bridge.send_file, bridge.send_image = (
        fakes.send_text, fakes.send_file, fakes.send_image)
    try:
        sent, failed = bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file, bridge.send_image = orig
    return sent, failed


def _drain_only(fakes):
    """Drain whatever is already in pending/ (for re-drain checks).

    Also clears the process-lifetime terminal set, simulating a
    fresh serve process where only on-disk flags persist: this is
    what makes the re-gate check below exercise the gate itself
    rather than the memory-set shortcut in drain's loop top.
    """
    bridge._OBS_QUEUED_SEEN.clear()
    bridge._TERMINAL_TASK_IDS.clear()
    bridge._IN_MEMORY_HALTED_FILES.clear()
    try:
        bridge._SEND_COOLDOWN.clear()
    except Exception:
        pass
    orig = (bridge.send_text, bridge.send_file, bridge.send_image)
    bridge.send_text, bridge.send_file, bridge.send_image = (
        fakes.send_text, fakes.send_file, fakes.send_image)
    try:
        sent, failed = bridge.drain_outbox({"ilink_user_id": "user-B"})
    finally:
        bridge.send_text, bridge.send_file, bridge.send_image = orig
    return sent, failed


def _timeline(state):
    p = os.path.join(state, "message_timeline.jsonl")
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _marker_text(state):
    out = []
    fdir = os.path.join(state, "failures_dir")
    for base in (bridge.FAILURES_DIR, fdir):
        if os.path.isdir(base):
            for fn in sorted(os.listdir(base)):
                if fn.endswith(".json"):
                    with open(os.path.join(base, fn), encoding="utf-8") as f:
                        out.append(f.read())
    return "\n".join(out)


# ---------------------------------------------------------------- unit
def test_unit_enum_and_mapping():
    tmp = tempfile.mkdtemp(prefix="rg-unit-")
    old = _setup(tmp)
    try:
        check("unit: recipient_missing in fixed failure-code enum",
              "recipient_missing" in bridge._OBS_FAILURE_REASONS)
        check("unit: code mapping for recipient_missing",
              bridge._obs_failure_code_for_job(
                  {"needs_check_reason": "recipient_missing"}) == "recipient_missing")
        check("unit: recipient_mismatch mapping unchanged",
              bridge._obs_failure_code_for_job(
                  {"needs_check_reason": "recipient_mismatch"}) == "recipient_mismatch")
    finally:
        _restore(old)


# ------------------------------------------------------- gate: local ok
def test_gate_local_inbox_record_sends():
    tmp = tempfile.mkdtemp(prefix="rg-inbox-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        mid = "910001"
        _write_json(os.path.join(bridge.INBOX, mid + ".json"),
                     {"message_id": mid, "text": "hello"})
        sent, failed = _drain(mid + ".json",
                              {"text": "reply-one", "client_id": "c-rg1"},
                              fakes)
        check("gate inbox: sent once, nothing failed",
              sent == [mid + ".json"] and failed == []
              and fakes.text_calls == ["reply-one"])
        archived = _read_json(os.path.join(bridge.OUTBOX_SENT, mid + ".json"))
        check("gate inbox: recipient filled with session account",
              archived.get("to_user_id") == "user-B")
        check("gate inbox: no recipient_missing timeline event",
              not any(e.get("failure_code") == "recipient_missing"
                      for e in _timeline(bridge.STATE)))
        check("gate inbox: no recipient_missing failures marker",
              "recipient_missing" not in _marker_text(bridge.STATE))
    finally:
        _restore(old)


def test_gate_local_done_record_sends():
    tmp = tempfile.mkdtemp(prefix="rg-done-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        mid = "910002"
        _write_json(os.path.join(bridge.DONE, mid + ".json"),
                     {"message_id": mid, "text": "hello"})
        sent, failed = _drain(mid + ".json",
                              {"text": "reply-two", "client_id": "c-rg2"},
                              fakes)
        check("gate done: sent once, recipient filled",
              sent == [mid + ".json"] and failed == []
              and fakes.text_calls == ["reply-two"]
              and _read_json(os.path.join(bridge.OUTBOX_SENT, mid + ".json")).get("to_user_id") == "user-B")
    finally:
        _restore(old)


# ------------------------------------------------------- gate: foreign
def test_gate_foreign_parked():
    tmp = tempfile.mkdtemp(prefix="rg-foreign-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        mid = "910003"
        planted_text = "SECRET-PLANTED-TEXT-910003"
        planted_token = "SECRET-PLANTED-TOKEN-910003"
        sent, failed = _drain(mid + ".json",
                              {"text": planted_text,
                               "context_token": planted_token,
                               "client_id": "c-rg3"},
                              fakes)
        check("gate foreign: nothing sent",
              sent == [] and fakes.text_calls == [])
        check("gate foreign: job parked in failed/",
              os.path.exists(os.path.join(bridge.OUTBOX_FAILED, mid + ".json"))
              and not os.path.exists(os.path.join(bridge.OUTBOX_PENDING, mid + ".json")))
        parked = _read_json(os.path.join(bridge.OUTBOX_FAILED, mid + ".json"))
        check("gate foreign: needs_check + recipient_missing + terminal",
              parked.get("delivery_state") == "needs_check"
              and parked.get("needs_check_reason") == "recipient_missing"
              and parked.get("terminal") is True)
        evs = [e for e in _timeline(bridge.STATE)
               if e.get("failure_code") == "recipient_missing"]
        check("gate foreign: timeline needs_check event with code",
              len(evs) == 1 and evs[0]["stage"] == "needs_check"
              and evs[0]["source"] == "drain_recipient_gate")
        marker = _marker_text(bridge.STATE)
        check("gate foreign: scrubbed failures marker, no text/token",
              "recipient_missing" in marker
              and planted_text not in marker
              and planted_token not in marker)
        view = bridge.get_message_observability(mid)
        check("gate foreign: view outcome needs_check + code",
              view["outcome"] == "needs_check"
              and "recipient_missing" in view["failure_codes"])
        summary = bridge.get_observability_summary()
        check("gate foreign: summary needs_check bucket counts it",
              summary["needs_check"] == 1 and summary["sent"] == 0)
        # Re-drain: still nothing sent, no duplicate park.
        sent2, _ = _drain_only(fakes)
        check("gate foreign: re-drain sends nothing",
              sent2 == [] and fakes.text_calls == [])
        # Re-gate: strip the flags, return the job to pending
        # unflagged; the gate must fire again, not send.
        parked2 = dict(parked)
        parked2.pop("terminal", None)
        parked2.pop("delivery_state", None)
        parked2.pop("needs_check_reason", None)
        _write_json(os.path.join(bridge.OUTBOX_PENDING, mid + ".json"), parked2)
        sent3, _ = _drain_only(fakes)
        reparked = _read_json(os.path.join(bridge.OUTBOX_FAILED, mid + ".json"))
        check("gate foreign: re-gated on return to pending, still no send",
              sent3 == [] and fakes.text_calls == []
              and reparked.get("needs_check_reason") == "recipient_missing"
              and reparked.get("terminal") is True)
    finally:
        _restore(old)


# ------------------------------------------------------- mismatch intact
def test_mismatch_and_explicit_ok_unchanged():
    tmp = tempfile.mkdtemp(prefix="rg-mismatch-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        sent, _ = _drain("910004.json",
                         {"text": "wrong-user", "to_user_id": "user-C",
                          "client_id": "c-rg4"},
                         fakes)
        parked = _read_json(os.path.join(bridge.OUTBOX_FAILED, "910004.json"))
        check("mismatch: still recipient_mismatch, never sent",
              sent == [] and fakes.text_calls == []
              and parked.get("needs_check_reason") == "recipient_mismatch")
        fakes2 = _Fakes()
        sent2, _ = _drain("910005.json",
                          {"text": "right-user", "to_user_id": "user-B",
                           "client_id": "c-rg5"},
                          fakes2)
        check("explicit correct: sent, gate untouched",
              sent2 == ["910005.json"] and fakes2.text_calls == ["right-user"])
        fakes3 = _Fakes()
        sent3, _ = _drain("910006.json",
                          {"text": "bridge-written", "to_user_id": "user-B",
                           "client_id": "c-rg6"},
                          fakes3)
        check("bridge-written with recipient, no record: sent",
              sent3 == ["910006.json"] and fakes3.text_calls == ["bridge-written"])
    finally:
        _restore(old)


# ------------------------------------------------------- plan + suffix
def test_plan_and_suffix_jobs():
    tmp = tempfile.mkdtemp(prefix="rg-plan-")
    old = _setup(tmp)
    try:
        fakes = _Fakes()
        mid = "910007"
        _write_json(os.path.join(bridge.INBOX, mid + ".json"),
                     {"message_id": mid, "text": "long"})
        text = ("第一段内容。" * 40 + "\n" + "第二段内容。" * 40 + "\n"
                + "第三段内容。" * 40)
        parts, sha = bridge._segment_text(text)
        job = {"text": text, "client_id": "c-rg7",
               "text_plan_version": 1, "text_parts": parts,
               "full_text_sha256": sha}
        sent, _ = _drain(mid + ".json", job, fakes)
        check("plan: recipient-less + record sends all parts in order",
              sent == [mid + ".json"]
              and fakes.text_calls == [p["display_text"] for p in parts])
        # Suffixed bridge receipt for a real local message: the
        # suffix is stripped before the record check, so it sends.
        fakes2 = _Fakes()
        sent2, _ = _drain(mid + "_progress.json",
                          {"text": "still working", "client_id": "c-rg8"},
                          fakes2)
        check("suffix: progress job for local mid sends",
              sent2 == [mid + "_progress.json"]
              and fakes2.text_calls == ["still working"])
        # Suffixed job for a mid with no local record: rejected.
        fakes3 = _Fakes()
        sent3, _ = _drain("910008_progress.json",
                          {"text": "foreign note", "client_id": "c-rg9"},
                          fakes3)
        check("suffix: progress job for foreign mid parked, not sent",
              sent3 == [] and fakes3.text_calls == []
              and os.path.exists(os.path.join(bridge.OUTBOX_FAILED, "910008_progress.json"))
              and _read_json(os.path.join(bridge.OUTBOX_FAILED, "910008_progress.json")).get("needs_check_reason") == "recipient_missing")
    finally:
        _restore(old)


def main():
    test_unit_enum_and_mapping()
    test_gate_local_inbox_record_sends()
    test_gate_local_done_record_sends()
    test_gate_foreign_parked()
    test_mismatch_and_explicit_ok_unchanged()
    test_plan_and_suffix_jobs()
    failed = [n for n, v in ok if not v]
    print(f"\nrecipient_gate: {len(ok) - len(failed)} passed, {len(failed)} failed")
    if failed:
        print("FAILED:", failed)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
