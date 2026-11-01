"""Offline tests for the Gate 1 remainder job-shape guard.

A pending file that parses as JSON but is not an object can never
be a valid task. Before the guard, the first job.get() in drain
raised AttributeError, aborting that drain pass and starving every
later-sorted job on every tick until the file was removed by hand.
Now drain parks it as needs_check (never sent, never re-entered),
preserves the original bytes under failures/, and keeps sending
later jobs in the same pass.

Run: python3 tests/test_job_shape_guard.py (exit 0 = all pass).
Temp dirs, fake send, no network, never touching the real bridge.
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


def _write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _read(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _drain(fakes):
    orig = (bridge.send_text, bridge.send_file, bridge.send_image)
    bridge.send_text = fakes[0]
    bridge.send_file = lambda *a, **k: {}
    bridge.send_image = lambda *a, **k: {}
    try:
        return bridge.drain_outbox({"ilink_user_id": "user-T"})
    finally:
        bridge.send_text, bridge.send_file, bridge.send_image = orig


def main():
    tmp = tempfile.mkdtemp(prefix="job-shape-")
    old = _setup(tmp)
    try:
        # Malformed list job sorts BEFORE the valid job.
        _write(os.path.join(bridge.OUTBOX_PENDING, "900001.json"), "[1, 2]")
        _write(os.path.join(bridge.OUTBOX_PENDING, "900002.json"),
               json.dumps({"text": "valid", "to_user_id": "user-T"}))
        calls = []

        def _fake(session, text, to_user_id=None, context_token="", client_id=None):
            calls.append(text)
            return {}

        sent, failed = _drain((_fake,))
        check("shape: later valid job still sent in same pass",
              calls == ["valid"] and "900002.json" in sent)
        check("shape: malformed job reported failed, never sent",
              "900001.json" in failed and "900001.json" not in sent)
        check("shape: malformed job left pending",
              not os.path.exists(os.path.join(bridge.OUTBOX_PENDING, "900001.json")))
        parked = _read(os.path.join(bridge.OUTBOX_FAILED, "900001.json"))
        check("shape: failed record is dict needs_check terminal",
              parked.get("delivery_state") == "needs_check"
              and parked.get("needs_check_reason") == "job_not_object"
              and parked.get("terminal") is True
              and parked.get("original_job_type") == "list")
        orig_path = os.path.join(bridge.FAILURES_DIR, "900001_job_not_object.orig")
        check("shape: original bytes preserved under failures",
              os.path.exists(orig_path)
              and open(orig_path, encoding="utf-8").read() == "[1, 2]")
        marker = os.path.join(bridge.FAILURES_DIR, "900001_job_not_object.json")
        check("shape: desensitized marker written",
              os.path.exists(marker)
              and _read(marker).get("kind") == "job_not_object")
        tl = os.path.join(bridge.STATE, "message_timeline.jsonl")
        events = [json.loads(line) for line in open(tl, encoding="utf-8")]
        check("shape: timeline needs_check with job_not_object",
              any(e.get("job") == "900001" and e.get("stage") == "needs_check"
                  and e.get("failure_code") == "job_not_object" for e in events))
        summary = bridge.get_observability_summary()
        check("shape: summary needs_check bucket counts it",
              summary["needs_check"] == 1)
        view = bridge.get_message_observability("900001.json")
        check("shape: view outcome needs_check with code",
              view["outcome"] == "needs_check"
              and "job_not_object" in view["failure_codes"])
        # Second drain: no re-send, no crash, valid job already gone.
        sent2, failed2 = _drain((_fake,))
        check("shape: second drain quiet, nothing resent",
              sent2 == [] and failed2 == [] and calls == ["valid"])
        # String job: same parking, shape recorded.
        _write(os.path.join(bridge.OUTBOX_PENDING, "900003.json"), '"oops"')
        sent3, failed3 = _drain((_fake,))
        parked3 = _read(os.path.join(bridge.OUTBOX_FAILED, "900003.json"))
        check("shape: string job parked with shape str",
              "900003.json" in failed3
              and parked3.get("original_job_type") == "str"
              and parked3.get("needs_check_reason") == "job_not_object")
    finally:
        _restore(old)
    failed = [n for n, c in ok if not c]
    print(f"\njob_shape_guard: {len(ok) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
