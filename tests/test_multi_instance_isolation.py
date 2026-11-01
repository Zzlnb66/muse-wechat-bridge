"""Offline tests for Gate 3 item 5: multi-instance isolation check.

Verification only. Nothing here implements --binding, logs into any
account, or touches the real running bridge. All scenarios run in temp
dirs with fake send functions, no network, no real WeChat, no upstream.

Covers, against the fix-copy bridge.py:
  1. Two copies of bridge.py loaded from two different temp roots get
     disjoint ROOT-derived trees: state, inbox, done, outbox/*, media/*,
     analysis, local_vault, failures, session file, lock file. This is
     the directory-level isolation claim: it holds by construction
     (ROOT = dirname(abspath(__file__))), not by convention.
  2. Dual-root drain behaviour (single module, globals rebound per
     root to simulate two separate serve processes): a job addressed
     to user-B drained under root A (session user-A) is refused, never
     sent, parked in root A's failed/; the same job name drained under
     root B (session user-B) is sent normally; each root's records are
     invisible to the other root.
  3. A job already sent in root A does not appear anywhere in root B
     before root B drains its own copy.
  4. R1 recipient gate (2026-11-01, user-approved hardening): a job
     WITHOUT to_user_id is filled with the draining root's own
     session user and sent ONLY when this root holds a receipt
     record for the job's message id (inbox/ or done/); with no
     local record it is parked as needs_check + recipient_missing
     and never sent. The old behaviour documented here before
     ("recipient-less always fills and sends") WAS risk R1 in
     docs/multi-instance-isolation-check.md, and is intentionally
     superseded for the record-less shape by the gate.
  5. --binding does not exist: `bridge.py --help` lists 14 subcommands
     with no binding entry, and the source contains no
     add_argument("--binding") and no add_parser("binding").
  6. Static guard (not behavioural proof): the inbound filter lines
     (group_id ignored, from_user_id must equal session ilink_user_id)
     are still present in the source, so a future edit cannot silently
     drop the filter without breaking this test.

What this file does NOT prove (needs a second real account, see the
report): live dual-account isolation, live misdelivery absence, live
no-cross-number behaviour, same-account-in-two-roots behaviour.

Run: python3 tests/test_multi_instance_isolation.py (exit 0 = all pass).
"""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402

FIX_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX_BRIDGE = os.path.join(FIX_DIR, "bridge.py")

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def _load_copy(root_dir, mod_name):
    os.makedirs(root_dir, exist_ok=True)
    # Gate 3 step 1: bridge_storage.py is a sibling module bridge.py
    # imports unguarded, so a copied root needs it too (same rule as
    # aes_inprocess.py). bridge_sqlite.py stays guarded-optional.
    for fname in ("bridge.py", "aes_inprocess.py", "bridge_storage.py"):
        shutil.copyfile(os.path.join(FIX_DIR, fname), os.path.join(root_dir, fname))
    spec = importlib.util.spec_from_file_location(mod_name, os.path.join(root_dir, "bridge.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_two_roots_disjoint():
    tmp = tempfile.mkdtemp(prefix="iso-roots-")
    mod_a = _load_copy(os.path.join(tmp, "rootA"), "bridge_iso_a")
    mod_b = _load_copy(os.path.join(tmp, "rootB"), "bridge_iso_b")
    check("two roots: ROOT differs", mod_a.ROOT != mod_b.ROOT)
    check("two roots: ROOT is each copy dir",
          mod_a.ROOT == os.path.join(tmp, "rootA") and mod_b.ROOT == os.path.join(tmp, "rootB"))
    attrs = ("STATE", "INBOX", "DONE", "OUTBOX_PENDING", "OUTBOX_SENT", "OUTBOX_FAILED",
             "MEDIA_INBOUND", "MEDIA_OUTBOUND", "ANALYSIS_DIR", "LOCAL_VAULT_DIR",
             "FAILURES_DIR", "SESSION_FILE", "LOCK_FILE", "CURSOR_FILE",
             "PROCESSED_FILE", "NEEDS_RELOGIN_FILE", "LOGIN_PENDING_FILE",
             "FILE_RECEIPTS_FILE", "SAFE_VIEW_FILE")
    for attr in attrs:
        pa, pb = getattr(mod_a, attr, None), getattr(mod_b, attr, None)
        check(f"two roots: {attr} exists and differs", pa is not None and pb is not None and pa != pb)
        check(f"two roots: {attr} under own ROOT",
              pa is not None and pa.startswith(mod_a.ROOT + os.sep)
              and pb is not None and pb.startswith(mod_b.ROOT + os.sep))


class _Root:
    """One temp bridge root: dirs + global-rebinding for drain tests."""

    def __init__(self, base, session_user):
        self.base = base
        self.state = os.path.join(base, "state")
        self.pend = os.path.join(base, "pending")
        self.sent = os.path.join(base, "sent")
        self.failed = os.path.join(base, "failed")
        self.done = os.path.join(base, "done")
        self.inbox = os.path.join(base, "inbox")
        for d in (self.state, self.pend, self.sent, self.failed, self.done,
                  self.inbox):
            os.makedirs(d, exist_ok=True)
        self.session = {"ilink_user_id": session_user}
        self.old = None

    def bind(self):
        self.old = (bridge.STATE, bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT,
                    bridge.OUTBOX_FAILED, bridge.FAILURES_DIR,
                    bridge.INBOX, bridge.DONE)
        bridge.STATE = self.state
        bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT, bridge.OUTBOX_FAILED = (
            self.pend, self.sent, self.failed)
        bridge.FAILURES_DIR = os.path.join(self.base, "failures")
        os.makedirs(bridge.FAILURES_DIR, exist_ok=True)
        bridge.INBOX = self.inbox
        bridge.DONE = self.done

    def unbind(self):
        (bridge.STATE, bridge.OUTBOX_PENDING, bridge.OUTBOX_SENT,
         bridge.OUTBOX_FAILED, bridge.FAILURES_DIR,
         bridge.INBOX, bridge.DONE) = self.old
        self.old = None

    def write_job(self, name, job):
        with open(os.path.join(self.pend, name), "w", encoding="utf-8") as f:
            json.dump(job, f)

    def listing(self, which):
        d = {"pending": self.pend, "sent": self.sent, "failed": self.failed,
             "done": self.done, "state": self.state}[which]
        return sorted(os.listdir(d)) if os.path.isdir(d) else []


def test_dual_root_drain():
    tmp = tempfile.mkdtemp(prefix="iso-drain-")
    root_a = _Root(os.path.join(tmp, "A"), "user-A")
    root_b = _Root(os.path.join(tmp, "B"), "user-B")
    job = {"text": "hello B", "to_user_id": "user-B", "client_id": "c1"}
    # Root A: explicit recipient mismatch -> refused, never sent.
    root_a.write_job("m1.json", dict(job))
    calls = []
    orig_send = bridge.send_text
    bridge.send_text = lambda *a, **k: calls.append((a, k)) or {}
    try:
        root_a.bind()
        try:
            sent_names, failed_names = bridge.drain_outbox(root_a.session)
        finally:
            root_a.unbind()
    finally:
        bridge.send_text = orig_send
    bridge._TERMINAL_TASK_IDS.discard("m1.json")
    check("dual drain: A refuses B-addressed job (no send)", len(calls) == 0)
    check("dual drain: A parks job in failed/", "m1.json" in failed_names
          and "m1.json" in root_a.listing("failed"))
    check("dual drain: A sent/ has no such job", "m1.json" not in root_a.listing("sent"))
    parked = json.load(open(os.path.join(root_a.failed, "m1.json"), encoding="utf-8"))
    check("dual drain: A parked job is needs_check recipient_mismatch",
          parked.get("delivery_state") == "needs_check"
          and parked.get("needs_check_reason") == "recipient_mismatch")
    # Root B has not drained yet: nothing about this job is visible there.
    check("dual drain: B sees nothing before its own drain",
          "m1.json" not in root_b.listing("pending")
          and "m1.json" not in root_b.listing("sent")
          and "m1.json" not in root_b.listing("failed")
          and "m1.json" not in root_b.listing("state"))
    # Root B: same job name, matching recipient -> sent normally.
    root_b.write_job("m1.json", dict(job))
    calls = []
    bridge.send_text = lambda *a, **k: calls.append((a, k)) or {}
    try:
        root_b.bind()
        try:
            sent_names, failed_names = bridge.drain_outbox(root_b.session)
        finally:
            root_b.unbind()
    finally:
        bridge.send_text = orig_send
    bridge._TERMINAL_TASK_IDS.discard("m1.json")
    check("dual drain: B sends matching job", len(calls) == 1 and "m1.json" in sent_names)
    check("dual drain: B sent/ has job, B failed/ does not",
          "m1.json" in root_b.listing("sent") and "m1.json" not in root_b.listing("failed"))
    check("dual drain: B drain leaves A failed/ untouched",
          "m1.json" in root_a.listing("failed") and "m1.json" not in root_a.listing("sent"))


def test_sent_in_a_invisible_in_b():
    tmp = tempfile.mkdtemp(prefix="iso-shared-")
    root_a = _Root(os.path.join(tmp, "A"), "user-A")
    root_b = _Root(os.path.join(tmp, "B"), "user-B")
    root_a.write_job("shared.json", {"text": "A only", "to_user_id": "user-A", "client_id": "s1"})
    orig_send = bridge.send_text
    bridge.send_text = lambda *a, **k: {}
    try:
        root_a.bind()
        try:
            sent_names, _ = bridge.drain_outbox(root_a.session)
        finally:
            root_a.unbind()
    finally:
        bridge.send_text = orig_send
    bridge._TERMINAL_TASK_IDS.discard("shared.json")
    check("shared id: A sent it", "shared.json" in sent_names)
    for which in ("pending", "sent", "failed", "done", "state"):
        check(f"shared id: B {which}/ has nothing before B drain",
              "shared.json" not in root_b.listing(which))


def test_recipient_less_defaults_to_own_root():
    # R1 gate (2026-11-01, supersedes the old gap documented here
    # before): a job without to_user_id is filled with the draining
    # root's own session user and sent ONLY when this root holds a
    # receipt record for the job's message id (inbox/ or done/).
    # A recipient-less job with NO local receipt record has the
    # exact shape of a task misfiled here from another root: it is
    # parked as needs_check + recipient_missing and never sent.
    # This test replaces the old expectation ("recipient-less
    # always fills and sends") on purpose: that old expectation
    # WAS the R1 gap, and the user-approved R1 hardening closes it
    # for the record-less shape while keeping legacy compat for
    # the record-backed shape.
    tmp = tempfile.mkdtemp(prefix="iso-norcp-")
    root_a = _Root(os.path.join(tmp, "A"), "user-A")
    # Variant A: no local record -> parked, never sent, not filled.
    root_a.write_job("norecp.json", {"text": "no recipient", "client_id": "n1"})
    calls = []
    orig_send = bridge.send_text
    bridge.send_text = lambda *a, **k: calls.append((a, k)) or {}
    try:
        root_a.bind()
        try:
            sent_names, _ = bridge.drain_outbox(root_a.session)
        finally:
            root_a.unbind()
    finally:
        bridge.send_text = orig_send
    bridge._TERMINAL_TASK_IDS.discard("norecp.json")
    check("recipient-less, no record: never sent (R1 gate)",
          "norecp.json" not in sent_names and calls == [])
    parked = json.load(open(os.path.join(root_a.failed, "norecp.json"), encoding="utf-8"))
    check("recipient-less, no record: parked recipient_missing",
          parked.get("needs_check_reason") == "recipient_missing"
          and parked.get("terminal") is True
          and parked.get("to_user_id") is None)
    # Variant B: inbox record for the message id -> legacy compat,
    # filled with the root's own session user and sent.
    with open(os.path.join(root_a.inbox, "910100.json"), "w", encoding="utf-8") as f:
        json.dump({"message_id": "910100", "text": "hello"}, f)
    root_a.write_job("910100.json", {"text": "legacy reply", "client_id": "n2"})
    calls2 = []
    bridge.send_text = lambda *a, **k: calls2.append((a, k)) or {}
    try:
        root_a.bind()
        try:
            sent_names2, _ = bridge.drain_outbox(root_a.session)
        finally:
            root_a.unbind()
    finally:
        bridge.send_text = orig_send
    bridge._TERMINAL_TASK_IDS.discard("910100.json")
    check("recipient-less, inbox record: sent (legacy compat)",
          "910100.json" in sent_names2 and len(calls2) == 1)
    archived = json.load(open(os.path.join(root_a.sent, "910100.json"), encoding="utf-8"))
    check("recipient-less, inbox record: archived job carries root A's user",
          archived.get("to_user_id") == "user-A")


def test_no_binding_cli():
    out = subprocess.run([sys.executable, FIX_BRIDGE, "--help"],
                         capture_output=True, text=True, timeout=60)
    check("CLI: --help exits 0", out.returncode == 0)
    check("CLI: no --binding in help", "--binding" not in out.stdout)
    check("CLI: no binding subcommand in help",
          "binding " not in out.stdout and "\n    binding" not in out.stdout)
    for cmd in ("login", "serve", "send", "status", "resend-text-part", "verify-channel",
                "safe-view", "observability", "observability-summary",
                "getupdates-anomalies", "send-video", "send-file", "health", "refresh-qr"):
        check(f"CLI: subcommand {cmd} listed", cmd in out.stdout)
    src = open(FIX_BRIDGE, encoding="utf-8").read()
    check("CLI: no add_argument(\"--binding\") in source",
          'add_argument("--binding"' not in src)
    check("CLI: no add_parser(\"binding\") in source", 'add_parser("binding"' not in src)


def test_inbound_filter_guard():
    src = open(FIX_BRIDGE, encoding="utf-8").read()
    check("inbound filter: group_id ignored still present", 'if msg.get("group_id"):' in src)
    check("inbound filter: from_user_id == session ilink_user_id still present",
          'from_user_id") != session.get("ilink_user_id")' in src)


test_two_roots_disjoint()
test_dual_root_drain()
test_sent_in_a_invisible_in_b()
test_recipient_less_defaults_to_own_root()
test_no_binding_cli()
test_inbound_filter_guard()

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
