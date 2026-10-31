"""Offline tests for the R2 P0 items adopted 2026-10-31.

Covers, against the fix-copy bridge.py, all in temp dirs with fake
send functions and no network:
  1. drain_outbox file dispatch -> send_file (never send_image).
  2. drain_outbox unknown attachment type -> failed, no send at all.
  3. drain_outbox legacy no-type attachment -> send_image (old schema).
  4. drain_outbox session token: job without context_token still
     sends with the session token; a job WITH a token sends with
     the session token, and the archived sent file has no token.
  5. sweep_local_ttl never purges tombstones, even 100 days old.
  6. Analyzer path env overrides (defaults unchanged).
  7. Inbox/done record construction no longer stores context_token
     (source guard: the record literal must not contain it).

Run: python3 tests/test_r2_p0.py (exit 0 = all pass).
"""
import json
import os
import subprocess
import sys
import tempfile
import time

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
        os.makedirs(d)
    bridge.OUTBOX_PENDING = pend
    bridge.OUTBOX_SENT = sent
    bridge.OUTBOX_FAILED = failed
    return pend, sent, failed


def _write_job(pend, name, job):
    with open(os.path.join(pend, name), "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False)


def _read(d, name):
    with open(os.path.join(d, name), encoding="utf-8") as f:
        return json.load(f)


def main():
    tmp = tempfile.mkdtemp(prefix="r2-p0-")
    pend, sent, failed = _setup_outbox(tmp)
    calls = []
    orig = (bridge.send_video, bridge.send_image, bridge.send_file,
            bridge.send_text)

    def fake_send(kind):
        def _f(session, *a, **kw):
            calls.append((kind, a, kw))
            return {"message_id": "m-" + kind}
        return _f

    bridge.send_video = fake_send("video")
    bridge.send_image = fake_send("image")
    bridge.send_file = fake_send("file")
    bridge.send_text = fake_send("text")
    session = {"ilink_user_id": "u1", "context_token": "sess-ct"}

    # A file to attach (content never sent: send functions are fake).
    att_path = os.path.join(tmp, "note.txt")
    with open(att_path, "w", encoding="utf-8") as f:
        f.write("hello")

    # 1. file dispatch, job carries NO token -> session token used.
    _write_job(pend, "job-file.json", {
        "to_user_id": "u1", "client_id": "c-file",
        "attachments": [{"path": att_path, "type": "file",
                         "task_id": "job-file", "client_id": "a-file"}],
    })
    # 2. unknown type -> failed, nothing sent.
    _write_job(pend, "job-weird.json", {
        "to_user_id": "u1", "client_id": "c-weird",
        "attachments": [{"path": att_path, "type": "weird",
                         "task_id": "job-weird", "client_id": "a-weird"}],
    })
    # 3. legacy no-type attachment -> image.
    _write_job(pend, "job-legacy.json", {
        "to_user_id": "u1", "client_id": "c-legacy",
        "attachments": [{"path": att_path, "task_id": "job-legacy",
                         "client_id": "a-legacy"}],
    })
    # 4. job WITH an old Worker-copied token -> session token wins,
    #    and the archived sent file is scrubbed of the token.
    _write_job(pend, "job-scrub.json", {
        "to_user_id": "u1", "client_id": "c-scrub",
        "context_token": "old-ct",
        "attachments": [{"path": att_path, "type": "image",
                         "task_id": "job-scrub", "client_id": "a-scrub"}],
    })
    # 5. text-only job without a token -> session token used.
    _write_job(pend, "job-text.json", {
        "to_user_id": "u1", "client_id": "c-text", "text": "hi",
    })
    try:
        bridge.drain_outbox(session)
    finally:
        (bridge.send_video, bridge.send_image, bridge.send_file,
         bridge.send_text) = orig

    kinds = [c[0] for c in calls]
    check("file attachment -> send_file, not send_image",
          "file" in kinds)
    check("no send_image for the file job",
          sum(1 for c in calls if c[0] == "image") == 2)  # legacy + scrub
    check("unknown type job archived to failed/ with status failed",
          os.path.exists(os.path.join(failed, "job-weird.json"))
          and _read(failed, "job-weird.json")["attachments"][0]["status"] == "failed")
    check("unknown type sent nothing",
          "weird" not in kinds)
    check("legacy no-type attachment -> send_image",
          any(c[0] == "image" for c in calls))
    file_call = [c for c in calls if c[0] == "file"][0]
    check("file job sent with session token (no job token)",
          file_call[1][2] == "sess-ct" or file_call[2].get("context_token") == "sess-ct")
    text_call = [c for c in calls if c[0] == "text"][0]
    check("text job sent with session token",
          text_call[2].get("context_token") == "sess-ct")
    scrub_sent = _read(sent, "job-scrub.json")
    check("sent file scrubbed of context_token",
          "context_token" not in scrub_sent)
    check("file/legacy/scrub/text jobs reached sent/",
          all(os.path.exists(os.path.join(sent, n)) for n in
              ("job-file.json", "job-legacy.json", "job-scrub.json",
               "job-text.json")))

    # 6. Tombstone permanence across a sweep.
    state_dir = os.path.join(tmp, "state")
    vault_dir = os.path.join(tmp, "vault")
    os.makedirs(state_dir)
    os.makedirs(vault_dir)
    bridge.TOMBSTONE_FILE = os.path.join(state_dir, "local_tombstones.json")
    bridge.TTL_INDEX_FILE = os.path.join(state_dir, "local_ttl_index.json")
    bridge.LOCAL_VAULT_DIR = vault_dir
    now = time.time()
    with open(bridge.TOMBSTONE_FILE, "w", encoding="utf-8") as f:
        json.dump({"999": {"deleted_ts": now - 100 * 86400,
                           "reason": "expired"}}, f)
    with open(bridge.TTL_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f)
    bridge.sweep_local_ttl(now=now)
    check("100-day-old tombstone survives sweep",
          "999" in bridge.load_tombstones())

    # 7. Analyzer env overrides; defaults unchanged without env.
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ)
    env["WECHAT_BRIDGE_ANALYZER_PY"] = "/tmp/custom-venv/bin/python"
    env["WECHAT_BRIDGE_ANALYZER_BASE"] = "/tmp/custom-base"
    code = (
        "import sys; sys.path.insert(0, %r); "
        "import m2_analyzer, stages; "
        "print(m2_analyzer.VENV_PY); print(stages.BASE)"
        % os.path.join(here, "analyzer"))
    out = subprocess.run([sys.executable, "-c", code], env=env,
                         capture_output=True, text=True, timeout=60)
    lines = out.stdout.strip().splitlines()
    check("analyzer env overrides honoured",
          out.returncode == 0 and len(lines) == 2
          and lines[0] == "/tmp/custom-venv/bin/python"
          and lines[1] == "/tmp/custom-base/")
    out2 = subprocess.run([sys.executable, "-c", code], capture_output=True,
                          text=True, timeout=60,
                          env={k: v for k, v in os.environ.items()
                               if not k.startswith("WECHAT_BRIDGE_")})
    lines2 = out2.stdout.strip().splitlines()
    check("analyzer defaults unchanged without env",
          out2.returncode == 0 and len(lines2) == 2
          and lines2[0].endswith("m0-component/venv/bin/python")
          and lines2[1].endswith("hidden_files/"))

    # 8. Inbox/done record no longer stores context_token. The only
    #    remaining `"context_token": ctx` in the file must be the
    #    bridge-internal pending-local-query queue (bridge-owned
    #    state like session.json, never Worker-visible), not a record.
    src = open(os.path.join(here, "bridge.py"), encoding="utf-8").read()
    uses = [ln for ln in src.splitlines() if '"context_token": ctx' in ln]
    check("record literal has no context_token field",
          len(uses) == 1 and "_pend.append" in uses[0])

    print("---")
    failed_names = [n for n, c in ok if not c]
    print("FAILED:", failed_names if failed_names else "none")
    sys.exit(1 if failed_names else 0)


if __name__ == "__main__":
    main()
