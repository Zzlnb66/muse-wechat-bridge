"""Disk watermark read-only fields: health + observability-summary.

health (bridge-health-0.1) and observability-summary each gain two
additive integer fields, both floored to whole MB:
  disk_free_mb: free space on the bridge root's disk;
  media_dir_size_mb: total size of <root>/media/ (the parent of
    MEDIA_INBOUND). Unreadable entries (dangling symlinks, unreadable
files) are skipped, never raised. Nothing is deleted, no threshold
alert is defined, and health's exit code stays 0.

Run against the fix-copy bridge.py, all in temp dirs with rebound
module paths; never touches the real running bridge.
Exit non-zero on any failure.
"""

import contextlib
import io
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


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS: %s" % name)
    else:
        FAIL += 1
        FAILURES.append(name)
        print("FAIL: %s %s" % (name, detail))


def _setup():
    tmp = tempfile.mkdtemp(prefix="disk-fields-test-")
    old = {}
    # Same rebind set as test_cli_send_health, except MEDIA_* use a
    # proper nested <tmp>/media/ layout so the watermark measures
    # exactly that tree.
    for n in ("STATE", "INBOX", "RUNNING", "DONE", "OUTBOX_PENDING",
              "OUTBOX_SENT", "OUTBOX_FAILED", "FAILURES_DIR",
              "ANALYSIS_DIR", "LOCAL_VAULT_DIR"):
        old[n] = getattr(bridge, n)
        d = os.path.join(tmp, n.lower())
        os.makedirs(d, exist_ok=True)
        setattr(bridge, n, d)
    media = os.path.join(tmp, "media")
    old["MEDIA_INBOUND"] = bridge.MEDIA_INBOUND
    old["MEDIA_OUTBOUND"] = bridge.MEDIA_OUTBOUND
    os.makedirs(os.path.join(media, "inbound"), exist_ok=True)
    os.makedirs(os.path.join(media, "outbound"), exist_ok=True)
    bridge.MEDIA_INBOUND = os.path.join(media, "inbound")
    bridge.MEDIA_OUTBOUND = os.path.join(media, "outbound")
    for n in ("NEEDS_RELOGIN_FILE", "CHANNEL_STATUS_FILE",
              "SERVE_RUN_FILE", "LOGIN_PENDING_FILE", "SESSION_FILE"):
        old[n] = getattr(bridge, n)
        setattr(bridge, n, os.path.join(bridge.STATE, os.path.basename(old[n])))
    return tmp, old


def _restore(old):
    for n, v in old.items():
        setattr(bridge, n, v)


def main():
    tmp, old = _setup()
    try:
        # Known media content: 1.5 MiB + 0.5 MiB = exactly 2 MiB.
        with open(os.path.join(bridge.MEDIA_INBOUND, "a.bin"), "wb") as f:
            f.write(b"x" * (1536 * 1024))
        with open(os.path.join(bridge.MEDIA_OUTBOUND, "b.bin"), "wb") as f:
            f.write(b"y" * (512 * 1024))
        # Outside media/: must not be counted.
        with open(os.path.join(bridge.STATE, "outside.bin"), "wb") as f:
            f.write(b"z" * (5 * 1024 * 1024))
        # Dangling symlink + symlink to a real file: skipped, never
        # counted twice, never raised.
        os.symlink("/nonexistent/disk-fields-target",
                   os.path.join(bridge.MEDIA_INBOUND, "dangling.bin"))
        os.symlink(os.path.join(bridge.MEDIA_INBOUND, "a.bin"),
                   os.path.join(bridge.MEDIA_OUTBOUND, "link-to-a.bin"))

        s = bridge.get_observability_summary()
        check("summary disk fields present",
              "disk_free_mb" in s and "media_dir_size_mb" in s, repr(s))
        check("summary disk_free_mb is a positive int",
              isinstance(s.get("disk_free_mb"), int)
              and not isinstance(s.get("disk_free_mb"), bool)
              and s["disk_free_mb"] > 0, repr(s.get("disk_free_mb")))
        check("summary media_dir_size_mb is exactly 2",
              s.get("media_dir_size_mb") == 2,
              repr(s.get("media_dir_size_mb")))

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = bridge.cmd_health([])
        check("health exit code still 0", rc == 0, repr(rc))
        h = json.loads(buf.getvalue())
        check("health schema unchanged", h.get("schema") == "bridge-health-0.1",
              repr(h.get("schema")))
        check("health disk fields present and ints",
              isinstance(h.get("disk_free_mb"), int)
              and isinstance(h.get("media_dir_size_mb"), int),
              repr((h.get("disk_free_mb"), h.get("media_dir_size_mb"))))
        check("health media_dir_size_mb is exactly 2",
              h.get("media_dir_size_mb") == 2,
              repr(h.get("media_dir_size_mb")))
        check("health disk_free_mb agrees with summary",
              h.get("disk_free_mb") == s.get("disk_free_mb"),
              repr((h.get("disk_free_mb"), s.get("disk_free_mb"))))
    finally:
        _restore(old)
        shutil.rmtree(tmp, ignore_errors=True)
    print("disk_fields: %d passed, %d failed" % (PASS, FAIL))
    if FAILURES:
        print("failures: %s" % FAILURES)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
