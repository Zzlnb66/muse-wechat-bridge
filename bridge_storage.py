"""Pure storage primitives for the WeChat bridge (Gate 3 step 1).

This module holds ONLY storage mechanics, no business judgment:
root-derived path constants, the standard directory set, atomic file
writes with private permissions, and safe JSON reads. Sending,
sanitizing, long-text segmentation, same-source merging, offline /
progress receipts, the recipient gate, and shadow-mode decisions all
stay in bridge.py; this module never decides what a message means,
whether it may be sent, or where it is routed.

Deployment note: like aes_inprocess.py, this file must be copied next
to bridge.py at deploy time. bridge.py imports it unguarded: if it is
missing, bridge.py does not start, instead of silently running with a
different storage layout.

History: extracted verbatim from bridge.py (Gate 3 module split,
first increment). The atomic writers below are byte-identical to the
originals: same tmp naming (path + "." + pid + ".tmp"), same private
creation mode from the first instant, same fsync + parent-dir fsync +
final chmod sequence for JSON, same replace semantics. Do not
"improve" them here without a matching regression run: concurrent
writers and crash-durability depend on the exact sequence.
"""

import json
import os
from types import SimpleNamespace


def paths_for_root(root):
    """Derive every bridge path from one root directory.

    All paths stay under root. When the root changes (a second bridge
    instance, a test copy), every path follows: callers must never
    hard-code a path that this function already derives.
    """
    state = os.path.join(root, "state")
    media_inbound = os.path.join(root, "media", "inbound")
    return SimpleNamespace(
        root=root,
        state=state,
        inbox=os.path.join(root, "inbox"),
        running=os.path.join(root, "running"),
        done=os.path.join(root, "done"),
        outbox_pending=os.path.join(root, "outbox", "pending"),
        outbox_sent=os.path.join(root, "outbox", "sent"),
        outbox_failed=os.path.join(root, "outbox", "failed"),
        media_inbound=media_inbound,
        media_outbound=os.path.join(root, "media", "outbound"),
        analysis_dir=os.path.join(root, "analysis"),
        local_vault_dir=os.path.join(root, "local_vault"),
        failures_dir=os.path.join(root, "failures"),
        file_inbound_dir=os.path.join(media_inbound, "files"),
        local_only_mode_file=os.path.join(state, "local_only_mode.json"),
        pending_local_queries_file=os.path.join(state, "pending_local_queries.json"),
        ttl_index_file=os.path.join(state, "local_ttl_index.json"),
        tombstone_file=os.path.join(state, "local_tombstones.json"),
        image_event_file=os.path.join(state, "last_image_event.json"),
        file_receipts_file=os.path.join(state, "file_receipts.json"),
        session_file=os.path.join(state, "session.json"),
        cursor_file=os.path.join(state, "cursor.txt"),
        processed_file=os.path.join(state, "processed_ids.json"),
        needs_relogin_file=os.path.join(state, "NEEDS_RELOGIN"),
        login_pending_file=os.path.join(state, "login_pending.json"),
        lock_file=os.path.join(state, "serve.lock"),
        channel_status_file=os.path.join(state, "channel_status.json"),
        serve_run_file=os.path.join(state, "serve_run.json"),
        analyzer_runs_file=os.path.join(state, "analyzer_runs.json"),
        safe_view_file=os.path.join(state, "safe_history_view.json"),
    )


def ensure_dirs(*dirs):
    """Create the given directories (exist_ok). Pure makedirs: no
    business state is written, no file is touched."""
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def _atomic_tmp_path(path):
    # Unique tmp name, so concurrent writers do not share one .tmp file.
    return f"{path}.{os.getpid()}.tmp"


def _open_private(path, mode):
    # Create the tmp file with the final mode from the first instant,
    # instead of creating 0644 under umask 022 and chmodding only
    # after replace. Use os.open with mode, then wrap in a file.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    return os.fdopen(fd, "w", encoding="utf-8")


def atomic_write_json(path, obj, mode=0o600):
    """Write obj as JSON atomically: tmp file + fsync + replace +
    parent-dir fsync + final chmod. On any failure before replace the
    original file is untouched and the tmp file is left for the caller
    to notice (same semantics as the original bridge.py writer)."""
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
    # Fsync parent dir so the rename itself is durable.
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
    """Write text atomically: tmp file + replace + final chmod."""
    tmp = _atomic_tmp_path(path)
    with _open_private(tmp, mode) as f:
        f.write(text)
    os.replace(tmp, path)
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def read_json_dict(path):
    """Safe JSON dict read: returns the parsed dict, or {} when the
    file is missing, unreadable, unparseable, or not a dict. Never
    raises, never returns a partial value."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
