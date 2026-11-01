"""Unified CI regression entry (v1.5.0 item 4, 'CI regression check').

Runs every suite listed in tests/CI-MANIFEST.md as a subprocess with
its documented command, arguments, interpreter and timeout, and
exits non-zero if ANY mandatory suite fails. A suite that prints
FAIL but exits 0 would still be caught: the runner also scans the
tail of each suite's output for a failure marker ("FAIL" / "failed"
with a non-zero count) and treats that as a failure even when the
exit code is 0. (No suite is currently known to do this; the scan
is defence in depth against the test_v101 false-green class.)

Usage:
  python3 tests/run_ci.py [--bridge bridge.py] [--skip-100mb]
    --bridge: path to the bridge.py under test (default: the
      fix-copy bridge.py next to this file's parent dir).
    --skip-100mb: skip test_file_inbound_100mb (it asserts the
      fix-copy-only 100 MiB constants; the public 10 MiB tree must
      not run it). The skip is printed, never silent.

Conditional skips (printed with reason, never silent):
  - test_m2_model_config: skipped when the M0-venv-only packages
    (faster-whisper, ppocr-lite, ai-edge-litert) are not importable
    with the current interpreter. The no-weight control logic it
    covers is otherwise exercised by test_analyzer_recovery.
  - test_file_inbound_100mb: skipped with --skip-100mb (see above).

Offline, synthetic data only. No production session, chat record or
real media is ever used: every suite runs in temp dirs by design.
Exit 0 = all mandatory suites passed; exit 1 = any failure.
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# (name, argv-template, timeout_s, mandatory, skip_reason_or_None)
# {BRIDGE} -> bridge.py path, {WATCHDOG} -> watchdog script path.
SUITES = [
    ("test_aes_switchover", ["{PY}", "tests/test_aes_switchover.py"], 300, True, None),
    ("test_aes_vectors", ["{PY}", "tests/test_aes_vectors.py"], 300, True, None),
    ("test_analyzer_recovery", ["{PY}", "tests/test_analyzer_recovery.py"], 600, True, None),
    ("test_backoff_tiers", ["{PY}", "tests/test_backoff_tiers.py", "{BRIDGE}"], 300, True, None),
    ("test_chat_commands", ["{PY}", "tests/test_chat_commands.py"], 300, True, None),
    ("test_cli_send_health", ["{PY}", "tests/test_cli_send_health.py"], 300, True, None),
    ("test_disk_fields", ["{PY}", "tests/test_disk_fields.py"], 300, True, None),
    ("test_doc_parser", ["{PY}", "tests/test_doc_parser.py"], 600, True, None),
    ("test_doc_parser_empty_layer", ["{PY}", "tests/test_doc_parser_empty_layer.py"], 600, True, None),
    ("test_doc_parser_pypdf", ["{PY}", "tests/test_doc_parser_pypdf.py"], 600, True, None),
    ("test_doc_parser_wiring", ["{PY}", "tests/test_doc_parser_wiring.py"], 600, True, None),
    ("test_evidence_audit", ["{PY}", "tests/test_evidence_audit.py"], 300, True, None),
    ("test_file_inbound", ["{PY}", "tests/test_file_inbound.py"], 600, True, None),
    ("test_file_inbound_100mb", ["{PY}", "tests/test_file_inbound_100mb.py"], 900, True, "fix-copy-only 100 MiB constants"),
    ("test_getupdates_anomalies", ["{PY}", "tests/test_getupdates_anomalies.py"], 300, True, None),
    ("test_gpt_followup", ["{PY}", "tests/test_gpt_followup.py"], 300, True, None),
    ("test_job_shape_guard", ["{PY}", "tests/test_job_shape_guard.py"], 300, True, None),
    ("test_long_text", ["{PY}", "tests/test_long_text.py"], 300, True, None),
    ("test_m2_model_config", ["{PY}", "tests/test_m2_model_config.py"], 600, False, "needs M0-venv-only packages"),
    ("test_multi_instance_isolation", ["{PY}", "tests/test_multi_instance_isolation.py"], 300, True, None),
    ("test_observability", ["{PY}", "tests/test_observability.py"], 300, True, None),
    ("test_offline_receipt", ["{PY}", "tests/test_offline_receipt.py"], 300, True, None),
    ("test_outbound_sanitize", ["{PY}", "tests/test_outbound_sanitize.py"], 300, True, None),
    ("test_progress_receipt", ["{PY}", "tests/test_progress_receipt.py"], 300, True, None),
    ("test_quote_v1", ["{PY}", "tests/test_quote_v1.py"], 300, True, None),
    ("test_r2_p0", ["{PY}", "tests/test_r2_p0.py"], 300, True, None),
    ("test_r8_timing", ["{PY}", "tests/test_r8_timing.py", "{BRIDGE}"], 300, True, None),
    ("test_recipient_gate", ["{PY}", "tests/test_recipient_gate.py"], 300, True, None),
    ("test_regression", ["{PY}", "tests/test_regression.py", "{BRIDGE}"], 600, True, None),
    ("test_same_source_merge", ["{PY}", "tests/test_same_source_merge.py"], 300, True, None),
    ("test_sqlite_shadow", ["{PY}", "tests/test_sqlite_shadow.py"], 300, True, None),
    ("test_sqlite_switch", ["{PY}", "tests/test_sqlite_switch.py"], 300, True, None),
    ("test_sqlite_switch_fixes", ["{PY}", "tests/test_sqlite_switch_fixes.py"], 300, True, None),
    ("test_storage_module", ["{PY}", "tests/test_storage_module.py"], 300, True, None),
    ("test_v101", ["{PY}", "tests/test_v101.py", "{BRIDGE}"], 300, True, None),
    ("test_v102", ["{PY}", "tests/test_v102.py", "{BRIDGE}"], 300, True, None),
    ("test_v103", ["{PY}", "tests/test_v103.py", "{BRIDGE}"], 300, True, None),
    ("test_v104", ["{PY}", "tests/test_v104.py", "{BRIDGE}"], 300, True, None),
    ("test_v104_diag", ["{PY}", "tests/test_v104_diag.py", "{BRIDGE}"], 300, True, None),
    ("test_v104_gpt", ["{PY}", "tests/test_v104_gpt.py", "{BRIDGE}"], 300, True, None),
    ("test_v104_worker_verify", ["{PY}", "tests/test_v104_worker_verify.py", "{BRIDGE}"], 300, True, None),
    ("test_video_v1", ["{PY}", "tests/test_video_v1.py"], 600, True, None),
    ("test_waiting_prompt_merge", ["{PY}", "tests/test_waiting_prompt_merge.py"], 300, True, None),
    ("test_v104_watchdog.sh", ["bash", "tests/test_v104_watchdog.sh", "{WATCHDOG}"], 300, True, None),
    ("test_v104_gpt_watchdog.sh", ["bash", "tests/test_v104_gpt_watchdog.sh", "{WATCHDOG}"], 300, True, None),
]

M0_PACKAGES = ("faster_whisper", "ppocr_lite", "ai_edge_litert")


def m0_available():
    for pkg in M0_PACKAGES:
        r = subprocess.run([sys.executable, "-c", "import %s" % pkg],
                           capture_output=True)
        if r.returncode != 0:
            return False, pkg
    return True, None


def output_has_failure_marker(text):
    # Tight on purpose: only the suites' own failure conventions count.
    # A loose substring rule would false-positive on innocent lines
    # such as "v1.0.1-3 failed move: 2 drains, only 1 send" (a PASS
    # line describing a move failure that was handled correctly).
    # And "FAILED: none" means ZERO failures, not a failure.
    for line in text.splitlines():
        low = line.strip().lower()
        if low.startswith("fail "):
            return True
        if low.startswith("failed:"):
            rest = low[len("failed:"):].strip()
            if rest not in ("", "none", "[]", "none."):
                return True
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bridge", default=os.path.join(ROOT, "bridge.py"))
    ap.add_argument("--skip-100mb", action="store_true")
    args = ap.parse_args()
    bridge = os.path.abspath(args.bridge)
    watchdog = os.path.join(ROOT, "watchdog", "wechat-bridge-watchdog.sh")

    results = []
    # Self-check first: the bridge under test must compile.
    rc = subprocess.run([sys.executable, "-m", "py_compile", bridge],
                        capture_output=True, text=True)
    results.append(("py_compile bridge.py", "PASS" if rc.returncode == 0 else "FAIL",
                    rc.returncode, ""))
    if rc.returncode != 0:
        print(rc.stdout[-2000:])
        print(rc.stderr[-2000:])

    m0_ok, m0_missing = m0_available()
    for name, argv_t, timeout, mandatory, skip_reason in SUITES:
        if name == "test_file_inbound_100mb" and args.skip_100mb:
            results.append((name, "SKIP", 0, "skipped: fix-copy-only 100 MiB constants (--skip-100mb)"))
            print("SKIP %s (fix-copy-only 100 MiB constants)" % name)
            continue
        if name == "test_m2_model_config" and not m0_ok:
            results.append((name, "SKIP", 0, "skipped: M0 package missing: %s" % m0_missing))
            print("SKIP %s (M0 package missing: %s)" % (name, m0_missing))
            continue
        argv = [a.replace("{PY}", sys.executable).replace("{BRIDGE}", bridge).replace("{WATCHDOG}", watchdog)
                for a in argv_t]
        t0 = time.time()
        try:
            r = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
            out = (r.stdout or "") + (r.stderr or "")
            tail = "\n".join(out.splitlines()[-15:])
            marker = output_has_failure_marker(out)
            status = "PASS" if (r.returncode == 0 and not marker) else "FAIL"
            results.append((name, status, r.returncode, tail if status == "FAIL" else ""))
            print("%s %s (%.1fs, exit %d)" % (status, name, time.time() - t0, r.returncode))
            if status == "FAIL":
                print(tail[-2000:])
        except subprocess.TimeoutExpired:
            results.append((name, "FAIL", -1, "timeout after %ds" % timeout))
            print("FAIL %s (timeout after %ds)" % (name, timeout))

    print("\n=== CI SUMMARY ===")
    failed = [n for n, s, _, _ in results if s == "FAIL"]
    skipped = [n for n, s, _, _ in results if s == "SKIP"]
    passed = [n for n, s, _, _ in results if s == "PASS"]
    for n, s, rc, _ in results:
        print("%-32s %-4s exit %s" % (n, s, rc))
    print("passed %d, failed %d, skipped %d" % (len(passed), len(failed), len(skipped)))
    if failed:
        print("FAILED SUITES:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
