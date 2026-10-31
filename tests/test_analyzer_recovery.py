"""Offline tests for item 1 (analyzer recovery), 2026-10-31 plan.

Covers, against the fix-copy bridge.py and analyzer:
  1. Stage output validation: exit code, valid JSON object, completed
     status, and run_id echo are all required; anything else is a
     structured failure, never an exception.
  2. run_stage with a missing interpreter: structured spawn_failed,
     no raise, and a pre-placed stale output is deleted, not reused.
  3. Local-query terminal-state reading: ready/failed/waiting, with
     run_id mismatch treated as waiting (not stale delivery).
  4. Analyzer-tree kill: identity-checked kill stops a registered run
     and never touches a PID whose identity does not match.
  5. REAL parent-child chain (no Popen mock): a nonexistent explicit
     interpreter falls through to a validated runtime, the real
     analyzer actually runs, light tracks complete, and a terminal
     state.json with a matching run_id appears.

Run: python3 tests/test_analyzer_recovery.py (exit 0 = all pass).
The real-chain test runs the actual analyzer on a 2 s synthetic video
and may take a couple of minutes; it is last on purpose.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "analyzer"))
import bridge  # noqa: E402
import m2_analyzer  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def test_validate_stage_output():
    tmp = tempfile.mkdtemp(prefix="ar-validate-")
    good = os.path.join(tmp, "good.json")
    json.dump({"_status": "completed", "_run_id": "R1"}, open(good, "w", encoding="utf-8"))
    check("validate: completed + run_id match ok",
          m2_analyzer._validate_stage_output(good, "R1", 0) == (True, None))
    check("validate: run_id mismatch rejected",
          m2_analyzer._validate_stage_output(good, "R2", 0)[0] is False)
    failed = os.path.join(tmp, "failed.json")
    json.dump({"_status": "failed", "_error": "stage_failed"}, open(failed, "w", encoding="utf-8"))
    check("validate: nonzero + failed body -> stage error",
          m2_analyzer._validate_stage_output(failed, "R1", 1)[1] == "stage_failed")
    bad = os.path.join(tmp, "bad.json")
    open(bad, "w", encoding="utf-8").write("{not json")
    check("validate: bad json rejected",
          m2_analyzer._validate_stage_output(bad, "R1", 0)[1] == "bad_json")
    check("validate: missing output rejected",
          m2_analyzer._validate_stage_output(os.path.join(tmp, "nope.json"), "R1", 0)[1] == "no_output")


def test_run_stage_spawn_fail_and_stale():
    tmp = tempfile.mkdtemp(prefix="ar-stage-")
    outdir = os.path.join(tmp, "out")
    os.makedirs(outdir, exist_ok=True)
    stale = os.path.join(outdir, "stage_probe.json")
    json.dump({"_status": "completed", "_run_id": "OLD"}, open(stale, "w", encoding="utf-8"))
    old_py = m2_analyzer.STAGE_PY
    m2_analyzer.STAGE_PY = "/nonexistent/py-xyz"
    try:
        res = m2_analyzer.run_stage("probe", {"video": "/tmp/nope.mp4"}, outdir, "R9")
    finally:
        m2_analyzer.STAGE_PY = old_py
    check("run_stage: missing interpreter -> structured spawn_failed, no raise",
          res.get("_status") == "failed" and res.get("_error") == "spawn_failed")
    check("run_stage: stale output deleted before launch, not reused",
          not os.path.exists(stale))


def test_local_query_state():
    tmp = tempfile.mkdtemp(prefix="ar-state-")

    def _mk(name, state_doc):
        d = os.path.join(tmp, name)
        os.makedirs(d, exist_ok=True)
        if state_doc is not None:
            if isinstance(state_doc, str):
                open(os.path.join(d, "state.json"), "w", encoding="utf-8").write(state_doc)
            else:
                json.dump(state_doc, open(os.path.join(d, "state.json"), "w", encoding="utf-8"))
        return d

    check("query state: completed + match -> ready",
          bridge._read_analyzer_state(_mk("a", {"state": "completed", "run_id": "R1"}), "R1") == "ready")
    check("query state: partial + match -> ready",
          bridge._read_analyzer_state(_mk("b", {"state": "partial", "run_id": "R1"}), "R1") == "ready")
    check("query state: failed + match -> failed",
          bridge._read_analyzer_state(_mk("c", {"state": "failed", "run_id": "R1"}), "R1") == "failed")
    check("query state: terminal for another run -> waiting, not stale",
          bridge._read_analyzer_state(_mk("d", {"state": "completed", "run_id": "R2"}), "R1") is None)
    check("query state: legacy (no state file) -> waiting",
          bridge._read_analyzer_state(_mk("e", None), "R1") is None)
    check("query state: bad state json -> waiting",
          bridge._read_analyzer_state(_mk("f", "{bad"), "R1") is None)
    check("query state: legacy query (no expected run) accepts terminal",
          bridge._read_analyzer_state(_mk("g", {"state": "completed", "run_id": "R9"}), None) == "ready")


def test_kill_helper():
    tmp = tempfile.mkdtemp(prefix="ar-kill-")
    old_runs = bridge.ANALYZER_RUNS_FILE
    bridge.ANALYZER_RUNS_FILE = os.path.join(tmp, "runs.json")
    try:
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            bridge._register_analyzer_run("run-kill1", os.path.join(tmp, "o1"),
                                          "killkey", proc.pid, sys.executable)
            killed = bridge._kill_analyzer_runs(video_key="killkey")
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
            alive = proc.poll() is None
            check("kill: identity-matched run stopped", "run-kill1" in killed and not alive)
        finally:
            try:
                proc.kill()
            except Exception:
                pass
        proc2 = subprocess.Popen(["sleep", "30"], start_new_session=True,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            bridge._register_analyzer_run("run-kill2", os.path.join(tmp, "o2"),
                                          "killkey2", proc2.pid, sys.executable)
            runs = bridge._load_analyzer_runs()
            runs["run-kill2"]["proc_starttime"] = "bogus"
            bridge._save_analyzer_runs(runs)
            bridge._kill_analyzer_runs(video_key="killkey2")
            alive2 = True
            try:
                os.kill(proc2.pid, 0)
            except Exception:
                alive2 = False
            check("kill: identity mismatch never killed", alive2)
        finally:
            try:
                proc2.kill()
            except Exception:
                pass
    finally:
        bridge.ANALYZER_RUNS_FILE = old_runs


def test_real_chain():
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        print("SKIP real chain: ffmpeg/ffprobe missing")
        return
    tmp = tempfile.mkdtemp(prefix="ar-chain-")
    video = os.path.join(tmp, "synth.mp4")
    r = subprocess.run(["ffmpeg", "-y", "-v", "error",
                        "-f", "lavfi", "-i", "testsrc=duration=2:size=320x240:rate=10",
                        "-f", "lavfi", "-i", "sine=frequency=440:duration=2",
                        "-shortest", "-c:v", "libx264", "-c:a", "aac", video],
                       capture_output=True, timeout=120)
    if r.returncode != 0 or not os.path.exists(video):
        print("SKIP real chain: synthetic video generation failed")
        return
    old_runs = bridge.ANALYZER_RUNS_FILE
    bridge.ANALYZER_RUNS_FILE = os.path.join(tmp, "runs.json")
    saved_env = os.environ.get("WECHAT_BRIDGE_ANALYZER_PY")
    os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = "/nonexistent/py-xyz"
    outdir = os.path.join(tmp, "analysis")
    os.makedirs(outdir, exist_ok=True)
    logf = open(os.path.join(tmp, "analyzer.log"), "ab")
    try:
        launched = bridge._launch_analyzer(video, outdir, "chain1", logf, subprocess.STDOUT)
    finally:
        logf.close()
    try:
        check("real chain: bad explicit interpreter does not block launch",
              "status" not in launched)
        if "status" in launched:
            return
        check("real chain: selected interpreter is not the bad path",
              launched.get("python") != "/nonexistent/py-xyz")
        state = None
        for _ in range(150):
            sp = os.path.join(outdir, "state.json")
            if os.path.exists(sp):
                try:
                    state = json.load(open(sp, encoding="utf-8"))
                    if state.get("state") in ("completed", "partial", "failed"):
                        break
                except Exception:
                    pass
            time.sleep(2)
        check("real chain: terminal state.json appears",
              isinstance(state, dict) and state.get("state") in ("completed", "partial", "failed"))
        if isinstance(state, dict):
            expect = "failed" if not state.get("completed_tracks") else (
                "partial" if (state.get("failed_tracks") or state.get("skipped_tracks")) else "completed")
            check("real chain: state value matches its own track lists",
                  state.get("state") == expect)
        if isinstance(state, dict):
            check("real chain: state run_id matches launched run_id",
                  state.get("run_id") == launched.get("run_id"))
            check("real chain: light tracks completed (no interpreter crash)",
                  "probe" in (state.get("completed_tracks") or [])
                  and "frames" in (state.get("completed_tracks") or []))
            evp = os.path.join(outdir, "evidence.json")
            if os.path.exists(evp):
                ev = json.load(open(evp, encoding="utf-8"))
                check("real chain: evidence run_id matches state",
                      ev.get("run_id") == state.get("run_id"))
            print("real chain: state=%s completed=%s failed=%s skipped=%s"
                  % (state.get("state"), state.get("completed_tracks"),
                     state.get("failed_tracks"), state.get("skipped_tracks")))
    finally:
        if saved_env is None:
            os.environ.pop("WECHAT_BRIDGE_ANALYZER_PY", None)
        else:
            os.environ["WECHAT_BRIDGE_ANALYZER_PY"] = saved_env
        bridge.ANALYZER_RUNS_FILE = old_runs
        try:
            bridge._kill_analyzer_runs(video_key="chain1")
        except Exception:
            pass


test_validate_stage_output()
test_run_stage_spawn_fail_and_stale()
test_local_query_state()
test_kill_helper()
test_real_chain()

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
sys.exit(1 if failed else 0)
