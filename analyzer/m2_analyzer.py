def build_narrative(ev):
    """Deterministic timeline narrative from evidence.json content only.

    Narrative v1: orders the already-extracted tracks by time (frames,
    OCR lines, ASR segments, high-score sound windows) and lists conflicts
    and uninspected ranges explicitly. It invents nothing: no speaker
    identity, no lyric claim, no pure-instrumental claim, no resolution
    of conflicts. Sound rows are reported as detector scores, not as
    facts about who spoke. Returns markdown text."""
    lines = []
    dur = ev.get("duration_s")
    lines.append("# 视频时间线叙事（确定性整理，非自由解读）")
    lines.append("")
    lines.append("- 时长：%s 秒" % (dur if dur is not None else "未知"))
    lines.append("- 已完成轨道：%s" % ("、".join(ev.get("completed_tracks") or ["无"])))
    lines.append("- 证据版本：%s" % ev.get("schema_version", "未知"))
    lines.append("")
    events = []
    for fr in (ev.get("stages", {}).get("frames", {}) or {}).get("frames", []) or []:
        events.append((float(fr.get("t_s", 0) or 0), "画面帧 %s" % fr.get("file", "?")))
    for fr in (ev.get("stages", {}).get("ocr", {}) or {}).get("ocr_frames", []) or []:
        texts = [l.get("text", "") for l in fr.get("lines", []) or [] if l.get("text")]
        if texts:
            events.append((float(fr.get("t_s", 0) or 0),
                           "屏幕文字（OCR）：%s" % " / ".join(texts)))
    for seg in (ev.get("stages", {}).get("asr", {}) or {}).get("segments", []) or []:
        events.append((float(seg.get("start_s", 0) or 0),
                       "语音转写 %s–%s 秒：%s" % (seg.get("start_s"), seg.get("end_s"), seg.get("text", ""))))
    for row in (ev.get("stages", {}).get("sound", {}) or {}).get("rows", []) or []:
        top = row.get("top1")
        score = row.get("top1_score")
        if top and score is not None and float(score) >= 0.50:
            events.append((float(row.get("start_s", 0) or 0),
                           "声音检测 %s–%s 秒：%s %.4f（Speech %.4f，Music %.4f）"
                           % (row.get("start_s"), row.get("end_s"), top, float(score),
                              float(row.get("Speech", 0) or 0), float(row.get("Music", 0) or 0))))
    events.sort(key=lambda e: e[0])
    lines.append("## 时间线")
    lines.append("")
    if not events:
        lines.append("- （无可用时间线事件）")
    for t, desc in events:
        lines.append("- %.2f 秒：%s" % (t, desc))
    lines.append("")
    lines.append("## 存疑与冲突")
    lines.append("")
    if ev.get("conflicts"):
        for c in ev["conflicts"]:
            lines.append("- %s：%s" % (c.get("type", "?"), c.get("note", "")))
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 未检查区间")
    lines.append("")
    if ev.get("uninspected_ranges"):
        for u in ev["uninspected_ranges"]:
            lines.append("- %s：%s" % (u.get("track", "?"), u.get("reason", "")))
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 局限")
    lines.append("")
    lines.append("- 本叙事只按时间重排已有证据，不补充、不猜测、不裁决冲突。")
    lines.append("- OCR 与转写可能误读；声音分数不是说话人身份、不是歌词内容、不是纯器乐证明。")
    lines.append("- 同一模型重复出现不算独立证实。")
    return "\n".join(lines) + "\n"


"""M2 practical analyzer v1 (deterministic evidence first, narrative v1 timeline).
Runs stages serially, each in its own process:
  probe (ffprobe) -> frames (ffmpeg 1/2s + scene detect) -> ocr -> asr -> sound -> fusion
Gate (M0 gate prototype, NOT a safety budget): MemAvailable >= 500 MiB ALLOW;
200-500 MiB WAIT up to 3 rechecks at 2 s; < 200 MiB REJECT. A REJECT skips
heavy stages (ocr/asr) and records them as skipped_gate, never as failed
evidence, and never retries them in the same run. Light stages always run.
Fusion is deterministic only: timeline, per-window/per-frame evidence,
digit conflicts (OCR digits vs ASR digits in the same window), and
music-background ASR suspicion (ASR segment overlapping a window with
Music >= 0.50 while Speech < 0.20). Narrative v1 (build_narrative) only
reorders those tracks by time into narrative.md; it does NOT claim
speaker identity, lyric content, or pure-instrumental status.
Uninspected ranges are listed explicitly.
Usage: python m2_analyzer.py <video> <outdir> [--run-id ID]
Output: <outdir>/evidence.json (+ frames/, stage jsons, audio_16k.wav)
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
# Item 1 (2026-10-31): stages ALWAYS run under this analyzer's own
# sys.executable. The old VENV_PY (re-read from the environment) is
# gone on purpose: when the bridge fell back to its own interpreter,
# the child inherited the stale invalid value and every stage died
# with FileNotFoundError. The bridge launcher now validates the
# interpreter by execution and passes the selected one via the
# environment, so the analyzer's sys.executable IS the selection.
# WECHAT_BRIDGE_ANALYZER_BASE is still honoured by stages.py for
# model files.
STAGE_PY = sys.executable


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def mem_available_kib():
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1])
    return None


def gate():
    """Returns (decision, mem_kib, waits). WAIT rechecks do not extend any deadline."""
    waits = 0
    while True:
        mem = mem_available_kib()
        if mem is None or mem >= 500 * 1024:
            return "ALLOW", mem, waits
        if mem < 200 * 1024:
            return "REJECT", mem, waits
        if waits >= 3:
            return "REJECT", mem, waits
        waits += 1
        time.sleep(2)


def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def _validate_stage_output(outp, expected_run_id, returncode):
    """Validate one stage output file. Returns (ok, error_category).

    A stage counts as completed ONLY on: exit 0, a fresh valid JSON
    object, _status completed, and a matching _run_id echo when one
    was requested. Anything else is a structured failure, never an
    exception and never a stale file mistaken for a fresh result."""
    if returncode != 0:
        try:
            data = json.load(open(outp, encoding="utf-8"))
            if isinstance(data, dict) and data.get("_status") == "failed":
                return False, data.get("_error") or "stage_failed"
        except Exception:
            pass
        return False, "nonzero_no_output" if not os.path.exists(outp) else "nonzero_output"
    if not os.path.exists(outp):
        return False, "no_output"
    try:
        data = json.load(open(outp, encoding="utf-8"))
    except Exception:
        return False, "bad_json"
    if not isinstance(data, dict):
        return False, "bad_json"
    if expected_run_id and data.get("_run_id") != expected_run_id:
        return False, "run_id_mismatch"
    if data.get("_status") != "completed":
        return False, data.get("_error") or "stage_failed"
    return True, None


def run_stage(stage, args, outdir, run_id=None, timeout=600):
    """Run one stage as its own process group, with structured errors.

    The previous run's output file is deleted BEFORE launch, so a
    killed stage can never leave a stale file that reads as fresh.
    Spawn failure, timeout (whole process group killed), non-zero exit,
    missing/bad output, and run_id mismatch all become a failed result
    dict; this function never raises for a stage problem."""
    inp = os.path.join(outdir, f"stage_{stage}_in.json")
    outp = os.path.join(outdir, f"stage_{stage}.json")
    try:
        os.remove(outp)
    except OSError:
        pass
    args = dict(args)
    if run_id:
        args["_run_id"] = run_id
    try:
        _atomic_write_json(inp, args)
    except Exception as e:
        return {"_stage": stage, "_status": "failed", "_error": "input_write_failed",
                "_error_detail": type(e).__name__, "_peak_rss_kib": None}
    try:
        proc = subprocess.Popen([STAGE_PY, os.path.join(HERE, "stages.py"), stage, inp, outp],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                stdin=subprocess.DEVNULL, start_new_session=True)
    except OSError as e:
        return {"_stage": stage, "_status": "failed", "_error": "spawn_failed",
                "_error_detail": type(e).__name__, "_peak_rss_kib": None}
    try:
        returncode = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, 9)
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        return {"_stage": stage, "_status": "failed", "_error": "timeout",
                "_peak_rss_kib": None}
    ok, err = _validate_stage_output(outp, run_id, returncode)
    if ok:
        return json.load(open(outp, encoding="utf-8"))
    result = {"_stage": stage, "_status": "failed", "_error": err, "_peak_rss_kib": None}
    try:
        data = json.load(open(outp, encoding="utf-8"))
        if isinstance(data, dict) and data.get("_error_detail"):
            result["_error_detail"] = data["_error_detail"]
    except Exception:
        pass
    return result


def _main_inner(video, outdir, run_id):
    os.makedirs(outdir, exist_ok=True)
    framedir = os.path.join(outdir, "frames")
    ev = {"schema_version": "m2-evidence-0.1",
          "run_id": run_id,
          "video_sha256": sha256_file(video),
          "video_bytes": os.path.getsize(video),
          "stages": {}, "completed_tracks": [], "failed_tracks": [],
          "skipped_tracks": [], "uninspected_ranges": [],
          "conflicts": [], "fusion_notes": []}
    # 1. probe (light)
    probe = run_stage("probe", {"video": video}, outdir, run_id)
    ev["stages"]["probe"] = probe
    if probe.get("_status") == "completed":
        ev["completed_tracks"].append("probe")
        fmt = probe.get("probe", {}).get("format", {})
        ev["duration_s"] = float(fmt.get("duration", 0) or 0)
    else:
        ev["failed_tracks"].append("probe")
        ev["uninspected_ranges"].append({"track": "probe", "reason": probe.get("_error")})
    # 2. frames (light)
    frames = run_stage("frames", {"video": video, "outdir": framedir}, outdir, run_id)
    ev["stages"]["frames"] = frames
    if frames.get("_status") == "completed":
        ev["completed_tracks"].append("frames")
    else:
        ev["failed_tracks"].append("frames")
        ev["uninspected_ranges"].append({"track": "frames", "reason": frames.get("_error")})
    # 3. audio extract (light, ffmpeg). A missing ffmpeg, a timeout, or
    # any spawn error used to kill the whole run; it is now a failed
    # audio track, and asr/sound are skipped on it, not crashed on.
    wav = os.path.join(outdir, "audio_16k.wav")
    try:
        r = subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", video, "-vn", "-ac", "1",
                            "-ar", "16000", "-c:a", "pcm_s16le", wav], capture_output=True, timeout=120)
        ev["audio_16k_extracted"] = bool(r.returncode == 0 and os.path.exists(wav) and os.path.getsize(wav) > 0)
        _audio_err = None if ev["audio_16k_extracted"] else "ffmpeg 16k extract failed"
    except Exception as e:
        ev["audio_16k_extracted"] = False
        _audio_err = type(e).__name__
    if not ev["audio_16k_extracted"]:
        ev["failed_tracks"].append("audio")
        ev["uninspected_ranges"].append({"track": "audio", "reason": _audio_err})
    # 4. gate for heavy stages
    decision, mem, waits = gate()
    ev["gate"] = {"decision": decision, "mem_available_kib": mem, "waits": waits,
                  "note": "Gate is a branch decision only, not a safety memory budget (M0)."}
    heavy = ["ocr", "asr", "sound"]
    if decision == "REJECT":
        for s in heavy:
            ev["stages"][s] = {"_stage": s, "_status": "skipped_gate"}
            ev["skipped_tracks"].append(s)
            ev["uninspected_ranges"].append({"track": s, "reason": "gate_reject"})
    else:
        # ocr (needs frames)
        if frames.get("_status") == "completed":
            ocr = run_stage("ocr", {"frames": frames["frames"], "framedir": framedir}, outdir, run_id)
            ev["stages"]["ocr"] = ocr
            if ocr.get("_status") == "completed":
                ev["completed_tracks"].append("ocr")
            else:
                ev["failed_tracks"].append("ocr")
                ev["uninspected_ranges"].append({"track": "ocr", "reason": ocr.get("_error")})
        else:
            ev["stages"]["ocr"] = {"_stage": "ocr", "_status": "skipped"}
            ev["skipped_tracks"].append("ocr")
            ev["uninspected_ranges"].append({"track": "ocr", "reason": "frames_failed"})
        # asr + sound (need audio)
        if ev["audio_16k_extracted"]:
            asr = run_stage("asr", {"audio_16k_wav": wav}, outdir, run_id)
            ev["stages"]["asr"] = asr
            if asr.get("_status") == "completed":
                ev["completed_tracks"].append("asr")
            else:
                ev["failed_tracks"].append("asr")
                ev["uninspected_ranges"].append({"track": "asr", "reason": asr.get("_error")})
            sound = run_stage("sound", {"audio_16k_wav": wav}, outdir, run_id)
            ev["stages"]["sound"] = sound
            if sound.get("_status") == "completed":
                ev["completed_tracks"].append("sound")
            else:
                ev["failed_tracks"].append("sound")
                ev["uninspected_ranges"].append({"track": "sound", "reason": sound.get("_error")})
        else:
            for s in ("asr", "sound"):
                ev["stages"][s] = {"_stage": s, "_status": "skipped"}
                ev["skipped_tracks"].append(s)
                ev["uninspected_ranges"].append({"track": s, "reason": "no_audio"})
    # 5. fusion (deterministic only)
    import re
    ocr_frames = ev["stages"].get("ocr", {}).get("ocr_frames", []) if isinstance(ev["stages"].get("ocr"), dict) else []
    asr_segs = ev["stages"].get("asr", {}).get("segments", []) if isinstance(ev["stages"].get("asr"), dict) else []
    sound_rows = ev["stages"].get("sound", {}).get("rows", []) if isinstance(ev["stages"].get("sound"), dict) else []
    # Boundary fix (2026-10-31): check ALL overlapping windows, not just
    # the first one. The old first-window rule missed real cases: a
    # segment starting in a silence window, or a Whisper hallucination
    # over a later pure-music window, was never flagged because the
    # first overlapping window was speech-dominant. One conflict per
    # segment, listing every music-dominant overlapping window.
    for seg in asr_segs:
        suspect = [row for row in sound_rows
                   if row["start_s"] < seg["end_s"] and seg["start_s"] < row["end_s"]
                   and row["Music"] >= 0.50 and row["Speech"] < 0.20]
        if suspect:
            ev["conflicts"].append({"type": "music_background_asr_suspect",
                                    "asr": seg, "sound_windows": suspect,
                                    "note": "ASR text overlaps music-dominant window(s); do not treat as clean speech transcript."})
    for fr in ocr_frames:
        digits_ocr = set(re.findall(r"\d+", " ".join(l["text"] for l in fr["lines"])))
        for seg in asr_segs:
            if seg["start_s"] <= fr["t_s"] <= seg["end_s"]:
                digits_asr = set(re.findall(r"\d+", seg["text"]))
                if digits_ocr and digits_asr and digits_ocr != digits_asr:
                    ev["conflicts"].append({"type": "digit_conflict_ocr_asr",
                                            "frame": fr, "asr": seg,
                                            "note": "OCR digits and ASR digits differ in the same window; both kept, neither wins."})
    ev["fusion_notes"].append("Fusion keeps all tracks side by side; same-model repetition is not independent confirmation; "
                              "Speech score alone does not prove who spoke or what was said; Music score alone does not prove lyrics or pure instrumental.")
    if "ocr" not in ev["completed_tracks"]:
        ev["uninspected_ranges"].append({"track": "ocr", "reason": "not_completed"})
    # Narrative v1: deterministic timeline only, written next to evidence.
    ev["narrative_file"] = "narrative.md"
    _atomic_write_json(os.path.join(outdir, "evidence.json"), ev)
    _narr_tmp = os.path.join(outdir, "narrative.md.tmp")
    with open(_narr_tmp, "w", encoding="utf-8") as f:
        f.write(build_narrative(ev))
    os.replace(_narr_tmp, os.path.join(outdir, "narrative.md"))
    print(json.dumps({"completed_tracks": ev["completed_tracks"], "gate": ev["gate"],
                      "conflicts": len(ev["conflicts"]),
                      "uninspected": ev["uninspected_ranges"]}, ensure_ascii=False, indent=2))
    return ev


def _stale_clean_outdir(outdir):
    """Remove only files this analyzer owns, so a retry can never read
    a previous run's stage output, evidence, frames, or state as its
    own. The outdir layout itself is unchanged: evidence.json,
    frames/, and stage jsons stay where the Worker already reads them."""
    import glob
    for pat in ("stage_*.json", "evidence.json", "narrative.md", "state.json"):
        for p in glob.glob(os.path.join(outdir, pat)):
            try:
                os.remove(p)
            except OSError:
                pass
    frames_dir = os.path.join(outdir, "frames")
    if os.path.isdir(frames_dir):
        for p in glob.glob(os.path.join(frames_dir, "frame_*.jpg")):
            try:
                os.remove(p)
            except OSError:
                pass


def _write_run_state(outdir, run_id, video, state, completed, failed, skipped, reasons, started_ts):
    try:
        sha = sha256_file(video)
    except Exception:
        sha = None
    doc = {"schema_version": "m2-run-state-0.1", "run_id": run_id,
           "input_sha256": sha, "state": state,
           "completed_tracks": completed, "failed_tracks": failed,
           "skipped_tracks": skipped, "reason_codes": reasons,
           "started_at": started_ts, "finished_at": time.time()}
    try:
        _atomic_write_json(os.path.join(outdir, "state.json"), doc)
    except Exception:
        pass
    return doc


def main(video, outdir, run_id=None):
    """Run one analysis to a terminal state. Never leaves a run without
    state.json: completed (all attempted tracks done), partial (some
    done, some failed/skipped, evidence published), or failed (crash
    before evidence). Evidence is published before the state file, and
    readers must only accept evidence whose state is terminal and whose
    run_id matches (legacy runs without state.json keep the old
    evidence-existence behavior)."""
    run_id = run_id or "run-%d-%s" % (time.time_ns(), uuid.uuid4().hex[:8])
    started = time.time()
    os.makedirs(outdir, exist_ok=True)
    _stale_clean_outdir(outdir)
    try:
        ev = _main_inner(video, outdir, run_id)
    except Exception as e:
        _write_run_state(outdir, run_id, video, "failed", [], [],
                         ["crash:" + type(e).__name__], started)
        raise
    failed = ev.get("failed_tracks", [])
    skipped = ev.get("skipped_tracks", [])
    completed = ev.get("completed_tracks", [])
    # Gate-skipped or dependency-skipped tracks are unavailable work,
    # not completed work: a run with light tracks done but heavy
    # tracks skipped is partial, never completed.
    if not completed:
        state = "failed"
    elif failed or skipped:
        state = "partial"
    else:
        state = "completed"
    reasons = ["failed:" + t for t in failed] + ["skipped:" + t for t in skipped]
    ev["state"] = state
    _atomic_write_json(os.path.join(outdir, "evidence.json"), ev)
    _write_run_state(outdir, run_id, video, state, completed, failed, skipped, reasons, started)
    return ev


if __name__ == "__main__":
    _video = sys.argv[1]
    _outdir = sys.argv[2]
    _rid = None
    if len(sys.argv) >= 5 and sys.argv[3] == "--run-id":
        _rid = sys.argv[4]
    main(_video, _outdir, _rid)
