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
Usage: venv_python m2_analyzer.py <video> <outdir>
Output: <outdir>/evidence.json (+ frames/, stage jsons, audio_16k.wav)
"""
import hashlib
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# R2 P0 F2-lite: overridable via environment, default unchanged,
# so the production install keeps working with no config change.
VENV_PY = os.environ.get("WECHAT_BRIDGE_ANALYZER_PY", "/home/hatch/workspace/goals/muse-wechat-bridge/hidden_files/m0-component/venv/bin/python")


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


def run_stage(stage, args, outdir):
    inp = os.path.join(outdir, f"stage_{stage}_in.json")
    outp = os.path.join(outdir, f"stage_{stage}.json")
    json.dump(args, open(inp, "w", encoding="utf-8"), ensure_ascii=False)
    r = subprocess.run([VENV_PY, os.path.join(HERE, "stages.py"), stage, inp, outp],
                       capture_output=True, timeout=600)
    if os.path.exists(outp):
        return json.load(open(outp, encoding="utf-8"))
    return {"_stage": stage, "_status": "failed", "_error": "no_output", "_peak_rss_kib": None}


def main(video, outdir):
    os.makedirs(outdir, exist_ok=True)
    framedir = os.path.join(outdir, "frames")
    ev = {"schema_version": "m2-evidence-0.1",
          "video_sha256": sha256_file(video),
          "video_bytes": os.path.getsize(video),
          "stages": {}, "completed_tracks": [], "uninspected_ranges": [],
          "conflicts": [], "fusion_notes": []}
    # 1. probe (light)
    probe = run_stage("probe", {"video": video}, outdir)
    ev["stages"]["probe"] = probe
    if probe.get("_status") == "completed":
        ev["completed_tracks"].append("probe")
        fmt = probe.get("probe", {}).get("format", {})
        ev["duration_s"] = float(fmt.get("duration", 0) or 0)
    else:
        ev["uninspected_ranges"].append({"track": "probe", "reason": probe.get("_error")})
    # 2. frames (light)
    frames = run_stage("frames", {"video": video, "outdir": framedir}, outdir)
    ev["stages"]["frames"] = frames
    if frames.get("_status") == "completed":
        ev["completed_tracks"].append("frames")
    else:
        ev["uninspected_ranges"].append({"track": "frames", "reason": frames.get("_error")})
    # 3. audio extract (light, ffmpeg)
    wav = os.path.join(outdir, "audio_16k.wav")
    r = subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", video, "-vn", "-ac", "1",
                        "-ar", "16000", "-c:a", "pcm_s16le", wav], capture_output=True, timeout=120)
    ev["audio_16k_extracted"] = bool(r.returncode == 0 and os.path.exists(wav) and os.path.getsize(wav) > 0)
    if not ev["audio_16k_extracted"]:
        ev["uninspected_ranges"].append({"track": "audio", "reason": "ffmpeg 16k extract failed"})
    # 4. gate for heavy stages
    decision, mem, waits = gate()
    ev["gate"] = {"decision": decision, "mem_available_kib": mem, "waits": waits,
                  "note": "Gate is a branch decision only, not a safety memory budget (M0)."}
    heavy = ["ocr", "asr", "sound"]
    if decision == "REJECT":
        for s in heavy:
            ev["stages"][s] = {"_stage": s, "_status": "skipped_gate"}
            ev["uninspected_ranges"].append({"track": s, "reason": "gate_reject"})
    else:
        # ocr (needs frames)
        if frames.get("_status") == "completed":
            ocr = run_stage("ocr", {"frames": frames["frames"], "framedir": framedir}, outdir)
            ev["stages"]["ocr"] = ocr
            if ocr.get("_status") == "completed":
                ev["completed_tracks"].append("ocr")
            else:
                ev["uninspected_ranges"].append({"track": "ocr", "reason": ocr.get("_error")})
        # asr + sound (need audio)
        if ev["audio_16k_extracted"]:
            asr = run_stage("asr", {"audio_16k_wav": wav}, outdir)
            ev["stages"]["asr"] = asr
            if asr.get("_status") == "completed":
                ev["completed_tracks"].append("asr")
            else:
                ev["uninspected_ranges"].append({"track": "asr", "reason": asr.get("_error")})
            sound = run_stage("sound", {"audio_16k_wav": wav}, outdir)
            ev["stages"]["sound"] = sound
            if sound.get("_status") == "completed":
                ev["completed_tracks"].append("sound")
            else:
                ev["uninspected_ranges"].append({"track": "sound", "reason": sound.get("_error")})
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
    json.dump(ev, open(os.path.join(outdir, "evidence.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    with open(os.path.join(outdir, "narrative.md"), "w", encoding="utf-8") as f:
        f.write(build_narrative(ev))
    print(json.dumps({"completed_tracks": ev["completed_tracks"], "gate": ev["gate"],
                      "conflicts": len(ev["conflicts"]),
                      "uninspected": ev["uninspected_ranges"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
