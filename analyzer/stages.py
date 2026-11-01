"""M2 stage runners, one stage per process.
Usage: python stages.py <stage> <input_json> <output_json>
(Run under the analyzer's own interpreter; see m2_analyzer.py item 1.)
Each runner prints its own peak RSS (ru_maxrss) into the output.
Stages: probe, frames, ocr, asr, sound.
- probe/frames use ffmpeg/ffprobe (light, no model).
- ocr uses ppocr-lite 0.4.0, score >= 0.80 filter (M0 frozen rule).
- asr uses faster-whisper small INT8 CPU, vad_filter=True, language auto-detect.
  Language is recorded, never assumed; empty segments = no speech detected.
- sound uses YAMNet (TF Hub official yamnet/classification tflite/1,
  SHA-256 10c95ea3eb9a7bb4cb8bddf6feb023250381008177ac162ce169694d05c317de,
  deployed 2026-10-31 replacing the former tflite-support test-data model;
  trial on 41 windows gave identical sound output to the old model).
  Input is 16 kHz mono wav, non-overlapping 0.975 s windows, tail discarded.
- Gate 3 item 1: model paths/versions come from model_config.json
  (see model_config.py). Every hashed file is verified before load;
  a missing file or hash mismatch fails that track cleanly, never
  silently substitutes another model. Each heavy stage records its
  resolved model info in _model_info for evidence.
"""
import json
import os
import resource
import subprocess
import sys

try:
    import model_config
except ImportError:  # loaded as a module from elsewhere: find sibling file
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import model_config

# R2 P0 F2-lite: overridable via environment, default unchanged.
# Used with plain string concatenation below, so normalise the
# trailing slash for an env-supplied value.
BASE = os.environ.get("WECHAT_BRIDGE_ANALYZER_BASE") or "/home/hatch/workspace/goals/muse-wechat-bridge/hidden_files/"
if not BASE.endswith("/"):
    BASE += "/"


def peak_kib():
    try:
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None


def _tool_version(cmd):
    """Best-effort first line of `<tool> -version`; never fails a stage."""
    try:
        r = subprocess.run([cmd, "-version"], capture_output=True, timeout=10)
        out = (r.stdout or b"").decode("utf-8", "ignore").strip().splitlines()
        return out[0][:120] if out else "unknown"
    except Exception:
        return "unknown"


def _pkg_version(name):
    try:
        import importlib.metadata as m
        return m.version(name)
    except Exception:
        return None


def run_probe(args):
    video = args["video"]
    r = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                        "format=duration,format_name,size:stream=codec_type,codec_name,width,height",
                        "-of", "json", video], capture_output=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError("ffprobe failed")
    return {"probe": json.loads(r.stdout.decode("utf-8", "ignore") or "{}"),
            "_model_info": {"tools": [{"name": "ffprobe", "version": _tool_version("ffprobe")}]}}


def run_frames(args):
    video, outdir = args["video"], args["outdir"]
    os.makedirs(outdir, exist_ok=True)
    # Fixed-interval frames, 1 per 2 s (practical M2 v1; scene detection added below).
    r = subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", video, "-vf", "fps=1/2",
                        "-q:v", "2", os.path.join(outdir, "frame_%03d.jpg")],
                       capture_output=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError("ffmpeg frame extraction failed")
    frames = sorted(f for f in os.listdir(outdir) if f.endswith(".jpg"))
    scenes = []
    try:
        from scenedetect import detect, ContentDetector
        cuts = detect(video, ContentDetector())
        scenes = [{"start_s": round(float(s.get_seconds()), 3),
                   "end_s": round(float(e.get_seconds()), 3)} for s, e in cuts]
    except Exception as e:
        scenes = [{"error": type(e).__name__, "note": "scene detection failed, fixed frames still used"}]
    return {"frames": [{"file": f, "t_s": round((i) * 2.0, 3)} for i, f in enumerate(frames)],
            "frame_interval_s": 2.0, "scenes": scenes,
            "_model_info": {"tools": [{"name": "ffmpeg", "version": _tool_version("ffmpeg")},
                                     {"name": "scenedetect", "package_version": _pkg_version("scenedetect")}]}}


def run_ocr(args):
    from ppocr_lite import PPOCRLite
    from ppocr_lite.models import ModelConfig as PPModelConfig
    cfg = model_config.load_config()
    det, rec, dic, info = model_config.ppocr_verified(cfg)
    # Explicit paths: PPOCRLite must use exactly these verified files,
    # never auto-download floating replacements. cls_model=False matches
    # current production behaviour (direction classifier not loaded).
    # ppocr_lite expects Path objects, not strings (its decoder calls
    # .read_bytes() on the dict path).
    from pathlib import Path
    engine = PPOCRLite(PPModelConfig(det_model=Path(det), rec_model=Path(rec),
                                     dict_path=Path(dic), cls_model=False))
    score_filter = info.get("score_filter") or 0.80
    out = []
    for fr in args["frames"]:
        path = os.path.join(args["framedir"], fr["file"])
        # ppocr-lite 0.4.0 API (as used in M0): engine.run(path) -> items with .text/.score
        result = engine.run(path)
        lines = []
        if result:
            for item in result:
                try:
                    text, score = item.text, float(item.score)
                except Exception:
                    continue
                if score >= score_filter:
                    lines.append({"text": text, "score": round(score, 4)})
        out.append({"frame": fr["file"], "t_s": fr["t_s"], "lines": lines})
    return {"ocr_frames": out, "score_filter": score_filter, "_model_info": info}


def run_asr(args):
    from faster_whisper import WhisperModel
    cfg = model_config.load_config()
    sec = model_config.track_config(cfg, "asr")
    # Pinned revision, local cache only: a missing snapshot fails here
    # instead of silently downloading another revision (Gate 3 item 1
    # closes the old "model name only, no revision" gap on purpose).
    model_arg, revision, snapshot_dir, bin_sha = model_config.whisper_resolved(cfg)
    vad_asset = model_config.whisper_vad_verified(cfg)
    model = WhisperModel(model_arg, device=sec.get("device") or "cpu",
                         compute_type=sec.get("compute_type") or "int8",
                         revision=revision, local_files_only=True)
    segments, info = model.transcribe(args["audio_16k_wav"], vad_filter=True, language=None)
    segs = [{"start_s": round(float(s.start), 3), "end_s": round(float(s.end), 3),
             "text": s.text.strip()} for s in segments if s.text and s.text.strip()]
    return {"language": getattr(info, "language", None),
            "language_probability": round(float(getattr(info, "language_probability", 0) or 0), 4),
            "segments": segs,
            "_model_info": {"engine": sec.get("engine"), "engine_version": sec.get("engine_version"),
                            "model": sec.get("model"), "repo": sec.get("repo"),
                            "revision": revision, "revision_pinned": sec.get("revision_pinned"),
                            "snapshot_dir": snapshot_dir, "model_bin_sha256": bin_sha,
                            "vad_asset": vad_asset,
                            "vad_asset_sha256": (sec.get("vad") or {}).get("sha256"),
                            "weight_license": sec.get("weight_license")}}


def run_sound(args):
    import wave
    import numpy as np
    from ai_edge_litert.interpreter import Interpreter
    cfg = model_config.load_config()
    model_path, labels_path, minfo = model_config.yamnet_verified(cfg)
    interp = Interpreter(model_path=model_path)
    interp.allocate_tensors()
    inp = interp.get_input_details()[0]["index"]
    outp = interp.get_output_details()[0]["index"]
    labels = open(labels_path).read().splitlines()
    idx = {n: labels.index(n) for n in ["Silence", "Speech", "Music"]}
    with wave.open(args["audio_16k_wav"]) as w:
        data = np.frombuffer(w.readframes(w.getnframes()), dtype=np.int16).astype(np.float32) / 32768.0
    nwin = len(data) // 15600
    rows = []
    for i in range(nwin):
        interp.set_tensor(inp, data[i * 15600:(i + 1) * 15600])
        interp.invoke()
        s = interp.get_tensor(outp)[0].astype(float)
        top = np.argsort(s)[::-1][:3]
        rows.append({"window": i, "start_s": round(i * 0.975, 3), "end_s": round((i + 1) * 0.975, 3),
                     "Silence": round(float(s[idx["Silence"]]), 4),
                     "Speech": round(float(s[idx["Speech"]]), 4),
                     "Music": round(float(s[idx["Music"]]), 4),
                     "top1": labels[int(top[0])], "top1_score": round(float(s[top[0]]), 4),
                     "top3": [(labels[int(t)], round(float(s[t]), 4)) for t in top]})
    return {"windows": nwin, "tail_samples_discarded": int(len(data) - nwin * 15600), "rows": rows,
            "_model_info": minfo}


STAGES = {"probe": run_probe, "frames": run_frames, "ocr": run_ocr, "asr": run_asr, "sound": run_sound}

if __name__ == "__main__":
    stage, inp_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    args = json.load(open(inp_path, encoding="utf-8"))
    try:
        result = STAGES[stage](args)
        result["_stage"] = stage
        result["_peak_rss_kib"] = peak_kib()
        result["_status"] = "completed"
    except Exception as e:
        result = {"_stage": stage, "_peak_rss_kib": peak_kib(), "_status": "failed",
                  "_error": type(e).__name__, "_error_detail": str(e)[:300]}
    if args.get("_run_id"):
        result["_run_id"] = args["_run_id"]
    # Atomic write (item 1): a killed stage must never leave a
    # half-written output that the next read mistakes for a result.
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, out_path)
    sys.exit(0 if result["_status"] == "completed" else 1)
