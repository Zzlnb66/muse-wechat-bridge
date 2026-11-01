"""Gate 3 item 1: M2 model config tests.

Run with the M0 venv interpreter (it owns faster-whisper, ppocr_lite,
ai-edge-litert); the bridge/system python lacks those packages.
All fixtures are generated locally in tmp dirs; no model is
downloaded, no real bridge state is touched, nothing is sent.
"""
import json
import os
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "analyzer"))
import model_config  # noqa: E402

ANALYZER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "analyzer")
STAGES = os.path.join(ANALYZER, "stages.py")
M0_BASE = "/home/hatch/workspace/goals/muse-wechat-bridge/hidden_files/"
PASS = 0


def check(name, cond, detail=""):
    global PASS
    assert cond, "FAIL %s %s" % (name, detail)
    PASS += 1
    print("PASS %s" % name)


def run_stage(stage, args, tmp, env_extra=None):
    inp = os.path.join(tmp, "in_%s.json" % stage)
    outp = os.path.join(tmp, "out_%s.json" % stage)
    with open(inp, "w", encoding="utf-8") as f:
        json.dump(args, f)
    env = dict(os.environ)
    env.pop("M2_MODEL_CONFIG_PATH", None)
    env.pop("M2_YAMNET_MODEL_PATH", None)
    env.pop("M2_YAMNET_LABELS_PATH", None)
    env.pop("M2_PPOCR_CACHE_DIR", None)
    env.pop("M2_PPOCR_DET_PATH", None)
    env.pop("M2_PPOCR_REC_PATH", None)
    env.pop("M2_PPOCR_DICT_PATH", None)
    env.pop("M2_WHISPER_MODEL_PATH", None)
    env.pop("M2_WHISPER_REVISION", None)
    if env_extra:
        env.update(env_extra)
    r = subprocess.run([sys.executable, STAGES, stage, inp, outp],
                       capture_output=True, timeout=600, env=env)
    data = None
    if os.path.exists(outp):
        with open(outp, encoding="utf-8") as f:
            data = json.load(f)
    return r.returncode, data


def make_fixtures(tmp):
    video = os.path.join(tmp, "sample.mp4")
    subprocess.run(["ffmpeg", "-y", "-v", "error",
                    "-f", "lavfi", "-i", "color=c=blue:s=320x240:d=2",
                    "-f", "lavfi", "-i", "sine=frequency=440:d=2",
                    "-shortest", video], check=True, timeout=60)
    wav = os.path.join(tmp, "audio_16k.wav")
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-i", video, "-vn",
                    "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", wav],
                   check=True, timeout=60)
    from PIL import Image, ImageDraw, ImageFont
    img = os.path.join(tmp, "text.jpg")
    canvas = Image.new("RGB", (800, 300), "white")
    draw = ImageDraw.Draw(canvas)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 72)
    except Exception:
        font = ImageFont.load_default(size=72)
    draw.text((40, 100), "HELLO 123", fill="black", font=font)
    canvas.save(img, quality=95)
    return video, wav, img


def test_config_defaults():
    cfg = model_config.load_config()
    check("config schema", cfg.get("schema_version") == "m2-model-config-0.1")
    sec = model_config.track_config(cfg, "asr")
    check("whisper revision pinned",
          sec.get("revision") == "536b0662742c02347bc0e980a01041f333bce120"
          and sec.get("revision_pinned") is True)
    check("whisper bin hash shape", len(sec.get("model_bin_sha256", "")) == 64)
    mp, lp = model_config.yamnet_paths(cfg)
    check("yamnet default path", mp == M0_BASE + "m0-component/yamnet/yamnet.tflite", mp)
    check("yamnet labels default path", lp == M0_BASE + "m0-component/yamnet/yamnet_521_labels.txt", lp)
    det, rec, dic = model_config.ppocr_paths(cfg)
    check("ppocr default filenames",
          det.endswith("ppocrv5_det.onnx") and rec.endswith("ppocrv5_rec.onnx")
          and dic.endswith("dict.txt"), (det, rec, dic))
    sec = model_config.track_config(cfg, "sound")
    check("yamnet license not overstated",
          sec["model"].get("license", "").startswith("未核实"), sec["model"].get("license"))
    sec = model_config.track_config(cfg, "ocr")
    check("ppocr det license not overstated",
          sec["det"].get("license", "").startswith("未核实"), sec["det"].get("license"))


def test_env_override_paths(tmp):
    cfg = model_config.load_config()
    copy = os.path.join(tmp, "yamnet_copy.tflite")
    shutil.copyfile(M0_BASE + "m0-component/yamnet/yamnet.tflite", copy)
    old = os.environ.get("M2_YAMNET_MODEL_PATH")
    os.environ["M2_YAMNET_MODEL_PATH"] = copy
    try:
        mp, lp, _info = model_config.yamnet_verified(cfg)
        check("yamnet env override honored", mp == copy, mp)
    finally:
        if old is None:
            os.environ.pop("M2_YAMNET_MODEL_PATH", None)
        else:
            os.environ["M2_YAMNET_MODEL_PATH"] = old
    cache = os.path.join(tmp, "ppocr_cache")
    os.makedirs(cache)
    for name in ("ppocrv5_det.onnx", "ppocrv5_rec.onnx", "dict.txt"):
        shutil.copyfile(os.path.expanduser("~/.cache/ppocr_lite/" + name),
                        os.path.join(cache, name))
    old = os.environ.get("M2_PPOCR_CACHE_DIR")
    os.environ["M2_PPOCR_CACHE_DIR"] = cache
    try:
        det, rec, dic, _info = model_config.ppocr_verified(cfg)
        check("ppocr cache env override honored", det == os.path.join(cache, "ppocrv5_det.onnx"), det)
    finally:
        if old is None:
            os.environ.pop("M2_PPOCR_CACHE_DIR", None)
        else:
            os.environ["M2_PPOCR_CACHE_DIR"] = old


def test_hash_mismatch_fails_cleanly(tmp):
    bad = os.path.join(tmp, "yamnet_bad.tflite")
    shutil.copyfile(M0_BASE + "m0-component/yamnet/yamnet.tflite", bad)
    with open(bad, "r+b") as f:
        f.seek(100)
        f.write(b"\x00")
    _rc, wav = None, None
    _v, wav, _i = make_fixtures(tmp)
    rc, data = run_stage("sound", {"audio_16k_wav": wav}, tmp,
                         {"M2_YAMNET_MODEL_PATH": bad})
    check("sound hash mismatch -> failed, no fallback",
          rc != 0 and data and data.get("_status") == "failed"
          and "hash mismatch" in (data.get("_error_detail") or ""), str(data)[:300])
    bad_dict = os.path.join(tmp, "dict_bad.txt")
    shutil.copyfile(os.path.expanduser("~/.cache/ppocr_lite/dict.txt"), bad_dict)
    with open(bad_dict, "a", encoding="utf-8") as f:
        f.write("\ncorrupt-line\n")
    framedir = os.path.join(tmp, "frames")
    os.makedirs(framedir, exist_ok=True)
    shutil.copyfile(_i, os.path.join(framedir, "frame_000.jpg"))
    rc, data = run_stage("ocr", {"frames": [{"file": "frame_000.jpg", "t_s": 0.0}],
                                 "framedir": framedir}, tmp,
                         {"M2_PPOCR_DICT_PATH": bad_dict})
    check("ocr dict hash mismatch -> failed, no fallback",
          rc != 0 and data and data.get("_status") == "failed"
          and "hash mismatch" in (data.get("_error_detail") or ""), str(data)[:300])


def test_missing_key_and_missing_file(tmp):
    cfg = model_config.load_config()
    broken = json.loads(json.dumps(cfg))
    del broken["tracks"]["sound"]["model"]["sha256"]
    broken_path = os.path.join(tmp, "broken_config.json")
    with open(broken_path, "w", encoding="utf-8") as f:
        json.dump(broken, f)
    _v, wav, _i = make_fixtures(tmp)
    rc, data = run_stage("sound", {"audio_16k_wav": wav}, tmp,
                         {"M2_MODEL_CONFIG_PATH": broken_path})
    check("missing key -> structured failure, no crash",
          rc != 0 and data and data.get("_status") == "failed"
          and "missing required key" in (data.get("_error_detail") or ""), str(data)[:300])
    rc, data = run_stage("probe", {"video": _v}, tmp,
                         {"M2_MODEL_CONFIG_PATH": broken_path})
    check("light stage unaffected by broken heavy config",
          rc == 0 and data and data.get("_status") == "completed", str(data)[:200])
    rc, data = run_stage("asr", {"audio_16k_wav": wav}, tmp,
                         {"M2_MODEL_CONFIG_PATH": os.path.join(tmp, "no_such.json")})
    check("missing config file -> structured failure",
          rc != 0 and data and data.get("_status") == "failed"
          and "not found" in (data.get("_error_detail") or ""), str(data)[:300])


def test_small_sample_per_track(tmp):
    video, wav, img = make_fixtures(tmp)
    rc, data = run_stage("probe", {"video": video}, tmp)
    check("small probe completed",
          rc == 0 and data.get("_status") == "completed"
          and abs(float(data["probe"]["format"]["duration"]) - 2.0) < 0.5, str(data)[:200])
    framedir = os.path.join(tmp, "frames")
    rc, data = run_stage("frames", {"video": video, "outdir": framedir}, tmp)
    check("small frames completed",
          rc == 0 and data.get("_status") == "completed" and len(data.get("frames", [])) >= 1,
          str(data)[:200])
    shutil.copyfile(img, os.path.join(framedir, "frame_000.jpg"))
    rc, data = run_stage("ocr", {"frames": [{"file": "frame_000.jpg", "t_s": 0.0}],
                                 "framedir": framedir}, tmp)
    check("small ocr completed with shape",
          rc == 0 and data.get("_status") == "completed"
          and data.get("score_filter") == 0.80
          and isinstance(data.get("ocr_frames"), list)
          and data["_model_info"]["det_sha256"].startswith("1eb7b4f7"), str(data)[:300])
    print("INFO ocr lines:", json.dumps(data.get("ocr_frames"), ensure_ascii=False)[:300])
    rc, data = run_stage("asr", {"audio_16k_wav": wav}, tmp)
    check("small asr completed, revision recorded",
          rc == 0 and data.get("_status") == "completed"
          and isinstance(data.get("segments"), list)
          and data["_model_info"]["revision"] == "536b0662742c02347bc0e980a01041f333bce120"
          and data["_model_info"]["model_bin_sha256"].startswith("3e305921"), str(data)[:300])
    rc, data = run_stage("sound", {"audio_16k_wav": wav}, tmp)
    check("small sound completed with shape",
          rc == 0 and data.get("_status") == "completed"
          and data.get("windows", 0) >= 1
          and data["rows"][0].get("top1")
          and data["_model_info"]["model_sha256"].startswith("10c95ea3"), str(data)[:300])


def test_partial_semantics(tmp):
    import m2_analyzer
    video, _wav, _img = make_fixtures(tmp)
    bad = os.path.join(tmp, "yamnet_bad2.tflite")
    shutil.copyfile(M0_BASE + "m0-component/yamnet/yamnet.tflite", bad)
    with open(bad, "r+b") as f:
        f.seek(200)
        f.write(b"\x00")
    old_gate = m2_analyzer.gate
    m2_analyzer.gate = lambda: ("ALLOW", 2 * 1024 * 1024, 0)
    old_env = os.environ.get("M2_YAMNET_MODEL_PATH")
    os.environ["M2_YAMNET_MODEL_PATH"] = bad
    try:
        outdir = os.path.join(tmp, "run_partial")
        ev = m2_analyzer.main(video, outdir, "test-partial-1")
        state = json.load(open(os.path.join(outdir, "state.json"), encoding="utf-8"))
        check("failed track -> state partial, never completed",
              state.get("state") == "partial" and "sound" in state.get("failed_tracks", [])
              and os.path.exists(os.path.join(outdir, "evidence.json")), str(state)[:300])
        check("evidence published alongside partial state",
              ev.get("state") == "partial" and "sound" in ev.get("failed_tracks", []))
    finally:
        m2_analyzer.gate = old_gate
        if old_env is None:
            os.environ.pop("M2_YAMNET_MODEL_PATH", None)
        else:
            os.environ["M2_YAMNET_MODEL_PATH"] = old_env


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory(prefix="m2cfg_") as tmp:
        test_config_defaults()
        test_env_override_paths(tmp)
        test_hash_mismatch_fails_cleanly(tmp)
        test_missing_key_and_missing_file(tmp)
        test_small_sample_per_track(tmp)
        test_partial_semantics(tmp)
    print("m2_model_config: %d passed, 0 failed" % PASS)
