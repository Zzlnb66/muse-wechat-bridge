"""M2 model configuration loader (Gate 3 item 1).

Reads analyzer/model_config.json (or the file named by
M2_MODEL_CONFIG_PATH), resolves paths with env overrides, and verifies
sha256 hashes where the config pins one. A missing file, a hash
mismatch, or a missing required key raises ModelConfigError with a
clear message; callers turn that into a clean failed track, never a
silent substitution and never a fake success.

Path placeholders: {BASE} is WECHAT_BRIDGE_ANALYZER_BASE (or the
production default); a leading ~ is expanded to the user's home.
Env overrides change the path only, never the pinned hash: an
override pointing at different bytes fails the hash check on purpose.
"""
import hashlib
import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(_HERE, "model_config.json")
DEFAULT_BASE = "/home/hatch/workspace/goals/muse-wechat-bridge/hidden_files/"


class ModelConfigError(Exception):
    """A model config problem: missing file, bad JSON, missing key,
    missing model file, or hash mismatch. Always fatal for the track
    that needs it, never a reason to silently pick another model."""


def base_dir():
    base = os.environ.get("WECHAT_BRIDGE_ANALYZER_BASE") or DEFAULT_BASE
    if not base.endswith("/"):
        base += "/"
    return base


def config_path():
    return os.environ.get("M2_MODEL_CONFIG_PATH") or DEFAULT_CONFIG_PATH


def load_config(path=None):
    """Load and structurally validate the config file."""
    path = path or config_path()
    try:
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        raise ModelConfigError("model config file not found: %s" % path)
    except json.JSONDecodeError as e:
        raise ModelConfigError("model config file is not valid JSON (%s): %s" % (path, e))
    if not isinstance(cfg, dict):
        raise ModelConfigError("model config must be a JSON object: %s" % path)
    if not cfg.get("schema_version"):
        raise ModelConfigError("model config missing schema_version: %s" % path)
    tracks = cfg.get("tracks")
    if not isinstance(tracks, dict) or not tracks:
        raise ModelConfigError("model config missing tracks section: %s" % path)
    return cfg


def _require(section, key, where):
    if not isinstance(section, dict) or key not in section or section[key] in (None, ""):
        raise ModelConfigError("model config %s missing required key %r" % (where, key))
    return section[key]


def resolve_path(raw):
    """Expand {BASE} and ~ in a configured path."""
    if not raw:
        raise ModelConfigError("model config contains an empty path")
    return os.path.expanduser(raw.replace("{BASE}", base_dir()))


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_file(path, expected_sha256, what):
    """Hash-check one file. Missing file or mismatch raises
    ModelConfigError; there is no fallback to another file."""
    if not os.path.exists(path):
        raise ModelConfigError("%s file not found: %s" % (what, path))
    if expected_sha256:
        actual = sha256_file(path)
        if actual != expected_sha256:
            raise ModelConfigError(
                "%s hash mismatch: %s (expected %s, got %s); refusing to load a different model"
                % (what, path, expected_sha256, actual))
    return path


def track_config(cfg, name):
    tracks = cfg.get("tracks") or {}
    sec = tracks.get(name)
    if not isinstance(sec, dict):
        raise ModelConfigError("model config missing track section %r" % name)
    return sec


def yamnet_paths(cfg):
    """Resolved (model_path, labels_path) with env overrides applied."""
    sec = track_config(cfg, "sound")
    model = _require(sec.get("model"), "path", "sound.model")
    labels = _require(sec.get("labels"), "path", "sound.labels")
    model_path = os.environ.get("M2_YAMNET_MODEL_PATH") or resolve_path(model)
    labels_path = os.environ.get("M2_YAMNET_LABELS_PATH") or resolve_path(labels)
    return model_path, labels_path


def yamnet_verified(cfg):
    """Hash-verified YAMNet paths plus the info block for evidence."""
    sec = track_config(cfg, "sound")
    model_path, labels_path = yamnet_paths(cfg)
    verify_file(model_path, _require(sec.get("model"), "sha256", "sound.model"), "YAMNet model")
    verify_file(labels_path, _require(sec.get("labels"), "sha256", "sound.labels"), "YAMNet labels")
    return model_path, labels_path, {
        "engine": sec.get("engine"), "engine_version": sec.get("engine_version"),
        "model_path": model_path, "model_sha256": sec["model"]["sha256"],
        "labels_path": labels_path, "labels_sha256": sec["labels"]["sha256"],
        "model_source": sec["model"].get("source"),
        "model_license": sec["model"].get("license"),
    }


def ppocr_paths(cfg):
    """Resolved (det, rec, dict) paths with env overrides applied."""
    sec = track_config(cfg, "ocr")
    cache = os.environ.get("M2_PPOCR_CACHE_DIR")
    det = os.environ.get("M2_PPOCR_DET_PATH")
    rec = os.environ.get("M2_PPOCR_REC_PATH")
    dic = os.environ.get("M2_PPOCR_DICT_PATH")
    if cache:
        cache = os.path.expanduser(cache)
        det = det or os.path.join(cache, "ppocrv5_det.onnx")
        rec = rec or os.path.join(cache, "ppocrv5_rec.onnx")
        dic = dic or os.path.join(cache, "dict.txt")
    return (
        det or resolve_path(_require(sec.get("det"), "path", "ocr.det")),
        rec or resolve_path(_require(sec.get("rec"), "path", "ocr.rec")),
        dic or resolve_path(_require(sec.get("dict"), "path", "ocr.dict")),
    )


def ppocr_verified(cfg):
    """Hash-verified PP-OCR paths plus the info block for evidence."""
    sec = track_config(cfg, "ocr")
    det, rec, dic = ppocr_paths(cfg)
    verify_file(det, _require(sec.get("det"), "sha256", "ocr.det"), "PP-OCR det model")
    verify_file(rec, _require(sec.get("rec"), "sha256", "ocr.rec"), "PP-OCR rec model")
    verify_file(dic, _require(sec.get("dict"), "sha256", "ocr.dict"), "PP-OCR dict")
    return det, rec, dic, {
        "engine": sec.get("engine"), "engine_version": sec.get("engine_version"),
        "det_path": det, "det_sha256": sec["det"]["sha256"],
        "rec_path": rec, "rec_sha256": sec["rec"]["sha256"],
        "dict_path": dic, "dict_sha256": sec["dict"]["sha256"],
        "score_filter": sec.get("score_filter"),
        "direction_classifier": sec.get("direction_classifier"),
    }


def whisper_resolved(cfg):
    """Resolve the Whisper snapshot WITHOUT downloading.

    Returns (model_size_or_path, revision, snapshot_dir_or_None,
    model_bin_sha256_or_None). When M2_WHISPER_MODEL_PATH is set, the
    caller loads that local directory directly; otherwise the pinned
    revision is resolved from the local HF cache only (local_files_only
    in both the resolution and the model construction), so a missing
    snapshot fails here instead of silently downloading another one.
    """
    sec = track_config(cfg, "asr")
    revision = os.environ.get("M2_WHISPER_REVISION") or _require(sec, "revision", "asr")
    model = _require(sec, "model", "asr")
    expected_bin = _require(sec, "model_bin_sha256", "asr")
    local_dir = os.environ.get("M2_WHISPER_MODEL_PATH")
    if local_dir:
        local_dir = os.path.expanduser(local_dir)
        bin_path = os.path.join(local_dir, "model.bin")
        verify_file(bin_path, expected_bin, "Whisper model.bin")
        return local_dir, revision, local_dir, expected_bin
    # NOTE: do NOT use snapshot_download(local_files_only=True) here: it
    # demands the COMPLETE snapshot, while the local cache only holds
    # the subset faster-whisper actually downloaded (hence
    # IncompleteSnapshotError on a perfectly usable cache). Resolve
    # just model.bin from cache instead, exactly the file we hash.
    try:
        from huggingface_hub import try_to_load_from_cache
    except Exception as e:
        raise ModelConfigError("huggingface_hub unavailable, cannot resolve pinned Whisper snapshot: %s" % type(e).__name__)
    bin_path = try_to_load_from_cache(repo_id=_require(sec, "repo", "asr"),
                                      filename="model.bin", revision=revision)
    if not bin_path:
        raise ModelConfigError(
            "pinned Whisper model.bin not available locally (repo %s, revision %s); "
            "refusing to download a different revision" % (sec.get("repo"), revision))
    verify_file(bin_path, expected_bin, "Whisper model.bin")
    return model, revision, os.path.dirname(bin_path), expected_bin


def whisper_vad_verified(cfg):
    """Hash-check the Silero VAD asset bundled in the faster-whisper
    wheel. This is a wheel-asset correspondence check, NOT a claim
    that Silero officially distributes this ONNX file."""
    sec = track_config(cfg, "asr")
    vad = sec.get("vad") or {}
    expected = vad.get("sha256")
    try:
        import faster_whisper
        asset = os.path.join(os.path.dirname(os.path.abspath(faster_whisper.__file__)),
                             "assets", "silero_vad_v6.onnx")
    except Exception as e:
        raise ModelConfigError("cannot locate faster-whisper package for VAD asset check: %s" % type(e).__name__)
    verify_file(asset, expected, "Silero VAD wheel asset")
    return asset
