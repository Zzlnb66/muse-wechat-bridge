"""Empty-text-layer verdict + failure category for doc_parser.py.

A parse that "succeeds" with zero quotable characters must not be
reported completed: that was a false positive (real case 2026-11-01:
a vector-outlined career-report PDF reported completed with 4 empty
anchors). Rules under test:

  * all anchors non-whitespace total == 0 -> failed / empty_text_layer
    (partial emptiness never triggers);
  * PDF stdlib-empty gets one pypdf recheck first: recheck finds text
    -> completed via pypdf; recheck still empty -> empty_text_layer;
  * notes.empty_text_hint is best-effort (scanned_image_only /
    vector_outlined_text / unknown) and never affects the verdict;
  * failure_code values are unchanged; category is an additive
    grouping only (policy / capability / corrupt / empty / unknown).

Run with system python3. All fixtures are generated locally in tmp
dirs; no download, no live bridge state, nothing sent.
Exit non-zero on any failure.
"""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
import zlib

ANALYZER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "analyzer")
PARSER = os.path.join(ANALYZER, "doc_parser.py")
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


def run_parser(src, tmp, env_extra=None, block_pypdf=False):
    out = os.path.join(tmp, "out-%s.json" % os.path.basename(src))
    env = dict(os.environ)
    for k in ("DOC_PARSER_MAX_FILE_BYTES", "DOC_PARSER_MAX_PDF_PAGES",
              "DOC_PARSER_MAX_COMPRESSION_RATIO", "DOC_PARSER_MAX_TEXT_CHARS",
              "DOC_PARSER_TEST_MODE", "DOC_PARSER_TEST_SLEEP_SECONDS",
              "PYTHONPATH"):
        env.pop(k, None)
    if env_extra:
        env.update(env_extra)
    if block_pypdf:
        blockdir = os.path.join(tmp, "blockdir")
        os.makedirs(blockdir, exist_ok=True)
        with open(os.path.join(blockdir, "sitecustomize.py"), "w", encoding="utf-8") as f:
            f.write("import sys, importlib.abc\n"
                    "class _B(importlib.abc.MetaPathFinder):\n"
                    "    def find_spec(self, fullname, path=None, target=None):\n"
                    "        if fullname.split('.')[0] in ('pypdf', 'PyPDF2'):\n"
                    "            raise ImportError('blocked for test: ' + fullname)\n"
                    "        return None\n"
                    "sys.meta_path.insert(0, _B())\n")
        env["PYTHONPATH"] = blockdir
    cmd = [sys.executable, PARSER, src, out]
    proc = subprocess.run(cmd, capture_output=True, timeout=120, env=env)
    assert proc.returncode == 0, "parser launcher exit %s: %s" % (
        proc.returncode, proc.stderr.decode("utf-8", "ignore")[-300:])
    with open(out, encoding="utf-8") as f:
        return json.load(f)


def build_pdf(pages, extra_objects=b"", page_resources=None, trailer_extra=b""):
    """Minimal valid PDF. pages: content-stream byte strings; the
    caller supplies per-page resource dicts via page_resources (a list
    of bytes, one per page) or the default simple-font resources."""
    objs = []
    objs.append((1, b"<< /Type /Catalog /Pages 2 0 R >>"))
    kid_refs = b" ".join(b"%d 0 R" % (3 + 2 * i) for i in range(len(pages)))
    objs.append((2, b"<< /Type /Pages /Kids [" + kid_refs +
                 b"] /Count %d >>" % len(pages)))
    for i, stream in enumerate(pages):
        res = page_resources[i] if page_resources else (
            b"<< /Font << /F1 << /Type /Font /Subtype /Type1 "
            b"/BaseFont /Helvetica >> >> >>")
        objs.append((3 + 2 * i,
                     b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                     b"/Contents %d 0 R /Resources " % (4 + 2 * i) + res + b" >>"))
        comp = zlib.compress(stream)
        objs.append((4 + 2 * i,
                     b"<< /Length %d /Filter /FlateDecode >>\nstream\n" % len(comp)
                     + comp + b"\nendstream"))
    if extra_objects:
        objs.append((900, extra_objects))
    buf = bytearray(b"%PDF-1.4\n")
    offsets = {}
    for num, body in objs:
        offsets[num] = len(buf)
        buf += b"%d 0 obj\n" % num + body + b"\nendobj\n"
    xref_pos = len(buf)
    maxnum = max(n for n, _ in objs)
    buf += b"xref\n0 %d\n" % (maxnum + 1)
    buf += b"0000000000 65535 f \n"
    for n in range(1, maxnum + 1):
        buf += b"%010d 00000 n \n" % offsets.get(n, 0)
    buf += (b"trailer\n<< /Size %d /Root 1 0 R " % (maxnum + 1)
            + trailer_extra + b" >>\nstartxref\n%d\n%%%%EOF\n" % xref_pos)
    return bytes(buf)


def build_form_pdf():
    """One page whose only text lives inside a Form XObject: the
    stdlib subset never follows Do, so it finds zero characters,
    while pypdf extracts the form text."""
    form_stream = b"BT /F1 12 Tf 100 700 Td (Hello from form) Tj ET"
    form = (b"<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Length %d >>\nstream\n%s\nendstream"
            % (len(form_stream), form_stream))
    page_stream = b"q /Fm1 Do Q"
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 4 0 R >> /XObject << /Fm1 5 0 R >> >> "
        b"/Contents 6 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        form,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(page_stream), page_stream),
    ]
    buf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for i, body in enumerate(objs, 1):
        offsets.append(len(buf))
        buf += ("%d 0 obj\n" % i).encode() + body + b"\nendobj\n"
    xref_pos = len(buf)
    n = len(objs) + 1
    buf += ("xref\n0 %d\n" % n).encode()
    buf += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        buf += ("%010d 00000 n \n" % off).encode()
    buf += ("trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n"
            % (n, xref_pos)).encode()
    return bytes(buf)


def build_image_pdf():
    """One page drawing a 1x1 image XObject and no text at all."""
    img = b"<< /Type /XObject /Subtype /Image /Width 1 /Height 1 " \
          b"/ColorSpace /DeviceGray /BitsPerComponent 8 /Length 1 >>\nstream\n\x00\nendstream"
    res = (b"<< /XObject << /Im1 7 0 R >> >>")
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 6 0 R /Resources " + res + b" >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length 0 >>\nstream\n\nendstream",
        b"<< /Length 34 >>\nstream\nq 100 0 0 100 100 100 cm /Im1 Do Q\nendstream",
        img,
    ]
    buf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for i, body in enumerate(objs, 1):
        offsets.append(len(buf))
        buf += ("%d 0 obj\n" % i).encode() + body + b"\nendobj\n"
    xref_pos = len(buf)
    n = len(objs) + 1
    buf += ("xref\n0 %d\n" % n).encode()
    buf += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        buf += ("%010d 00000 n \n" % off).encode()
    buf += ("trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n"
            % (n, xref_pos)).encode()
    return bytes(buf)


def build_empty_pptx():
    """One slide with no text runs at all."""
    slide = (b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
             b'<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
             b'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
             b'<p:cSld><p:spTree></p:spTree></p:cSld></p:sld>')
    buf = __import__("io").BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ppt/slides/slide1.xml", slide)
    return buf.getvalue()


def content_tj(text):
    esc = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return b"BT /F1 24 Tf 100 700 Td (" + esc.encode("latin-1") + b") Tj ET"


PATH_ONLY = b"10 10 m 100 10 l 100 100 l 10 100 l h f"


def _load_module():
    spec = importlib.util.spec_from_file_location("doc_parser_under_test", PARSER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    tmp = tempfile.mkdtemp(prefix="docparser-empty-test-")
    try:
        # ---- vector-outlined PDF: stdlib 0 chars -> pypdf recheck
        # still 0 -> failed / empty_text_layer, hint vector
        src_vec = os.path.join(tmp, "vector.pdf")
        with open(src_vec, "wb") as f:
            f.write(build_pdf([PATH_ONLY]))
        out_vec = run_parser(src_vec, tmp)
        check("vector pdf empty_text_layer",
              out_vec["status"] == "failed"
              and out_vec["failure_code"] == "empty_text_layer",
              json.dumps(out_vec)[:300])
        check("vector pdf category empty",
              out_vec.get("category") == "empty", str(out_vec.get("category")))
        check("vector pdf backend is last tried (pypdf)",
              out_vec["notes"].get("pdf_backend") == "pypdf",
              str(out_vec["notes"]))
        check("vector pdf hint vector_outlined_text",
              out_vec["notes"].get("empty_text_hint") == "vector_outlined_text",
              str(out_vec["notes"]))

        # ---- vector PDF with pypdf blocked: last tried is stdlib
        out_blk = run_parser(src_vec, tmp, block_pypdf=True)
        check("blocked vector still empty_text_layer",
              out_blk["status"] == "failed"
              and out_blk["failure_code"] == "empty_text_layer",
              json.dumps(out_blk)[:300])
        check("blocked vector backend stdlib-subset",
              out_blk["notes"].get("pdf_backend") == "stdlib-subset",
              str(out_blk["notes"]))
        check("blocked vector hint unknown (no decoder)",
              out_blk["notes"].get("empty_text_hint") == "unknown",
              str(out_blk["notes"]))

        # ---- image-only PDF: hint scanned_image_only
        src_img = os.path.join(tmp, "scanned.pdf")
        with open(src_img, "wb") as f:
            f.write(build_image_pdf())
        out_img = run_parser(src_img, tmp)
        check("image pdf empty_text_layer",
              out_img["status"] == "failed"
              and out_img["failure_code"] == "empty_text_layer",
              json.dumps(out_img)[:300])
        check("image pdf hint scanned_image_only",
              out_img["notes"].get("empty_text_hint") == "scanned_image_only",
              str(out_img["notes"]))

        # ---- form-XObject PDF: stdlib 0 chars, pypdf recheck finds text
        src_form = os.path.join(tmp, "form.pdf")
        with open(src_form, "wb") as f:
            f.write(build_form_pdf())
        out_form = run_parser(src_form, tmp)
        check("form pdf rescued to completed",
              out_form["status"] == "completed", out_form.get("failure_reason"))
        check("form pdf backend pypdf",
              out_form["notes"].get("pdf_backend") == "pypdf",
              str(out_form["notes"]))
        check("form pdf text intact",
              any("Hello from form" in a["text"] for a in out_form["anchors"]),
              repr(out_form["anchors"]))
        check("form pdf no category on success",
              "category" not in out_form, str(out_form.get("category")))

        # ---- partial emptiness never triggers
        src_part = os.path.join(tmp, "partial.pdf")
        with open(src_part, "wb") as f:
            f.write(build_pdf([PATH_ONLY, content_tj("Hello")]))
        out_part = run_parser(src_part, tmp)
        check("partial pdf still completed",
              out_part["status"] == "completed", out_part.get("failure_reason"))
        check("partial pdf backend stdlib-subset",
              out_part["notes"].get("pdf_backend") == "stdlib-subset",
              str(out_part["notes"]))

        # ---- whitespace-only text counts as empty
        src_ws = os.path.join(tmp, "ws.pdf")
        with open(src_ws, "wb") as f:
            f.write(build_pdf([content_tj("   ")]))
        out_ws = run_parser(src_ws, tmp)
        check("whitespace-only pdf empty_text_layer",
              out_ws["status"] == "failed"
              and out_ws["failure_code"] == "empty_text_layer",
              json.dumps(out_ws)[:300])

        # ---- non-PDF empty document: PPTX with an empty slide
        src_pptx = os.path.join(tmp, "empty.pptx")
        with open(src_pptx, "wb") as f:
            f.write(build_empty_pptx())
        out_pptx = run_parser(src_pptx, tmp)
        check("empty pptx empty_text_layer",
              out_pptx["status"] == "failed"
              and out_pptx["failure_code"] == "empty_text_layer",
              json.dumps(out_pptx)[:300])
        check("empty pptx category empty",
              out_pptx.get("category") == "empty", str(out_pptx.get("category")))
        check("empty pptx hint unknown",
              out_pptx["notes"].get("empty_text_hint") == "unknown",
              str(out_pptx["notes"]))
        check("empty pptx has no pdf_backend key",
              "pdf_backend" not in out_pptx["notes"], str(out_pptx["notes"]))

        # ---- category mapping, full table (pure function)
        mod = _load_module()
        expected = {
            "file_too_large": "policy", "page_limit": "policy",
            "item_limit": "policy", "compression_ratio": "policy",
            "uncompressed_too_large": "policy", "text_limit": "policy",
            "timeout": "policy", "encrypted": "policy",
            "unsupported_encoding": "capability",
            "corrupt": "corrupt",
            "empty_text_layer": "empty",
            "input_error": "unknown", "missing_dependency": "unknown",
            "unsupported_format": "unknown", "worker_failed": "unknown",
            "made_up_code_xyz": "unknown",
        }
        bad = {c: mod._failure_category(c) for c, want in expected.items()
               if mod._failure_category(c) != want}
        check("category mapping full table", not bad, repr(bad))

        # ---- category appears in real failure outputs; codes unchanged
        out_big = run_parser(src_form, tmp, {"DOC_PARSER_MAX_FILE_BYTES": "10"})
        check("file_too_large category policy",
              out_big["failure_code"] == "file_too_large"
              and out_big.get("category") == "policy",
              json.dumps(out_big)[:300])
        src_enc = os.path.join(tmp, "enc.pdf")
        with open(src_enc, "wb") as f:
            f.write(build_pdf([content_tj("Hello")],
                              trailer_extra=b"/Encrypt 901 0 R"))
        out_enc = run_parser(src_enc, tmp)
        check("encrypted category policy",
              out_enc["failure_code"] == "encrypted"
              and out_enc.get("category") == "policy",
              json.dumps(out_enc)[:300])
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("doc_parser_empty_layer: %d passed, %d failed" % (PASS, FAIL))
    if FAILURES:
        print("failures: %s" % FAILURES)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
