"""Gate 3 item 2 follow-up: pypdf rescue backend for doc_parser.py.

Run with system python3 (it owns pypdf --user, python-docx, openpyxl).
All fixtures are generated locally in tmp dirs; no download, no live
bridge state, nothing sent. The two real production PDFs are NOT used
here (they live under the running bridge); they were verified manually
against /tmp copies and the results are in the delivery report.

Design under test (see doc_parser.py docstring):
  subset first (exact old behavior), pypdf rescue only on
  unsupported_encoding / corrupt; policy failures and encryption are
  never retried; pypdf missing -> silent stdlib-only fallback.

Exit non-zero on any failure.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unicodedata
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


def run_parser(src, tmp, env_extra=None, timeout=None, block_pypdf=False):
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
    if timeout is not None:
        cmd += ["--timeout", str(timeout)]
    proc = subprocess.run(cmd, capture_output=True, timeout=120, env=env)
    assert proc.returncode == 0, "parser launcher exit %s: %s" % (
        proc.returncode, proc.stderr.decode("utf-8", "ignore")[-300:])
    with open(out, encoding="utf-8") as f:
        return json.load(f)


def pdf_escape(text):
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf(pages, extra_objects=b"", trailer_extra=b""):
    objs = []
    objs.append((1, b"<< /Type /Catalog /Pages 2 0 R >>"))
    kid_refs = b" ".join(b"%d 0 R" % (3 + 2 * i) for i in range(len(pages)))
    objs.append((2, b"<< /Type /Pages /Kids [" + kid_refs +
                 b"] /Count %d >>" % len(pages)))
    for i, stream in enumerate(pages):
        objs.append((3 + 2 * i,
                     b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                     b"/Contents %d 0 R /Resources << /Font << /F1 "
                     b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> "
                     b">> >> >>" % (4 + 2 * i)))
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


def content_tj(text):
    return b"BT /F1 24 Tf 100 700 Td (" + pdf_escape(text).encode("latin-1") + b") Tj ET"


TOUNICODE_OBJ = (b"<< /Type /Font /Subtype /Type0 /BaseFont /ABCDEE+Foo "
                 b"/ToUnicode 901 0 R >>")


def main():
    tmp = tempfile.mkdtemp(prefix="docparser-pypdf-test-")
    try:
        p1 = content_tj("Hello PDF page one")

        # ---- rescue: ToUnicode-marked PDF completes via pypdf
        src = os.path.join(tmp, "rescue.pdf")
        with open(src, "wb") as f:
            f.write(build_pdf([p1], extra_objects=TOUNICODE_OBJ))
        out = run_parser(src, tmp)
        check("rescue tounicode completed",
              out["status"] == "completed", out.get("failure_reason"))
        check("rescue backend is pypdf",
              out["notes"].get("pdf_backend") == "pypdf", str(out["notes"]))
        amap = {a["anchor"].get("page_no"): a["text"] for a in out["anchors"]}
        check("rescue page text intact",
              amap.get(1, "").strip() == "Hello PDF page one", repr(amap))

        # ---- rescue: JS action string still never leaks (via pypdf path)
        js_marker = "MARKER_JS_NEVER_EXTRACT_XYZ"
        src_js = os.path.join(tmp, "rescue-js.pdf")
        with open(src_js, "wb") as f:
            f.write(build_pdf([p1], extra_objects=(
                TOUNICODE_OBJ + b" << /Type /Action /S /JavaScript /JS ("
                + js_marker.encode() + b") >>")))
        out_js = run_parser(src_js, tmp)
        check("rescue js not leaked",
              out_js["status"] == "completed"
              and js_marker not in json.dumps(out_js, ensure_ascii=False))

        # ---- subset-first: plain 2-page PDF keeps exact old behavior
        # (incl. UTF-16BE hex page, which pypdf decodes worse)
        hex_text = "Hello hex".encode("utf-16-be")
        hexs = b"FEFF" + hex_text.hex().upper().encode("ascii")
        p2 = b"BT /F1 24 Tf 100 700 Td <" + hexs + b"> Tj ET"
        src_plain = os.path.join(tmp, "plain.pdf")
        with open(src_plain, "wb") as f:
            f.write(build_pdf([p1, p2]))
        out_plain = run_parser(src_plain, tmp)
        pmap = {a["anchor"].get("page_no"): a["text"] for a in out_plain["anchors"]}
        check("subset-first plain completed",
              out_plain["status"] == "completed", out_plain.get("failure_reason"))
        check("subset-first backend is stdlib-subset",
              out_plain["notes"].get("pdf_backend") == "stdlib-subset",
              str(out_plain["notes"]))
        check("subset-first hex page exact",
              pmap.get(1, "").strip() == "Hello PDF page one"
              and pmap.get(2, "").strip() == "Hello hex", repr(pmap))

        # ---- encrypted (real, via pypdf writer): clean encrypted failure,
        # rescue is never attempted on encryption
        from pypdf import PdfWriter
        enc_src = os.path.join(tmp, "real-enc.pdf")
        w = PdfWriter()
        w.add_blank_page(612, 792)
        w.encrypt("secret-user-password")
        with open(enc_src, "wb") as f:
            w.write(f)
        out_enc = run_parser(enc_src, tmp)
        check("real encrypted clean failure",
              out_enc["status"] == "failed" and out_enc["failure_code"] == "encrypted",
              json.dumps(out_enc)[:200])
        check("encrypted backend recorded",
              out_enc["notes"].get("pdf_backend") == "stdlib-subset",
              str(out_enc["notes"]))

        # ---- corrupt: subset failure kept (pypdf also cannot parse)
        src_bad = os.path.join(tmp, "bad.pdf")
        with open(src_bad, "wb") as f:
            f.write(b"%PDF-1.4\n%garbage, no objects here\n")
        out_bad = run_parser(src_bad, tmp)
        check("corrupt clean failure kept",
              out_bad["status"] == "failed" and out_bad["failure_code"] == "corrupt",
              json.dumps(out_bad)[:200])

        # ---- policy fuses on the rescue path
        src_two = os.path.join(tmp, "rescue-two.pdf")
        with open(src_two, "wb") as f:
            f.write(build_pdf([p1, p1], extra_objects=TOUNICODE_OBJ))
        out_lim = run_parser(src_two, tmp, {"DOC_PARSER_MAX_PDF_PAGES": "1"})
        check("rescue page limit fuse",
              out_lim["status"] == "failed" and out_lim["failure_code"] == "page_limit",
              json.dumps(out_lim)[:200])
        out_txt = run_parser(src, tmp, {"DOC_PARSER_MAX_TEXT_CHARS": "5"})
        check("rescue text limit fuse",
              out_txt["status"] == "failed" and out_txt["failure_code"] == "text_limit",
              json.dumps(out_txt)[:200])

        # ---- fallback: pypdf import blocked -> old semantics preserved
        out_fb = run_parser(src, tmp, block_pypdf=True)
        check("fallback tounicode still fails clean",
              out_fb["status"] == "failed"
              and out_fb["failure_code"] == "unsupported_encoding",
              json.dumps(out_fb)[:200])
        check("fallback backend is stdlib-subset",
              out_fb["notes"].get("pdf_backend") == "stdlib-subset",
              str(out_fb["notes"]))
        out_fb2 = run_parser(src_plain, tmp, block_pypdf=True)
        fmap = {a["anchor"].get("page_no"): a["text"] for a in out_fb2["anchors"]}
        check("fallback plain pdf still completes",
              out_fb2["status"] == "completed"
              and fmap.get(2, "").strip() == "Hello hex", repr(fmap))

        # ---- NFKC note: a ToUnicode-mapped variant glyph is a property
        # of the source font, not a parser defect; record the mechanism
        # (real case 2026-11-01: the OS midterm PDF maps U+81F3 to the
        # Kangxi radical form U+2F84, which NFKC maps back to U+81F3).
        check("nfkc variant mechanism",
              unicodedata.normalize("NFKC", "\u2f84") == "\u81f3")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("doc_parser_pypdf: %d passed, %d failed" % (PASS, FAIL))
    if FAILURES:
        print("failures: %s" % FAILURES)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
