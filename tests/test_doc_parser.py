"""Gate 3 item 2: office-document extraction pipeline (doc_parser.py).

Run with system python3 (it owns python-docx and openpyxl; the M0
venv lacks them). All fixtures are generated locally in tmp dirs;
no download, no live bridge state, nothing sent.

Exit non-zero on any failure.
"""
import hashlib
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


def run_parser(src, tmp, env_extra=None, timeout=None):
    out = os.path.join(tmp, "out.json")
    env = dict(os.environ)
    for k in ("DOC_PARSER_MAX_FILE_BYTES", "DOC_PARSER_MAX_PDF_PAGES",
              "DOC_PARSER_MAX_COMPRESSION_RATIO", "DOC_PARSER_MAX_TEXT_CHARS",
              "DOC_PARSER_TEST_MODE", "DOC_PARSER_TEST_SLEEP_SECONDS"):
        env.pop(k, None)
    if env_extra:
        env.update(env_extra)
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
    """pages: list of content-stream byte strings (already BT/ET blocks).
    Returns minimal valid PDF bytes with correct xref offsets."""
    objs = []  # (num, body bytes)
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


def main():
    tmp = tempfile.mkdtemp(prefix="docparser-test-")
    try:
        # ---- PDF: 2 pages, literal + hex UTF-16BE strings
        hex_text = "Hello hex".encode("utf-16-be")
        hexs = b"FEFF" + hex_text.hex().upper().encode("ascii")
        p1 = content_tj("Hello PDF page one")
        p2 = b"BT /F1 24 Tf 100 700 Td <" + hexs + b"> Tj ET"
        pdf = build_pdf([p1, p2])
        src = os.path.join(tmp, "two.pdf")
        with open(src, "wb") as f:
            f.write(pdf)
        out = run_parser(src, tmp)
        check("pdf status completed", out["status"] == "completed", out.get("failure_reason"))
        check("pdf schema", out["schema_version"] == "doc-evidence-0.1")
        check("pdf sha matches", out["source"]["sha256"] == hashlib.sha256(pdf).hexdigest())
        check("pdf format", out["source"]["format"] == "pdf")
        anchors = {a["anchor"].get("page_no"): a["text"] for a in out["anchors"]}
        check("pdf page1 anchor", anchors.get(1, "").strip() == "Hello PDF page one", repr(anchors))
        check("pdf page2 hex utf16", anchors.get(2, "").strip() == "Hello hex", repr(anchors))
        check("pdf scanned note", "OCR" in out["notes"].get("scanned_images", ""))
        check("pdf scratch removed", out["scratch_cleanup"] == "removed"
              and not os.path.exists(out["scratch_dir"] or ""))

        # ---- PDF: ToUnicode font -> pypdf rescue when available
        # Parent decision 2026-11-01: ToUnicode/CID no longer means clean
        # failure; the stdlib path fails and the pypdf rescue completes.
        # Without pypdf the old clean-failure expectation still holds.
        tou = build_pdf([p1], extra_objects=(
            b"<< /Type /Font /Subtype /Type0 /BaseFont /ABCDEE+Foo "
            b"/ToUnicode 901 0 R >>"))
        src2 = os.path.join(tmp, "tounicode.pdf")
        with open(src2, "wb") as f:
            f.write(tou)
        out2 = run_parser(src2, tmp)
        try:
            import pypdf  # noqa: F401
            _has_pypdf = True
        except ImportError:
            _has_pypdf = False
        if _has_pypdf:
            check("pdf tounicode rescue completes",
                  out2["status"] == "completed"
                  and out2["notes"].get("pdf_backend") == "pypdf"
                  and any("Hello PDF page one" in a["text"] for a in out2["anchors"]),
                  json.dumps(out2)[:200])
        else:
            check("pdf tounicode clean failure",
                  out2["status"] == "failed" and out2["failure_code"] == "unsupported_encoding",
                  json.dumps(out2)[:200])
            check("pdf tounicode no anchors claimed", out2["anchors"] == [])

        # ---- PDF: encrypted marker -> clean encrypted failure
        enc = build_pdf([p1], trailer_extra=b"/Encrypt 901 0 R")
        src3 = os.path.join(tmp, "enc.pdf")
        with open(src3, "wb") as f:
            f.write(enc)
        out3 = run_parser(src3, tmp)
        check("pdf encrypted clean failure",
              out3["status"] == "failed" and out3["failure_code"] == "encrypted")

        # ---- PDF: corrupt (header only, no objects)
        src4 = os.path.join(tmp, "bad.pdf")
        with open(src4, "wb") as f:
            f.write(b"%PDF-1.4\n%garbage, no objects here\n")
        out4 = run_parser(src4, tmp)
        check("pdf corrupt clean failure",
              out4["status"] == "failed" and out4["failure_code"] == "corrupt")

        # ---- PDF: JavaScript action string must never leak into output
        js_marker = "MARKER_JS_NEVER_EXTRACT_XYZ"
        js = build_pdf([p1], extra_objects=(
            b"<< /Type /Action /S /JavaScript /JS (" + js_marker.encode() + b") >>"))
        src5 = os.path.join(tmp, "js.pdf")
        with open(src5, "wb") as f:
            f.write(js)
        out5 = run_parser(src5, tmp)
        check("pdf js not executed/leaked",
              out5["status"] == "completed"
              and js_marker not in json.dumps(out5, ensure_ascii=False))

        # ---- PDF: page limit fuse
        src6 = os.path.join(tmp, "many.pdf")
        with open(src6, "wb") as f:
            f.write(build_pdf([p1, p2]))
        out6 = run_parser(src6, tmp, {"DOC_PARSER_MAX_PDF_PAGES": "1"})
        check("pdf page limit fuse",
              out6["status"] == "failed" and out6["failure_code"] == "page_limit")

        # ---- DOCX: paragraphs + table + injected macro part
        import docx
        dsrc = os.path.join(tmp, "sample.docx")
        doc = docx.Document()
        doc.add_paragraph("First paragraph text")
        doc.add_paragraph("Second paragraph text")
        table = doc.add_table(rows=2, cols=2)
        table.cell(0, 0).text = "Cell A1"
        table.cell(0, 1).text = "Cell B1"
        table.cell(1, 0).text = "Cell A2"
        table.cell(1, 1).text = "Cell B2"
        doc.save(dsrc)
        macro_marker = "MARKER_MACRO_NEVER_EXTRACT_XYZ"
        with zipfile.ZipFile(dsrc, "a") as zf:
            zf.writestr("vbaProject.bin", macro_marker.encode())
        out7 = run_parser(dsrc, tmp)
        check("docx status completed", out7["status"] == "completed", out7.get("failure_reason"))
        amap = {}
        for a in out7["anchors"]:
            amap[json.dumps(a["anchor"], sort_keys=True)] = a["text"]
        check("docx paragraph anchors",
              amap.get('{"paragraph_idx": 0}') == "First paragraph text"
              and amap.get('{"paragraph_idx": 1}') == "Second paragraph text", str(amap)[:300])
        check("docx table anchors",
              amap.get('{"cell_idx": 1, "row_idx": 0, "table_idx": 0}') == "Cell B1"
              and amap.get('{"cell_idx": 0, "row_idx": 1, "table_idx": 0}') == "Cell A2",
              str(amap)[:300])
        check("docx macro never extracted",
              macro_marker not in json.dumps(out7, ensure_ascii=False)
              and "macro_part" in out7["notes"])

        # ---- XLSX: sheets + cells, formula text never emitted
        import openpyxl
        xsrc = os.path.join(tmp, "sample.xlsx")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Data"
        ws["A1"] = "Name"
        ws["B2"] = 42
        ws["C3"] = "=1+1"  # no cached value -> skipped, formula text never emitted
        ws2 = wb.create_sheet("Second")
        ws2["D5"] = "hello cell"
        wb.save(xsrc)
        out8 = run_parser(xsrc, tmp)
        check("xlsx status completed", out8["status"] == "completed", out8.get("failure_reason"))
        xmap = {}
        for a in out8["anchors"]:
            xmap[(a["anchor"]["sheet_name"], a["anchor"]["cell_range"])] = a["text"]
        check("xlsx anchors",
              xmap.get(("Data", "A1")) == "Name"
              and xmap.get(("Data", "B2")) == "42"
              and xmap.get(("Second", "D5")) == "hello cell", str(xmap)[:300])
        check("xlsx formula text never emitted",
              "=1+1" not in [a["text"] for a in out8["anchors"]])

        # ---- PPTX: stdlib slide XML parsing
        slide_tpl = ('<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
                     'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
                     '<p:cSld><p:spTree><p:sp><p:txBody>%s</p:txBody></p:sp>'
                     '</p:spTree></p:cSld></p:sld>')
        para_tpl = '<a:p><a:r><a:t>%s</a:t></a:r></a:p>'
        psrc = os.path.join(tmp, "sample.pptx")
        with zipfile.ZipFile(psrc, "w") as zf:
            zf.writestr("[Content_Types].xml", "<Types/>")
            zf.writestr("ppt/slides/slide1.xml",
                        slide_tpl % (para_tpl % "Slide one title"))
            zf.writestr("ppt/slides/slide2.xml",
                        slide_tpl % ((para_tpl % "Second slide") + (para_tpl % "line two")))
        out9 = run_parser(psrc, tmp)
        check("pptx status completed", out9["status"] == "completed", out9.get("failure_reason"))
        pmap = {a["anchor"].get("slide_no"): a["text"] for a in out9["anchors"]}
        check("pptx slide anchors",
              pmap.get(1, "").strip() == "Slide one title"
              and "Second slide" in pmap.get(2, "") and "line two" in pmap.get(2, ""),
              repr(pmap))

        # ---- compression ratio fuse: tiny file, huge uncompressed part
        csrc = os.path.join(tmp, "bomb.docx")
        with zipfile.ZipFile(csrc, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("word/document.xml", "<w:document/>")
            zf.writestr("word/big.xml", "A" * 200000)
        out10 = run_parser(csrc, tmp)
        check("zip compression ratio fuse",
              out10["status"] == "failed" and out10["failure_code"] == "compression_ratio",
              json.dumps(out10)[:200])

        # ---- file size fuse
        out11 = run_parser(dsrc, tmp, {"DOC_PARSER_MAX_FILE_BYTES": "100"})
        check("file size fuse",
              out11["status"] == "failed" and out11["failure_code"] == "file_too_large")

        # ---- text limit fuse (failure, not silent truncation)
        long_doc = os.path.join(tmp, "long.docx")
        doc2 = docx.Document()
        doc2.add_paragraph("x" * 5000)
        doc2.save(long_doc)
        out12 = run_parser(long_doc, tmp, {"DOC_PARSER_MAX_TEXT_CHARS": "100"})
        check("text limit fuse",
              out12["status"] == "failed" and out12["failure_code"] == "text_limit")

        # ---- unknown format
        tsrc = os.path.join(tmp, "note.txt")
        with open(tsrc, "w", encoding="utf-8") as f:
            f.write("plain text, not a document\n")
        out13 = run_parser(tsrc, tmp)
        check("unknown format clean failure",
              out13["status"] == "failed" and out13["failure_code"] == "unsupported_format")

        # ---- timeout fuse (test-only worker sleep hook)
        out14 = run_parser(dsrc, tmp, {"DOC_PARSER_TEST_MODE": "1",
                                       "DOC_PARSER_TEST_SLEEP_SECONDS": "10"},
                           timeout=2)
        check("timeout fuse",
              out14["status"] == "failed" and out14["failure_code"] == "timeout",
              json.dumps(out14)[:200])
        check("timeout claims nothing complete",
              out14["anchors"] == [] and out14["status"] == "failed")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("doc_parser: %d passed, %d failed" % (PASS, FAIL))
    if FAILURES:
        print("failures: %s" % FAILURES)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
