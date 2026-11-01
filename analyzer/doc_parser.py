"""Gate 3 item 2: office-document text extraction pipeline, orthogonal to M2.

Usage:
    python3 doc_parser.py <input_file> <output_json> [--timeout SECONDS]
    python3 doc_parser.py parse <input_file> <output_json>   # worker (internal)

The launcher (default mode) spawns the worker in a subprocess with a
timeout fuse, exactly like stages.py stage runners: the worker prints its
own peak RSS (ru_maxrss) into the output. A crashing or hanging worker
cannot kill the caller, and a timeout is a clean failure, never a
partial result claimed complete.

Output schema: doc-evidence-0.1, written to <output_json>:
    source: {file_name, sha256, size_bytes, format}
    status: completed | failed
    failure_code / failure_reason: only when failed
    category (failed only, additive): policy | capability | corrupt |
      empty | unknown -- a grouping of failure_code, never a rename
    empty_text_layer: when a parse succeeds but every anchor has zero
      non-whitespace characters, the result is failed (never completed);
      PDF gets one pypdf recheck first when the stdlib pass produced
      the empty result; notes.empty_text_hint is best-effort
      (scanned_image_only | vector_outlined_text | unknown)
    anchors: [{anchor, text}] with format-specific anchors:
      PDF  -> {"page_no": N}                (1-based)
      DOCX -> {"paragraph_idx": i} | {"table_idx": j, "row_idx": r, "cell_idx": c}
      XLSX -> {"sheet_name": S, "cell_range": "B3"}
      PPTX -> {"slide_no": N}                (1-based)
    notes: capability limits, macro handling, scanned-image disclaimer
    notes.pdf_backend (PDF only): "stdlib-subset" | "pypdf" -- the backend
      that produced the final result (success or final failure)
    peak_rss_kib: worker peak RSS (ru_maxrss), or null

Scope limits (all enforced, all failures clean, never silent truncation):
  - file size, page/slide/sheet/cell counts, compression ratio,
    total uncompressed size, total extracted text, wall time.
  - Only text-layer extraction. Scanned-image text (OCR) is NOT part
    of this pipeline and is never claimed: see notes.scanned_images.
  - Extraction results are data only. Macros, embedded objects, PDF
    JavaScript/actions and spreadsheet formulas are never executed:
    macro parts (vbaProject.bin) are skipped unread, formula cells
    are read with cached values only (data_only), and PDF action
    dictionaries are never parsed for text.

Library policy (no downloads at runtime; install-time deps only):
  - DOCX via python-docx, XLSX via openpyxl (both already installed
    for the system python3; the M0 venv lacks them, so run this tool
    with system python3, not the M0 venv).
  - PPTX via stdlib zipfile + xml.etree (slide XML text runs only).
  - PDF via two backends, stdlib first: the stdlib subset parser
    (uncompressed or FlateDecode streams, literal/hex strings,
    Tj/TJ/'/" operators, simple fonts only) runs first, so every file
    it already handled keeps its exact old behavior -- including
    UTF-16BE hex strings, which pypdf decodes worse for simple fonts
    (verified 2026-11-01: pypdf garbles a BOM hex string that the
    subset decodes exactly). When the subset fails on a decoder
    limitation (unsupported_encoding, e.g. ToUnicode/CID fonts) or an
    unparseable structure (corrupt), pypdf is tried as the rescue
    backend (PdfReader page-by-page extract_text, page_no from 1).
    Policy failures (page/text/compression limits) and encryption
    are never retried: encrypted PDFs fail cleanly, never with a
    password attempt. When pypdf is not installed, behavior is
    exactly the old stdlib-only behavior (silent fallback, no new
    failure mode). pypdf pin: pypdf==6.21.4, pip install --user for
    system python3 (same user-site practice as pycryptodome); the M0
    venv lacks pypdf and therefore always runs stdlib-only.
  - The backend that produced the final PDF result is recorded in
    notes.pdf_backend ("stdlib-subset" | "pypdf"); on a dual-attempt
    failure it names the stdlib subset as the final arbiter, except
    for empty_text_layer, where it names the last engine actually
    attempted (pypdf when the recheck ran, stdlib-subset when pypdf
    was unavailable).
  - DOCX headers/footers are NOT extracted in v1 (body paragraphs
    and tables only); this is recorded in notes, not hidden.

Environment overrides (tests only; defaults are the production limits):
  DOC_PARSER_MAX_FILE_BYTES, DOC_PARSER_MAX_PDF_PAGES,
  DOC_PARSER_MAX_DOCX_PARAGRAPHS, DOC_PARSER_MAX_DOCX_TABLES,
  DOC_PARSER_MAX_XLSX_SHEETS, DOC_PARSER_MAX_XLSX_CELLS,
  DOC_PARSER_MAX_PPTX_SLIDES, DOC_PARSER_MAX_COMPRESSION_RATIO,
  DOC_PARSER_MAX_UNCOMPRESSED_BYTES, DOC_PARSER_MAX_TEXT_CHARS,
  DOC_PARSER_TIMEOUT_SECONDS,
  DOC_PARSER_TEST_MODE=1 + DOC_PARSER_TEST_SLEEP_SECONDS=N
    (test-only hook: worker sleeps N seconds before extracting,
    so the launcher timeout fuse can be exercised deterministically)
"""

import hashlib
import io
import json
import os
import re
import resource
import shutil
import subprocess
import sys
import tempfile
import zipfile

SCHEMA = "doc-evidence-0.1"


def _env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _env_float(name, default):
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


def limits():
    return {
        "max_file_bytes": _env_int("DOC_PARSER_MAX_FILE_BYTES", 10 * 1024 * 1024),
        "max_pdf_pages": _env_int("DOC_PARSER_MAX_PDF_PAGES", 200),
        "max_docx_paragraphs": _env_int("DOC_PARSER_MAX_DOCX_PARAGRAPHS", 10000),
        "max_docx_tables": _env_int("DOC_PARSER_MAX_DOCX_TABLES", 500),
        "max_xlsx_sheets": _env_int("DOC_PARSER_MAX_XLSX_SHEETS", 50),
        "max_xlsx_cells": _env_int("DOC_PARSER_MAX_XLSX_CELLS", 100000),
        "max_pptx_slides": _env_int("DOC_PARSER_MAX_PPTX_SLIDES", 500),
        "max_compression_ratio": _env_float("DOC_PARSER_MAX_COMPRESSION_RATIO", 100.0),
        "max_uncompressed_bytes": _env_int("DOC_PARSER_MAX_UNCOMPRESSED_BYTES", 100 * 1024 * 1024),
        "max_text_chars": _env_int("DOC_PARSER_MAX_TEXT_CHARS", 1000000),
        "timeout_seconds": _env_int("DOC_PARSER_TIMEOUT_SECONDS", 60),
    }


def peak_kib():
    try:
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return None


class DocFail(Exception):
    def __init__(self, code, reason, backend=None):
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.backend = backend


class _PypdfFailed(Exception):
    """pypdf rescue unavailable for this file; keep the subset failure."""


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _atomic_write_json(path, obj):
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".doc-evidence-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------- PDF subset
def _pdf_unescape(data):
    out = bytearray()
    i = 0
    while i < len(data):
        c = data[i]
        if c == 0x5C and i + 1 < len(data):  # backslash
            n = data[i + 1]
            mapping = {0x6E: 0x0A, 0x72: 0x0D, 0x74: 0x09,
                       0x62: 0x08, 0x66: 0x0C, 0x28: 0x28,
                       0x29: 0x29, 0x5C: 0x5C}
            if n in mapping:
                out.append(mapping[n])
                i += 2
            elif 0x30 <= n <= 0x37:  # up to 3 octal digits
                j = i + 1
                oct_digits = b""
                while j < len(data) and len(oct_digits) < 3 and 0x30 <= data[j] <= 0x37:
                    oct_digits += bytes([data[j]])
                    j += 1
                out.append(int(oct_digits, 8) & 0xFF)
                i = j
            elif n in (0x0A, 0x0D):  # line continuation
                i += 2
                if n == 0x0D and i < len(data) and data[i] == 0x0A:
                    i += 1
            else:
                out.append(n)
                i += 2
        else:
            out.append(c)
            i += 1
    return bytes(out)


def _pdf_decode_string(raw):
    if raw[:2] == b"\xfe\xff":
        try:
            return raw[2:].decode("utf-16-be")
        except UnicodeDecodeError:
            return raw.decode("latin-1")
    return raw.decode("latin-1")


def _pdf_stream_text(content):
    """Extract text from one page content stream (bytes)."""
    texts = []
    i = 0
    n = len(content)
    stack = []  # (kind, value): kind 'str' | 'array'
    while i < n:
        c = content[i]
        if c in b" \t\r\n\x0c\x00":
            i += 1
        elif c == 0x28:  # literal string
            depth = 1
            j = i + 1
            buf = bytearray()
            while j < n and depth > 0:
                ch = content[j]
                if ch == 0x5C and j + 1 < n:
                    buf.append(ch)
                    buf.append(content[j + 1])
                    j += 2
                    continue
                if ch == 0x28:
                    depth += 1
                elif ch == 0x29:
                    depth -= 1
                    if depth == 0:
                        break
                buf.append(ch)
                j += 1
            stack.append(("str", _pdf_decode_string(_pdf_unescape(bytes(buf)))))
            i = j + 1
        elif c == 0x3C and i + 1 < n and content[i + 1] != 0x3C:  # hex string
            j = content.find(b">", i + 1)
            if j == -1:
                break
            hexs = re.sub(rb"[^0-9A-Fa-f]", b"", content[i + 1:j])
            if len(hexs) % 2 == 1:
                hexs += b"0"
            try:
                raw = bytes.fromhex(hexs.decode("ascii"))
            except ValueError:
                raw = b""
            stack.append(("str", _pdf_decode_string(raw)))
            i = j + 1
        elif c == 0x5B:  # array start
            stack.append(("mark", None))
            i += 1
        elif c == 0x5D:  # array end
            items = []
            while stack and stack[-1][0] != "mark":
                items.append(stack.pop())
            if stack:
                stack.pop()
            items.reverse()
            stack.append(("array", [v for k, v in items if k == "str"]))
            i += 1
        elif c in b"+-.0123456789":
            j = i
            while j < n and content[j] in b"+-.0123456789":
                j += 1
            stack.append(("num", content[i:j].decode("ascii", "ignore")))
            i = j
        elif c == 0x2F:  # name
            j = i + 1
            while j < n and content[j] not in b" \t\r\n\x0c\x00()<>[]{}/%":
                j += 1
            stack.append(("name", content[i + 1:j].decode("latin-1")))
            i = j
        else:  # operator word (or lone delimiter / comment start)
            j = i
            while j < n and content[j] not in b" \t\r\n\x0c\x00()<>[]{}/%":
                j += 1
            if j == i:  # single delimiter char, e.g. '<', '>', '%': skip it
                i += 1
                continue
            op = content[i:j].decode("latin-1")
            if op in ("Tj", "'", '"'):
                arg = None
                for k in range(len(stack) - 1, -1, -1):
                    if stack[k][0] == "str":
                        arg = stack[k][1]
                        break
                if arg is not None:
                    texts.append(arg)
                    if op in ("'", '"'):
                        texts.append("\n")
            elif op == "TJ":
                arg = None
                for k in range(len(stack) - 1, -1, -1):
                    if stack[k][0] == "array":
                        arg = stack[k][1]
                        break
                if arg is not None:
                    texts.append("".join(arg))
            elif op in ("Td", "TD", "T*"):
                texts.append("\n")
            stack = []
            i = j
    return "".join(texts)


def _pdf_extract_stdlib(data, lim):
    if not data[:1024].startswith(b"%PDF-"):
        raise DocFail("corrupt", "missing %PDF- header")
    # Encryption check on non-stream bytes only (streams may contain
    # arbitrary bytes that merely look like the key).
    no_streams = re.sub(rb"stream\r?\n.*?endstream", b"", data, flags=re.S)
    if re.search(rb"/Encrypt\b", no_streams):
        raise DocFail("encrypted", "PDF is encrypted; not supported")
    if re.search(rb"/ToUnicode\b", no_streams) or re.search(rb"/Subtype\s*/Type0\b", no_streams):
        raise DocFail("unsupported_encoding",
                      "PDF uses ToUnicode/CID fonts; stdlib subset cannot decode reliably")
    objects = {}
    for m in re.finditer(rb"(\d+)\s+\d+\s+obj\b(.*?)endobj", data, flags=re.S):
        objects[int(m.group(1))] = m.group(2)
    if not objects:
        raise DocFail("corrupt", "no PDF objects found")
    # Page order from /Pages /Kids; fallback to /Type /Page objects.
    page_nums = []
    for body in objects.values():
        km = re.search(rb"/Type\s*/Pages\b.*?/Kids\s*\[(.*?)\]", body, flags=re.S)
        if km:
            page_nums = [int(x) for x in re.findall(rb"(\d+)\s+\d+\s+R", km.group(1))]
            break
    if not page_nums:
        page_nums = sorted(num for num, body in objects.items()
                           if re.search(rb"/Type\s*/Page\b", body)
                           and not re.search(rb"/Type\s*/Pages\b", body))
    if not page_nums:
        raise DocFail("corrupt", "no page objects found")
    if len(page_nums) > lim["max_pdf_pages"]:
        raise DocFail("page_limit", "PDF has %d pages, limit %d"
                      % (len(page_nums), lim["max_pdf_pages"]))
    anchors = []
    decompressed_total = 0
    for idx, pnum in enumerate(page_nums):
        body = objects.get(pnum, b"")
        cm = re.search(rb"/Contents\s+(\d+)\s+\d+\s+R", body)
        refs = []
        if cm:
            refs = [int(cm.group(1))]
        else:
            am = re.search(rb"/Contents\s*\[(.*?)\]", body, flags=re.S)
            if am:
                refs = [int(x) for x in re.findall(rb"(\d+)\s+\d+\s+R", am.group(1))]
        page_text = []
        for rnum in refs:
            cbody = objects.get(rnum, b"")
            sm = re.search(rb"stream\r?\n", cbody)
            if not sm:
                continue
            lm = re.search(rb"/Length\s+(\d+)", cbody)
            if lm:
                raw = cbody[sm.end():sm.end() + int(lm.group(1))]
            else:
                em = re.search(rb"endstream", cbody[sm.end():])
                raw = cbody[sm.end():sm.end() + em.start()] if em else b""
            if b"/FlateDecode" in cbody:
                try:
                    import zlib
                    raw = zlib.decompress(raw)
                except Exception:
                    raise DocFail("corrupt", "page %d content stream is not valid Flate data"
                                  % (idx + 1))
            decompressed_total += len(raw)
            page_text.append(_pdf_stream_text(raw))
        anchors.append({"anchor": {"page_no": idx + 1},
                        "text": "".join(page_text)})
    if decompressed_total and len(data) and \
            decompressed_total / float(len(data)) > lim["max_compression_ratio"]:
        raise DocFail("compression_ratio",
                      "PDF stream expansion ratio %.1f exceeds %.1f"
                      % (decompressed_total / float(len(data)),
                         lim["max_compression_ratio"]))
    return anchors


def _pdf_reader_class():
    """PdfReader class from pypdf, else PyPDF2 (same API), else None.

    Import fallback only; there is exactly one rescue implementation
    below, shared by both library names.
    """
    try:
        from pypdf import PdfReader
        return PdfReader
    except ImportError:
        pass
    try:
        from PyPDF2 import PdfReader
        return PdfReader
    except ImportError:
        return None


def _pdf_extract_pypdf(data, lim):
    """Rescue backend. Returns anchors, or raises DocFail (policy /
    encryption, backend="pypdf") or _PypdfFailed (unusable here)."""
    Reader = _pdf_reader_class()
    if Reader is None:
        raise _PypdfFailed("pypdf/PyPDF2 not installed")
    try:
        reader = Reader(io.BytesIO(data))
    except Exception as e:
        raise _PypdfFailed("pypdf cannot open PDF: %s" % type(e).__name__)
    try:
        encrypted = bool(getattr(reader, "is_encrypted", False))
    except Exception:
        raise _PypdfFailed("pypdf encryption check failed")
    if encrypted:
        raise DocFail("encrypted", "PDF is encrypted; not supported",
                      backend="pypdf")
    try:
        pages = reader.pages
    except Exception as e:
        raise _PypdfFailed("pypdf cannot list pages: %s" % type(e).__name__)
    if len(pages) > lim["max_pdf_pages"]:
        raise DocFail("page_limit", "PDF has %d pages, limit %d"
                      % (len(pages), lim["max_pdf_pages"]), backend="pypdf")
    anchors = []
    total_chars = 0
    for idx, page in enumerate(pages):
        try:
            text = page.extract_text() or ""
        except Exception:
            raise _PypdfFailed("pypdf cannot extract page %d" % (idx + 1))
        anchors.append({"anchor": {"page_no": idx + 1}, "text": text})
        total_chars += len(text)
    if total_chars > lim["max_text_chars"]:
        raise DocFail("text_limit", "extracted text %d chars exceeds %d"
                      % (total_chars, lim["max_text_chars"]), backend="pypdf")
    # Compression analogue for the pypdf path: extracted text vs file
    # size (the subset path measures decompressed stream bytes instead;
    # extracted text is always the smaller of the two, so this is the
    # more lenient of the two measures, never stricter).
    if len(data) and total_chars / float(len(data)) > lim["max_compression_ratio"]:
        raise DocFail("compression_ratio",
                      "PDF text expansion ratio %.1f exceeds %.1f"
                      % (total_chars / float(len(data)),
                         lim["max_compression_ratio"]), backend="pypdf")
    return anchors


_FAILURE_CATEGORIES = {
    "policy": {"file_too_large", "page_limit", "item_limit",
               "compression_ratio", "uncompressed_too_large",
               "text_limit", "timeout", "encrypted"},
    "capability": {"unsupported_encoding"},
    "corrupt": {"corrupt"},
    "empty": {"empty_text_layer"},
}


def _failure_category(code):
    """Additive grouping of a failure_code; never renames the code.

    Unlisted codes (input_error, missing_dependency,
    unsupported_format, worker_failed, ...) map to "unknown": no new
    failure_code is invented here, especially not for unproven cases
    such as ObjStm handling.
    """
    for cat, codes in _FAILURE_CATEGORIES.items():
        if code in codes:
            return cat
    return "unknown"


def _non_ws_chars(anchors):
    total = 0
    for a in anchors:
        t = a.get("text")
        if not isinstance(t, str):
            t = str(t) if t is not None else ""
        total += sum(1 for ch in t if not ch.isspace())
    return total


def _pdf_empty_hint(data):
    """Best-effort hint for an all-empty PDF. Never raises.

    scanned_image_only: an image XObject is declared in the raw
    bytes (resource dicts are plain objects, so this survives
    Flate-compressed content streams). vector_outlined_text: page
    content decodes to path operators with no text operators at all.
    Anything else (including pypdf missing) is unknown. The hint
    never affects the failed/completed verdict.
    """
    try:
        if re.search(rb"/Subtype\s*/Image\b", data):
            return "scanned_image_only"
    except Exception:
        pass
    try:
        Reader = _pdf_reader_class()
        if Reader is None:
            return "unknown"
        reader = Reader(io.BytesIO(data))
        has_path = False
        for page in reader.pages:
            try:
                contents = page.get_contents()
                raw = contents.get_data() if contents is not None else b""
            except Exception:
                continue
            if re.search(rb"\bBT\b", raw):
                return "unknown"
            if re.search(rb"\bm\b|\bl\b|\bc\b|\bre\b", raw):
                has_path = True
        return "vector_outlined_text" if has_path else "unknown"
    except Exception:
        return "unknown"


def _pdf_empty_recheck(data, lim, backend):
    """One pypdf recheck for a PDF whose first pass found no text.

    Returns (anchors, "pypdf") when the recheck finds quotable text.
    Otherwise raises DocFail("empty_text_layer") with backend naming
    the last engine actually attempted. A text_limit from the recheck
    propagates unchanged: it proves text exists, so calling the file
    empty would be a lie. Runs inside the worker, so the existing
    launcher timeout fuse still bounds the whole run.
    """
    if backend == "pypdf":
        raise DocFail("empty_text_layer",
                      "PDF parsed but all pages have no extractable text",
                      backend="pypdf")
    try:
        re_anchors = _pdf_extract_pypdf(data, lim)
    except _PypdfFailed:
        raise DocFail("empty_text_layer",
                      "PDF parsed but all pages have no extractable text",
                      backend="pypdf" if _pdf_reader_class() is not None
                      else "stdlib-subset")
    except DocFail as e:
        if e.code == "text_limit":
            raise
        raise DocFail("empty_text_layer",
                      "PDF parsed but all pages have no extractable text",
                      backend="pypdf")
    if _non_ws_chars(re_anchors) > 0:
        return re_anchors, "pypdf"
    raise DocFail("empty_text_layer",
                  "PDF parsed but all pages have no extractable text",
                  backend="pypdf")


def _pdf_extract(data, lim):
    """Dispatcher. Returns (anchors, backend).

    Subset first (exact old behavior for everything it handled);
    pypdf rescue only on decoder-limitation failures
    (unsupported_encoding, corrupt). Policy failures and encryption
    are never retried. On rescue failure the original subset failure
    is re-raised with backend="stdlib-subset".
    """
    try:
        return _pdf_extract_stdlib(data, lim), "stdlib-subset"
    except DocFail as e:
        if e.code in ("unsupported_encoding", "corrupt"):
            try:
                return _pdf_extract_pypdf(data, lim), "pypdf"
            except _PypdfFailed:
                pass
        e.backend = "stdlib-subset"
        raise


# ---------------------------------------------------------------- zip formats
def _zip_checks(path, size, lim):
    try:
        zf = zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile:
        raise DocFail("corrupt", "not a valid zip container")
    try:
        infos = zf.infolist()
        total_uncomp = sum(i.file_size for i in infos)
        if size and total_uncomp / float(size) > lim["max_compression_ratio"]:
            raise DocFail("compression_ratio",
                          "zip expansion ratio %.1f exceeds %.1f"
                          % (total_uncomp / float(size), lim["max_compression_ratio"]))
        if total_uncomp > lim["max_uncompressed_bytes"]:
            raise DocFail("uncompressed_too_large",
                          "zip uncompressed size %d exceeds %d"
                          % (total_uncomp, lim["max_uncompressed_bytes"]))
        return zf, [i.filename for i in infos]
    except DocFail:
        zf.close()
        raise


def _macro_note(names):
    if "vbaProject.bin" in names:
        return "macro part vbaProject.bin present; skipped unread, never executed"
    return None


def _docx_extract(path, lim):
    try:
        import docx
    except ImportError:
        raise DocFail("missing_dependency", "python-docx is not installed for this interpreter")
    try:
        doc = docx.Document(path)
    except Exception as e:
        raise DocFail("corrupt", "python-docx cannot open document: %s" % type(e).__name__)
    paras = doc.paragraphs
    if len(paras) > lim["max_docx_paragraphs"]:
        raise DocFail("item_limit", "DOCX has %d paragraphs, limit %d"
                      % (len(paras), lim["max_docx_paragraphs"]))
    tables = doc.tables
    if len(tables) > lim["max_docx_tables"]:
        raise DocFail("item_limit", "DOCX has %d tables, limit %d"
                      % (len(tables), lim["max_docx_tables"]))
    anchors = []
    for i, p in enumerate(paras):
        anchors.append({"anchor": {"paragraph_idx": i}, "text": p.text})
    for j, t in enumerate(tables):
        for r, row in enumerate(t.rows):
            for c, cell in enumerate(row.cells):
                anchors.append({"anchor": {"table_idx": j, "row_idx": r, "cell_idx": c},
                                "text": cell.text})
    return anchors


def _xlsx_extract(path, lim):
    try:
        import openpyxl
    except ImportError:
        raise DocFail("missing_dependency", "openpyxl is not installed for this interpreter")
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as e:
        raise DocFail("corrupt", "openpyxl cannot open workbook: %s" % type(e).__name__)
    try:
        sheets = wb.sheetnames
        if len(sheets) > lim["max_xlsx_sheets"]:
            raise DocFail("item_limit", "XLSX has %d sheets, limit %d"
                          % (len(sheets), lim["max_xlsx_sheets"]))
        anchors = []
        count = 0
        for name in sheets:
            ws = wb[name]
            for row in ws.iter_rows():
                for cell in row:
                    v = cell.value
                    if v is None or (isinstance(v, str) and v == ""):
                        continue
                    count += 1
                    if count > lim["max_xlsx_cells"]:
                        raise DocFail("item_limit", "XLSX has more than %d non-empty cells"
                                      % lim["max_xlsx_cells"])
                    anchors.append({"anchor": {"sheet_name": name,
                                               "cell_range": cell.coordinate},
                                    "text": str(v)})
        return anchors
    finally:
        try:
            wb.close()
        except Exception:
            pass


PPTX_NS = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}


def _pptx_extract(path, lim):
    import xml.etree.ElementTree as ET
    try:
        zf = zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile:
        raise DocFail("corrupt", "not a valid zip container")
    try:
        slide_names = []
        for info in zf.infolist():
            m = re.fullmatch(r"ppt/slides/slide(\d+)\.xml", info.filename)
            if m:
                slide_names.append((int(m.group(1)), info.filename))
        slide_names.sort()
        if len(slide_names) > lim["max_pptx_slides"]:
            raise DocFail("item_limit", "PPTX has %d slides, limit %d"
                          % (len(slide_names), lim["max_pptx_slides"]))
        anchors = []
        for num, fname in slide_names:
            try:
                root = ET.fromstring(zf.read(fname))
            except Exception:
                raise DocFail("corrupt", "slide %d XML is not parseable" % num)
            paras = []
            for p in root.iter("{http://schemas.openxmlformats.org/drawingml/2006/main}p"):
                runs = [t.text or "" for t in
                        p.iter("{http://schemas.openxmlformats.org/drawingml/2006/main}t")]
                paras.append("".join(runs))
            anchors.append({"anchor": {"slide_no": num},
                            "text": "\n".join(paras)})
        return anchors
    finally:
        zf.close()


def _detect_format(path, data_head):
    if data_head.startswith(b"%PDF-"):
        return "pdf"
    if data_head.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(path, "r") as zf:
                names = zf.namelist()
        except zipfile.BadZipFile:
            raise DocFail("corrupt", "zip signature present but container is invalid")
        if any(n.startswith("word/") for n in names):
            return "docx"
        if any(n.startswith("xl/") for n in names):
            return "xlsx"
        if any(n.startswith("ppt/") for n in names):
            return "pptx"
        raise DocFail("unsupported_format", "zip container is not DOCX/XLSX/PPTX")
    raise DocFail("unsupported_format", "unrecognized file format")


# ---------------------------------------------------------------- worker
def worker_parse(input_path, output_path):
    lim = limits()
    scratch_dir = tempfile.mkdtemp(prefix="doc_parser-")
    out = {
        "schema_version": SCHEMA,
        "source": {"file_name": os.path.basename(input_path),
                   "sha256": None, "size_bytes": None, "format": None},
        "status": "failed",
        "failure_code": None,
        "failure_reason": None,
        "anchors": [],
        "notes": {
            "scanned_images": "本管线只做文本层提取；扫描件图片中的文字（OCR）不在本次能力内，不会出现在 anchors 里。",
            "macros": "提取结果只当数据；宏、嵌入对象、PDF 动作与公式一律不执行。",
            "docx_headers_footers": "DOCX v1 只提取正文段落与表格；页眉页脚不在本次提取范围内。",
        },
        "peak_rss_kib": None,
        "scratch_dir": scratch_dir,
        "scratch_cleanup": None,
    }
    try:
        if os.environ.get("DOC_PARSER_TEST_MODE") == "1":
            import time
            time.sleep(_env_int("DOC_PARSER_TEST_SLEEP_SECONDS", 0))
        if not os.path.isfile(input_path):
            raise DocFail("input_error", "input file does not exist")
        size = os.path.getsize(input_path)
        out["source"]["size_bytes"] = size
        out["source"]["sha256"] = _sha256_file(input_path)
        if size > lim["max_file_bytes"]:
            raise DocFail("file_too_large", "file size %d exceeds %d"
                          % (size, lim["max_file_bytes"]))
        with open(input_path, "rb") as f:
            head = f.read(8)
        fmt = _detect_format(input_path, head)
        out["source"]["format"] = fmt
        if fmt == "pdf":
            with open(input_path, "rb") as f:
                data = f.read()
            anchors, pdf_backend = _pdf_extract(data, lim)
            out["notes"]["pdf_backend"] = pdf_backend
            notes_extra = []
        else:
            zf, names = _zip_checks(input_path, size, lim)
            zf.close()
            macro = _macro_note(names)
            notes_extra = [macro] if macro else []
            if fmt == "docx":
                anchors = _docx_extract(input_path, lim)
            elif fmt == "xlsx":
                anchors = _xlsx_extract(input_path, lim)
            else:
                anchors = _pptx_extract(input_path, lim)
        total_chars = sum(len(a["text"]) for a in anchors)
        if total_chars > lim["max_text_chars"]:
            raise DocFail("text_limit", "extracted text %d chars exceeds %d"
                          % (total_chars, lim["max_text_chars"]))
        if _non_ws_chars(anchors) == 0:
            # Empty-text rule (all formats): a "completed" result with
            # no quotable text is a false positive (real case: a
            # vector-outlined PDF reported completed with 4 empty
            # anchors). Partial emptiness never triggers: only a
            # zero total does. Like every other failure in this
            # pipeline, the output keeps anchors [] (the failure_code
            # and hint carry the verdict); no page structure is
            # claimed quotable.
            if fmt == "pdf":
                try:
                    anchors, pdf_backend = _pdf_empty_recheck(
                        data, lim, pdf_backend)
                    out["notes"]["pdf_backend"] = pdf_backend
                except DocFail as empty_e:
                    out["notes"]["pdf_backend"] = (
                        getattr(empty_e, "backend", None) or pdf_backend)
                    try:
                        out["notes"]["empty_text_hint"] = _pdf_empty_hint(data)
                    except Exception:
                        out["notes"]["empty_text_hint"] = "unknown"
                    raise empty_e
            else:
                out["notes"]["empty_text_hint"] = "unknown"
                raise DocFail("empty_text_layer",
                              "parsed but no extractable text in any anchor")
        out["anchors"] = anchors
        out["status"] = "completed"
        if notes_extra:
            out["notes"]["macro_part"] = notes_extra[0]
    except DocFail as e:
        out["failure_code"] = e.code
        out["failure_reason"] = e.reason
        out["category"] = _failure_category(e.code)
        if out["source"]["format"] == "pdf":
            out["notes"]["pdf_backend"] = getattr(e, "backend", None) or "stdlib-subset"
    except Exception as e:  # never leak a traceback as "completed"
        out["failure_code"] = "worker_failed"
        out["failure_reason"] = "%s: %s" % (type(e).__name__, str(e)[:200])
        out["category"] = "unknown"
    finally:
        try:
            shutil.rmtree(scratch_dir, ignore_errors=True)
            out["scratch_cleanup"] = "removed"
        except Exception:
            out["scratch_cleanup"] = "failed"
        out["peak_rss_kib"] = peak_kib()
        _atomic_write_json(output_path, out)
    return 0


# ---------------------------------------------------------------- launcher
def launcher_run(input_path, output_path, timeout=None):
    lim = limits()
    if timeout is None:
        timeout = lim["timeout_seconds"]
    tmpdir = tempfile.mkdtemp(prefix="doc_parser-launcher-")
    worker_out = os.path.join(tmpdir, "worker.json")
    try:
        cmd = [sys.executable, os.path.abspath(__file__), "parse",
               input_path, worker_out]
        try:
            proc = subprocess.run(cmd, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            fail = {
                "schema_version": SCHEMA,
                "source": {"file_name": os.path.basename(input_path),
                           "sha256": None, "size_bytes": None, "format": None},
                "status": "failed",
                "failure_code": "timeout",
                "category": "policy",
                "failure_reason": "worker did not finish within %d seconds" % timeout,
                "anchors": [],
                "notes": {"launcher_synthesized": True},
                "peak_rss_kib": None,
                "scratch_dir": None,
                "scratch_cleanup": None,
            }
            try:
                fail["source"]["size_bytes"] = os.path.getsize(input_path)
                fail["source"]["sha256"] = _sha256_file(input_path)
            except OSError:
                pass
            _atomic_write_json(output_path, fail)
            return 0
        if proc.returncode != 0 or not os.path.isfile(worker_out):
            fail = {
                "schema_version": SCHEMA,
                "source": {"file_name": os.path.basename(input_path),
                           "sha256": None, "size_bytes": None, "format": None},
                "status": "failed",
                "failure_code": "worker_failed",
                "category": "unknown",
                "failure_reason": "worker exit %s without valid output"
                                % proc.returncode,
                "anchors": [],
                "notes": {"launcher_synthesized": True,
                          "worker_stderr_tail": proc.stderr.decode("utf-8", "ignore")[-300:]},
                "peak_rss_kib": None,
                "scratch_dir": None,
                "scratch_cleanup": None,
            }
            _atomic_write_json(output_path, fail)
            return 0
        with open(worker_out, encoding="utf-8") as f:
            out = json.load(f)
        if out.get("schema_version") != SCHEMA or "status" not in out:
            raise ValueError("worker output schema mismatch")
        # Integrity: worker-reported hash must match the input file.
        if out.get("source", {}).get("sha256") != _sha256_file(input_path):
            raise ValueError("worker output sha256 mismatch")
        # Scratch dir must be gone; if the worker leaked it, remove it
        # here and say so instead of silently pretending it was clean.
        sdir = out.get("scratch_dir")
        if sdir and os.path.exists(sdir):
            shutil.rmtree(sdir, ignore_errors=True)
            out["scratch_cleanup"] = "launcher_removed"
        _atomic_write_json(output_path, out)
        return 0
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main(argv):
    if len(argv) >= 2 and argv[1] == "parse":
        if len(argv) != 4:
            print("usage: doc_parser.py parse <input> <output>", file=sys.stderr)
            return 2
        return worker_parse(argv[2], argv[3])
    if len(argv) < 3:
        print("usage: doc_parser.py <input> <output> [--timeout SECONDS]", file=sys.stderr)
        return 2
    timeout = None
    if "--timeout" in argv:
        i = argv.index("--timeout")
        try:
            timeout = int(argv[i + 1])
        except (IndexError, ValueError):
            print("bad --timeout", file=sys.stderr)
            return 2
    return launcher_run(argv[1], argv[2], timeout)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
