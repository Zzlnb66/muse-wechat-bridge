"""Gate 2 switchover tests: bridge AES paths run in-process.

Scope (2026-11-01, user-approved switch): bridge.aes_ecb_encrypt /
aes_ecb_decrypt now delegate to aes_inprocess (pycryptodome ECB +
strict PKCS#7) instead of the openssl subprocess. The public contract
is unchanged: encrypt returns ciphertext, decrypt returns plaintext,
any strict failure raises bridge.ApiError, and no failure ever yields
plaintext (or ciphertext-as-plaintext) to a caller.

What is tested:
 1. Switched bridge functions == frozen fixture vectors, both ways.
 2. Switched output == openssl CLI output, byte for byte, on vectors
    plus deterministic random samples (proves the switch did not
    change the cipher, only the mechanism).
 3. Switched functions work with PATH emptied (no subprocess left
    in the AES path at all, including the diagnostic block probe).
 4. Diagnostic single-block probe (nopad) == openssl -nopad output,
    bad inputs -> None, never raises, never leaks key/plaintext.
 5. Inbound key formats: hex32 / b64_16 / b64_hex32 all parse via
    parse_aes_key_diag and then decrypt to the plaintext; invalid
    formats yield KEY_FORMAT_ERROR, missing key yields no key (and
    callers refuse the plaintext fallback -- that refusal logic is
    unchanged handler code, guarded statically below).
 6. Failure contract: truncated / bad-padding / wrong-key / empty
    ciphertext / bad key length / non-bytes input all raise ApiError
    from the switched functions; decrypt failure returns nothing.
 7. Static guards (labelled as such): the three inbound decrypt call
    sites still route through aes_ecb_decrypt and map failures to
    DECRYPT_FAILED; the outbound aes_key construction still uses
    Base64-of-hex-ASCII (the protocol quirk), never Base64-of-raw-key.
 8. Missing dependency fails loudly: with Crypto import-blocked, both
    aes_inprocess and bridge imports raise ImportError (no silent
    openssl fallback anywhere).

Run (system python3, pycryptodome==4.2.0 installed --user):
  python3 tests/test_aes_switchover.py
(exit 0 = all pass). No real bridge state, inbox, outbox, media or
network is touched; the only subprocesses spawned are local openssl
CLI reference calls and the import-blocker probe.
"""

import base64
import inspect
import json
import os
import random
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aes_inprocess  # noqa: E402
import bridge  # noqa: E402

ok = []


def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


def raises(fn, exc_types):
    try:
        fn()
    except exc_types:
        return True
    except Exception:
        return False
    return False


FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "fixtures", "aes_vectors.json")
with open(FIXTURES, encoding="utf-8") as f:
    FIX = json.load(f)

KEY = bytes.fromhex(FIX["key_hex"])
WRONG_KEY = bytes.fromhex(FIX["wrong_key_hex"])
VECTORS = FIX["vectors"]
BADPAD = bytes.fromhex(FIX["bad_padding_ciphertext_hex"])


def openssl_cli(data, key, decrypt=False):
    cmd = ["openssl", "enc", "-aes-128-ecb", "-d" if decrypt else "-e",
           "-K", key.hex(), "-nosalt"]
    r = subprocess.run(cmd, input=data, capture_output=True, timeout=30)
    assert r.returncode == 0, f"openssl reference failed: {r.stderr[:200]!r}"
    return r.stdout


# 1. Switched bridge functions == frozen fixtures.
for v in VECTORS:
    pt = bytes.fromhex(v["plaintext_hex"])
    ct = bytes.fromhex(v["ciphertext_hex"])
    check(f"switched-encrypt {v['id']}",
          bridge.aes_ecb_encrypt(pt, KEY) == ct)
    check(f"switched-decrypt {v['id']}",
          bridge.aes_ecb_decrypt(ct, KEY) == pt)

# 2. Switched == openssl CLI on deterministic random samples.
rng = random.Random(20261101)
lengths = [0, 1, 15, 16, 17, 31, 32, 33, 100, 1000, 4097, 65536]
for n in lengths:
    pt = bytes(rng.randrange(256) for _ in range(n))
    ct_sw = bridge.aes_ecb_encrypt(pt, KEY)
    ct_ossl = openssl_cli(pt, KEY, decrypt=False)
    check(f"switched==openssl-encrypt len={n}", ct_sw == ct_ossl)
    check(f"switched-decrypt-openssl-ct len={n}",
          bridge.aes_ecb_decrypt(ct_ossl, KEY) == pt)
    check(f"openssl-decrypt-switched-ct len={n}",
          openssl_cli(ct_sw, KEY, decrypt=True) == pt)

# 3. PATH emptied: switched AES still works (no subprocess left).
old_path = os.environ.get("PATH", "")
os.environ["PATH"] = ""
try:
    check("switched-encrypt PATH-empty",
          bridge.aes_ecb_encrypt(b"hello-path", KEY)
          == aes_inprocess.aes_inprocess_encrypt(b"hello-path", KEY))
    check("switched-decrypt PATH-empty",
          bridge.aes_ecb_decrypt(
              bridge.aes_ecb_encrypt(b"hello-path", KEY), KEY) == b"hello-path")
finally:
    os.environ["PATH"] = old_path

# 4. Diagnostic single-block probe (nopad).
# openssl -nopad of exactly one block: encrypt 16 bytes with -nopad.
r = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-e", "-K", KEY.hex(),
                    "-nosalt", "-nopad"], input=bytes(range(16)),
                   capture_output=True, timeout=30)
assert r.returncode == 0
nopad_ct = r.stdout
check("diag-block == openssl-nopad",
      bridge._raw_aes128_ecb_decrypt_block(KEY, nopad_ct) == bytes(range(16)))
check("diag-block bad-key-len -> None",
      bridge._raw_aes128_ecb_decrypt_block(b"short", nopad_ct) is None)
check("diag-block bad-block-len -> None",
      bridge._raw_aes128_ecb_decrypt_block(KEY, b"short") is None)
check("diag-block non-bytes -> None",
      bridge._raw_aes128_ecb_decrypt_block(KEY, "not-bytes") is None)
os.environ["PATH"] = ""
try:
    check("diag-block PATH-empty",
          bridge._raw_aes128_ecb_decrypt_block(KEY, nopad_ct)
          == bytes(range(16)))
finally:
    os.environ["PATH"] = old_path

# 5. Inbound key formats x decrypt.
sample_pt = b"inbound-key-format-sample!"
sample_ct = bridge.aes_ecb_encrypt(sample_pt, KEY)
hex32 = KEY.hex()
b64_16 = base64.b64encode(KEY).decode()
b64_hex32 = base64.b64encode(hex32.encode("ascii")).decode()
for name, item in [
        ("hex32", {"aeskey": hex32}),
        ("b64_16", {"media": {"aes_key": b64_16}}),
        ("b64_hex32", {"media": {"aes_key": b64_hex32}})]:
    k, _diag, err = bridge.parse_aes_key_diag(item)
    check(f"key-parse {name}", k == KEY and err is None)
    check(f"key-format-decrypt {name}",
          bridge.aes_ecb_decrypt(sample_ct, k) == sample_pt)
for name, item in [
        ("hex32-invalid", {"aeskey": "zz" * 16}),
        ("b64-other-length", {"media": {"aes_key": base64.b64encode(b"12345").decode()}}),
        ("b64-decode-failed", {"media": {"aes_key": "!!!not-base64!!!###"}})]:
    _k, _d, err = bridge.parse_aes_key_diag(item)
    check(f"key-parse-invalid {name} -> KEY_FORMAT_ERROR",
          err == "KEY_FORMAT_ERROR")
_k, _d, err = bridge.parse_aes_key_diag({})
check("key-parse missing -> no key, no format error",
      _k is None and err is None)

# 6. Failure contract: switched functions raise ApiError, return nothing.
V16 = next(v for v in VECTORS if v["id"] == "exact-16")
CT16 = bytes.fromhex(V16["ciphertext_hex"])
check("switched-decrypt wrong-key raises ApiError",
      raises(lambda: bridge.aes_ecb_decrypt(CT16, WRONG_KEY), bridge.ApiError))
check("switched-decrypt truncated raises ApiError",
      raises(lambda: bridge.aes_ecb_decrypt(CT16[:-1], KEY), bridge.ApiError))
check("switched-decrypt bad-padding raises ApiError",
      raises(lambda: bridge.aes_ecb_decrypt(BADPAD, KEY), bridge.ApiError))
check("switched-decrypt empty raises ApiError",
      raises(lambda: bridge.aes_ecb_decrypt(b"", KEY), bridge.ApiError))
for bad_key in (b"0123456789abcde", b"0123456789abcdefg"):
    check(f"switched-encrypt key-len-{len(bad_key)} raises ApiError",
          raises(lambda k=bad_key: bridge.aes_ecb_encrypt(b"x", k),
                 bridge.ApiError))
    check(f"switched-decrypt key-len-{len(bad_key)} raises ApiError",
          raises(lambda k=bad_key: bridge.aes_ecb_decrypt(CT16, k),
                 bridge.ApiError))
check("switched-encrypt str raises ApiError",
      raises(lambda: bridge.aes_ecb_encrypt("hello", KEY), bridge.ApiError))
check("switched-decrypt str raises ApiError",
      raises(lambda: bridge.aes_ecb_decrypt("00" * 16, KEY), bridge.ApiError))
# Ciphertext-size semantics guard: video_size is the padded length.
for n in (0, 1, 15, 16, 17, 100):
    pt = b"a" * n
    expect = ((n // 16) + 1) * 16
    check(f"cipher-len padded len={n}",
          len(bridge.aes_ecb_encrypt(pt, KEY)) == expect)

# 7. Static guards (labelled): call sites + key quirk construction.
_bridge_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(_bridge_dir, "bridge.py"), encoding="utf-8") as f:
    bridge_src = f.read()
# 2026-11-01 100 MiB raise: the file path moved to chunked decrypt
# (_stream_decrypt_file), which uses the same in-process module's
# nopad primitive plus its own strict final-block PKCS#7 check.
# Intent unchanged (every real decrypt path stays in-process, no
# openssl subprocess): video + image still call aes_ecb_decrypt,
# file calls aes_inprocess_decrypt_nopad via the streaming helper.
check("static: in-process decrypt call sites (video+image via wrapper, file via chunked nopad)",
      bridge_src.count("aes_ecb_decrypt(") >= 3  # 1 def + 2 call sites
      and bridge_src.count("aes_inprocess_decrypt_nopad(") >= 2  # probe + streaming helper
      and "_stream_decrypt_file" in bridge_src)
check("static: DECRYPT_FAILED mapping present",
      bridge_src.count("DECRYPT_FAILED") >= 3)
check("static: outbound quirk x3 (Base64 of hex ASCII)",
      bridge_src.count('base64.b64encode(aeskey_hex.encode("utf-8"))') == 3)
check("static: no Base64-of-raw-key construction",
      "b64encode(key)" not in bridge_src
      and "b64encode(aeskey_hex.encode" in bridge_src)
# Quirk property: the sent aes_key decodes to hex ASCII, NOT raw key.
quirk_b64 = base64.b64encode(hex32.encode("utf-8")).decode()
check("quirk property: b64 decodes to hex ASCII, not raw key",
      base64.b64decode(quirk_b64) == hex32.encode("utf-8")
      and base64.b64decode(quirk_b64) != KEY)
# No openssl/subprocess CALL inside the switched wrappers or block
# probe (docstrings may mention openssl historically; an actual
# invocation would be a subprocess.* call or an "openssl" string).
for _fn in (bridge.aes_ecb_encrypt, bridge.aes_ecb_decrypt,
            bridge._raw_aes128_ecb_decrypt_block):
    _src = inspect.getsource(_fn)
    check(f"static: {_fn.__name__} has no openssl/subprocess call",
          "subprocess." not in _src and '"openssl"' not in _src
          and "'openssl'" not in _src)

# 8. Missing dependency fails loudly (subprocess probe, Crypto blocked).
_probe = (
    "import sys\n"
    "class _Blocker:\n"
    "    def find_spec(self, name, path=None, target=None):\n"
    "        if name == 'Crypto' or name.startswith('Crypto.'):\n"
    "            raise ImportError('blocked Crypto for test')\n"
    "        return None\n"
    "sys.meta_path.insert(0, _Blocker())\n"
    "import sys as _s\n"
    "_s.path.insert(0, %r)\n"
    "try:\n"
    "    import aes_inprocess\n"
    "    print('NO-FAIL-AES')\n"
    "except ImportError as e:\n"
    "    print('AES-IMPORT-ERROR', 'pycryptodome' in str(e))\n"
    "try:\n"
    "    import bridge\n"
    "    print('NO-FAIL-BRIDGE')\n"
    "except ImportError:\n"
    "    print('BRIDGE-IMPORT-ERROR')\n"
) % _bridge_dir
r = subprocess.run([sys.executable, "-c", _probe],
                   capture_output=True, text=True, timeout=60)
check("missing-dep: aes_inprocess ImportError mentions pycryptodome",
      "AES-IMPORT-ERROR True" in r.stdout)
check("missing-dep: bridge import fails loudly (no fallback)",
      "BRIDGE-IMPORT-ERROR" in r.stdout
      and "NO-FAIL-BRIDGE" not in r.stdout)

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
