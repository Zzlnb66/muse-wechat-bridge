"""Gate 2 tests: frozen AES-128-ECB + PKCS#7 vectors, switched path.

Vectors come from tests/fixtures/aes_vectors.json, generated ONLY with
the system openssl CLI (OpenSSL 3.0.13) -- never with the code under
test, so agreement is not circular. Two independent implementations
(openssl CLI and pycryptodome 4.2.0) were also verified to agree on
all vectors before this file was written.

What is tested:
 1. In-process encrypt == fixture ciphertext, byte for byte.
 2. In-process decrypt == fixture plaintext.
 3. In-process roundtrip restores the plaintext (incl. binary/long).
 4. The SWITCHED bridge path (bridge.aes_ecb_*) agrees on the same
    vectors, both directions (proves the switch kept the semantics).
 5. Failure is loud, never silent garbage: wrong key, truncated
    ciphertext, tampered padding, empty ciphertext all raise; a key
    that is not exactly 16 bytes is refused; non-bytes input refused.
 6. Coupling (switched, 2026-11-01): bridge.py DOES import
    aes_inprocess, and the switched AES functions contain no openssl
    subprocess call anymore.

Run (requires pycryptodome==4.2.0, installed --user for system
python3; see the Gate 2 delivery report):
  python3 tests/test_aes_vectors.py
(exit 0 = all pass). No real bridge state, inbox, outbox, media or
network is touched; only temp-free pure crypto calls run.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import aes_inprocess
except ImportError as e:
    print("FAIL import aes_inprocess:", e)
    print("This test needs pycryptodome==4.2.0 installed for system "
          "python3 (pip install --user pycryptodome==4.2.0).")
    sys.exit(2)

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

# Fixture sanity: ciphertexts are non-empty multiples of 16.
for v in VECTORS:
    ct = bytes.fromhex(v["ciphertext_hex"])
    check(f"fixture-shape {v['id']}", len(ct) >= 16 and len(ct) % 16 == 0)

for v in VECTORS:
    pt = bytes.fromhex(v["plaintext_hex"])
    ct = bytes.fromhex(v["ciphertext_hex"])
    check(f"draft-encrypt {v['id']}",
          aes_inprocess.aes_inprocess_encrypt(pt, KEY) == ct)
    check(f"draft-decrypt {v['id']}",
          aes_inprocess.aes_inprocess_decrypt(ct, KEY) == pt)
    check(f"draft-roundtrip {v['id']}",
          aes_inprocess.aes_inprocess_decrypt(
              aes_inprocess.aes_inprocess_encrypt(pt, KEY), KEY) == pt)
    check(f"oldpath-encrypt {v['id']}",
          bridge.aes_ecb_encrypt(pt, KEY) == ct)
    check(f"oldpath-decrypt {v['id']}",
          bridge.aes_ecb_decrypt(ct, KEY) == pt)

V16 = next(v for v in VECTORS if v["id"] == "exact-16")
CT16 = bytes.fromhex(V16["ciphertext_hex"])
BADPAD = bytes.fromhex(FIX["bad_padding_ciphertext_hex"])

# Wrong key: draft raises ValueError (padding check); old path raises ApiError.
check("draft-decrypt wrong-key raises",
      raises(lambda: aes_inprocess.aes_inprocess_decrypt(CT16, WRONG_KEY),
             ValueError))
check("oldpath-decrypt wrong-key raises",
      raises(lambda: bridge.aes_ecb_decrypt(CT16, WRONG_KEY),
             bridge.ApiError))

# Truncated ciphertext (not a multiple of 16).
TRUNC = CT16[:-1]
check("draft-decrypt truncated raises",
      raises(lambda: aes_inprocess.aes_inprocess_decrypt(TRUNC, KEY),
             ValueError))
check("oldpath-decrypt truncated raises",
      raises(lambda: bridge.aes_ecb_decrypt(TRUNC, KEY), bridge.ApiError))

# Tampered padding (fixture byte-flip, openssl-verified to fail).
check("draft-decrypt bad-padding raises",
      raises(lambda: aes_inprocess.aes_inprocess_decrypt(BADPAD, KEY),
             ValueError))
check("oldpath-decrypt bad-padding raises",
      raises(lambda: bridge.aes_ecb_decrypt(BADPAD, KEY), bridge.ApiError))

# Empty ciphertext.
check("draft-decrypt empty raises",
      raises(lambda: aes_inprocess.aes_inprocess_decrypt(b"", KEY),
             ValueError))

# Key length: 15 and 17 bytes refused on both operations.
for bad_key in (b"0123456789abcde", b"0123456789abcdefg"):
    check(f"draft-encrypt key-len-{len(bad_key)} raises",
          raises(lambda k=bad_key: aes_inprocess.aes_inprocess_encrypt(b"x", k),
                 ValueError))
    check(f"draft-decrypt key-len-{len(bad_key)} raises",
          raises(lambda k=bad_key: aes_inprocess.aes_inprocess_decrypt(CT16, k),
                 ValueError))

# Non-bytes input refused.
check("draft-encrypt str raises",
      raises(lambda: aes_inprocess.aes_inprocess_encrypt("hello", KEY),
             TypeError))
check("draft-decrypt str raises",
      raises(lambda: aes_inprocess.aes_inprocess_decrypt("00" * 16, KEY),
             TypeError))

# Coupling (switched 2026-11-01): bridge.py imports aes_inprocess,
# and the switched AES wrappers contain no openssl subprocess call.
import inspect as _inspect

with open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "bridge.py"), encoding="utf-8") as f:
    bridge_src = f.read()
check("bridge.py imports aes_inprocess",
      "from aes_inprocess import" in bridge_src)
# Docstrings may mention openssl historically; what must be absent is
# an actual subprocess invocation (subprocess.* call or "openssl"
# command string) inside the switched wrappers.
for _fn in (bridge.aes_ecb_encrypt, bridge.aes_ecb_decrypt):
    _src = _inspect.getsource(_fn)
    check(f"switched {_fn.__name__} has no openssl/subprocess call",
          "subprocess." not in _src and '"openssl"' not in _src
          and "'openssl'" not in _src)

failed = [n for n, c in ok if not c]
print(f"{len(ok) - len(failed)}/{len(ok)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
