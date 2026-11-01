"""Gate 2 SWITCHED (2026-11-01): in-process AES-128-ECB + PKCS#7.

bridge.py imports this module for ALL real AES paths (send_video /
send_file / send_image encrypt, video / file / image inbound decrypt,
and the diagnostic single-block probe). The openssl-subprocess path
is gone from AES: no subprocess is spawned for crypto, and key bytes
never appear in a command-line argv anymore.

Dependency: pycryptodome is REQUIRED (pinned pycryptodome==4.2.0 in
requirements.txt). If it is not installed, importing this module
raises a clear error instead of silently falling back to the openssl
subprocess: a silent fallback would hide the missing dependency and
would not be an in-process implementation at all.

Semantics (must match the locked bridge contract):
- AES-128-ECB, no IV, no salt. Key is exactly 16 bytes.
- PKCS#7 padding, always applied on encrypt (even for inputs that are
  already a multiple of 16 bytes, a full 16-byte block is added).
- Decrypt is strict: ciphertext must be non-empty and a multiple of
  16 bytes, and the padding bytes must all equal a pad value in
  1..16. Any violation raises ValueError -- never return stripped
  plaintext that failed validation, and never return the ciphertext
  as if it were plaintext.
- Wrong-key rejection is padding-based, not cryptographically
  guaranteed for every (key, ciphertext) pair: a wrong key usually
  yields invalid padding and therefore an error, but a wrong key that
  happens to yield valid padding would decrypt to garbage without
  error. Callers that need authenticity must check content (e.g. a
  hash) after decrypt; this module alone does not authenticate.
"""

try:
    from Crypto.Cipher import AES as _AES
except ImportError as _e:  # pragma: no cover - dependency gap path
    raise ImportError(
        "aes_inprocess requires pycryptodome (pip install pycryptodome); "
        "it is intentionally NOT falling back to the openssl subprocess, "
        "because that would not be an in-process implementation."
    ) from _e

_BLOCK = 16


def _check_key(key):
    if not isinstance(key, (bytes, bytearray)):
        raise TypeError("AES key must be bytes")
    if len(key) != _BLOCK:
        raise ValueError("AES-128 key must be exactly 16 bytes, "
                         f"got {len(key)}")


def _pad(data):
    pad_len = _BLOCK - (len(data) % _BLOCK)
    return data + bytes([pad_len]) * pad_len


def _unpad(data):
    if not data or len(data) % _BLOCK != 0:
        raise ValueError("ciphertext length must be a non-empty multiple of 16")
    pad_len = data[-1]
    if pad_len < 1 or pad_len > _BLOCK:
        raise ValueError("bad PKCS#7 padding: pad value out of range")
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        raise ValueError("bad PKCS#7 padding: pad bytes inconsistent")
    return data[:-pad_len]


def aes_inprocess_encrypt(plaintext, key):
    """Encrypt bytes with AES-128-ECB + PKCS#7, in-process."""
    if not isinstance(plaintext, (bytes, bytearray)):
        raise TypeError("plaintext must be bytes")
    _check_key(key)
    cipher = _AES.new(bytes(key), _AES.MODE_ECB)
    return cipher.encrypt(_pad(bytes(plaintext)))


def aes_inprocess_decrypt(ciphertext, key):
    """Decrypt bytes with AES-128-ECB + strict PKCS#7, in-process."""
    if not isinstance(ciphertext, (bytes, bytearray)):
        raise TypeError("ciphertext must be bytes")
    _check_key(key)
    cipher = _AES.new(bytes(key), _AES.MODE_ECB)
    return _unpad(cipher.decrypt(bytes(ciphertext)))


def aes_inprocess_decrypt_nopad(ciphertext, key):
    """Decrypt 16-byte block(s) with NO unpadding, in-process.

    Diagnostic use ONLY (single-block magic probing by bridge.py's
    _raw_aes128_ecb_decrypt_block). Formal decrypt MUST use
    aes_inprocess_decrypt with strict PKCS#7 validation; using this
    function on a real message would hide padding errors."""
    if not isinstance(ciphertext, (bytes, bytearray)):
        raise TypeError("ciphertext must be bytes")
    _check_key(key)
    if not ciphertext or len(ciphertext) % _BLOCK != 0:
        raise ValueError("nopad ciphertext must be a non-empty multiple of 16")
    cipher = _AES.new(bytes(key), _AES.MODE_ECB)
    return cipher.decrypt(bytes(ciphertext))
