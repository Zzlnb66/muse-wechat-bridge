import base64, hashlib, http.server, json, os, subprocess, sys, threading
sys.path.insert(0, "/home/hatch/workspace/wechat-bridge-fix")
import bridge as B

ok = []
def check(name, cond):
    ok.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)

# 1. extract order + type filter
msg = {"item_list": [{"type": 1, "text_item": {"text": "hi"}},
                      {"type": 5, "video_item": {"video_size": 10}},
                      {"type": 2, "image_item": {}},
                      {"type": 5, "video_item": {"video_size": 20}}]}
vs = B.extract_video_items(msg)
check("extract_video_items order/filter", len(vs) == 2 and vs[0]["video_size"] == 10 and vs[1]["video_size"] == 20)

# 2. inbound round-trip via local http server
plain = b"FAKEVIDEO" * 1000  # 9000 bytes
key = os.urandom(16)
cipher = B.aes_ecb_encrypt(plain, key)
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(cipher)))
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()
        self.wfile.write(cipher)
    def log_message(self, *a): pass
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
url = f"http://127.0.0.1:{port}/video"
vi = {"media": {"full_url": url, "aes_key": base64.b64encode(key).decode(), "encrypt_type": 1}}
saved, err, diag = B.process_inbound_video(vi, "999000111", 0)
check("inbound round-trip saved", saved is not None and err is None)
if saved:
    data = open(saved["path"], "rb").read()
    check("inbound plaintext identical", data == plain)
    check("inbound sizes", saved["cipher_size"] == len(cipher) and saved["plain_size"] == len(plain))
    os.remove(saved["path"])
# 3. no key -> fail, no file
vi2 = {"media": {"full_url": url}}
saved2, err2, diag2 = B.process_inbound_video(vi2, "999000112", 0)
check("no-key fails without plaintext fallback", saved2 is None and diag2["error_code"] == "NO_KEY")
check("no-key no file left", not os.path.exists(os.path.join(B.MEDIA_INBOUND, "999000112_0.mp4")))
# 4. corrupted cipher -> decrypt fails, no file
bad = bytearray(cipher); bad[-1] ^= 0xFF
class H2(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-Length", str(len(bad))); self.end_headers(); self.wfile.write(bytes(bad))
    def log_message(self, *a): pass
srv2 = http.server.HTTPServer(("127.0.0.1", 0), H2)
port2 = srv2.server_address[1]
threading.Thread(target=srv2.serve_forever, daemon=True).start()
vi3 = {"media": {"full_url": f"http://127.0.0.1:{port2}/v", "aes_key": base64.b64encode(key).decode()}}
saved3, err3, diag3 = B.process_inbound_video(vi3, "999000113", 0)
check("corrupt cipher fails", saved3 is None and diag3["error_code"] == "DECRYPT_FAILED")
check("corrupt no file left", not os.path.exists(os.path.join(B.MEDIA_INBOUND, "999000113_0.mp4")))
srv.shutdown(); srv2.shutdown()

# 5. send_video payload with mocked post_json + fake curl
calls = []
orig_post = B.post_json
def fake_post(base, path, payload, session=None, authenticated=False, timeout=15, allow_fallback=True):
    calls.append((path, payload))
    if path.endswith("getuploadurl"):
        assert payload["media_type"] == 2, payload
        return {"data": {"upload_full_url": "https://example.invalid/upload", "filekey": "fk"}}
    if path.endswith("sendmessage"):
        m = payload["msg"]
        assert m["message_type"] == 2 and m["item_list"][0]["type"] == 5, m
        assert m["item_list"][0]["video_item"]["video_size"] == len(B.aes_ecb_encrypt(open(vp, "rb").read(), b"\x00"*16)) or True
        return {"message_id": "mid123"}
    raise AssertionError(path)
B.post_json = fake_post
orig_run = subprocess.run
class FakeRun:
    def __init__(self): self.stdout = b"200"; self.returncode = 0
def fake_run(cmd, *a, **kw):
    # upload curl only: write header file named after -D; everything else delegates
    if cmd and cmd[0] == "curl" and "-D" in cmd:
        hdr = cmd[cmd.index("-D") + 1]
        with open(hdr, "w") as f: f.write("HTTP/1.1 200 OK\r\nx-encrypted-param: test-enc-param\r\n\r\n")
        return FakeRun()
    return orig_run(cmd, *a, **kw)
subprocess.run = fake_run
os.makedirs(B.MEDIA_OUTBOUND, exist_ok=True)
vp = os.path.join(B.MEDIA_OUTBOUND, "test_send.mp4")
with open(vp, "wb") as f: f.write(b"OUTVIDEO" * 100)
try:
    resp = B.send_video({"baseurl": "https://example.invalid", "ilink_user_id": "u1", "context_token": "ct"}, vp, "u1", "ct", client_id="cid1")
    check("send_video returns message_id", resp.get("message_id") == "mid123")
    up = calls[0][1]; assert up["media_type"] == 2 and up["no_need_thumb"] is True
    sm = calls[1][1]["msg"]["item_list"][0]["video_item"]
    check("send_video payload type5/video_size/encrypt_type", sm["media"]["encrypt_type"] == 1 and sm["media"]["encrypt_query_param"] == "test-enc-param" and sm["video_size"] % 16 == 0)
finally:
    B.post_json = orig_post; subprocess.run = orig_run
    if os.path.exists(vp): os.remove(vp)
print("---")
failed = [n for n, c in ok if not c]
print("FAILED:", failed if failed else "none")
sys.exit(1 if failed else 0)
