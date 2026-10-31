"""Offline tests for quote v1 (extract_quote + resolve_quote).

Runs against the fix-copy bridge.py with DONE/INBOX redirected to a
temp dir and tombstones monkeypatched. No network, no live serve.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402


def _setup(tmp):
    done = os.path.join(tmp, "done")
    inbox = os.path.join(tmp, "inbox")
    os.makedirs(done)
    os.makedirs(inbox)
    bridge.DONE = done
    bridge.INBOX = inbox
    return done, inbox


def _write(d, mid, rec):
    with open(os.path.join(d, "%s.json" % mid), "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False)


def _msg_with_ref(ref):
    return {"message_id": "999", "item_list": [
        {"type": 1, "text_item": {"text": "这是什么"}, "ref_msg": ref},
    ]}


def main():
    tmp = tempfile.mkdtemp(prefix="quote-v1-")
    done, _inbox = _setup(tmp)
    bridge.load_tombstones = lambda: {"555": {"reason": "manual"}}
    _write(done, "111", {"message_id": "111", "text": "请帮我看看这张表"})
    _write(done, "222", {"message_id": "222", "text": "", "local_only": True,
                          "local_video_count": 1})
    _write(done, "333", {"message_id": "333", "text": "",
                          "images": [{"path": "/x/a.jpg"}, {"path": "/x/b.jpg"}]})

    # 1. svr_id quote of a normal text record -> resolved text from done.
    q = bridge.resolve_quote(bridge.extract_quote(
        _msg_with_ref({"svr_id": "111"})))
    assert q["quoted_message_id"] == "111" and q["quoted_resolved"] is True
    assert q["quoted_text"] == "请帮我看看这张表", q

    # 2. svr_id quote of a local-only record -> guard only, no content,
    #    even when the payload carries inline quoted text.
    q = bridge.resolve_quote(bridge.extract_quote(_msg_with_ref({
        "svr_id": "222",
        "message_item": {"msg_id": "222", "type": 5,
                         "item_list": [{"type": 1, "text_item": {"text": "秘密"}}]},
    })))
    assert q["quoted_local_only"] is True and "quoted_text" not in q, q
    assert q["quoted_deleted"] is False, q

    # 3. Tombstoned id -> guard + deleted, inline text stripped.
    q = bridge.resolve_quote(bridge.extract_quote(_msg_with_ref({
        "svr_id": "555", "title": "摘要",
        "message_item": {"msg_id": "555", "type": 1,
                         "item_list": [{"type": 1, "text_item": {"text": "秘密"}}]},
    })))
    assert q["quoted_local_only"] is True and q["quoted_deleted"] is True, q
    assert "quoted_text" not in q and "quoted_title" not in q, q

    # 4. Image record -> count + re-attached image entries (paths are
    #    the point of re-attachment now), but no quoted text.
    q = bridge.resolve_quote(bridge.extract_quote(
        _msg_with_ref({"svr_id": "333"})))
    assert q["quoted_image_count"] == 2 and "quoted_text" not in q, q
    assert q["quoted_images"] == [{"path": "/x/a.jpg"}, {"path": "/x/b.jpg"}], q

    # 5. Inline-only quote (no bridge record) -> inline text used.
    q = bridge.resolve_quote(bridge.extract_quote(_msg_with_ref({
        "message_item": {"msg_id": "777", "type": 1,
                         "item_list": [{"type": 1, "text_item": {"text": "内联原文"}}]},
    })))
    assert q["quoted_message_id"] == "777" and q["quoted_text"] == "内联原文", q

    # 6. Unknown id, no inline -> unresolved, never guessed.
    q = bridge.resolve_quote(bridge.extract_quote(
        _msg_with_ref({"svr_id": "888"})))
    assert q["quoted_unresolved"] is True and "quoted_text" not in q, q

    # 7. No ref_msg at all -> None.
    assert bridge.extract_quote({"item_list": [
        {"type": 1, "text_item": {"text": "普通消息"}}]}) is None

    # 8. Ref type 0 (live-test blemish) on a text record -> derived 1.
    q = bridge.resolve_quote(bridge.extract_quote(_msg_with_ref({
        "svr_id": "111",
        "message_item": {"msg_id": "111", "type": 0},
    })))
    assert q["quoted_type"] == 1, q

    # 9. Image / video records -> derived 2 / 5, not the ref type.
    #    Image records also re-attach their image entries.
    _write(done, "444", {"message_id": "444", "text": "",
                          "videos": [{"path": "/x/v.mp4"}]})
    q = bridge.resolve_quote(bridge.extract_quote(
        _msg_with_ref({"svr_id": "333"})))
    assert q["quoted_type"] == 2, q
    assert q["quoted_images"] == [{"path": "/x/a.jpg"}, {"path": "/x/b.jpg"}], q
    # Local-only targets never re-attach anything.
    q = bridge.resolve_quote(bridge.extract_quote(
        _msg_with_ref({"svr_id": "222"})))
    assert "quoted_images" not in q and "quoted_text" not in q, q
    q = bridge.resolve_quote(bridge.extract_quote(
        _msg_with_ref({"svr_id": "444"})))
    assert q["quoted_type"] == 5, q

    # 10. Inline-only text quote with ref type 0 -> type 1 from inline text.
    q = bridge.resolve_quote(bridge.extract_quote(_msg_with_ref({
        "message_item": {"msg_id": "777", "type": 0,
                         "item_list": [{"type": 1, "text_item": {"text": "内联原文"}}]},
    })))
    assert q["quoted_type"] == 1 and q["quoted_text"] == "内联原文", q

    print("quote v1: 10/10 PASS")


if __name__ == "__main__":
    main()
