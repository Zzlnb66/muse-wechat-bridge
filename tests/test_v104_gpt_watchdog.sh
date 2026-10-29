#!/usr/bin/env bash
# GPT ruling watchdog test: unconsumed inbox re-remind (P0 blocking).
# Offline, no real bridge, no real hook runtime.
# Acceptance (GPT ruling,裁决2):
#  - t=0: new inbox file wakes inbox_new, tracking last_wake = t0.
#  - t=5..295s (simulated by keeping tracking at t0): silent, no wake.
#  - t=300s (tracking moved 300s back): SAME file wakes again
#    (merged re-remind), tracking refreshed to now, NOT a new message.
#  - After the file leaves inbox/, it is pruned: no wake at t=600s.
#  - Suppressed ticks do NOT refresh tracking: moving tracking back
#    by 299s then ticking must stay silent, and moving it to 300s
#    must wake (i.e. the silent tick did not extend the deadline).
set -euo pipefail
SCRIPT="${1:?usage: test_v104_gpt_watchdog.sh <watchdog-script>}"
TMP="$(mktemp -d)"
export HOME="$TMP"
mkdir -p "$TMP/workspace/wechat-bridge/state" "$TMP/workspace/wechat-bridge/inbox" "$TMP/hooks/state"
cat > "$TMP/runtime.sh" <<'EOF'
hook_state_get() { if [[ -f "$HATCH_HOOK_STATE_FILE" ]]; then cat "$HATCH_HOOK_STATE_FILE"; else echo '{}'; fi; }
hook_state_set() { printf '%s\n' "$1" > "$HATCH_HOOK_STATE_FILE"; }
silent() { echo "DECISION:silent:$1"; exit 0; }
wake() { echo "DECISION:wake:$1"; exit 0; }
log() { echo "LOG:$1:$2"; }
EOF
export HATCH_HOOK_RUNTIME="$TMP/runtime.sh"
export HATCH_HOOK_STATE_FILE="$TMP/hooks/state/wechat-bridge-watchdog.json"
echo '{}' > "$HATCH_HOOK_STATE_FILE"
# Fake a running serve so ticks go through the recovery/inbox branch,
# not serve_down: a python process whose argv is bridge.py serve.
cat > "$TMP/bridge.py" <<'PY'
import time
time.sleep(30)
PY
python3 "$TMP/bridge.py" serve & FAKE_PID=$!
echo "$FAKE_PID" > "$TMP/workspace/wechat-bridge/state/serve.pid"
run_tick() { bash "$SCRIPT" 2>&1 || true; }
fail() { echo "FAIL: $1"; kill "$FAKE_PID" 2>/dev/null || true; exit 1; }
set_tracking_ago_secs() {
  python3 - "$HATCH_HOOK_STATE_FILE" "$1" <<'PY'
import json, sys, time
p, secs = sys.argv[1], int(sys.argv[2])
d = json.load(open(p))
for k in list(d.get("inbox_wake", {}).keys()):
    d["inbox_wake"][k] = int(time.time()*1000) - secs*1000
json.dump(d, open(p, "w"))
PY
}
echo '{"x":1}' > "$TMP/workspace/wechat-bridge/inbox/msg_test.json"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:inbox_new" || fail "t0 should wake inbox_new, got: $out"
jq -e '.inbox_wake["msg_test.json"]' "$HATCH_HOOK_STATE_FILE" >/dev/null || fail "tracking missing after t0 wake"
# Suppressed tick must NOT refresh tracking: assert the tracking
# value itself is unchanged across the 299s tick (not just that the
# next tick still wakes after we rewrite it to 300s, which would mask
# an erroneous refresh). This is the review-round-2 test-strength fix.
set_tracking_ago_secs 299
before_track="$(jq -c '.inbox_wake' "$HATCH_HOOK_STATE_FILE")"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:no_change" || fail "299s ago should stay silent, got: $out"
after_track="$(jq -c '.inbox_wake' "$HATCH_HOOK_STATE_FILE")"
[[ "$before_track" == "$after_track" ]] || fail "299s silent tick must NOT refresh tracking: before=$before_track after=$after_track"
# Now move to exactly 300s ago: same file must re-wake
set_tracking_ago_secs 300
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:inbox_new" || fail "300s ago should re-wake, got: $out"
echo "$out" | grep -q "msg_test.json" || fail "re-remind wake should carry msg_test.json, got: $out"
# After the file leaves inbox/, no wake at 600s (pruned)
rm "$TMP/workspace/wechat-bridge/inbox/msg_test.json"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:no_change" || fail "after file leaves, should be no_change, got: $out"
jq -e '.inbox_wake | has("msg_test.json") | not' "$HATCH_HOOK_STATE_FILE" >/dev/null || fail "tracking should be pruned after file leaves"
kill "$FAKE_PID" 2>/dev/null || true; wait "$FAKE_PID" 2>/dev/null || true
echo "test_v104_gpt_watchdog: PASS (t0 wake, 299s silent without refreshing, 300s re-remind, pruned after leave)"
rm -rf "$TMP"
