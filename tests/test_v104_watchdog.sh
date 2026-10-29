#!/usr/bin/env bash
# v1.0.4 watchdog dedup + snapshot test, review patch.
# Offline, no real bridge, no real hook runtime.
# Simulates the hook runtime (hook_state_get/set, silent/wake/log) and
# runs the real watchdog script with HOME pointed at a temp dir.
# The stub log() MUST preserve the JSON second argument, unlike the
# pre-review stub that dropped it: otherwise snapshot fields cannot
# be asserted at all (review Finding 2).
# Acceptance:
#  - continuous absence (3 rapid ticks) -> exactly ONE initial wake
#  - every terminating branch logs a full snapshot: checked_at,
#    serve_running, pid, check_reason, proc_exists, cmdline_first,
#    boot_id, event_id, wake_kind, new_inbox, seconds_since_last_wake
#  - initial/retry down with inbox marks that file seen BEFORE waking,
#    so the next tick does not re-wake it as new (Finding 3)
#  - inbox_new still wakes while serve is down (suppressed_down_with_inbox)
#  - recovery clears the event silently, and the next down is a NEW
#    initial wake
#  - boot_id change with last_wake NOT expired -> NEW initial wake
#    immediately, new event_id, wake_count 1 (not a suppressed tick)
#  - 300s boundary: last_wake 299s ago -> suppressed, same event_id;
#    last_wake 300s ago -> retry, same event_id, wake_count +1
#  - a wake that is never delivered does not permanently suppress:
#    advancing last_wake_ms past RETRY_SECS (300s) makes the next tick
#    wake again (retry). This is simulated by editing hook state, the
#    same file the script reads, not by waiting 300s.
set -euo pipefail
SCRIPT="${1:?usage: test_v104_watchdog.sh <watchdog-script>}"
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

run_tick() {
  bash "$SCRIPT" 2>&1 || true
}
fail() { echo "FAIL: $1"; exit 1; }
assert_snapshot() {
  # $1 = tick output, $2 = expected wake_kind substring
  local out="$1" kind="$2"
  for field in '"checked_at"' '"serve_running"' '"pid"' '"check_reason"' '"proc_exists"' '"cmdline_first"' '"boot_id"' '"event_id"' '"wake_kind"' '"new_inbox"' '"seconds_since_last_wake"'; do
    echo "$out" | grep -q "$field" || fail "snapshot missing $field for $kind, got: $out"
  done
  echo "$out" | grep -q "\"wake_kind\":\"$kind\"" || fail "snapshot wake_kind should be $kind, got: $out"
}
set_last_wake_ago_secs() {
  python3 - "$HATCH_HOOK_STATE_FILE" "$1" <<'PY'
import json, sys, time
p, secs = sys.argv[1], int(sys.argv[2])
d = json.load(open(p))
d["serve_down_event"]["last_wake_ms"] = int(time.time()*1000) - secs*1000
json.dump(d, open(p, "w"))
PY
}

# Tick 1: serve down, no inbox -> initial wake, full snapshot
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:serve_down" || fail "tick1 should wake serve_down, got: $out"
assert_snapshot "$out" "initial"
# Tick 2-3: same outage, within retry window -> silent, full snapshot
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:serve_down_suppressed" || fail "tick2 should be suppressed, got: $out"
assert_snapshot "$out" "suppressed"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:serve_down_suppressed" || fail "tick3 should be suppressed, got: $out"
# inbox_new while down -> wake inbox_new, down event stays recorded
echo '{"x":1}' > "$TMP/workspace/wechat-bridge/inbox/111.json"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:inbox_new" || fail "inbox while down should wake inbox_new, got: $out"
assert_snapshot "$out" "suppressed_down_with_inbox"
jq -e '.serve_down_event.event_id' "$HATCH_HOOK_STATE_FILE" >/dev/null || fail "down event lost after inbox wake"
# Finding 3, suppressed path: the same file must NOT re-wake next tick
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:serve_down_suppressed" || fail "same inbox file should not re-wake, got: $out"
# Recovery: fake a python process whose argv is bridge.py serve
cat > "$TMP/bridge.py" <<'PY'
import time
time.sleep(30)
PY
python3 "$TMP/bridge.py" serve & FAKE_PID=$!
echo "$FAKE_PID" > "$TMP/workspace/wechat-bridge/state/serve.pid"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:no_change" || fail "recovery tick should be no_change, got: $out"
assert_snapshot "$out" "none"
jq -e '.serve_down_event == null' "$HATCH_HOOK_STATE_FILE" >/dev/null || fail "down event not cleared on recovery"
kill "$FAKE_PID" 2>/dev/null || true; wait "$FAKE_PID" 2>/dev/null || true
# Next down after recovery -> NEW initial wake, not suppressed, with snapshot
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:serve_down" || fail "down after recovery should be new initial wake, got: $out"
assert_snapshot "$out" "initial"
# Finding 3, initial path: reset state, pre-place an inbox file, first
# tick wakes serve_down carrying it, second tick must NOT re-wake it.
echo '{}' > "$HATCH_HOOK_STATE_FILE"
echo '{"x":2}' > "$TMP/workspace/wechat-bridge/inbox/222.json"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:serve_down" || fail "initial down with inbox should wake serve_down, got: $out"
assert_snapshot "$out" "initial"
echo "$out" | grep -q "222.json" || fail "initial wake snapshot should carry new_inbox 222.json, got: $out"
jq -e '.seen | index("222.json")' "$HATCH_HOOK_STATE_FILE" >/dev/null || fail "222.json should be marked seen after initial down wake"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:serve_down_suppressed" || fail "222.json should not re-wake next tick, got: $out"
# Boot change with last_wake NOT expired -> NEW initial wake immediately
old_event="$(jq -r '.serve_down_event.event_id' "$HATCH_HOOK_STATE_FILE")"
python3 - "$HATCH_HOOK_STATE_FILE" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p))
d["serve_down_event"]["boot_id"] = "different-boot-for-test"
json.dump(d, open(p, "w"))
PY
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:serve_down" || fail "boot change should wake immediately, got: $out"
assert_snapshot "$out" "initial"
new_event="$(jq -r '.serve_down_event.event_id' "$HATCH_HOOK_STATE_FILE")"
[[ "$new_event" != "$old_event" ]] || fail "boot change should create a new event_id"
[[ "$(jq -r '.serve_down_event.wake_count' "$HATCH_HOOK_STATE_FILE")" == "1" ]] || fail "boot change event wake_count should be 1"
# 300s boundary: 299s ago -> suppressed, same event_id; 300s ago -> retry
event_before="$(jq -r '.serve_down_event.event_id' "$HATCH_HOOK_STATE_FILE")"
set_last_wake_ago_secs 299
out="$(run_tick)"; echo "$out" | grep -q "DECISION:silent:serve_down_suppressed" || fail "299s ago should still be suppressed, got: $out"
[[ "$(jq -r '.serve_down_event.event_id' "$HATCH_HOOK_STATE_FILE")" == "$event_before" ]] || fail "299s tick should keep same event_id"
set_last_wake_ago_secs 300
count_before="$(jq -r '.serve_down_event.wake_count' "$HATCH_HOOK_STATE_FILE")"
out="$(run_tick)"; echo "$out" | grep -q "DECISION:wake:serve_down" || fail "300s ago should retry, got: $out"
assert_snapshot "$out" "retry"
[[ "$(jq -r '.serve_down_event.event_id' "$HATCH_HOOK_STATE_FILE")" == "$event_before" ]] || fail "retry should keep same event_id"
[[ "$(jq -r '.serve_down_event.wake_count' "$HATCH_HOOK_STATE_FILE")" == "$((count_before + 1))" ]] || fail "retry should increment wake_count"
echo "test_v104_watchdog: PASS (snapshot on all branches, inbox seen on initial/retry, boot change, 300s boundary, recovery clears, retry)"
rm -rf "$TMP"
