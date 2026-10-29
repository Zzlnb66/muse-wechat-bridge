#!/usr/bin/env bash
# wechat-bridge-watchdog.sh, v1.0.4 review patch (P0: dedup + snapshot + expired-wake).
# Contract verified against ~/hooks/runtime/hatch_hook_runtime.sh:
# wake() and silent() print HATCH_HOOK_RESULT and EXIT the script, so
# every terminating branch MUST log its snapshot BEFORE calling wake
# or silent. Nothing after a wake/silent call can be relied on.
# Changes vs the pre-review v1.0.4 script:
#  - Unified snapshot: every terminating branch logs the same fields
#    (checked_at, serve_running, pid, check_reason, proc_exists,
#    cmdline_first, boot_id, event_id, wake_kind, new_inbox,
#    seconds_since_last_wake, needs_relogin) via one snapshot_json()
#    helper, before wake/silent. No branch wakes with a bare payload.
#  - PID field name is unified to "pid" everywhere (the old script
#    mixed "pid" and "pid_file" across branches).
#  - Read failure is not folded into "empty": cat failure on an
#    existing PID file is pid_file_unreadable, not pid_file_empty.
#  - Initial/retry down wakes that also carry new inbox files now
#    mark those files seen BEFORE waking (same semantics as the
#    normal inbox_new branch: seen = "already included in a wake",
#    NOT "already processed by the Worker"). Without this, the same
#    file re-triggered inbox_new on the next tick.
#  - Same serve_down outage produces ONE initial wake. Later ticks
#    only log (serve_down_suppressed) until RETRY_SECS (300s) after
#    the last wake, when one retry wake is sent. Recovery closes the
#    event silently, with no extra user notification for the old wake.
#  - A wake that is never delivered / a Worker that never runs does
#    NOT permanently suppress recovery: the event stays in hook
#    state, and after RETRY_SECS the next tick wakes again.
#    hook_state_set is skipped on dry runs by the runtime, so a dry
#    run never consumes a detection or a retry either.
#  - If boot_id changes, a leftover event from the previous boot is
#    treated as a NEW event and wakes immediately, instead of
#    inheriting the old event's last_wake_ms.
#  - inbox_new is NOT suppressed by serve_down dedup: if there are
#    new inbox files, they still wake (with serve_running:false in
#    the snapshot, so the Worker restarts serve first).
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE_DIR="$HOME/workspace/wechat-bridge"
INBOX_DIR="$BRIDGE_DIR/inbox"
NEEDS_RELOGIN="$BRIDGE_DIR/state/NEEDS_RELOGIN"
PID_FILE="$BRIDGE_DIR/state/serve.pid"
RETRY_SECS=300

checked_at="$(date '+%Y-%m-%dT%H:%M:%S%z')"
now_ms="$(date +%s%3N 2>/dev/null || echo $(( $(date +%s) * 1000 )))"
boot_id="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null | tr -d '[:space:]' || true)"
[[ -n "$boot_id" ]] || boot_id="unknown"

# Unified snapshot. $1 = wake_kind, $2 = event_id (may be empty),
# $3 = seconds_since_last_wake (integer). serve_running, pid,
# check_reason, proc_exists, cmdline_first are globals set below.
# For the needs_relogin branch these globals are still at their
# defaults (false / "" / pid_file_missing / false / ""), which is
# itself the correct snapshot for that branch.
snapshot_json() {
  local wake_kind="$1" event_id_arg="$2" secs_arg="$3"
  jq -cn \
    --arg checked_at "$checked_at" \
    --argjson serve_running "$serve_running" \
    --arg pid "$serve_pid" \
    --arg check_reason "$check_reason" \
    --argjson proc_exists "$proc_exists" \
    --arg cmdline_first "$cmdline_first" \
    --arg boot_id "$boot_id" \
    --arg event_id "$event_id_arg" \
    --arg wake_kind "$wake_kind" \
    --argjson new_inbox "$new_json" \
    --argjson seconds_since_last_wake "$secs_arg" \
    --argjson needs_relogin "$needs_relogin_flag" \
    '{checked_at:$checked_at, serve_running:$serve_running, pid:$pid, check_reason:$check_reason, proc_exists:$proc_exists, cmdline_first:$cmdline_first, boot_id:$boot_id, event_id:$event_id, wake_kind:$wake_kind, new_inbox:$new_inbox, seconds_since_last_wake:$seconds_since_last_wake, needs_relogin:$needs_relogin}'
}

# Defaults before liveness is computed (used by the relogin branch).
serve_running="false"
serve_pid=""
check_reason="pid_file_missing"
cmdline_first=""
proc_exists="false"
needs_relogin_flag="false"
new_json="[]"

# 1. Token expired: never relaunch, stay silent, with snapshot.
if [[ -f "$NEEDS_RELOGIN" ]]; then
  needs_relogin_flag="true"
  log "wechat bridge needs relogin, not relaunching" "$(snapshot_json "none" "" 0)"
  silent "needs_relogin"
fi

# 2. Serve liveness, robustly: PID file ONLY, plus /proc/<pid>/cmdline
# must be a python process whose argv is bridge.py serve.
# Do NOT use pgrep -f "bridge.py serve": that also matches bash shells
# whose command string merely contains those words (e.g. a worker
# checking pgrep), giving a false "running" result after a real kill.
if [[ -f "$PID_FILE" ]] && [[ ! -r "$PID_FILE" ]]; then
  check_reason="pid_file_unreadable"
elif [[ -f "$PID_FILE" ]]; then
  cat_out="$(cat "$PID_FILE" 2>/dev/null || echo "__CAT_FAILED__")"
  if [[ "$cat_out" == "__CAT_FAILED__" ]]; then
    check_reason="pid_file_unreadable"
  else
    serve_pid="$(printf '%s' "$cat_out" | tr -d '[:space:]')"
    if [[ -z "$serve_pid" ]]; then
      check_reason="pid_file_empty"
    elif [[ ! -f "/proc/$serve_pid/cmdline" ]]; then
      check_reason="proc_missing"
    else
      proc_exists="true"
      cmdline="$(tr '\0' ' ' < "/proc/$serve_pid/cmdline" 2>/dev/null || true)"
      cmdline_first="$(printf '%s' "$cmdline" | awk '{print $1}')"
      case "$cmdline" in
        python*bridge.py\ serve*|python3*bridge.py\ serve*)
          if kill -0 "$serve_pid" 2>/dev/null; then
            serve_running="true"
            check_reason="ok"
          else
            check_reason="kill_failed"
          fi
          ;;
        *)
          check_reason="proc_cmdline_mismatch"
          ;;
      esac
    fi
  fi
else
  check_reason="pid_file_missing"
fi

# 3. Hook state: seen inbox list + one serve_down event at most.
state="$(hook_state_get)"
seen_list="$(printf '%s' "$state" | jq -r '.seen // [] | .[]' 2>/dev/null | grep -v '^$' || true)"
event_id="$(printf '%s' "$state" | jq -r '.serve_down_event.event_id // empty' 2>/dev/null || true)"
event_boot="$(printf '%s' "$state" | jq -r '.serve_down_event.boot_id // empty' 2>/dev/null || true)"
event_first_ms="$(printf '%s' "$state" | jq -r '.serve_down_event.first_seen_ms // 0' 2>/dev/null || true)"
event_last_wake_ms="$(printf '%s' "$state" | jq -r '.serve_down_event.last_wake_ms // 0' 2>/dev/null || true)"
event_wake_count="$(printf '%s' "$state" | jq -r '.serve_down_event.wake_count // 0' 2>/dev/null || true)"

# GPT ruling (blocking, P0): inbox re-remind, NOT permanent seen.
# Old semantics: a file in `seen` never woke again, even if the wake
# was lost and the file stayed in inbox/ forever while serve was
# alive. New semantics: hook state keeps inbox_wake {file: last_wake_ms}.
# A file wakes when it is first seen OR when it is still in inbox/
# and >=300s have passed since its last wake. Tracking is updated
# ONLY when a wake is actually sent (mark_wake, below), never on a
# suppressed tick, so time is never silently extended tick by tick.
# Files that leave inbox/ are pruned from tracking when state is saved.
all_inbox_files=()
if [[ -d "$INBOX_DIR" ]]; then
  for f in "$INBOX_DIR"/*.json; do
    [[ -e "$f" ]] || continue
    all_inbox_files+=("$(basename "$f")")
  done
fi
all_inbox_json="$(jq -cn --args '$ARGS.positional' "${all_inbox_files[@]}")"
inbox_wake_json="$(printf '%s' "$state" | jq -c '.inbox_wake // {}' 2>/dev/null || echo '{}')"
# Due files: first seen, or still present and due for re-remind.
# `seen` is kept for backward compatibility only; the wake decision
# uses inbox_wake, not seen alone, so an old seen entry without a
# tracking entry is treated as due (one extra wake after upgrade).
new_files=()
for _base in "${all_inbox_files[@]}"; do
  _last="$(printf '%s' "$inbox_wake_json" | jq -r --arg k "$_base" '.[$k] // 0' 2>/dev/null || echo 0)"
  if [[ "$_last" == "0" ]] || [[ $(( (now_ms - _last) / 1000 )) -ge "$RETRY_SECS" ]]; then
    new_files+=("$_base")
  fi
done

# jq --args correctly gives [] for an empty array, unlike printf|jq -s.
new_json="$(jq -cn --args '$ARGS.positional' "${new_files[@]}")"

save_state() {
  # $1 = serve_down_event JSON object, or null to clear it.
  local event_json="$1"
  local seen_json
  if [[ -z "$(printf '%s' "$seen_list" | tr -d '[:space:]')" ]]; then
    seen_json='[]'
  else
    seen_json="$(printf '%s
' "$seen_list" | grep -v '^$' | sort -u | jq -R . | jq -s .)"
  fi
  # Prune tracking to files still in inbox/ at save time.
  inbox_wake_json="$(jq -cn --argjson track "$inbox_wake_json" --argjson inbox "$all_inbox_json" '$track | with_entries(select(.key as $k | $inbox | index($k)))')"
  hook_state_set "$(jq -cn --argjson seen "$seen_json" --argjson event "$event_json" --argjson track "$inbox_wake_json" '{seen:$seen, serve_down_event:$event, inbox_wake:$track}')"
}

mark_seen() {
  # Only call immediately before a wake. Updates seen AND tracking
  # for exactly the files in this wake, with last_wake_ms = now_ms.
  # Empty new_files is a no-op (grep empty-input + pipefail + set -e
  # would otherwise kill the script before it can wake).
  if [[ "${#new_files[@]}" -eq 0 ]]; then
    return 0
  fi
  seen_list="$(printf '%s
' "$seen_list" "${new_files[@]}" | grep -v '^$' | sort -u)"
  for _base in "${new_files[@]}"; do
    inbox_wake_json="$(printf '%s' "$inbox_wake_json" | jq -c --arg k "$_base" --argjson v "$now_ms" '.[$k] = $v')"
  done
}

build_event_json() {
  jq -cn --arg id "$event_id" --arg boot "$boot_id" --argjson first "$event_first_ms" --argjson last "$event_last_wake_ms" --argjson count "$event_wake_count" --arg reason "$check_reason" '{event_id:$id, boot_id:$boot, first_seen_ms:$first, last_wake_ms:$last, wake_count:$count, last_reason:$reason}'
}

# 4. Recovery closes the event silently (no extra user notification
# for an old wake that arrives after recovery).
if [[ "$serve_running" == "true" ]]; then
  if [[ -n "$event_id" ]]; then
    save_state "null"
  fi
  if [[ "${#new_files[@]}" -gt 0 ]]; then
    mark_seen
    save_state "null"
    log "wechat bridge new inbox" "$(snapshot_json "inbox_new" "$event_id" 0)"
    wake "inbox_new" "$(snapshot_json "inbox_new" "$event_id" 0)"
  fi
  # GPT ruling: prune tracking for files that left inbox/, even on
  # a silent no-change tick, so a stale entry cannot suppress a
  # future file with the same name and cannot accumulate forever.
  if [[ "$(printf '%s' "$inbox_wake_json" | jq -r --argjson inbox "$all_inbox_json" 'keys[] | select(. as $k | $inbox | index($k) | not) ' 2>/dev/null | head -1)" != "" ]]; then
    save_state "null"
  fi
  log "wechat bridge ok, no change" "$(snapshot_json "none" "$event_id" 0)"
  silent "no_change"
fi

# 5. Serve is down: decide initial wake vs suppressed tick vs retry.
# A boot change makes a leftover event a new event (wake immediately).
if [[ -z "$event_id" ]] || [[ "$event_boot" != "$boot_id" ]]; then
  event_id="down-$(date +%s)-$$"
  event_first_ms="$now_ms"
  event_last_wake_ms="$now_ms"
  event_wake_count=1
  mark_seen
  save_state "$(build_event_json)"
  log "wechat bridge serve down (initial wake)" "$(snapshot_json "initial" "$event_id" 0)"
  wake "serve_down" "$(snapshot_json "initial" "$event_id" 0)"
fi

elapsed_since_wake=$(( (now_ms - event_last_wake_ms) / 1000 ))
if [[ "$elapsed_since_wake" -ge "$RETRY_SECS" ]]; then
  event_last_wake_ms="$now_ms"
  event_wake_count=$((event_wake_count + 1))
  mark_seen
  save_state "$(build_event_json)"
  log "wechat bridge serve down (retry wake)" "$(snapshot_json "retry" "$event_id" "$elapsed_since_wake")"
  wake "serve_down" "$(snapshot_json "retry" "$event_id" "$elapsed_since_wake")"
fi

# Suppressed tick: same outage, within the retry window. Do NOT wake
# for serve_down, but do NOT lose inbox_new either: if there are new
# inbox files, wake for them (Worker restarts serve first, and the
# down event stays recorded for the next retry tick).
if [[ "${#new_files[@]}" -gt 0 ]]; then
  mark_seen
  save_state "$(build_event_json)"
  log "wechat bridge new inbox while serve down (down wake suppressed)" "$(snapshot_json "suppressed_down_with_inbox" "$event_id" "$elapsed_since_wake")"
  wake "inbox_new" "$(snapshot_json "suppressed_down_with_inbox" "$event_id" "$elapsed_since_wake")"
fi

log "wechat bridge serve down suppressed" "$(snapshot_json "suppressed" "$event_id" "$elapsed_since_wake")"
silent "serve_down_suppressed"
