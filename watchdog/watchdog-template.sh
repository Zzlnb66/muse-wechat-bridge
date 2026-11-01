#!/bin/sh
# 妈妈桥看门狗模板（只管拉起逻辑，不管 prompt）
#
# 来源：按现行生产看门狗的通用逻辑重写（serve.pid + /proc/<pid>/cmdline
# 判活，不用 pgrep；NEEDS_RELOGIN 存在绝不重拉；死则直拉重拉）。
# 本模板不是生产脚本的复制件；生产脚本的任何内部标识、prompt 文本均不在此。
#
# 使用方法（妈妈号）：
#   1. 把 BRIDGE_ROOT 改成妈妈桥根目录的真实路径（或以环境变量传入）。
#   2. 把本文件作为 hook 的轮询脚本（参考轮询 5 秒、超时 600 秒），
#      或由妈妈号的 hook 定义按自己的机制调用。
#   3. 妈妈号的 hook 定义与 Worker prompt 由妈妈号自己建（素材见包内
#      worker-prompt-assembly.md）；本模板只负责“serve 死了就拉起来”，
#      不读、不改、不生成任何 prompt。
#   4. serve 本身持有 <BRIDGE_ROOT>/state/serve.lock 的 flock；本脚本不
#      另外加锁、不杀进程、不碰 session、不发消息。
#
# 退出码：0 = serve 活着，或 NEEDS_RELOGIN 存在（故意不拉），或已拉起；
#         2 = BRIDGE_ROOT 未设置或根目录下没有 bridge.py（配置错）。

# ---- 根目录：部署时改成真实路径，或以环境变量 BRIDGE_ROOT 传入 ----
BRIDGE_ROOT="${BRIDGE_ROOT:-}"

if [ -z "$BRIDGE_ROOT" ] || [ ! -f "$BRIDGE_ROOT/bridge.py" ]; then
    echo "watchdog-template: BRIDGE_ROOT unset or bridge.py missing: $BRIDGE_ROOT" >&2
    exit 2
fi

PID_FILE="$BRIDGE_ROOT/state/serve.pid"
NEEDS_RELOGIN="$BRIDGE_ROOT/state/NEEDS_RELOGIN"
LOG_FILE="$BRIDGE_ROOT/state/serve.log"

# NEEDS_RELOGIN 存在：会话过期，绝不重拉，等用户重新登录。
if [ -f "$NEEDS_RELOGIN" ]; then
    exit 0
fi

# 判活：pid 文件存在、pid 为数字、/proc/<pid>/cmdline 存在且同时含
# bridge.py 与 serve（cmdline 以 NUL 分隔，先转空格再匹配）。
is_alive() {
    [ -f "$PID_FILE" ] || return 1
    pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
    case "$pid" in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ -r "/proc/$pid/cmdline" ] || return 1
    cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null) || return 1
    case "$cmdline" in
        *bridge.py*serve*) return 0 ;;
        *) return 1 ;;
    esac
}

if is_alive; then
    exit 0
fi

# 死了（或 pid 陈旧、cmdline 不符）：直拉一个新 serve，后台 detached，
# 日志追加到 state/serve.log（不覆盖旧日志）。
cd "$BRIDGE_ROOT" || exit 2
setsid nohup python3 bridge.py serve >>"$LOG_FILE" 2>&1 < /dev/null &
exit 0
