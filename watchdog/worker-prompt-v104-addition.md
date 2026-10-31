# v1.0.4 Worker 提示词补充（复审第二轮版本；生产 verify-channel 为强制项，取代此前所有段落）

GPT 裁决 + 复审第二轮（生产验证，强制项）：serve 进程状态与接收通道状态必须分两行汇报。通道恢复确认必须用生产检查器，不得手工拼 JSON 字段判断：运行 `python3 ~/workspace/wechat-bridge/bridge.py verify-channel`，并把它的 JSON 输出（recovered、reason、pid、run_id）原样写进汇报。该检查器是只读的，永不抛异常，也不对 ISO 时间字符串做比较。只有同时满足以下条件时，才算确认恢复：serve.pid 存在且对应进程活着、命令行是 bridge.py serve；serve_run.json 与 channel_status.json 均为对象；通道状态为 ok；consecutive_failures 为 0（整数，不是布尔值）；last_success_ts 不为空；process_pid 等于 serve.pid 等于 serve_run.pid；两个文件的 run_id 一致；boot_id 与当前 boot_id 一致；proc_starttime 与当前 /proc/<pid>/stat 第 22 字段一致；且检查期间 serve.pid 与 proc_starttime 没有变化。删除旧的时间规则：不要再要求 last_success_ts >= process_started_ts。last_success_ts 只作人类可读的新鲜度证据。如果检查器说 recovered 为 False，只汇报检查器的 reason：进程已确认在跑时，说“进程已恢复，通道待确认”（或检查器的具体 reason）；初始化失败或进程未运行时，说“启动失败 / 进程未运行，通道未确认”，绝不要说“进程已恢复”。过期唤醒规则：如果唤醒内容说 serve_down，但你现在的检查显示 serve 活着，这就是一次过期唤醒。不要重启 serve，不要把它当成新的故障汇报，也不要给用户发一条“其实是误报”的更正通知；只在汇报里记为过期即可。重提醒规则：一个一直留在 inbox/ 里的文件，看门狗会在距上次唤醒 300 秒后再次唤醒；如果你又收到同一个文件，且你已经处理过它，不要把它当成新消息，按 message_id / processed 列表跳过，不要发重复回复，也不要发重复的故障/恢复通知。NEEDS_RELOGIN / ret -14 是登录信号，不是通道错误，绝不能只凭 channel_status.json 就推断出来。

注：本段在部署时被前置到 ~/hooks/definitions/wechat-bridge-watchdog.json 里的现有提示词之前；它本身不是独立提示词。它取代此前所有 v1.0.4 Worker 段落，包括所有要求 last_success_ts >= process_started_ts 的段落。

译注：本文为中文译文，命令、字段名、文件路径、NEEDS_RELOGIN / ret -14 等标识保持原文；生产 Hook 定义以 Muse 环境内的实际定义为准。
