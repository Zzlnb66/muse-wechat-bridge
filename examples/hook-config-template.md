# Hook / 看门狗配置模板（脱敏，D3 补齐）

> 本模板描述实际行为，不包含真实 Hook 定义、真实路径以外的凭据。
> 看门狗本身属于 Muse 环境配置，读者需要在自己的 Muse 环境里重建。

## 轮询

- 间隔：5 秒（实际项目里 Muse Hook 的系统最低间隔）。
- 检查对象：`inbox/*.json` 新文件、`state/serve.pid` 存活、`state/NEEDS_RELOGIN` 是否存在。

## 存活判断（硬规则）

只认三项同时成立才算运行中：
1. `state/serve.pid` 存在；
2. `/proc/<pid>/cmdline` 首词为 `python`/`python3`，参数为 `bridge.py serve`；
3. `kill -0 <pid>` 成功。

不要只用 `pgrep -f "bridge.py serve"` 判断（会误匹配含该字符串的 shell）。

## 发现新消息时

- 唤醒 Worker，Worker 按 `examples/worker-prompt-template.md` 处理；
- Worker 处理完后，桥接的发送线程/循环负责把 `outbox/pending/` 排空到微信。

## 发现 serve 死亡时

- 如果 `NEEDS_RELOGIN` 存在：**不要拉起**，报告需要重新登录，停止。
- 否则：用 `setsid nohup python3 <桥接目录>/bridge.py serve >> <桥接目录>/state/serve.log 2>&1 < /dev/null &` 拉起，记录旧 PID 与新 PID，等待 3 秒后按存活规则复核，并确认 `serve.log` 在增长。

## 队列输入输出样例（脱敏）

`inbox/<message_id>.json` 示例：

```json
{
  "message_id": "EXAMPLE_MESSAGE_ID",
  "text": "EXAMPLE_USER_TEXT",
  "from_user_id": "EXAMPLE_USER_ID",
  "context_token": "EXAMPLE_CONTEXT_TOKEN",
  "ts": 0,
  "images": [
    {"path": "media/inbound/EXAMPLE_MESSAGE_ID_0.jpg", "format": "jpg", "width": 800, "height": 600, "size": 12345, "index": 0}
  ]
}
```

`outbox/pending/<message_id>.json` 示例：

```json
{
  "text": "EXAMPLE_REPLY_TEXT",
  "to_user_id": "EXAMPLE_USER_ID",
  "context_token": "EXAMPLE_CONTEXT_TOKEN",
  "client_id": "EXAMPLE_UUID_HEX"
}
```

## 配置步骤（给没有参与过项目的人）

1. 准备一台能运行 Python 的 Linux 环境，安装 Pillow、openssl、curl；
2. 把 `bridge.py` 放进一个目录，运行它创建 `state/`、`inbox/`、`done/`、`outbox/`、`media/`、`failures/`；
3. 按本地登录流程取得自己的 `session.json`（0600），不要用别人的、不要提交；
4. 在 Muse 环境里按本模板和 Worker 模板创建看门狗与 Worker；
5. 启动 `python3 bridge.py serve`，确认 `serve.log` 有 `lock acquired`，再在微信里发一条测试消息，确认能收到回复且没有重复。
