# 架构

## 数据流图（文字版）

```
微信用户私聊
  │  iLink getupdates（POST /ilink/bot/getupdates）
  ▼
bridge.py serve（云端虚拟机，flock 单例）
  │  白名单：group_id 非空忽略；from_user_id 必须精确等于 session ilink_user_id
  │  幂等（修复版 R2）：每条消息只有在其 inbox 文件或直接回复文件落盘后才记 processed；整批游标只在整批循环结束后保存；崩溃后重取整批，已处理消息靠磁盘 processed 去重
  ├─ /ping、/status：serve 只写 outbox/pending（修复版 R8，只排队，不直接走网络），由发送线程发送，不进 inbox
  ├─ 语音 type=3：extract_voice 取 voice_item.text，无转写则直接回“请改发文字”，不进 inbox
  ├─ 图片 type=2：process_inbound_image 下载/解密/校验，落盘 media/inbound/
  └─ 文字 / 带图文字：写 inbox/<message_id>.json（含 images、image_errors、image_reference）
  ▼
Muse 看门狗 Hook（Muse 环境，5 秒轮询）
  │  发现新 inbox 文件，唤醒 Worker
  ▼
Worker（Muse 代理，不是本仓库代码）
  │  读 done/ + outbox/sent/ 最近 10 轮（按 message_id 升序）
  │  读图片文件本身（不是只看文件名）
  │  生成回复，写 outbox/pending/<message_id>.json（text + to_user_id + context_token + 新 client_id；
  │  发图时 attachments 指向 media/outbound/ 里的真实文件）
  │  把 inbox 文件移到 done/
  ▼
bridge.py 的 _sender_loop（独立发送线程，每 1 秒排空，修复版 R8；主循环只做长轮询）调用的 drain_outbox
  │  text 和 image 各自一次 sendmessage（message_type=2，message_state=2，from_user_id=""）
  │  成功 → outbox/sent/；可重试的预发送失败 → 留 pending，≤3 次；最终发送未知 → failed，不自动重试；
  │  明确拒绝 → failed，立即
  ▼
微信用户收到回复
```

## 组件职责

- `serve_loop()`（`bridge.py`）：轮询 `getupdates`（45 秒超时）、写队列、排队提示、typing 注册；排空 `outbox` 由独立发送线程 `_sender_loop`（1 秒）负责，主循环只收集发送结果。
- `drain_outbox()`（`bridge.py`）：发送状态机，见上图。启动时 `recover_sending_on_startup()` 把残留 `delivery_state="sending"` 的任务移到 `outbox/failed/`，标记为重启后未知，**不重试**。
- `process_inbound_image()`（`bridge.py`）：收图全流程，见排障文档。
- `send_image()`（`bridge.py`）：`getuploadurl` → 上传密文 → `sendmessage` 类型 2。
- `send_text()` / `send_typing()` / `get_typing_ticket()`（`bridge.py`）：文本与输入状态。
- `fast_command_reply()`（`bridge.py`）：`/ping`、`/status` 的快速回复内容生成；修复版中实际发送改为只写 outbox/pending，由发送线程发送，不再直接走网络（见 R8）。
- `cmd_login()` / `cmd_status()`（`bridge.py`）：登录与状态命令；登录的二维码渲染用 `qrcode` 包。
- 看门狗：只做“发现新 inbox + 发现 serve 死亡并唤醒 Worker”；**看门狗脚本本身不直接重启 serve**，拉起动作由被唤醒的 Worker 执行（这是实际实现，早期“看门狗自己拉起”的说法不准确，见开发史）。
- Worker：生成回复的唯一主体；桥接本身不生成自然语言回复，不做图像理解。

## 登录态 session.json

- 位置：`state/session.json`，权限 0600，**绝不公开**。
- 字段（仅字段名，值一律占位）：`baseurl`、`bot_token`、`context_token`、`ilink_bot_id`、`ilink_user_id`。
- 取得方式（实际走通过的那条）：用户在本地 Windows 电脑上创建本地桥接目录并登录，得到 `session.json`，人工上传到云端并设 0600；云端二维码登录在 2026-10-27 多次失败（二维码约 120 秒过期、POST 取码挂起），已被废弃。
- 使用方式：`load_session()` 读入；`make_headers(authenticated=True)` 时才在 `Authorization: Bearer <bot_token>` 里使用；`sendmessage` 的 `to_user_id` 取 `session["ilink_user_id"]`；`sendtyping` / `getconfig` / `getuploadurl` 也需要显式带 `ilink_user_id`（这正是几次修复的共同原因）。
- 过期恢复：任何调用返回 `ret=-14` 或 `errcode=-14` 时，`check_ret()` 触发 `write_needs_relogin()`，写 `state/NEEDS_RELOGIN` 并退出，**绝不循环重试**；看门狗看到该文件也不拉起。恢复需要用户重新登录，重新得到 `session.json`。
- 补充：认证前 `sendmessage` 曾返回 `ret=-2`“prepare failed”，原因是还没有入站消息带来的 `context_token`，不是 token 过期；不要把它当成重新登录信号。

## 看门狗与“5 秒最低间隔”

- 看门狗在 Muse 环境里按 5 秒间隔轮询 `inbox/` 和 `serve` 存活。
- “5 秒”是本项目里 Muse Hook 调度的**系统最低间隔**（2026-10-28 由用户决策从 15 秒改到 5 秒时确认），不是 `bridge.py` 自己的 sleep，也不是微信侧的限制。
- 存活判断（实际规则）：只认 `state/serve.pid` + `/proc/<pid>/cmdline` 首词为 `python`/`python3` 且参数为 `bridge.py serve` + `kill -0` 成功；**不能**只用 `pgrep -f "bridge.py serve"`，因为它会误匹配含该字符串的 bash 进程（2026-10-27 实测过的误报，已修复）。
- 注意：2026-10-28 一次手动重启后，看门狗没有在预期时间内自动拉起，最终由人工手动 `nohup python3 bridge.py serve` 拉起（新 PID 26959）。所以“看门狗一定能拉起”**没有被证实为永真**，只能写成“按配置应拉起，实测有过成功，也有过一次未及时拉起”。

## 消息标识、上下文、图片关联

- 消息标识：微信 `message_id`（数字字符串），文件名就是它；`processed_ids.json` + `cursor.txt` 做去重和游标。
- 会话上下文：`context_token` 来自入站消息，写进 `outbox` 记录并用于 `sendmessage`；`session.json` 里也会更新最新 `context_token`。
- 图片事件：`state/last_image_event.json`，字段为 `message_id`、`status`（`pending` → `ok` 或 `failed`）、`paths` 或 `errors`/`reason`、`ts`。新图片先写 `pending`，下载完成才变 `ok`，失败则保持 `failed`；普通文字**不清除**失败状态；重启后仍保留。
- 追问关联：`serve_loop` 在追问类文字到达时，如果最新图片事件是 `pending`/`failed`，就给 `inbox` 记录加 `image_reference={message_id, status:"unavailable", reason}`；Worker 必须按该字段回复“图片接收失败，请重发”，**不回落旧图**。只有用户明确点名旧 `message_id` 时，才查那条 `done/` 记录。
- 连续追问：Worker 读 `done/` + `outbox/sent/` 最近 10 轮，按 `message_id` 升序；例如上一轮 137×29=3973，下一轮“再加 58”应答 4031。

## 排队、并发、重复、失败、重启

- 排队：`inbox/` 是单文件队列；Worker 一次处理一个文件，处理完移到 `done/`。
- 并发：`serve` 持 `fcntl.flock`（`state/serve.lock`），第二个 `serve` 直接退出；Worker 与 `serve` 通过文件目录协作，没有数据库锁。
- 重复消息（修复版 R2，已改）：`processed_ids.json` 只在该消息的 inbox 文件落盘后写，整批游标只在整批结束后写；崩溃后重取整批，已处理消息靠磁盘 processed 去重，未处理消息不会因游标提前而丢失。残留风险是崩溃后重取可能导致已处理消息被重新入队一次（需靠 processed 去重挡住），**不能写成“永不重复、永不丢消息”**。Phase 1 的旧顺序（先写 processed+cursor 再写 inbox，崩溃会丢消息）已废弃，不要再引用。
- 失败重试：发送前失败（`getuploadurl`/上传）可重试 ≤3 次；最终 `sendmessage` 失败结果未知时立即进 `failed`，不自动重试；明确拒绝立即进 `failed`。
- 进程中断：`recover_sending_on_startup()` 把残留 `sending` 任务移到 `failed`，标记未知，不重试；图片事件、队列文件、游标都已落盘，重启后可继续。
- 只存在内存的：当前 `full_url`、AES 密钥、下载字节、重试快照；这些**不落盘**（脱敏设计），所以失败后无法仅凭 `failures/*.json` + `.download.bin` 离线重放解密或重下同一 URL（见排障文档）。
- 已持久化的：`session.json`、`cursor.txt`、`processed_ids.json`、`last_image_event.json`、队列文件、图片文件、失败记录。

## 脱敏副本与运行版本的对应关系

- 本修复版 `bridge.py` 共 2337 行（原始公开导出为 2092 行，修复后行数已变，不要再写“2092 行修复版”），与 2026-10-28 06:02 部署的运行版本字节对应，SHA-256 7cd359dd5ecbab4904f5cbea6f9bc153adae37a368a134dc4533df39175177ba。
- 唯一需要提醒的注释滞后：`process_inbound_image()` 里仍有一段注释写“完整重试一次”，实际最新代码是“`urllib` 第一次不完整就直接转 `curl`”，不再做第二次 `urllib`（见开发史最后一项）。注释未同步更新，代码行为以实际循环为准。
- 看门狗定义、Worker 提示词本身**不在本仓库**（它们属于 Muse 环境配置，不是 `bridge.py`），但本仓库含 `examples/hook-config-template.md` 和 `examples/worker-prompt-template.md` 模板；本文件只整理了它们的行为，读者需要在自己的 Muse 环境里重建。
