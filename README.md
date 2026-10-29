# Muse 接入微信私聊：iLink 桥接（技术交接脱敏版）

> **本项目是 Muse 环境的微信桥接器，非独立聊天机器人。** 没有 Muse 账号和运行环境、没有人工配置的看门狗与 Worker，单靠本仓库不能完整运行。

> **如何获取初始 session.json（本地扫码上传流程）：** 云端二维码登录已失败并废弃；在本地电脑上运行 `python3 bridge.py login`，用微信扫码（二维码约 2 分钟过期），登录成功后得到 `state/session.json`（0600），人工上传到云端虚拟机的同一目录，再启动 `bridge.py serve`。`cmd_login` 在云端直接跑通不是默认路径，README 里不要假设它一定能用。

> 本仓库已发布到 GitHub：https://github.com/Zzlnb66/muse-wechat-bridge
> 它包含真实运行代码的脱敏副本，以及按实际记录整理的架构、开发史、排障和测试范围。
> 运行服务已于 2026-10-28 06:02 部署修复版（进程 44402），并于 06:02–06:11 通过最小微信实测（见 docs/testing.md）。此前整理导出时未重做全部历史实测，不等于从未实测。

## 这个项目是什么

用户在微信私聊里发消息给一个 ClawBot 机器人账号，云端桥接把消息写进文件队列，
由 Muse 环境里的一个 Worker（见下文“哪些不是本仓库代码”）生成回复，
桥接再把回复通过 iLink 发回微信。

目标体验：用户**不用开电脑、不用打开 Muse App**，只在微信里聊天。

目前已支持（真实微信实测通过，见 `docs/testing.md`）：

- 私聊文字收发；白名单只处理扫描登录的那个用户，群消息忽略。
- 语音：使用微信端转写文字（`voice_item.text`，类型 3），不做付费转写；无转写时直接提示改发文字。
- `/ping`、`/status` 由桥接直接回复，不进 Worker 队列。
- 排队提示轮换；`sendtyping` 对方正在输入。
- 图片接收：下载、AES 解密、魔数、像素和完整解码校验，顺序保留。
- 图片发送：`getuploadurl` 上传密文，再 `sendmessage` 类型 2 发送。
- 连续追问：用最近历史 + 图片事件关联，不回落到旧图。
- 敏感操作：微信端不执行，只给“回 Muse App 确认”的指引。

## 云端自托管，实际指什么

“云端自托管”在本项目里**不是**“买一台普通云服务器就能跑”。

实际部署是：

| 组件 | 实际运行位置 | 是否本仓库代码 |
|---|---|---|
| 桥接进程 `bridge.py serve` | Muse 提供的云端虚拟机（Linux，`~/workspace/wechat-bridge/`） | 是，本仓库 `bridge.py` |
| 文件队列 `inbox/`、`done/`、`outbox/pending|sent|failed/`、`media/`、`failures/`、`state/` | 同一台虚拟机上的本地目录 | 是，`bridge.py` 的目录约定 |
| 看门狗 Hook 脚本 | Muse 的 Hook 调度环境，每 5 秒轮询一次（见架构文档） | 部分：本仓库含 `examples/hook-config-template.md` 模板，Hook 本身的实际定义属于 Muse 环境配置，**不在本仓库** |
| Worker（生成回复的 Muse 代理） | Muse 的代理运行环境，由 Hook 唤醒 | **不是本仓库代码**；它的提示词、历史加载、图片阅读、工具权限都依赖 Muse |
| 微信 / iLink / CDN | 腾讯侧：`https://ilinkai.weixin.qq.com`，CDN `https://novac2c.cdn.weixin.qq.com/c2c` | 第三方平台，不是本项目开发 |

## 陌生人拿到仓库，能复现到哪一步

**能复现的：**

- 读懂协议调用、队列格式、收发图逻辑、失败留证格式；
- 在有合法 iLink 登录态的前提下，运行 `bridge.py` 的登录、状态、收发、排队部分做本地模拟和单元检查。

**不能只靠本仓库完整复现的（确切缺口）：**

1. **必须拥有 Muse 账号和运行环境**。Worker、Hook 调度、图片视觉理解、会话管理都不是 `bridge.py` 能替代的。
2. **必须人工配置一个 Muse 任务/会话**：看门狗 Hook、Worker 提示词、历史加载规则、敏感操作指引、图片事件关联规则，都需要在 Muse 环境里另行配置。本仓库含 `examples/hook-config-template.md` 和 `examples/worker-prompt-template.md` 模板，但没有导出 Muse 私有内部实现，也没有一个“一键导入”文件。
3. **必须自己取得登录态**。`state/session.json` 里的 `bot_token`、`context_token`、`ilink_user_id` 是私有的，不能公开，也不能从本仓库推导。取得方式见 `docs/architecture.md`“登录态”一节：本地登录取得后人工放到云端，文件权限 0600。
4. **iLink 账号本身**：需要在微信里有 ClawBot 插件/机器人账号，并完成扫码登录。云端二维码在本项目里实际失败过（见开发史），不要假设云端扫码一定能用。
5. **图片视觉理解**：Worker 读图依赖 Muse 的视觉能力，`bridge.py` 只负责把图片落盘并把路径写进 `inbox` 记录，不做 OCR 或图像理解。

所以结论是：**桥接层可以开源，完整“微信私聊里的 Muse”不能只靠桥接层复现。**

## 快速开始（给已经有 Muse 环境和 iLink 账号的人）

```bash
python3 --version   # 实际使用环境为 Python 3.x，标准库为主
pip install -r requirements.txt  # Pillow（运行时图片解码）+ qrcode（仅登录渲染二维码用）
# 系统还需 openssl 和 curl（非 pip 包）：openssl 做 AES-128-ECB，curl 做传输与图片回退
python3 bridge.py status
python3 bridge.py serve
```

- 配置见 `examples/session.example.json`、`examples/config.example.md`。真实 `session.json` 不要提交。
- 启动前先确认目录结构：`state/`、`inbox/`、`done/`、`outbox/pending|sent|failed/`、`media/inbound|outbound/`、`failures/`，`bridge.py` 的 `ensure_dirs()` 会创建它们（队列目录是自动创建的，不需要手动“修复”）。
- 停止：向 `serve` 进程发终止信号；看门狗在 Muse 环境里会按配置处理拉起（本仓库不含看门狗定义）。
- 观察状态：`python3 bridge.py status`、`state/serve.log`、`state/serve.pid`、队列目录计数。**注意：`status` 只检查本地文件（session 是否存在、NEEDS_RELOGIN 是否存在、inbox 数量、游标是否存在），不代表在线、不验证 iLink 连通性。** 在线与否要看 `serve.log` 里的 getupdates 结果，那里 v1.0.2 起同时记录 curl 错误和 urllib 错误，以及脱敏后的目标 host:port 和生效代理 host:port（不含账密）。

## 陌生人从零部署清单（v1.0.2 起，仍需 Muse 环境，不是独立机器人）

1. **装依赖**：`pip install -r requirements.txt`，系统装好 `openssl` 和 `curl`（非 pip 包）。
2. **本地登录**：在能扫码的本地机上跑 `python3 bridge.py login`，微信扫码确认后得到 `state/session.json`（0600，含 bot_token、context_token、ilink_user_id，绝不提交、绝不公开）。把这个文件人工放到云端运行目录的 `state/` 下，权限保持 0600。云端扫码在 2026-10-27 实测中失败过（二维码约 2 分钟过期），本地登录后上传是当时走通的路线。
3. **配 Hook（看门狗）**：按 `examples/hook-config-template.md` 在 Muse 环境里配置一个每 5 秒轮询一次的 Hook：检查 `inbox/` 有新文件就唤醒 Worker。Hook 本身不在本仓库，模板只给配置字段和轮询规则。
4. **配 Worker**：按 `examples/worker-prompt-template.md` 配置 Worker 提示词：按 message_id 升序读最近 10 轮 `done/` + `outbox/sent/` 历史，读图只读 `media/inbound/` 落盘文件，敏感操作只回 App 确认、不执行。**Worker 写回复文件必须是原子写**：先写同目录临时文件（0600），再 `os.replace` 到 `outbox/pending/<message_id>.json`，绝不能直接写 pending 文件导致桥接读到半截 JSON；回复文件里的 to_user_id 取 session 的 ilink_user_id。
5. **启动与最小验收**：跑 `python3 bridge.py serve`，先看 `state/serve.log` 有没有 getupdates 成功，再在微信里依次发 `/ping`、一句文字、一张单图、一次回传图请求，四个都回了才算通。`status` 显示正常不等于在线，见上条。

## 依赖

- Python 标准库为主：`argparse`、`json`、`os`、`subprocess`、`urllib`、`uuid` 等。
- `Pillow`：图片完整解码（`im.load()`，不只读头）。
- `openssl` 命令行：AES-128-ECB 解密/加密（`openssl enc -aes-128-ecb`），本项目未使用 Python `cryptography` 库（当时环境不可用，见开发史）。
- `curl` 命令行：`post_json`/`get_json` 的首选传输（`--http1.1`），失败时回落 `urllib`；图片 `curl` 回退也用它。
- `qrcode`（仅登录渲染用，虚拟环境里使用；运行时 `bridge.py` 本身不依赖它）。

依赖声明见根目录 `requirements.txt`（Pillow 运行时 + qrcode 仅登录用；openssl、curl 为系统命令，非 pip 包，不在该文件里）。没有版本锁文件：原项目没有锁文件，**不要伪造**版本号。

## 已知限制（摘要，完整版见各文档）

- 回复慢：看门狗 5 秒轮询 + 下载重试/`curl` + Worker 看图排队，实测两张图约 1 分钟才陆续发出。
- 一条消息里同时带两张图：**代码分支未验证**——一次选两张图时 iLink 会拆成两条独立单图消息（2026-10-28 06:07–06:09 实测，两张均收到），用户体验上已通，但“一条消息带 2 张图”的代码分支至今未真正跑过（见 docs/testing.md）。
- 虚拟机整体替换恢复：**未验证**（只验证过进程被杀后拉起）。
- 提示注入：只通过了两个样例（发邮件、连接 OpenClaw），**不能写成彻底解决**。
- 下载截断的底层根因：已证实 `urllib` 截断、`curl` 能救回，但 CDN、代理、网络链路哪一环导致，**无法确认**。
- 失败留证即使 0600 也不适合公开；本导出不含任何真实 `failures/`、队列、日志、图片样本。

## 目录

- `bridge.py`：可公开的完整桥接源码（2572 行，SHA-256 d41d9b0d86df8377119c56c32715f81e31482b979ae731d78bef7fadf690fd57，v1.0.3 最终版已部署并实测：2026-10-29 01:20 部署到运行服务（新 serve PID 28153），01:23-01:27 最小微信实测 4 项全部通过（/ping、普通文本、单图、回传图），GitHub 已打 Tag v1.0.3（见下）；v1.0.2 已部署并实测：2026-10-29 00:01 部署到运行服务（新 serve PID 7535），00:11-00:13 最小微信实测 4 项全部通过（/ping、普通文本、单图、回传图），GitHub 已打 Tag v1.0.2（见下）；此前运行服务是 2026-10-28 06:02 部署的 2337 行版，SHA-256 7cd359dd5ecbab4904f5cbea6f9bc153adae37a368a134dc4533df39175177ba）。
- `examples/`：配置示例和脱敏请求结构示例。
- `docs/architecture.md`：架构、组件、消息流、状态。
- `docs/development-history.md`：路线、开发经过、关键修复。
- `docs/troubleshooting.md`：图片、解密、收发、运行故障排查。
- `docs/testing.md`：测试、证据、通过范围、未验证项。
- `SECURITY.md`：凭据、权限边界、安全限制。
- `.gitignore`：排除凭据、运行数据、日志、媒体、临时文件。

## needs_check 人工处置
`outbox/failed/` 下出现 `needs_check` / 终态任务时：先按子项逐个在微信确认是否已收到；仅把确认未送达的子项改回 `pending`——文本子项改顶层 `"text_status"` 且 `"text_attempts"` 重置为 `0`，附件子项改 `attachments[i]` 里的 `"status"` 且 `"attempts"` 重置为 `0`，旧格式单图改顶层 `"image_status"`——删掉顶层 `"terminal": true`，移回 `outbox/pending/` 后重启 `serve` 或改名重投；未确认前不要反复重发。完整 5 步见 `docs/troubleshooting.md`。

## 账号安全表述
走腾讯官方 iLink 授权通道，风险低于传统逆向外挂，但不承诺免受平台灰度规则变动影响，也不代表永久免责或零风险。


## v1.0.4（P0 收敛版，2026-10-29，仅供独立复核，尚未部署、尚未打 Tag）

- 范围只限三项：(1) 同一掉线事件去重 + 过期唤醒处理；(2) 时间戳与现场快照；(3) 进程与通道分开报告。另附两个针对性回归测试。
- 不包含：并发启动新增锁（已证伪，唯一 PID 写入点在 flock 成功之后，失败启动者绝不写 PID，见 tests/test_v104.py）；网络根因定位（单独验证，不以“网络错误消失”作为 v1.0.4 验收目标）。
- bridge.py 2625 行，SHA-256 f4aa1946619ececeff9afb09b806a0fdb9a612b305d075d092aa5a8947b810ea。
- 看门狗脚本 watchdog/wechat-bridge-watchdog.sh（部署时覆盖 ~/hooks/scripts/wechat-bridge-watchdog.sh），复核补丁后 235 行，SHA-256 3ed45aa79fc532d7d3d9018cabe11daea8a90731b2a8311d46523e5d9cc30897。
- 看门狗定义 ~/hooks/definitions/wechat-bridge-watchdog.json 的 Worker 提示词已补进程/通道分开报告与过期唤醒规则（部署时同步更新，哈希以部署后实测为准，不在本文档冒充）。
- 验收目标：消除重复误导、留下可定位证据。连续缺席 90 秒只产生一次初始唤醒；长期未恢复每 300 秒重试一次；恢复后到达的旧事件不重复通知；inbox_new 正常触发；进程已启动但请求仍失败时只报“进程已恢复，通道待确认”，有效 getupdates 响应（含成功空响应）才确认通道恢复。
- 测试：test_v104.py、test_v104_watchdog.sh 全部通过，v101/v102/v103/regression/r8_timing 全部通过。部署前必须经独立复核通过，再按“上传 -> 部署 -> 微信实测 -> 打 Tag”走。

## v1.0.4 复核补丁（2026-10-29，独立复核暂不通过后退回修改，仍未部署、未打 Tag）

- 发现 1：serve 获锁后立即生成 run_id，先写 state/serve_run.json 和 pending 的 channel_status，再写 serve.pid；Worker 确认通道恢复必须同时满足 status ok、失败数 0、process_pid 等于当前 serve.pid、run_id 等于 serve_run.run_id、last_success_ts 不为空且不早于 process_started_ts，删除 ok/0 兜底。
- 发现 2：看门狗统一快照，快照在 wake/silent 之前记录，全部终止分支携带 checked_at、serve_running、pid、check_reason、proc_exists、cmdline_first、boot_id、event_id、wake_kind、new_inbox、seconds_since_last_wake、needs_relogin，PID 字段统一为 pid。
- 发现 3：首次/重试掉线同时有 inbox 时，先把该批文件记入 seen 再唤醒，下一 tick 不再把它当新文件。
- 发现 4：channel_status 字段类型异常时诊断函数不抛异常、不导致 serve 退出，失败数重置为 0 并打印可见警告；写入失败也打印警告。
- 另修：看门狗 mark_seen 在空 inbox 时不再因 grep 空输入 + pipefail + set -e 导致脚本静默退出，该问题由更新后的看门狗测试发现。
- 旧包（bridge.py SHA f4aa1946…、看门狗 SHA ae0a3540…）已作废，以复核补丁后的哈希为准。


> **最新规则声明（部署时只用这一段，历史段已取代）：** Worker 通道确认必须用生产命令 `python3 bridge.py verify-channel` 的 JSON 结果，禁止用 `last_success_ts >= process_started_ts` 的字符串时间比较；初始化失败即启动失败（返回 1，不写 serve.pid）；未消费 inbox 按最后唤醒时间 300 秒重提醒。README 历史段中任何旧时间比较规则、旧哈希均已作废，不得照抄部署。

## v1.0.4 GPT 裁决补丁（2026-10-29，GPT 裁决后，仍未部署、未打 Tag）

- 裁决结论：暂不放行，修好两项阻断并跑通故障注入后才放行。旧复核补丁包（0060e51e…）已作废，以本节哈希为准。
- 阻断 1：初始化失败即启动失败，不写 serve.pid，返回 1（不是 -14 token 过期）；serve_run.json 增加 boot_id 与 /proc/self/stat 第 22 字段 proc_starttime；Worker 删除 last_success_ts 时间比较，改内核启动时间 + run_id 匹配；初始化失败或进程未运行时报“启动失败/进程未运行，通道未确认”，只有确认当前进程运行时才说“进程已恢复，通道待确认”。
- 阻断 2：未消费 inbox 按最后唤醒时间 300 秒重提醒；只有实际发出唤醒时才刷新 last_wake_ms，被抑制的 tick 不顺延；文件离开 inbox 后清理提醒记录；serve_down 与 inbox 重试分别管理，进程恢复不取消未消费文件的重提醒。重复调度由 Worker 按 message_id 去重，不重复发用户通知。
- 非阻断：运行期诊断更新失败仍只警告继续；日志格式、统计等后续迭代。
- bridge.py 2757 行，SHA-256 f0a94d564d780924c9af088a7efc3d534a21a61fff58b245a244ebb4b418fd36；看门狗脚本 264 行，SHA-256 4f35eea90633ff20e721421634bf7045c762a5ef9b5c08e4a2421a9bf4550f58。

## v1.0.4 复核第二轮补丁（2026-10-29，仍未部署、未打 Tag）

- 本轮不推翻 GPT 裁决补丁，只补两项阻断验收：生产 Worker 核验端到端覆盖、诊断警告外逃修复、299 秒测试直接断言。
- 生产核验：bridge.py 新增 verify_channel_recovery() 与 `verify-channel` CLI（只读 JSON，永不抛异常、不比较时间字符串）；Worker 必须用该命令结果，不得手工拼字段判断。核验条件：serve.pid 存在且进程活着、cmdline 是 bridge.py serve、两文件均为对象、status ok、失败数 0（非布尔）、last_success 非空、process_pid 等于 serve.pid 等于 serve_run.pid、run_id 一致、boot_id 一致、proc_starttime 等于当前 /proc/<pid>/stat 第 22 字段、检查期间 PID 与启动时间不变。任一不满足只报未确认。
- 诊断外逃：新增 _safe_warn()，警告输出单独用 try/except Exception 保护，不用 BaseException，不吞 SystemExit/KeyboardInterrupt；_record_channel_status 全程警告均走 _safe_warn，写入失败 + BrokenPipeError 组合失败仍返回不抛。
- bridge.py 2880 行，SHA-256 89888b0443d044b9a05858d1e3fe0aab15d8711b4f3bf4b79422f1cc56e2468c；看门狗脚本未变，SHA-256 4f35eea90633ff20e721421634bf7045c762a5ef9b5c08e4a2421a9bf4550f58。

## v1.0.4 部署与微信实测（2026-10-29）

- 上传：2026-10-29 18:46 到 GitHub main，4 个提交 e3967a7（bridge.py、README.md、FIX_NOTES.md）、9882ed5（docs/troubleshooting.md 已与授权文件一致，0 file changed）、7ce8c2d（6 个新测试）、21f2e6f（watchdog/ 2 个新文件），仓库提交总数 25 -> 29。
- 部署：2026-10-29 18:49-18:51，旧 v1.0.3（PID 1758，SHA d41d9b0d…）备份到 workspace/wechat-bridge-backup-20261029-pre-v104/ 后停掉；新 serve PID 18981，serve_run run_id 08b95f00ee10402db3aa2bc9ce29aeef，通道 pending -> ok 于 18:50:58，verify-channel recovered=true；session.json 未动，NEEDS_RELOGIN 不存在，inbox/pending 为空；一次并发重启尝试被 flock 挡回，未双跑。
- 微信实测（部署后真实消息）：/ping 快速回复“桥接在线，serve 正常”（18:55:41，快速通道不进 done/）；普通文字、用户图片、生成花枝鼠形象并回图，4 条全部有回复；19:02:14 verify-channel 仍 ok，19:04:53 最近一次成功。
- Tag v1.0.4 随后在 GitHub Releases 创建，指向本次文档提交。
