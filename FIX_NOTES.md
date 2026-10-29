# 开源前修复说明（按审查编号逐项，独立副本）

> **当前状态说明（v1.0.1 补丁前置，读本文档先读这一段）：本文档是迭代修复过程记录，不是单次发布说明。**
> - R1–R8 修复版：已于 2026-10-28 06:02 部署到运行服务（新进程 44402），并于 06:02–06:11 通过最小微信实测；下文“仍未部署 / 未重做实测”那几句是 05:41–06:01 独立副本阶段的历史说法，不要再拿它否定已部署的版本。
> - v1.0.1 补丁（发图超时脱敏、/ping 入队失败、归档失败防重发，见文末）：目前只在本地修复副本里改完并跑通离线测试，**尚未部署到运行服务，也尚未做微信实测**，发布 v1.0.1 前必须先部署 + 最小微信实测。
> - v1.0.2 补丁（Bearer 脱敏、磁盘终态识别、双路径错误日志，见文末）：已于 2026-10-29 00:01 部署到运行服务（新 serve PID 7535，bridge.py 2534 行，SHA 3535d58b…），并于 00:11-00:13 通过最小微信实测 4 项（/ping 快捷回复、普通文本、单图接收与描述、回传小猫图，附件状态均为 sent），GitHub Tag v1.0.2 已打。
> - 仓库已于 2026-10-28 发布到 GitHub（见 README.md），本文档的“未发布”说法同样是历史说法。

## R1：最终发送不再自动重发

- `post_json(..., allow_fallback=False)`：最终 `sendmessage`（`send_text`、`send_image`）在 curl 失败、空体、非 JSON 时直接抛错，不再用 urllib 第二次提交。
- `send_direct_or_outbox`：`UnknownDeliveryError` 不再进 `outbox/pending`，改为写 `outbox/failed/` 的 `needs_check` 记录；其他失败仍按旧 pending 逻辑。
- 验收：首次发送可能已成功但回执丢失时，最终发送请求计数为 1；任务状态为 `needs_check`，重启不自动重发。离线测试通过。

## R2：游标不再提前提交

- `serve_loop` 不再在处理第一条消息前保存 `processed` + 整批游标；每条消息只有在其 `inbox` 文件或直接回复文件落盘后才记 `processed`；整批游标只在整批循环结束后保存，`session` 的 context_token 也在批末保存。
- 崩溃 mid-batch 时，游标仍为旧值，`processed` 为空，下次 getupdates 会重取整批，已处理消息去重，未处理消息不丢。
- 注意：如果为了恢复而保存原始消息（含下载凭据），必须用 0600 私密存储并及时清理；本修复没有引入新的原始消息持久化文件，只用已有的 `processed` + 批末游标 + 重取机制。
- 验收：第一条、中间条、最后一条、图片处理前后中断，全部消息仍可恢复。离线测试：第一条图片处理时 SystemExit，游标未前进，`processed` 为空。

## R3：OpenSSL 超时不再泄露密钥

- `_sanitize_error`：`TimeoutExpired` 只返回类型名，不返回命令参数；其他错误同时处理 `'-K', '<hex>'` 和 `-K <hex>` 两种形式，并删除 32 字符 hex、URL 查询部分。
- 验收：虚构 AES 密钥、bot token、context token、带凭据 URL 均不能出现在日志和失败 JSON 中。离线测试：超时错误返回 `TimeoutExpired`，不含密钥。

## R4：临时文件权限与忽略规则

- `atomic_write_json` / `atomic_write_text` 改为 `os.open(..., mode)` 从创建瞬间就是 0600，临时文件名改为 `<path>.<pid>.tmp`（唯一，不共享）；`ensure_dirs` 把 `state/` 设为 0700（尽力而为）。
- `.gitignore` 改为忽略整个运行状态目录：`state/`、`inbox/`、`done/`、`running/`、`outbox/`、`media/`、`failures/`、`secret_verify.txt`、`login_qr.png`、`*.tmp` 等，覆盖 `session.json.tmp`、`login_pending.json.tmp`。
- 注意：`.gitignore` 不能清除已存在的提交历史，安全文档已避免这样暗示。

## R5：发送状态落盘失败不再继续发送

- `drain_outbox` 文本与图片两处：发送前写 `delivery_state="sending"` 失败时，设置 `_state_ok=False`，在发送调用前抛错，由现有分类逻辑记为 `needs_check`，不调用 `send_text`/`send_image`。
- 验收：发送前状态落盘失败，网络发送调用次数为 0。离线测试通过。

## R6：重新登录成功清除过期标记

- `cmd_login` 确认后，先 `save_session`，再删除 `NEEDS_RELOGIN`，再删除 `login_pending`。只有新会话保存成功后才清标记，避免旧会话未更换时盲目删除。
- 验收：预置过期标记 + 登录成功返回 0 后，标记不存在，`serve` 不再因旧标记返回 3。离线测试通过。

## R7：图片读异常也进入 curl 回退，并受大小预算约束

- `process_inbound_image` 在 `data is None` 且错误不是 `over_25MB` 时，也做一次 `curl` 回退；`curl` 退出码 0 且字节数在 (0, 25MB] 内才使用，否则记 `curl_diag` 后失败。
- `curl` 分支在哈希前先查文件大小，超过 25MB 不读入内存做哈希。
- 验收：模拟读异常时 `curl` 调用次数从 0 变为 ≥1；超过上限的永久拒绝不重试。离线测试：读异常时 curl 调用 1 次。

## R8：接收长轮询与发送队列解耦

- 新增 `_sender_loop` 独立发送线程：每 1 秒排空 `outbox/pending`，把已发/失败文件名经队列交给主循环做 typing 取消；主循环不再在长轮询前同步 `drain_outbox`。
- `drain_outbox` 仍是唯一发送者；发送线程不生成回复，只发送已排队文件。
- 注意：本修复是结构性改动，离线测试只验证线程函数存在与导入；完整验证需要模拟 getupdates 长轮询 + 待发送回复的时序测试，以及真实微信实测，**未完成**，不要写成已验收。

## D1：时间线矛盾（已更正，审查基于旧快照）

- 审查指出旧导出同时写“04:57–04:58 完成修改”和“05:01 成功发生在修改之前”，两者矛盾。
- 回查记录：优化重启约 04:58（PID 26959），05:01 的 OpenClaw 截图在重启之后，且 `serve.log` 有 `curl fallback recovered 7520952771389668232 declared=100688 curl_bytes=100688`，所以 05:01 是修改后的第一次实测，旧说法错误。
- 本修复版文档已按此更正；审查报告本身是基于 05:24 旧快照，不能用它否定 05:31 已更正的版本。

## D2：安全层描述更正

- 已把 SECURITY.md 的“程序层保障”改为事实分层：提示词规则（模板见 examples/）、桥接代码范围（无邮件/删文件代码，不等于 Worker 被程序禁止）、平台审批（未验证）、工具权限（未验证/缺失）。
- 两个截图测试保留为样例结果，不写成彻底解决。

## D3：部署模板补齐

- 新增 `examples/worker-prompt-template.md`（历史、图片、图片事件、敏感操作、发图、输出报告，全部占位符）和 `examples/hook-config-template.md`（轮询、存活规则、拉起步骤、队列输入输出样例、配置步骤）。
- 一键导入仍未提供；可供照做的配置说明已先补齐。

## D4：其他表述更正

- 0.2 秒间隔：改为手工流程记录，当前 `drain_outbox()` 无该等待。
- 网关 Protobuf 丢弃：改为推测，无服务端证据；能确认的是字段移入 `media` 后实测恢复。
- 源码头部：已更新为当前依赖（标准库 + Pillow + openssl + curl + qrcode 仅登录）和最终发送不自动重试的说明。
- 测试程序：本目录 `tests/test_regression.py` 是针对修复版的回归测试，不是旧缺陷复现脚本；旧复现脚本的 `issue_reproduced: true` 不能作为新版验收。

## 许可证核查（审查补充）

- 审查者核对到 Tencent 上游**当前 main** 分支 LICENSE 为 MIT，并要求在副本中保留版权和许可声明。
- 历史 commit `874637a0dbc24050f72d1385add4fc6170a2d726` 的 LICENSE 本次仍未成功取得，**不能**冒充历史版本已核查。
- 本修复版没有自行给 `bridge.py` 定 MIT；作者仍需明确 LICENSE 文件，并建立来源清单（协议字段名来自公开 protocol.md，`bridge.py` 里未发现逐字复制的上游代码，但该判断未做逐行比对，列为待确认）。

## 未完成 / 仍需微信实测

- R8 线程解耦的真实时序、R2 整批崩溃的真实微信恢复、R1 未知发送后的人工核对流程，均未做微信实测。
- 本修复版没有部署到运行服务；部署前应在测试环境先跑上述回归测试，再选必要微信用例（单图、追问、回传、问题图）复测，不要机械重复全部历史验收。


## Gemini 意见结合后的第二轮修复（Step 1，离线收口）

- R8 唯一发送者：`send_direct_or_outbox` 改为只写 `outbox/pending`，不再直接调用网络；主线程的 /ping、排队提示、语音提示、图片失败提示全部经发送线程发送，`_sender_loop` 成为唯一网络发送者。
- 共享状态锁：新增 `_OUTBOX_LOCK`（RLock）和 `_TYPING_LOCK` + `_TYPING_MAP`；发送线程在发送完成后自己做 typing 取消，不再等主循环 45 秒长轮询返回后才取消。
- 线程生命周期：`_sender_loop` 已有 `stop_event.wait(1.0)` 和 daemon=True；新增时序回归测试验证 stop 后线程干净退出，不挂起。
- R2 持久化：`atomic_write_json` 在替换前 flush + fsync 文件，替换后 fsync 父目录，确保 processed/游标落盘。
- R3 收窄：不再全局删除所有 32 字符 hex，只处理 -K 形式和显式凭据查询字段，避免误伤 MD5/UUID/媒体标识。
- R5 兜底：最终状态写失败时保持原 pending 文件不动并中止该任务，不再把写失败的文件移到 sent/failed。
- R7 预算：curl 命令加 `--max-filesize 26214400`，防止下载过程中把磁盘写满后再查大小。
- 时序回归测试：`tests/test_r8_timing.py` 通过，证明主线程模拟长轮询阻塞时，发送线程能在 5 秒内发出新排队回复，且 stop 后线程干净退出。
- 仍未做（按 Gemini 顺序）：Step 2 最小微信实测（4 个动作）、Step 3 许可证与 THIRD_PARTY_NOTICES 闭环。本修复版仍未部署到运行服务。

## Step 3 许可证与来源（2026-10-28 06:14，独立副本）
- 新增 THIRD_PARTY_NOTICES.md：列出参考来源 Tencent/openclaw-weixin commit 874637a...、参考内容（protocol.md 接口定义与加密流程）、未逐字复制说明、上游 MIT 核查记录（含 main LICENSE 正文开头、当前提交恰好就是该提交、元数据 NOASSERTION 疑点、本地 git show 取证命令）。
- 自有代码许可证：尚未由作者最终选定，本包不含自有代码 LICENSE 文件，发布前作者需选定并添加；不擅自代定 MIT。
- 依赖清单已写入 THIRD_PARTY_NOTICES.md（标准库、Pillow、openssl、curl、qrcode）。

## 最终 LICENSE（2026-10-28）
- 作者选定自有代码许可证为 MIT，新增仓库根目录 LICENSE 文件（Copyright (c) 2026 ZZLNB），THIRD_PARTY_NOTICES.md 已同步更新，不再是“待选定”。

## v1.0.1 补丁（GPT 故障注入反馈，2026-10-28，本地，未部署、未实机验证）

- **问题 ① 发图加密超时密钥脱敏（已确认，修法有修正）**：原 `aes_ecb_encrypt/decrypt` 用 `subprocess.run(timeout=30)`，`TimeoutExpired` 的 str 里确实含完整 argv（含 `-K <hex>`）；原收图路径用了 `_sanitize_error`，但 `drain_outbox` 的图片/文本错误落盘用的是 `str(e)[:500]`，所以发图超时真的会把密钥写进 pending/failed 任务文件。GPT 说的 `CalledProcessError` 在本代码里不成立（没有 `check=True`，不会抛该异常），不要按那个修。修法：`aes_ecb_*` 捕获 `TimeoutExpired` 后直接抛不含密钥的 `ApiError("openssl ... timeout")`；`drain_outbox` 全部错误落盘与打印改用 `_sanitize_error(e)`。
- **问题 ② /ping 入队失败仍推进游标（已确认）**：原 `serve_loop` 的 fast/voice/image-fail 分支忽略了 `send_direct_or_outbox` 的返回值（True/False），入队失败也照样 `processed.add`，批末游标照样前进，消息会被静默吞掉。修法：只有入队返回 True 才记 processed；入队失败设 `_batch_enqueue_failed`，批末不保存游标，下轮重取重试；普通 inbox 写失败也不再直接抛崩，改为同样不记 processed、批末不推进游标。
- **问题 ③ 归档失败死循环重发（已确认，机制比 GPT 说的更具体）**：原 `drain_outbox` 的终态分支里 `os.replace` 到 failed/ 失败时只打印，文件仍留在 pending，且下轮 `text_status != "sent"` 会再次进入发送分支，确实会重发。修法：新增内存 `_TERMINAL_TASK_IDS` + `_is_terminal_job()`；终态分支先记终态、再尝试移动，移动失败也进终态集合；下轮 drain 开头凡是终态任务直接跳过，绝不调用发送接口；`recover_sending_on_startup` 恢复失败同样进终态集合。
- **问题 ④ .gitignore 漏传（已确认，原因与 GPT 说的一致）**：本地修复副本里本来就有 `.gitignore`，GitHub 首次网页上传时因拖拽 `*` 不含隐藏文件而漏掉，不是本地没有。修法：GitHub 上用 Add file → Create new file 显式创建 `.gitignore`（内容与本地一致），不要只靠拖拽。
- **问题 ⑤ 依赖与文档矛盾（部分确认，修法有修正）**：原 README 写“没有 requirements.txt，不要伪造”确实会让新用户不知道装什么，但不能把系统 openssl/curl 写进 pip 文件。修法：新增根目录 `requirements.txt`，只写 Pillow（运行时）+ qrcode（仅登录用），并注释 openssl/curl 为系统命令；README 快速开始改为 `pip install -r requirements.txt`；本文件顶部已加当前状态说明，消除“未部署/未发布”历史说法与 README 的冲突。
- **测试**：新增 `tests/test_v101.py`（3 个离线模拟测试，全部 PASS）；原 `tests/test_regression.py` 的 R3 断言随 v1.0.1 行为同步更新（新行为是抛不含密钥的 ApiError，不再是原始 TimeoutExpired 对象）；`tests/test_r8_timing.py` 仍 PASS。真实微信实测尚未做，v1.0.1 不能写成已验证。

## v1.0.2 补丁（用户对 v1.0.1 包的复核反馈，2026-10-28，本地，未部署、未实机验证）

- **问题 ① Bearer 与上传 URL 脱敏（v1.0.1 遗漏，已确认）**：`_post_via_curl` / `_get_via_curl` 在 `subprocess.run` 抛异常时直接把 `e` 展开进 `ApiError`，`TimeoutExpired` 的 str 含完整 argv（含 `-H 'Authorization: Bearer <bot_token>'` 和 `-d` 请求体），外层 `_sanitize_error(e)` 此时已不是 TimeoutExpired 类型，且正则未覆盖 `Bearer\s+\S+`，所以 bot_token 会进日志和失败任务记录。`send_image` 第 1476 行附近的 CDN 上传 curl 也没有捕获 `TimeoutExpired`，超时后完整 `upload_full_url` 会逃逸到 `drain_outbox` 被判为 needs_check，而不是上传阶段本应归类的 RetryableError。修法：三处 curl 调用在调用位置捕获 `TimeoutExpired` 直接抛不含参数的安全错误（`ApiError("curl POST/GET timeout")` / `RetryableError("upload curl timeout")`），其他异常只保留 `type(e).__name__`；`_sanitize_error` 补上 `Bearer\s+[^\s'"]+` → `Bearer [redacted]`，以及 `"aeskey"` / `"context_token"` 的 JSON 字段脱敏。注意：不能用 `-pass stdin` 替换 `-K`，前者是派生密钥的口令，后者是原始密钥，替换会改变加密结果，本补丁没有这样改。
- **问题 ② 磁盘终态识别（v1.0.1 只修了一半，已确认 + 1 处修正）**：`recover_sending_on_startup` 把中断任务写为 `delivery_state="unknown_restart"` 但未改 text/附件状态；`_is_terminal_job` 只看 text_status 和 attachments 的 status，不看 delivery_state，重启后会漏判。附件循环和文本分支也只排除了 `"sent"`，部分成功、部分 failed/needs_check 的任务在归档失败 + 重启后，失败子项会被再次发送。旧格式单图 `image_path` / `image_status` 也有同样问题：`_is_terminal_job` 在归一化之前被调用，且只读 `attachments`，没看 `image_path`。修法：`recover_sending_on_startup` 和 `drain_outbox` 的终态分支落盘前显式写顶层 `"terminal": True` 并持久化；`_is_terminal_job` 改为：`terminal is True` 或 `delivery_state == "unknown_restart"` 或（全部子项均处于 sent/failed/needs_check 且至少一项为 failed/needs_check）即为终态，`_terminal_attachments()` 同时覆盖旧格式 `image_path`；drain 入口遇到终态任务不再进入发送流程，只重试一次 `os.replace` 归档；文本分支改为 `text_status not in ("sent","failed","needs_check")` 才发，附件循环改为 `status in ("sent","failed","needs_check")` 就跳过。**对复核建议的一处修正**：复核建议里写 `delivery_state in ("failed","needs_check","unknown_restart")` 即为整单终态，但 drain 本身会把 delivery_state 写成最后一个子项的状态，所以“文本 failed + 附件 pending”的任务 delivery_state 就是 failed，却不是整单终态；按建议原样写会挡住那个 pending 附件。因此本补丁只把 `unknown_restart` 当作 delivery_state 层面的整单终态，failed/needs_check 的 delivery_state 仍走子项判定，这一冲突已用测试 2d 覆盖（failed 文本 + needs_check 附件不发，pending 附件照常发一次）。
- **问题 ③ /ping 游标（v1.0.1 已完整修复，v1.0.2 不改）**：复核确认 serve_loop 在 /ping、语音提示、图片失败提示及普通 inbox 落盘失败时均不记 processed 并设 `_batch_enqueue_failed`，批末跳过游标保存；排队提示在 inbox 落盘并记完 processed 之后发送，失败不设批失败标志，不会触发 Worker 重复入队。逻辑严密，无需再改。
- **问题 ④ Connection refused 双路径日志（已确认）**：`post_json` / `get_json` 在 allow_fallback=True 时 `except ApiError: pass` 直接吞掉了 curl 的退出码与错误原因，getupdates 失败时日志只打印回退后 urllib 的 `<urlopen error [Errno 111] Connection refused>`，无法区分 curl 当时报的是什么错。修法：保存 `curl_err`，urllib 也失败时抛出的 ApiError 同时记录 `curl_err` 与 `urllib_err`，以及脱敏后的目标 host:port 和当前生效代理 host:port（不含账密，见 `_desensitized_host_port` / `_effective_proxy_host_port`）；serve 的 getupdates 错误打印也改用 `_sanitize_error(e)` 再做一层防御。另两处纠正：不能说“80% 以上是本地代理”（无证据，只能说优先查代理是因为检查成本低）；`state/status_last.json` 的 `expired` 不能直接解释成运行会话失效（仓库没有生成该文件的代码，源码里 `status == "expired"` 是扫码二维码过期，运行会话失效走 -14 和 NEEDS_RELOGIN）。
- **问题 ⑤ 陌生人复现文档与 README 同步（已确认）**：`cmd_status` 只输出 session、needs_relogin、inbox 数量、cursor 四项本地文件状态，不代表在线，README 已补注；README 新增“陌生人从零部署清单”：装依赖、本地扫码后人工上传 session.json（0600）、按模板配 Hook（5 秒轮询）与 Worker（原子写 outbox/pending，tmp + os.replace，0600）、启动与最小验收（/ping、文本、单图、回传图）。源码已自动创建队列目录、按脚本位置生成路径，这两点不再重复写成“修复”。README 目录里的旧 2337 行 / SHA 7cd359dd… 已同步为 v1.0.2 的 2534 行 / SHA 3535d58bfc5c210abbca2aca3c49b1a6ea16ba1fb6ee478ebb5eeeb75021fec7，并注明运行服务仍是 06:02 部署的 2337 行版。
- **测试**：新增 `tests/test_v102.py`（Bearer 脱敏 + curl/upload 超时、unknown_restart / 全终态 / 旧格式 image_path 在清空内存集合模拟重启后仍不重发、子项拦截、双路径错误脱敏，全部 PASS）；`tests/test_v101.py`、`tests/test_regression.py`、`tests/test_r8_timing.py` 仍 PASS。真实微信实测尚未做，v1.0.2 不能写成已验证。

- **v1.0.2 边界补修（用户复核，部署前）**：`drain_outbox` 原来用 `any_terminal`（任一子项 failed/needs_check）就把整单写 `terminal=True` 并移入 failed/，导致同单里 RetryableError 的子项（pending，attempts=1）失去剩下 2 次重试。修法：归档条件收紧为 `_is_terminal_job(job)`（全部子项已 sent/failed/needs_check 且至少一项 failed/needs_check）；只要还有子项 pending，就留在 pending/，下一轮子项拦截会跳过已终态子项、只重试 pending 子项。测试 `test_v102.py` 新增 2d/4：第一轮 retryable+permanent 后任务仍在 pending、terminal 未置、状态分别为 pending/failed；第二轮 pending 子项成功、failed 子项跳过，任务才归档到 failed/。本补丁后 bridge.py 为 2534 行，SHA 3535d58bfc5c210abbca2aca3c49b1a6ea16ba1fb6ee478ebb5eeeb75021fec7，README 已同步；2026-10-29 00:01 已部署，00:11-00:13 最小微信实测 4 项全部通过，Tag v1.0.2 已打。


## v1.0.3（2026-10-29，用户独立复核 v1.0.2 后的收尾，本地修复副本，未部署、未发布、未做微信实测）
- **网络诊断**：新增 `_describe_urllib_error(e)`——`URLError` 的具体原因在 `e.reason` 里，日志现在写 `URLError(ConnectionRefusedError: errno=111: ...)` 或 `URLError(TimeoutError: ...)`，连接拒绝和超时一眼可分，且不带请求 URL；`_effective_proxy_host_port(target_url)` 在返回代理前先用 `urllib.request.proxy_bypass(host)` 判定，命中 `NO_PROXY` / `no_proxy` 时返回 `none (no_proxy bypass)`，与 urllib 自身的绕过行为一致，不再误报代理。post_json / get_json 的 curl+urllib 双路径错误和 HTTPError 路径都已传 target_url。
- **异常链切断**：6 处包装点全部加 `from None`：aes_ecb_encrypt / aes_ecb_decrypt 的 TimeoutExpired；_post_via_curl / _get_via_curl 的 TimeoutExpired 和通用异常；send_image CDN 上传 curl 的 TimeoutExpired 和通用异常；send_image CDN 上传 urllib 回退的通用异常。注意：`from None` 设置的是 `__suppress_context__ = True` / `__cause__ = None`，**不会把 `__context__` 清空**——断言 `__context__ is None` 对正确代码也会失败，正确断言是抑制标志 + 格式化堆栈里没有 "During handling of the above exception" 且没有密钥/URL。
- **人工重投文档**：docs/troubleshooting.md 的 needs_check 处置已按 v1.0.3 修正为 5 步（逐子项确认未送达 → 仅该子项改 pending 且 attempts 重置为 0 → 删顶层 terminal → 移回 pending/ → 重启 serve 或改名 `<id>_retry.json` 以绕过内存 _TERMINAL_TASK_IDS）；不改 _TERMINAL_TASK_IDS 的拦截优先级，因为它只防“移入 failed/ 失败的终态任务”被重复发，磁盘修好后重启/改名即可，逻辑最简单。
- **测试**：新增 tests/test_v103.py（urllib 原因区分 + NO_PROXY bypass + 6 处 traceback 脱敏，含 6 处调用点全覆盖）；v101、v102、原回归、R8 时序全部仍 PASS。
- 本补丁后 bridge.py 为 2572 行，SHA d41d9b0d86df8377119c56c32715f81e31482b979ae731d78bef7fadf690fd57（v1.0.3 最终版，含下面 P1/P2/P3），README 已同步；2026-10-29 01:20 已部署，01:23-01:27 最小微信实测 4 项全部通过，Tag v1.0.3 已打。

- **v1.0.3 最终版（P1/P2/P3，2026-10-29，新 Gemini 复核后）**：
  - P1（仅文档，不改 bridge.py 旧 SHA 之外的部分）：docs/troubleshooting.md 第 3 步和 README needs_check 段落已分开写清——文本子项改顶层 text_status 且 text_attempts 重置为 0，附件子项改 attachments[i].status 且 attempts 重置为 0，旧格式单图改顶层 image_status；漏改 text_attempts 的后果（只剩 1 次机会）已写明；改名重投的 typing 票据小副作用（改名后查不到原票据，但人工重投时 typing 早已超时，无实际危害）已注明。
  - P2（改代码）：_effective_proxy_host_port 改为先找代理、无代理直接返回 "none"，有代理且命中 proxy_bypass 才返回 "none (no_proxy bypass)"；test_v103.py 补了无代理 + NO_PROXY=* 时返回 "none" 的用例。
  - P3（改代码）：post_json / get_json 的 4 处双路径 ApiError 汇总报错（HTTPError 和通用异常各 2 处）全部加 from None，使汇总错误本身的堆栈也干净；TokenExpired 的控制流信号未动。
  - 最终版 bridge.py 为 2572 行，SHA d41d9b0d86df8377119c56c32715f81e31482b979ae731d78bef7fadf690fd57，5 套测试全部仍 PASS，复核包为 11 个文件（上一版说明误写 12 个，已更正）。


## v1.0.4 P0 收敛版（2026-10-29，独立复核中，尚未部署）

- 去重：看门狗 hook state 增加 serve_down_event（event_id、boot_id、first_seen_ms、last_wake_ms、wake_count、last_reason），同一掉线只初始唤醒一次，300 秒后重试一次；恢复时静默关闭事件；boot_id 变化时遗留事件按新事件处理，立即唤醒；dry run 不消耗检测（运行时 hook_state_set 跳过 dry run）。
- 快照：serve.log 关键行（lock acquired、getupdates error、queued inbox）加本地 ISO 时间戳，旧 grep 子串保留；看门狗每 tick 记录 checked_at、pid_file 值、proc 存在性、cmdline 首词、check_reason（pid_file_missing / pid_file_empty / pid_file_unreadable / proc_missing / proc_cmdline_mismatch / kill_failed），不再全折叠成 false。
- 通道状态：bridge 新增 state/channel_status.json（atomic_write_json，0600），每次 getupdates 后更新：status、last_attempt_ts、last_success_ts、consecutive_failures、last_error（经 _sanitize_error 脱敏截断）、process_pid。成功空响应算成功；无新消息不算通道失败；ret -14 走 NEEDS_RELOGIN，不记入通道错误。
- 并发启动：已证伪，无新增锁。serve_loop 先 flock，失败返回 2，PID 写入在 flock 成功之后；tests/test_v104.py 用持锁进程验证失败启动者不改 serve.pid。
- 修正：测试中发现 save_state 空 seen 列表时 jq 管道回退会产出 "[]\n[]" 导致 hook state 写空，dedup 失效；已改为 seen 为空时直接用 '[]'，并由 test_v104_watchdog.sh 覆盖。
- errno 111 排查优先级（收敛后）：在确认默认 urllib 走 HTTP 代理的前提下，优先排查“桥接器 -> 代理”；代理 CONNECT 返回非 200 时 CPython 通常报 Tunnel connection failed，不会原样传回代理机上的 errno 111；最终以实际连接地址为准，日志 target 不能替代实际连接地址。
- “45+45 秒”只是串行尝试的粗略描述，不是严格总耗时上限（urllib timeout 针对阻塞操作）。
- 网络测试不必全部等待 P0：代理配置检查、仅记录连接地址的跟踪、带独立时间戳的连通性测试，现在就有意义；P0 只是让真实故障更容易关联。
- 发布记录要求：Tag 说明必须同时包含 bridge.py 和看门狗脚本的版本/哈希，部署结果可核对。

## v1.0.4 复核补丁（2026-10-29，退回修改后，仍未部署）

- 实例标识：_init_channel_status() 在 flock 成功后、serve.pid 写入前执行，生成 uuid run_id，写 serve_run.json（run_id、pid、started_ts）和 pending channel_status（last_success_ts 为空）。后续 _record_channel_status 保留该 run_id 和 process_started_ts。PID 复用也不能把旧成功当成新成功。
- Worker 规则：删除 ok/0 兜底，确认通道恢复必须同时满足 status ok、consecutive_failures 0、process_pid 等于当前 serve.pid、run_id 等于 serve_run.run_id、last_success_ts 不为空且不早于 process_started_ts；任一不满足只报“进程已恢复，通道待确认”。
- 快照：snapshot_json() 统一字段，全部 wake/silent 分支先 log 快照再退出；真实运行时 wake/silent 会退出脚本，已按 ~/hooks/runtime/hatch_hook_runtime.sh 核对，不依赖 wake 后的补记。PID 字段统一为 pid；PID 文件 cat 失败记 pid_file_unreadable，不冒充空文件。
- seen：首次/重试掉线带 inbox 时先 mark_seen 再唤醒；seen 的语义是“已包含在一次唤醒里”，不是“已被 Worker 处理”，不等于严格一次投递。
- 健壮性：_record_channel_status 全程不抛异常；prev 非对象、失败数非整数/布尔/负数、last_success_ts 非字符串时重置为安全默认值并打印警告；写入失败打印警告，不静默吞掉。
- 测试补齐：test_v104.py 增加实例隔离与字段类型异常；test_v104_watchdog.sh 的 stub log 改为保留 JSON 第二参数，并增加全部终止分支快照字段、首次/重试 inbox 去重、boot_id 变化立即新事件、299 秒仍抑制/300 秒重试同 event_id 且 wake_count 加 1。
- 复核清单第 6 项：真实 hook runtime 的 wake/silent 退出、hook state 写入与 dry run 契约已核对运行时脚本；同一 hook tick 串行性未在包内断言，部署后以实际 hook 日志为准。

## v1.0.4 GPT 裁决补丁（2026-10-29，仍未部署）

- 启动期与运行期失败策略解耦：_init_channel_status 任一必要写入失败即抛异常，serve_loop 捕获后返回 1，不写 serve.pid，不冒充 token 过期；_record_channel_status 写入失败只警告继续，不逃出循环。
- 内核身份：_boot_id() 读 /proc/sys/kernel/random/boot_id；_proc_starttime() 解析 /proc/self/stat 在最后一个右括号后的第 22 字段（jiffies），comm 含空格/括号也不会错位。serve_run.json 含 run_id、pid、boot_id、proc_starttime；channel_status 也保留 boot_id/proc_starttime。
- Worker 规则：删除 last_success_ts >= process_started_ts 的时间比较；last_success_ts 只作可读新鲜度证据。确认通道恢复必须同时满足 status ok、失败数 0、process_pid 等于当前 serve.pid、run_id 等于 serve_run.run_id、boot_id 等于当前 boot_id、proc_starttime 等于当前 /proc/<pid>/stat 第 22 字段。任一缺失/不匹配/读取中变化都只能是未确认。
- inbox 重提醒：hook state 增加 inbox_wake {文件: last_wake_ms}；首次见或仍在 inbox 且距上次唤醒 >=300 秒时纳入唤醒；只有实际唤醒时刷新；被抑制 tick 不刷新；文件离开 inbox 后在下次保存状态时清理；进程恢复不取消重提醒。seen 仍保留作兼容，但唤醒决策以 inbox_wake 为准。
- 故障注入测试 test_v104_gpt.py：旧匹配文件 + PID 复用（旧 proc_starttime=1）下新初始化覆盖为 pending 新 run_id；写 serve_run 前失败与写 channel_status 前失败均返回 1 且 serve.pid 哨兵不动；无注入时先 pending 再空成功才变 ok/0；运行期写失败只警告不抛。
- 看门狗重提醒测试 test_v104_gpt_watchdog.sh：t0 唤醒、299 秒静默且不刷新、300 秒同一文件重提醒、文件离开后清理且不再唤醒。
- GPT 对 Gemini 修正结论的限定已采纳：初始化失败终止不是唯一理论正确架构，但在当前补丁采用启动失败策略更容易验收；TTL 不是唯一解，必要的是未消费文件具有有界重试或可靠确认机制，本补丁采用 300 秒重提醒。

## v1.0.4 复核第二轮补丁（2026-10-29，仍未部署）

- Worker 端到端核验：test_v104_worker_verify.py 用真实 dummy 进程（argv 为 python3 bridge.py serve）调用生产 verify_channel_recovery()，覆盖旧匹配文件 + PID 复用（旧 proc_starttime=1）必为 proc_starttime_mismatch、初始化中间态（新 serve_run、旧 channel）必为 run_id_mismatch、同实例 pending 必不恢复、ok 必恢复、旧格式无内核身份必不恢复、进程死亡必为 process_not_running；另在 test_v104_gpt.py 中补 PID 复用初始化失败（serve.pid 已等于测试进程 PID，失败仍返回 1 且 PID 不变，生产核验必不恢复）与 replace 阶段失败（os.replace 抛错仍返回 1、PID 不变、旧 JSON 仍可解析）。
- 诊断外逃：test_v104_diag.py 同时让状态写入失败与 print 抛 BrokenPipeError，_record_channel_status 仍返回 error，不向外抛。
- 299 秒测试强度：test_v104_gpt_watchdog.sh 在 299 秒静默 tick 前后直接断言 inbox_wake 值不变，再做 300 秒边界；不再靠后续改写时间掩盖错误刷新。
- README 最新规则声明已加：部署时只用最新 Worker 规则与最新哈希，历史段旧时间比较规则与旧哈希已作废，不得照抄。

## v1.0.4 部署与微信实测（2026-10-29）

- 上传 18:46：GitHub main，4 个提交 e3967a7 / 9882ed5 / 7ce8c2d / 21f2e6f，提交总数 25 -> 29。
- 部署 18:49-18:51：旧 v1.0.3（PID 1758，SHA d41d9b0d…）备份到 workspace/wechat-bridge-backup-20261029-pre-v104/；新 serve PID 18981，run_id 08b95f00ee10402db3aa2bc9ce29aeef，通道 pending -> ok 于 18:50:58，verify-channel recovered=true；session.json 未动，NEEDS_RELOGIN 不存在，inbox/pending 为空；并发重启尝试被 flock 挡回。
- 微信实测：/ping 快速回复（18:55:41）、普通文字、用户图片、生成花枝鼠形象并回图，4 条全部有回复；19:02:14 verify-channel 仍 ok，19:04:53 最近一次成功。
- Tag v1.0.4 随后在 GitHub Releases 创建，指向本次文档提交。
