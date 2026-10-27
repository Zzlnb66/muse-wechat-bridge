# 开源前修复说明（按审查编号逐项，独立副本）

> 本目录是**独立修复副本**，不在正在运行的 `~/workspace/wechat-bridge/` 上修改。
> 运行服务仍是 PID 26959 的旧版本；本修复版**没有**部署到运行服务，也**没有**重做微信实测。
> 修复后需要先跑离线回归测试，再在测试环境做必要的微信实测，才能发布。

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
