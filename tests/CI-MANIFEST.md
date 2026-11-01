# CI 回归检查 Manifest（v1.5.0 item 4，改名说明见下）

- 入口：`python3 tests/run_ci.py [--bridge bridge.py] [--skip-100mb]`，从仓库根运行。
- 名称：本项不阻止合并、发布或部署，故称“CI 回归检查 / 质量信号”，不称“门禁”。以后若设发布门禁，须另行决定：只拦正式发布节点，且要求正式发布对应候选 CI 通过（GitHub required checks 才实际约束合并；本轮不设）。
- 退出码铁律：任一必跑套件失败 → 入口以非零退出并保留结果；禁止日志写 FAIL 但进程返回 0。入口另扫描各套件输出的失败标记（`FAIL <名>` / `FAILED:`），防止 test_v101 旧假绿一类“日志 FAIL 但退出 0”。
- 通过标准：必跑套件以退出 0 为准；凡打印显式计数的套件，计数须与文件名义一致（如 quote 10/10）；入口输出的 SUMMARY 为最终口径。
- 预期项数口径：凡套件自身打印计数的，以该计数为准；其余以“退出 0 + 无失败标记”为准，不虚构数字。
- 解释器：CI 用系统 python3（它拥有 python-docx、openpyxl、pypdf；M0 venv 缺这些包）。本地跑同一入口时也用系统 python3。
- 平台：首版 Ubuntu 非 root，覆盖 Linux flock、/proc、0600 与 shell Hook。
- 工具：curl、jq、openssl、ffmpeg/ffprobe 由 workflow 事先准备；缺依赖不得悄悄变 skip——入口只允许下面明记的两类条件跳过，且每次跳过都打印原因。
- 隔离：不使用生产 session、聊天记录或真实媒体；全部套件在临时目录用合成样本运行。
- 版本铁律：CI 与发布验收必须指向最终组合代码的确定 SHA-256/commit；修复副本通过不等于 hunk 移植后的产物通过；不得把公开 CI 未跑过的组合写成已通过。

## 条件跳过（仅这两类，全部打印原因）

| 套件 | 跳过条件 | 原因 |
|---|---|---|
| test_m2_model_config | M0 专属包（faster-whisper / ppocr-lite / ai-edge-litert）任一不可导入 | 需 M0 venv；其无权重控制逻辑另由 test_analyzer_recovery 覆盖 |
| test_file_inbound_100mb | `--skip-100mb`（GitHub CI 必带） | 断言修复副本专属的 100 MiB 常量；公开树为 10 MiB 系，不适用 |

## 已知覆盖缺口（不虚构覆盖）

- **提醒故障路径**：修订稿第 3 节要求进 CI，但提醒功能属第 1 项（已延期单独重设计），目前无对应代码与套件。本 manifest 不把别项测试冒充为提醒覆盖；第 1 项实施时必须补测试并更新本表，否则 CI 不得宣称覆盖提醒。

## 套件表（命令均相对仓库根；{BRIDGE} = 被测 bridge.py 路径）

| 套件 | 命令 | 参数 | 依赖 | 平台 | 通过标准 | 必跑 | 覆盖映射 |
|---|---|---|---|---|---|---|---|
| py_compile bridge.py（入口自检） | `python3 -m py_compile` | {BRIDGE} | 系统 python | Linux | 退出 0 | 是 | 编译 |
| test_aes_switchover | `python3 tests/test_aes_switchover.py` | — | pycryptodome | Linux | 退出 0 + N/N passed | 是 | 进程内 AES |
| test_aes_vectors | `python3 tests/test_aes_vectors.py` | — | pycryptodome、fixtures/aes_vectors.json | Linux | 退出 0 + N/N passed | 是 | AES 向量 |
| test_analyzer_recovery | `python3 tests/test_analyzer_recovery.py` | — | analyzer/stages.py（伪造解释器） | Linux | 退出 0 + N/N passed | 是 | 无权重控制逻辑（阶段校验、run_id、缺解释器） |
| test_backoff_tiers（新增） | `python3 tests/test_backoff_tiers.py` | {BRIDGE} | — | Linux | 退出 0 + ALL PASSED | 是 | 第 2 项退避：序列、封顶、空成功重置、非限流打断、-14 优先、停止可中断、凭据脱敏 |
| test_chat_commands | `python3 tests/test_chat_commands.py` | — | — | Linux | 退出 0 | 是 | 新指令（第一档指令表） |
| test_cli_send_health | `python3 tests/test_cli_send_health.py` | — | — | Linux | 退出 0 | 是 | CLI 入口 |
| test_disk_fields | `python3 tests/test_disk_fields.py` | — | — | Linux | 退出 0 | 是 | 磁盘水位字段 |
| test_doc_parser | `python3 tests/test_doc_parser.py` | — | python-docx、openpyxl（系统 python） | Linux | 退出 0 | 是 | 文档锚点 |
| test_doc_parser_empty_layer | `python3 tests/test_doc_parser_empty_layer.py` | — | pypdf（系统 python） | Linux | 退出 0 | 是 | 空文本层 |
| test_doc_parser_pypdf | `python3 tests/test_doc_parser_pypdf.py` | — | pypdf（系统 python） | Linux | 退出 0 | 是 | pypdf 救援、锚点 |
| test_doc_parser_wiring | `python3 tests/test_doc_parser_wiring.py` | — | — | Linux | 退出 0 + N/N passed | 是 | 锚点自动接入、损坏文件（未知字节） |
| test_evidence_audit | `python3 tests/test_evidence_audit.py` | — | — | Linux | 退出 0 + N/N passed | 是 | 证据审计 |
| test_file_inbound | `python3 tests/test_file_inbound.py` | — | 本地 HTTP 服务器（curl 路径） | Linux | 退出 0 + N/N passed | 是 | 文件接收、上限、损坏文件（截断密文、超 cap） |
| test_file_inbound_100mb | `python3 tests/test_file_inbound_100mb.py` | — | 大合成文件（临时目录） | Linux | 退出 0 + N/N passed | 是（修复副本内） | 100 MiB 系上限（修复副本专属） |
| test_getupdates_anomalies | `python3 tests/test_getupdates_anomalies.py` | — | — | Linux | 退出 0 | 是 | -14 计数、分类器（限流/网络/未知） |
| test_gpt_followup | `python3 tests/test_gpt_followup.py` | — | — | Linux | 退出 0 + N/N passed | 是 | R1 mismatch、分析器路径 |
| test_job_shape_guard | `python3 tests/test_job_shape_guard.py` | — | — | Linux | 退出 0 | 是 | 任务形状守卫 |
| test_long_text | `python3 tests/test_long_text.py` | — | — | Linux | 退出 0 | 是 | 长文分段、长文阻断 |
| test_m2_model_config | `python3 tests/test_m2_model_config.py` | — | M0 venv 专属包 | Linux | N passed, 0 failed | 条件 | 模型配置、缺权重、哈希不匹配、超时、旧 run_id、partial/failed 判定 |
| test_multi_instance_isolation | `python3 tests/test_multi_instance_isolation.py` | — | — | Linux | 退出 0 | 是 | 多实例隔离核查 |
| test_observability | `python3 tests/test_observability.py` | — | — | Linux | 退出 0 | 是 | 可观测字段、汇总口径 |
| test_offline_receipt | `python3 tests/test_offline_receipt.py` | — | — | Linux | 退出 0 | 是 | 离线回执 |
| test_outbound_sanitize | `python3 tests/test_outbound_sanitize.py` | — | — | Linux | 退出 0 | 是 | 出站净化、脱敏 |
| test_progress_receipt | `python3 tests/test_progress_receipt.py` | — | — | Linux | 退出 0 | 是 | 进度回执 |
| test_quote_v1 | `python3 tests/test_quote_v1.py` | — | — | Linux | 退出 0 + 10/10 PASS | 是 | 引用解析 |
| test_r2_p0 | `python3 tests/test_r2_p0.py` | — | — | Linux | 退出 0 | 是 | R2 P0 项 |
| test_r8_timing | `python3 tests/test_r8_timing.py` | {BRIDGE} | — | Linux | 退出 0 | 是 | 发送时序 |
| test_recipient_gate | `python3 tests/test_recipient_gate.py` | — | — | Linux | 退出 0 | 是 | R1 门禁（recipient_missing/mismatch） |
| test_regression | `python3 tests/test_regression.py` | {BRIDGE} | — | Linux | 退出 0 + ALL PASSED | 是 | 回归总集、游标提交 |
| test_same_source_merge | `python3 tests/test_same_source_merge.py` | — | — | Linux | 退出 0 | 是 | 同源合并 |
| test_sqlite_shadow | `python3 tests/test_sqlite_shadow.py` | — | sqlite3 | Linux | 退出 0 | 是 | shadow 三档、发送开始门、终态镜像、崩溃未知不重发 |
| test_sqlite_switch | `python3 tests/test_sqlite_switch.py` | — | sqlite3 | Linux | 退出 0 | 是 | 模式切换（生产期望档） |
| test_sqlite_switch_fixes | `python3 tests/test_sqlite_switch_fixes.py` | — | sqlite3 | Linux | 退出 0 | 是 | 切换修复项 |
| test_storage_module | `python3 tests/test_storage_module.py` | — | — | Linux | 退出 0 | 是 | 存储模块（原子写、权限） |
| test_v101 | `python3 tests/test_v101.py` | {BRIDGE} | Pillow（系统 python） | Linux | 退出 0 + ALL PASSED | 是 | 发送超时脱敏（1b）、失败移动不重发、游标不推进；1a 已修假绿（进程内 AES 无 subprocess） |
| test_v102 | `python3 tests/test_v102.py` | {BRIDGE} | — | Linux | 退出 0 + ALL PASSED | 是 | Bearer/凭据脱敏 |
| test_v103 | `python3 tests/test_v103.py` | {BRIDGE} | — | Linux | 退出 0 + ALL PASSED | 是 | urllib 错误可区分 |
| test_v104 | `python3 tests/test_v104.py` | {BRIDGE} | — | Linux | 退出 0 | 是 | 通道状态（成功重置、失败计数） |
| test_v104_diag | `python3 tests/test_v104_diag.py` | {BRIDGE} | — | Linux | 退出 0 | 是 | 通道状态写失败不抛 |
| test_v104_gpt | `python3 tests/test_v104_gpt.py` | {BRIDGE} | — | Linux | 退出 0 | 是 | 内核身份绑定（run_id/pid/boot_id） |
| test_v104_worker_verify | `python3 tests/test_v104_worker_verify.py` | {BRIDGE} | — | Linux | 退出 0 | 是 | verify-channel 恢复核验 |
| test_video_v1 | `python3 tests/test_video_v1.py` | — | ffmpeg/ffprobe（伪造 curl 可替代部分路径） | Linux | 退出 0 | 是 | 视频收发 |
| test_waiting_prompt_merge | `python3 tests/test_waiting_prompt_merge.py` | — | — | Linux | 退出 0 | 是 | 等待提示合并 |
| test_v104_watchdog.sh | `bash tests/test_v104_watchdog.sh` | watchdog 脚本路径 | bash、jq | Linux | 退出 0 | 是 | Hook 去重（看门狗快照、唤醒去重） |
| test_v104_gpt_watchdog.sh | `bash tests/test_v104_gpt_watchdog.sh` | watchdog 脚本路径 | bash | Linux | 退出 0 | 是 | Hook 去重（300 秒重提醒、静默不刷新） |

## 前置修复记录（test_v101 假绿，GPT 指出、已修）

- 旧 test_1：patch subprocess.run 后调现行进程内 AES，超时根本不触发，随后 `assert False` 又被紧随的 `except Exception` 捕获，只核查字符串不含密钥即当通过。
- 新 test_1a：只断言进程内行为——计数 mock 必须记录零次 subprocess 调用、加解密往返正确、坏密钥只 raise ApiError；不捕获宽泛异常。
- 新 test_1b：保留原 drain 级超时脱敏（send_image 抛带密钥 argv 的 TimeoutExpired，落盘任务不得含密钥）。
