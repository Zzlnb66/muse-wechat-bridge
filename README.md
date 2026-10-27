# Muse 接入微信私聊：iLink 桥接（技术交接脱敏版）

> **本项目是 Muse 环境的微信桥接器，非独立聊天机器人。** 没有 Muse 账号和运行环境、没有人工配置的看门狗与 Worker，单靠本仓库不能完整运行。

> **如何获取初始 session.json（本地扫码上传流程）：** 云端二维码登录已失败并废弃；在本地电脑上运行 `python3 bridge.py login`，用微信扫码（二维码约 2 分钟过期），登录成功后得到 `state/session.json`（0600），人工上传到云端虚拟机的同一目录，再启动 `bridge.py serve`。`cmd_login` 在云端直接跑通不是默认路径，README 里不要假设它一定能用。

> 本目录是一次**技术交接导出**，不是一个可以直接 `git clone` 就跑通的完整产品。
> 它包含真实运行代码的脱敏副本，以及按实际记录整理的架构、开发史、排障和测试范围。
> **没有发布到 GitHub，没有修改正在运行的服务，没有重做微信实测。**

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
| 看门狗 Hook 脚本 | Muse 的 Hook 调度环境，每 5 秒轮询一次（见架构文档） | 部分：本导出只含桥接侧说明，Hook 定义本身属于 Muse 环境配置，**不在本仓库** |
| Worker（生成回复的 Muse 代理） | Muse 的代理运行环境，由 Hook 唤醒 | **不是本仓库代码**；它的提示词、历史加载、图片阅读、工具权限都依赖 Muse |
| 微信 / iLink / CDN | 腾讯侧：`https://ilinkai.weixin.qq.com`，CDN `https://novac2c.cdn.weixin.qq.com/c2c` | 第三方平台，不是本项目开发 |

## 陌生人拿到仓库，能复现到哪一步

**能复现的：**

- 读懂协议调用、队列格式、收发图逻辑、失败留证格式；
- 在有合法 iLink 登录态的前提下，运行 `bridge.py` 的登录、状态、收发、排队部分做本地模拟和单元检查。

**不能只靠本仓库完整复现的（确切缺口）：**

1. **必须拥有 Muse 账号和运行环境**。Worker、Hook 调度、图片视觉理解、会话管理都不是 `bridge.py` 能替代的。
2. **必须人工配置一个 Muse 任务/会话**：看门狗 Hook、Worker 提示词、历史加载规则、敏感操作指引、图片事件关联规则，都需要在 Muse 环境里另行配置。本导出只整理了它们的**行为描述**，没有导出 Muse 私有内部实现，也没有一个“一键导入”文件。
3. **必须自己取得登录态**。`state/session.json` 里的 `bot_token`、`context_token`、`ilink_user_id` 是私有的，不能公开，也不能从本仓库推导。取得方式见 `docs/architecture.md`“登录态”一节：本地登录取得后人工放到云端，文件权限 0600。
4. **iLink 账号本身**：需要在微信里有 ClawBot 插件/机器人账号，并完成扫码登录。云端二维码在本项目里实际失败过（见开发史），不要假设云端扫码一定能用。
5. **图片视觉理解**：Worker 读图依赖 Muse 的视觉能力，`bridge.py` 只负责把图片落盘并把路径写进 `inbox` 记录，不做 OCR 或图像理解。

所以结论是：**桥接层可以开源，完整“微信私聊里的 Muse”不能只靠桥接层复现。**

## 快速开始（给已经有 Muse 环境和 iLink 账号的人）

```bash
python3 --version   # 实际使用环境为 Python 3.x，标准库为主
pip install Pillow  # 图片完整解码用；qrcode 仅登录时渲染二维码 PNG 用
python3 bridge.py status
python3 bridge.py serve
```

- 配置见 `examples/session.example.json`、`examples/config.example.md`。真实 `session.json` 不要提交。
- 启动前先确认目录结构：`state/`、`inbox/`、`done/`、`outbox/pending|sent|failed/`、`media/inbound|outbound/`、`failures/`，`bridge.py` 的 `ensure_dirs()` 会创建它们。
- 停止：向 `serve` 进程发终止信号；看门狗在 Muse 环境里会按配置处理拉起（本仓库不含看门狗定义）。
- 观察状态：`python3 bridge.py status`、`state/serve.log`、`state/serve.pid`、队列目录计数。

## 依赖

- Python 标准库为主：`argparse`、`json`、`os`、`subprocess`、`urllib`、`uuid` 等。
- `Pillow`：图片完整解码（`im.load()`，不只读头）。
- `openssl` 命令行：AES-128-ECB 解密/加密（`openssl enc -aes-128-ecb`），本项目未使用 Python `cryptography` 库（当时环境不可用，见开发史）。
- `curl` 命令行：`post_json`/`get_json` 的首选传输（`--http1.1`），失败时回落 `urllib`；图片 `curl` 回退也用它。
- `qrcode`（仅登录渲染用，虚拟环境里使用；运行时 `bridge.py` 本身不依赖它）。

没有 `requirements.txt` 锁文件可导出：原项目没有锁文件，**不要伪造**。上面清单就是实际核对到的依赖。

## 已知限制（摘要，完整版见各文档）

- 回复慢：看门狗 5 秒轮询 + 下载重试/`curl` + Worker 看图排队，实测两张图约 1 分钟才陆续发出。
- 一条消息里同时带两张图：**未验证**（已验证的是分两条各发一张）。
- 虚拟机整体替换恢复：**未验证**（只验证过进程被杀后拉起）。
- 提示注入：只通过了两个样例（发邮件、连接 OpenClaw），**不能写成彻底解决**。
- 下载截断的底层根因：已证实 `urllib` 截断、`curl` 能救回，但 CDN、代理、网络链路哪一环导致，**无法确认**。
- 失败留证即使 0600 也不适合公开；本导出不含任何真实 `failures/`、队列、日志、图片样本。

## 目录

- `bridge.py`：可公开的完整桥接源码（与当前运行版本对应，脱敏说明见 `docs/architecture.md` 末尾）。
- `examples/`：配置示例和脱敏请求结构示例。
- `docs/architecture.md`：架构、组件、消息流、状态。
- `docs/development-history.md`：路线、开发经过、关键修复。
- `docs/troubleshooting.md`：图片、解密、收发、运行故障排查。
- `docs/testing.md`：测试、证据、通过范围、未验证项。
- `SECURITY.md`：凭据、权限边界、安全限制。
- `.gitignore`：排除凭据、运行数据、日志、媒体、临时文件。

## needs_check 人工处置
`outbox/failed/` 下出现 `needs_check` 文件时：先在微信确认对方是否已收到；已收到则删除或移入 `sent/`；未收到则改回 `pending` 并移回 `outbox/pending/` 重投；未确认前不要反复重发。详见 `docs/troubleshooting.md`。

## 账号安全表述
走腾讯官方 iLink 授权通道，风险低于传统逆向外挂，但不承诺免受平台灰度规则变动影响，也不代表永久免责或零风险。
