# THIRD_PARTY_NOTICES

## 参考来源（非本项目自有代码）

### Tencent/openclaw-weixin

- 仓库：Tencent/openclaw-weixin
- 参考提交：874637a0dbc24050f72d1385add4fc6170a2d726
- 参考内容：`docs/protocol.md` 中的 iLink 后端接口定义（登录、长轮询、发送、上传、typing 等字段与流程）、接口字段名、加密流程说明（AES-128-ECB、密钥字段优先级）。
- 未逐字复制：本仓库的 `bridge.py` 是按上述协议文档自行实现的 Python 桥接与队列实现，未发现逐字复制上游 TypeScript 代码；此前未做逐行比对，如作者发现有逐字复制段落，应在此处补列文件与行范围。
- 上游许可证（核查记录）：
  - 2026-10-28 查到的上游 `main` 分支 `LICENSE` 文件正文开头写明：“Tencent is pleased to support the open source community by making openclaw-weixin available. openclaw-weixin is licensed under the MIT.”
  - 其后为标准 MIT 许可段落，包括：免费授予任何获得副本的人使用、复制、修改、合并、发布、分发、再许可和/或出售副本的权利，且上述版权声明须包含在所有副本或实质部分中；软件按“现状”提供，不作任何明示或暗示保证。
  - 同一快照下，上游仓库的当前提交恰好就是上述 874637a... 提交（仓库页面显示 Current commit: 874637a0dbc24050f72d1385add4fc6170a2d726），因此本次核查的 main LICENSE 与该提交在该快照下是同一版本。
  - 保留的疑点：上游仓库元数据里许可证一栏曾显示 Other (NOASSERTION)，与 LICENSE 文件正文的 MIT 不完全一致；审查报告此前也曾未能成功取得该历史提交的 LICENSE。彻底闭环的本地取证命令（作者本地电脑执行）：
    ```
    git clone https://github.com/Tencent/openclaw-weixin.git
    cd openclaw-weixin
    git show 874637a0dbc24050f72d1385add4fc6170a2d726:LICENSE
    git show 874637a0dbc24050f72d1385add4fc6170a2d726:docs/protocol.md
    git log -1 874637a0dbc24050f72d1385add4fc6170a2d726
    ```
    将上述命令的实际输出贴入本文件后，方可视为 100% 闭环；在此之前，本文件中的上游 MIT 结论以 2026-10-28 的网页快照为准，不可直接写成已本地验证。

## 本项目自有代码

- `bridge.py`：桥接、文件队列、看门狗配合、图片收发、失败证据、发送状态机、独立发送线程等实现，为本项目原创。
- `docs/`、`examples/`、`tests/`：本项目原创文档、模板与回归测试。
- 自有代码的许可证：MIT，见仓库根目录 LICENSE 文件（Copyright (c) 2026 ZZLNB）。上游为 MIT，本项目选用 MIT，与上游兼容。

## 依赖

- Python 标准库
- Pillow（图片完整解码）
- 系统 openssl（AES-128-ECB）
- 系统 curl（首选传输与图片 curl 回退）
- qrcode（仅登录时渲染二维码，虚拟环境内使用）
