# 配置示例（全部为占位符）

- 运行目录：`wechat-bridge/`（本仓库根目录即为运行根目录，`bridge.py` 用自身所在目录推导 `state/` 等路径）。
- 真实登录态：`state/session.json`，权限 0600，格式见 `session.example.json`，**不要提交真实文件**。
- 队列目录（`bridge.py` 的 `ensure_dirs()` 会创建）：
  - `state/`：`session.json`、`cursor.txt`、`processed_ids.json`、`last_image_event.json`、`serve.pid`、`serve.log`、`serve.lock`、`queue_notice_counter.txt`
  - `inbox/`、`done/`、`running/`
  - `outbox/pending/`、`outbox/sent/`、`outbox/failed/`
  - `media/inbound/`、`media/outbound/`
  - `failures/`（失败留证，0600，不公开）
- 看门狗：属于 Muse 环境配置，不在本仓库；轮询间隔在实际项目里为 5 秒（系统最低），需要读者在自己的 Muse 环境里重建。
- Worker 提示词：属于 Muse 环境配置，不在本仓库；需要重建的内容包括：历史最近 10 轮、敏感操作回 App、视觉注入硬规则、图片事件关联规则、发图时先把文件放进 `media/outbound/` 再写 `outbox`。
- 依赖：Python 3.x、Pillow、openssl、curl；qrcode 仅登录渲染用。
