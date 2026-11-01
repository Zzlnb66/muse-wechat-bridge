# systemd 可选托管说明（v1.5.0 item 3，仅交付模板，不启用）

- 交付物：`watchdog/wechat-bridge.service` 模板 + 本说明。**本项只交模板，不做生产 serve 接管实测**（会惊动现行服务）；模板语法核对 + 文档核对即为本项完成。发布说明必须保留“运行接管未验证”。
- 本模板未启用：复制到 /etc/systemd/system/、填好全部占位符、执行 `systemctl daemon-reload` 后才生效；启用本身是另一次需要单独确认的操作。

## 启用前盘点（必须先做）

1. 盘点旧 cron、后台会话（tmux/screen/nohup）、手工拉起命令，逐一停用或记录，避免与 systemd 抢 serve 启动权。
2. 加维护/停用状态：人工 `systemctl stop` 后，必须确认看门狗 Hook 不会又把服务拉起来；否则停用无意义。
3. 确认运行用户能读取 session、能读写 state/inbox/done/outbox/media/failures 等目录、解释器路径与模型路径可用。

## 二选一的准确含义

- 二选一的是 **serve 启动权**，不是整个 Hook。Hook 的收件检测、Worker 唤醒、心跳与告警职责在 systemd 模式下**保留**。
- systemd 模式下，删除 Worker 提示词和脚本内直接 nohup/setsid 拉 serve 的路径，使用本 unit 作为唯一固定的管理入口。
- 绝不同时启用两个 serve 启动器：flock 能挡住第二个 serve 的轮询，但 systemd 那边会因抢不到锁失败、然后反复重启刷日志。

## flock 边界

- flock 只约束同一锁文件；另一部署根、锁路径不一致、或活进程持锁时替换锁文件，都可绕开。
- **绝不删除活实例仍持有的 lock 文件。**

## 退出码与重启限制

- 锁冲突返回 2；预启动 NEEDS_RELOGIN 存在、缺会话或会话缺关键字段返回 3；运行期 TokenExpired 返回 14（代码注释支持，**unit 定稿前以 serve 入口实际返回值为准再核一次**）。
- unit 配 RestartPreventExitStatus=2 3 14，避免 on-failure 反复拉起已知不可恢复状态；配 StartLimitIntervalSec/StartLimitBurst 限制真正异常的重启。
- 注意：on-failure 在正常退出或常规 SIGTERM 下不一定重启；RestartPreventExitStatus 只针对 ExecStart 主进程，不能兜住 ExecStartPre 的所有失败；协议 ret=-14 不是进程退出码，不得混用。

## 启用、停用、日志

```
# 启用（占位符填好、daemon-reload 之后）
sudo systemctl enable --now wechat-bridge.service
# 停用
sudo systemctl disable --now wechat-bridge.service
# 状态与日志
systemctl status wechat-bridge.service
journalctl -u wechat-bridge.service -f
# 语法核对（仅证明 unit 能解析，不证明托管可用）
systemd-analyze verify /etc/systemd/system/wechat-bridge.service
```

## Type=exec 与通道恢复

- Type=exec 成功只说明程序执行阶段成立，**不代表微信通道已恢复**；每次启动后仍须查 `verify-channel`，以 recovered=true 为准。
- 现场 systemd 版本不支持 Type=exec 时改用 Type=simple。

## 故意不加的字段

- 不盲加 ProtectHome=yes：现有根目录、解释器、模型可能在 /home；按实际访问清单测试后再定。
- ProtectSystem/ReadWritePaths/PrivateTmp 等按实际访问清单测试后再定。
- 未实现通知协议时不加 WatchdogSec。

## systemd-analyze verify 的限度（必须写进验收）

它无法证明：实际用户能读取 session、Worker 仍能唤醒、代理环境正确、不会重启循环。这些只能由启用后的实际运行验证，而本项不做启用验证。
