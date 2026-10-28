# 排障

## 收图：地址、密钥、加密、校验（`process_inbound_image()`）

- **下载地址优先级**：`media.full_url` 优先；没有时用 `media.encrypt_query_param` 或 `image_item.encrypt_query_param` 构造
  `https://novac2c.cdn.weixin.qq.com/c2c/download?encrypted_query_param=<quote(enc)>`；两者都没有 → `NO_DOWNLOAD_PARAM`。
- **密钥字段优先级**：`image_item.aeskey` 优先；其次 `image_item.media.aes_key`。
  - `aeskey`：32 字符 hex → 16 字节。
  - `media.aes_key`：Base64 解出 16 字节，或 Base64 解出 32 字节 ASCII hex 再 `fromhex` 得 16 字节。
  - 字段存在但解析不出 16 字节 → `KEY_FORMAT_ERROR`，不再悄悄走明文。
  - 完全没有密钥字段 → 明文路径，但必须魔数确认 PNG/JPEG/GIF/WEBP，否则失败。
- **加密**：AES-128-ECB，PKCS#7 填充，`openssl enc -aes-128-ecb -K <hex> -nosalt`。
  - `encrypt_type` 在腾讯定义里描述的是文件 ID/缩略图/中尺寸封装，不是 AES 模式，也不是“明文”标志；本项目收图路径不依赖它，只记诊断字段；发图时 `send_image()` 用 `encrypt_type:1`。
- **解密后校验**：魔数（PNG/JPEG/GIF/WEBP）→ 像素上限 50,000,000、单边 ≤20000 → `Pillow` 完整解码 `im.load()`（不只读头）→ 原子发布 `.tmp.<pid>` + `os.replace` 到 `media/inbound/`。
- **多图顺序**：`extract_images()` 按 `item_list` 原顺序取 `type=2` 的 `image_item`；`inbox` 记录的 `images` 数组保持该顺序。
- **追问关联**：见架构文档图片事件；失败事件不被普通文字清除。

## 长度字段：各自统计什么

| 字段 | 来源 | 统计对象 |
|---|---|---|
| `declared_meta_size` / `meta_sizes.hd_size|mid_size|thumb_size` | 微信消息 JSON 里的尺寸字段 | 消息体声明的规格尺寸（不一定等于 HTTP 头，也不一定等于密文） |
| `http_content_length` / `content_length_header` | HTTP 响应头 `Content-Length` | 服务器声明的响应体字节数 |
| `actual_read_bytes` / `actual_bytes` | `download_bytes_diag()` 实际 `read(65536)` 累计 | socket 实际读到的字节数 |
| 落盘字节数 | `failures/*.attempt*.download.bin` 文件大小；成功时 `media/inbound/*.jpg` 大小 | 前者是密文下载体，后者是解密后明文图片；**两者不能直接比较** |
| 解密后长度 | 解密输出长度 | 明文图片字节数；密文长度应为明文长度向上取 16 字节倍数 + 填充（1-16 字节），不是相等 |

**哪些应该相等**：同一下载体上，`http_content_length`（有声明、非分块、无编码时）应等于 `actual_read_bytes`；三次计数点（HTTP 读完、解密前、失败文件落盘后）应相同。
**哪些不能直接比较**：`declared_meta_size` 与 `actual_read_bytes`（规格可能不同）；密文落盘大小与解密后图片大小（填充 + 加密）。

## 为什么下载不完整也可能表现为 bad decrypt

- 密文是 16 字节一块的 ECB；真正的 PKCS#7 填充在**最后一块**。
- 如果传输在约 73KB 处提前断流，拿到的是中间块，最后一块缺失；`openssl` 对最后一块做填充校验时必然报 `bad decrypt`，即使前面 99% 的块都正确。
- 所以“解密失败”不等于“密钥错”；先查长度，再查密钥。
- 反向也成立：一段随机截断在 16 字节边界的密文，最后一字节解密后恰好为 `0x01` 的概率约 1/256，单靠 PKCS#7 通过不能证明完整；还需要稳定短体 + 严格图片完整解码（含 JPEG `FF D9`、PNG `IEND` 尾标）。

## 前缀关系说明了什么

- **同一 URL 两次下载，长度不同、短的是长的前缀**：更支持“同一对象、传输在不同位置结束”。
  - 例：04:35 同一消息 56976 → 65506，共同前缀 56976；04:53 问题图 67664 → 52880，共同前缀 52880。
- **两次长度和哈希完全相同**：可能是固定坏对象、固定截断或错误长度头，**不能直接判为头写错**。
- **同一 URL 两次内容从开头就不同**：暂停拼接，查对象是否变化、对象标识、本地字节处理。
- **跨微信重发的两条消息，零公共块**：**不能**做前缀比较；重加密、重编码、取到不同对象、本地偏移都可能造成。失败 3 与失败 4 的零公共块只能说明“不是同一字节流的前缀”，不能证明“每次重发一定换密钥”。

## curl 成功证实了什么，又没证实什么

- **证实了**：在 04:53 的两张图上，`urllib` 两次都短、最终图完整，而代码里唯一新增的下载路径是 `curl`；因此 `curl` 能拿到完整体、`urllib` 在当时拿不到，是**有证据的对比**。
- **没有证实**：底层是哪一环导致 `urllib` 截断。候选包括 Python `http.client` 按块读到 EOF 不抛异常的行为、CDN 边缘断流、云主机出口/中间盒、连接复用等；仅凭客户端记录**无法排序**，需要服务端/CDN 日志才能定位。不要在公开材料里写成“CDN 故障”或“urllib bug 定性”。
- **证据缺口**：成功路径不写失败记录，04:53 的 `curl_diag` 没有落盘；后续优化已加 `serve.log` 的 `curl fallback recovered` 日志，但修改后的第一次微信实测尚未完成。

## 当前完整收图流程（`process_inbound_image()`）

1. 取地址（`full_url` 优先，否则构造 CDN 下载 URL）；无地址 → `NO_DOWNLOAD_PARAM`。
2. `urllib` 下载一次，`download_bytes_diag()`：`Accept-Encoding: identity`，超时 15 秒，按 65536 字节读，>25MB 直接拒绝，读中异常不返回残体。
3. 长度检查：声明存在、非分块、无编码时，实际 ≠ 声明 → 不送解密，进入 `curl` 回退。
4. `curl` 回退：`curl -sS --http1.1 --max-time 30 --raw -H "Accept-Encoding: identity" -H "User-Agent: Mozilla/5.0" -H "iLink-App-Id: bot" -D 头文件 -o 临时文件 <同一 full_url>`；退出码 0 且字节数 = 声明才使用，否则记 `curl_diag`（退出码、字节数、SHA-256，不记 URL）。
5. 仍短 → 写失败记录 + 安全密文诊断（见下），发“图片接收失败，请重发”一次。
6. 完整 → 解析密钥（见上），解密前检查非空、长度为 16 倍数（有密钥时）；`openssl` 解密；魔数、像素、完整解码；原子发布。
7. 失败分类：`download`（`DOWNLOAD_LENGTH_MISMATCH` / `DOWNLOAD_INCOMPLETE` / `NO_DOWNLOAD_PARAM` / `OVER_25MB`）、`key_parse`（`KEY_FORMAT_ERROR`）、`decrypt`（`DECRYPT_FAILED`）、`validate`、`publish`。
8. 失败留证：`failures/<id>_<index>.json`（0600）+ `.attempt*.download.bin`（0600，隔离，不进 `media/inbound/`）；只记字段名、长度、格式、下载状态、SHA、失败阶段、ETag/Accept-Ranges（当时多为 null）、安全诊断；**不记密钥值、完整 URL、正文**。
9. 失败后防误用：写 `state/last_image_event.json` 为 `failed`；追问时 `image_reference` 为 `unavailable`，Worker 只回“图片接收失败，请重发”，不描述旧图、不附旧图。

## 安全密文诊断（Gemini 建议，`build_safe_cipher_diagnostics()`）

- 只输出分类：`attempt1_len`、`attempt1_mod16`、`attempt2_len`、`shared_prefix_identical`、`cipher_head_magic`、`ecb_first_block_magic`、`ecb_last_block_pad_type`（`valid_pkcs7` / `trailing_zeros` / `invalid_pad_or_truncated`）。
- 首块用 `-nopad` 单块 ECB 解密，只判魔数，不输出明文；本地模拟通过：完整密文首块能解出 PNG 魔数。
- 注意：截断体的尾块分类几乎总是 `invalid_pad_or_truncated`，这表示“尾块不是合法填充”，不能直接读成“填充格式非标”。

## 发图：脱敏请求结构

```json
{
  "to_user_id": "EXAMPLE_USER_ID",
  "message_type": 2,
  "message_state": 2,
  "from_user_id": "",
  "client_id": "EXAMPLE_UUID_HEX",
  "context_token": "EXAMPLE_CONTEXT_TOKEN",
  "item_list": [
    {
      "type": 2,
      "image_item": {
        "mid_size": 123456,
        "media": {
          "encrypt_query_param": "EXAMPLE_ENCRYPT_QUERY_PARAM",
          "aes_key": "BASE64_OF_32_CHAR_HEX_UTF8",
          "encrypt_type": 1
        }
      }
    }
  ]
}
```

- 上传：`getuploadurl`（`media_type=1`，`rawsize`、`rawfilemd5`、`filesize`=密文大小、`filekey`、随机 16 字节密钥的 32 字符 hex、`no_need_thumb=true`）→ 用返回的 `upload_full_url` 或 `upload_param` 构造地址，`POST application/octet-stream` 上传密文；成功严格要求 HTTP 200 且 `x-encrypted-param` 非空（去前后空白后用）。
- 加密：随机 16 字节密钥，AES-128-ECB + PKCS#7 加密明文图片，得到密文；`aes_key` 是该密钥 32 字符 hex 的 UTF-8 再 Base64。
- **为什么以前服务端回消息 ID、微信却收不到**：`encrypt_query_param`、`encrypt_type` 放在 `image_item` 顶层时，网关只校验外层信封、映射到 Protobuf 时丢弃顶层字段，`media` 里只剩 `aes_key`、没有下载参数；服务端仍返回消息 ID，但客户端无法下载图片，于是表现为“没收到”。D4 更正：内部丢弃机制是推测，没有服务端证据；能确认的是字段移入 `media` 后 03:29 实测恢复。
- 文本说明与图片是两次独立 `sendmessage`，各有独立 `client_id`。D4 更正：0.2 秒间隔是某次手工流程的记录，当前 `drain_outbox()` 代码里没有该等待，不要把它写成当前实现。

## 常见故障速查

- `getuploadurl ret=-2`：先查 `ilink_user_id` 是否显式带上；再查是否有入站 `context_token`。
- `sendtyping ret=-2 ilink_user_id required`：同上，补 `ilink_user_id` + `to_user_id`。
- 图片一直“请重发”且失败记录是 `DOWNLOAD_LENGTH_MISMATCH`：先看两次尝试长度和前缀关系，再看 `curl_diag`；不要直接换密钥或重发新图。
- `NEEDS_RELOGIN` 存在：不要重启循环，必须重新登录。
- `serve.pid` 存在但 `/proc` 无该进程：按架构文档的存活规则判断为死亡，不要用 `pgrep` 误判。

## needs_check / 终态任务人工处置（Gemini 补点，v1.0.3 修正）

> v1.0.3 之前这里的第 3 步是错的：只把状态改回 `pending` 并移回
> `outbox/pending/`，**下轮仍会被拦住**，因为文件里还留着
> `"terminal": true`，且当前 `serve` 进程内存里还有
> `_TERMINAL_TASK_IDS` 集合。必须按下面 5 步走。

当 `outbox/failed/` 下出现 `needs_check` / `terminal` 任务时，管理员按以下步骤处理：

1. **先在微信确认对方是否已收到**：按子项逐个确认（文本、附件各算一项），不要整单笼统确认。
2. **已收到的子项不动**：保持 `sent`，不要改回 `pending`；整单都已收到则删除该文件，或移入 `outbox/sent/`。
3. **仅把确认未送达的子项改回 `pending`，字段名文本和附件不一样，别改错**：文本子项把顶层 `"text_status"` 改为 `"pending"`，且顶层 `"text_attempts"` 重置为 `0`；附件子项把 `attachments[i]` 里的 `"status"` 改为 `"pending"`，且该附件的 `"attempts"` 重置为 `0`；旧格式单图（只有 `image_path` / `image_status`，还没被归一化）把顶层 `"image_status"` 改为 `"pending"` 即可，归一化时重试计数从 0 起算。其他子项（`sent` / `failed` / `needs_check`）一律不动——`failed` / `needs_check` 子项下轮会被跳过，只重试 `pending` 子项。漏改 `text_attempts` 的后果：文本项原来已是 3 次，再失败一次就直接变 `failed`，等于只剩 1 次机会。
4. **删掉整单的终态标记**：把文件顶层的 `"terminal": true` 删掉（或改为 `false`），再把文件从 `outbox/failed/` 移回 `outbox/pending/`。
5. **重启 `serve`，或把重投文件改名**：当前进程内存中的 `_TERMINAL_TASK_IDS` 只认旧文件名，所以要么重启 `serve`（内存集合清空），要么把文件改名为新的 `<id>_retry.json` 再放回 `pending/`。改名的一个小副作用：`_sender_loop` 取消 typing 时用文件名去查 `_TYPING_MAP`，改名后查不到原票据；但走到人工重投时早过了几分钟，微信 typing 状态早已自然超时，无实际危害。**不要改 `_TERMINAL_TASK_IDS` 的拦截优先级**：它存在的唯一作用，是防止“留在 `pending/` 且移入 `failed/` 失败的终态任务”被重复发；磁盘上的任务一旦被人工修好（`_is_terminal_job(job)` 已为 `False`），重启 / 改名后就不会再被拦，逻辑最简单、最不容易引入新洞。

**绝对不要做的事**：不要在未确认的情况下反复重发；不要把状态未知的子项（`unknown_restart`、或送达结果不明的 `needs_check`）直接改回 `pending` 重投——它可能已经送达，重投会造成重复消息。不要直接删除未确认的文件。
