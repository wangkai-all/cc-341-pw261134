# 多租户压缩包容错队列系统

接收队列里有多个租户的 gzip(JSONL) 压缩包。单个坏包反复失败只会**隔离该租户的队头**，
不会堵住整队，也不会被悄悄跳过并把租户宣称为完整。入口、解包处理器、消费端是
**三个独立进程**，外加一个只读为主的 Web 管控台。全部基于 Python 3.11 标准库，无第三方依赖。

## 1. 架构与职责边界

```
spool/  ──►  ingress(进程)  ──► SQLite(WAL)  ──►  processor(进程)  ──►  messages 表  ──►  consumer(进程)  ──►  consumer_results + jsonl 文件
 ▲            登记/序号/回放                     解压/额度/隔离/租约                     顺序消费/恰好一次
 │
 └── web(进程)：只读视图 + 上传(spool) + 处置请求(control_requests)，不直接改业务状态
```

| 组件 | 进程 | 职责 | 不做什么 |
|---|---|---|---|
| ingress | `bin/run.py ingress` | 扫描 spool、分配租户内递增序号、去重、回放原位复位、跳号处置 | 不解压 |
| processor | `bin/run.py processor` | 公平轮转调度、流式解压、内存/临时盘/解压额度控制、隔离、租约恢复 | 不消费消息 |
| consumer | `bin/run.py consumer` | 租户内按序消费、恰好一次、外部结果文件与库对账 | 不改包状态机（除完成推进） |
| web | `bin/run.py web` | 额度/缺口/隔离原因/审计展示、上传、回放与跳号下单、消费者核对 | 只通过 spool 与 control_requests 间接驱动 |

Web 的任何处置都不直接写业务表：上传走 spool 由入口登记，重试/跳号写 `control_requests`
由入口事务化执行，组件边界在人工操作下依然成立。

## 2. 关键机制如何对应需求

- **坏包不堵整队**：处理器按租户 round-robin，隔离/处理中的租户本轮跳过，其他租户继续。
- **不能假装完整**：租户只有在不存在任何 `queued/processing/quarantined/ready` 包时才算
  `is_complete`；隔离与跳号都作为 `gaps` 永久展示，跳号包状态为 `dead` 而非成功。
- **租户内顺序**：入口为每个租户分配严格递增的包序号；处理器只取租户队头；
  消费者只取租户内最小未消费消息序号。
- **回放原位接回**：修正包文件名 `replay__<packet_id>__<tenant>__<uid>.gz`，
  入口把隔离包在**原 tenant、原 seq** 复位为 `queued`，不分配新序号；
  新到达的包只能排在它后面。复位时清除可能残留的 staged 消息，配合
  `messages UNIQUE(tenant_id, seq)` 与 `consumer_results UNIQUE(message_id)`，
  重放不会重复影响已处理数据。
- **额度限制**（`limits.json`，可按需覆盖）：
  - `max_uncompressed_bytes`：流式解压边读边计数，超限立即中止并隔离 `UNCOMPRESSED_LIMIT`；
  - `temp_disk_bytes`：`tmp_extract/` 总量超限全队退避，单包写入触顶隔离 `TEMP_DISK_QUOTA`；
  - `memory_bytes`：处理器 RSS 超软限不接新包，等回落；
  - 采样线程每秒记录 RSS / 临时盘 / spool 占用，页面展示百分比。
- **崩溃安全**：
  - 处理器包级租约 `lease_seconds`，启动时与循环中都会回收过期 `processing` 租约，
    清理临时目录残留后幂等重解（消息先删后插 + 唯一约束）；
  - 消费端"结果落库 + 状态推进"同一事务，提交前崩溃则重启重放同一条、不会产生第二条结果；
  - 外部结果文件 `consumer_results.jsonl` 启动时按数据库对账重建，去掉崩溃残留脏行。
- **核对消费者结果**（`GET /api/verify`）：每租户消息序号连续无重复、消费为消息序号前缀、
  全局无重复结果、外部 jsonl 与库内（租户, 序号, 输出哈希）多重集完全一致。

## 3. 快速开始

```bash
# 1. 初始化一个全新空数据目录（额度可在 home/limits.json 覆盖）
python3 -I bin/run.py init ./data

# 2. 分别启动三个处理组件（建议三个终端）与 Web
python3 -I bin/run.py ingress   ./data
python3 -I bin/run.py processor ./data
python3 -I bin/run.py consumer  ./data
python3 -I bin/run.py web       ./data 8080
# 打开 http://127.0.0.1:8080/
```

包格式：gzip 压缩的 UTF-8 JSONL，每行 `{"id": <str|int>, "payload": <任意 JSON>}`。
投递方式二选一：

```bash
# A. 直接放入 spool（文件名约定 <tenant>__<client_uid>.gz）
cp some.gz ./data/spool/tenant-a__uid-001.gz

# B. 通过网页 / HTTP 上传
curl -X POST http://127.0.0.1:8080/api/upload \
  -H "Content-Type: application/gzip" \
  -H "X-Tenant-Id: tenant-a" -H "X-Client-Uid: uid-001" \
  --data-binary @some.gz
```

隔离后在网页包明细表操作，或调用 API：

```bash
# 上传修正包并在原 seq 回放（不传 body 则用原 blob 重试）
curl -X POST http://127.0.0.1:8080/api/packets/<packet_id>/retry \
  -H "Content-Type: application/gzip" --data-binary @fixed.gz
# 人工确认跳号：该 seq 成为永久缺口，后续包继续流动
curl -X POST http://127.0.0.1:8080/api/packets/<packet_id>/discard
# 核对消费者结果
curl http://127.0.0.1:8080/api/verify
```

## 4. 四个故障样例（一键端到端演示）

所有演示都在**全新空目录**上真实拉起 4 个进程，通过 HTTP / 文件系统 / SIGKILL 驱动，
不存在任何预设成功状态：

```bash
python3 -I demo/run_all.py ./run_data/demo_all
# 也可单独运行：
python3 -I demo/scenario1_overlimit.py     # 解压超限：炸弹隔离，其他租户照常，修正回放原位接回
python3 -I demo/scenario2_corrupt.py       # 格式损坏：截断 gzip + 坏 JSON 行；回放与确认跳号
python3 -I demo/scenario3_replay_arrival.py# 回放进行中并发新包到达，租户内顺序不乱
python3 -I demo/scenario4_crash.py         # SIGKILL 处理器/消费端：租约接管与恰好一次
```

夹具生成器（只造数据，不碰运行库）：`python3 -I demo/make_fixtures.py <dir>`。

## 5. 测试

```bash
python3 -I tests/test_packetq.py     # 11 个单元测试：状态机/公平调度/隔离/回放/跳号/恰好一次/核对
```

## 6. 数据目录布局

```
data/
├── packetq.db            # WAL 模式 SQLite，全部状态与审计
├── limits.json           # 额度覆盖（可选，缺省用 config.DEFAULT_LIMITS）
├── spool/                # 入口投递区（.tmp 写入 + 原子改名，只扫描稳定文件）
├── blobs/                # 已登记压缩包（回放保留历史版本 <id>.v<n>.gz）
├── rejected/             # 入口拒收文件（带原因前缀）+ 审计留痕
├── tmp_extract/<packet_id>/  # 处理器临时解包目录，成功/失败/崩溃都会清理
└── consumer_results.jsonl    # 消费端外部审计产物，启动按库对账重建
```

## 7. 包状态机

```
queued ──租约──► processing ──解压成功──► ready ──消息全部消费──► done
                    │                       
                    ├──解压/格式错误──► quarantined ──修正回放──► queued（原 seq）
                    │                   └──确认跳号──► dead（永久缺口，后续包可越过）
                    └──进程崩溃(租约超时)──► queued（幂等重解）
```

隔离原因码：`UNCOMPRESSED_LIMIT` / `TEMP_DISK_QUOTA` / `FORMAT_CORRUPT` /
`BAD_MESSAGE` / `TOO_MANY_MESSAGES` / `BLOB_MISSING`。
