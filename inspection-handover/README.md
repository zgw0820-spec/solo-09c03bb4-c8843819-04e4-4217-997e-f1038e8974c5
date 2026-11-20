# 巡检工单库交接演示

FastAPI + 双 PostgreSQL 15（逻辑复制）实现的工单库单向交接：旧库（发布端）
暂停接单、等在途写入结束、新库（订阅端）收齐已提交工单并对齐序列后切换接单。

## 目录结构

```
.
├── docker-compose.yml            # old-db / new-db / app 三服务
├── app/                          # FastAPI 应用（含 Dockerfile、requirements）
│   └── app/
│       ├── main.py               # 路由与演示网页（工单、交接、订阅暂停/恢复、日志）
│       ├── cutover.py            # 交接状态机：OLD -> SWITCHING -> NEW，失败/超时回滚
│       ├── state.py              # 接单库状态持久化（SQLite，重启沿用）
│       ├── db.py                 # PG 访问：工单写入、序列快照、复制状态查询
│       └── config.py             # 环境变量配置
├── db/old/init/01_schema.sql     # 旧库：表 + CACHE 5 序列 + 发布
├── db/new/init/                  # 新库：同名表/序列 + 订阅创建脚本
├── scripts/
│   ├── local_pg.sh               # 无 Docker 时用本地 PG 二进制起两库并配置复制
│   └── local_app.sh              # 无 Docker 时本地启动应用
└── tests/e2e_test.py             # 端到端试跑（27 项断言）
```

## 方式一：Docker Compose（交付目标）

```bash
docker compose up --build
# 应用 http://localhost:8000 ；旧库 5433、新库 5434
```

- `old-db` 以 `wal_level=logical` 启动并创建发布 `inspection_pub`；
- `new-db` 启动后带重试创建订阅 `inspection_sub`（`copy_data=true` 初始同步）；
- 应用状态保存在 `app-data` 卷，容器重启后沿用已生效的接单库。

## 方式二：本机无 Docker 试跑（本仓库实际验证方式）

当前环境没有 Docker，用解包到用户目录的 PostgreSQL 15 二进制起两库：

```bash
./scripts/local_pg.sh start      # 初始化并启动两库 + 逻辑复制（55433 旧 / 55434 新）
./scripts/local_app.sh           # 启动 FastAPI（http://127.0.0.1:8000，超时阈值 8s）
python -I tests/e2e_test.py      # 端到端试跑
./scripts/local_pg.sh status     # 查看两库行数与订阅状态
./scripts/local_pg.sh stop       # 停止；clean 可清理全部本地数据
```

## 交接语义与关键设计

1. **统一入口**：网页 `POST /api/work-orders` 按当前接单库路由，响应返回工单
   编号与 `accepted_by`（old/new）。
2. **单向交接**：`POST /api/cutover`（需 `confirm=true`）。状态机
   `OLD -> SWITCHING -> NEW`，交接串行，进行中再次发起返回冲突提示。
3. **旧库暂停接单**：进入 SWITCHING 后网页提交返回 503。
4. **等在途写入结束**：进程内在途计数 + 旧库 `pg_stat_activity` 中
   `xact_start/backend_xid` 非空的客户端事务双重确认（覆盖 idle in transaction）。
5. **收齐才接单**：轮询发布端 `pg_current_wal_lsn()` 与
   `pg_stat_replication.replay_lsn` 一致且 `state=streaming`，再核对两库
   行数与最大编号一致。
6. **编号接续（含预留号）**：序列 `CACHE 5`。PostgreSQL 后端预取整块时会把
   块上界写入序列关系，`pg_sequence_last_value()` 返回的即“已预留到的最高号”，
   已包含缓存未用号（短连接预取后断连的块内剩余号）与事务回滚空号。新库
   `setval(seq, 预留最高号, true)`，首个新编号 = 预留最高号 + 1，严格大于旧库
   序列预留的全部号码。
7. **失败/超时回滚**：任一步异常或超过 `CUTOVER_TIMEOUT_SECONDS` 未追平，
   状态回到 OLD，旧库恢复接单，事件日志记录原因。
8. **重启保持与幂等**：状态落 SQLite；NEW 状态下重复点击交接只返回当前接单库，
   不重复切换。
9. **订阅暂停/恢复**：网页按钮执行 `ALTER SUBSCRIPTION DISABLE/ENABLE`。
   暂停期间旧库继续写入可制造复制积压，发起交接即可演示追平超时与自动回滚。

## 已验证结果（tests/e2e_test.py，29/29 通过）

- 旧库受理工单并自动编号；逻辑复制实时同步到新库；
- 制造回滚空号与缓存未用号后，实际最大发号 < 序列预留最高号，交接计算的
  首个新编号严格大于该预留最高号；
- 暂停订阅 → 交接在 8s 超时 → 504 并自动回滚 OLD，旧库恢复接单；
- 恢复订阅追平后，在途慢事务结束才完成交接；
- 新库序列对齐、新库接单编号接续；重复交接幂等；应用重启后仍为 NEW。

## 已知边界/剩余事项

- 状态机为单应用进程设计（在途计数在进程内）；多副本部署需把在途协调放到
  数据库（如 `pg_advisory_lock`/状态表），本演示未包含。
- 切换到 NEW 后未实现回切；重复初始化容器时订阅创建脚本已做幂等，但已存在的
  卷上旧数据不会自动清理，重置演示请 `docker compose down -v`。
- 生产化还需补充：认证授权、TLS、连接池、监控指标与告警、旧库最终只读/下线流程。
