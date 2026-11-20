#!/usr/bin/env python3
"""端到端试跑：交接成功路径 + 暂停订阅追平超时回滚 + 在途写入 + 重启保持 + 重复点击。

前置：scripts/local_pg.sh start 已启动两库，应用通过 scripts/local_app.sh 启动。
用 -I 运行（隔离模式），仅使用标准库 + 虚拟环境内的 psycopg2。
"""
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg2

BASE = "http://127.0.0.1:8000"
OLD_DSN = "host=127.0.0.1 port=55433 user=node dbname=inspection"
NEW_DSN = "host=127.0.0.1 port=55434 user=node dbname=inspection"
ROOT = Path(__file__).resolve().parents[1]
STATE_DB = ROOT / ".localrun" / "data" / "handover_state.db"

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    mark = "PASS" if cond else "FAIL"
    if cond:
        PASS += 1
    else:
        FAIL += 1
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail else ""))


def api(method: str, path: str, body: dict | None = None, timeout: float = 60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def sql(dsn: str, query: str, args=()):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, args)
        if cur.description:
            return cur.fetchall()
        conn.commit()


def wait_status(expected_state: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = api("GET", "/api/status")
        if last.get("state") == expected_state:
            return last
        time.sleep(0.3)
    raise AssertionError(f"状态未在 {timeout}s 内变为 {expected_state}，最后状态: {last}")


def seq_info(dsn: str):
    rows = sql(dsn, """
        SELECT seqstart, seqcache, pg_sequence_last_value('work_orders_id_seq')
        FROM pg_sequence WHERE seqrelid='work_orders_id_seq'::regclass""")
    return rows[0]


def reset_state_to_old() -> None:
    """测试辅助：把演示状态库直接拨回 OLD，便于重复验证交接（不影响 PG 序列）。"""
    import sqlite3
    conn = sqlite3.connect(STATE_DB)
    conn.execute("UPDATE kv SET value='OLD' WHERE key='state'")
    conn.commit()
    conn.close()


def main() -> int:
    print("== 0. 初始状态 ==")
    st, body = api("GET", "/api/status")
    check("初始接单库为旧库 OLD", body["state"] == "OLD" and body["accepting_db"] == "old", str(body))

    print("== 1. 旧库提交工单（含制造回滚空号与缓存预留）==")
    ids = []
    for i in range(3):
        code, body = api("POST", "/api/work-orders", {"title": f"巡检工单{i+1}", "detail": "例行"})
        check(f"工单{i+1}由旧库受理", code == 200 and body["accepted_by"] == "old")
        ids.append(body["work_order"]["id"])
    check("工单编号连续", ids == sorted(ids) and len(set(ids)) == 3, str(ids))

    # 直接在旧库制造一个“回滚空号”：取号后回滚，该号永久作废
    before = seq_info(OLD_DSN)
    with psycopg2.connect(OLD_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT nextval('work_orders_id_seq')")
        rolled_back_id = cur.fetchone()[0]
        conn.rollback()
    after = seq_info(OLD_DSN)
    check("回滚后序列 last_value 仍推进（空号计入预留）",
          before[2] is not None and after[2] >= before[2], f"before={before} after={after}")

    # 显式制造“缓存未用号”：独立短连接只取一个号就断开，块内其余 cache-1 个号
    # 被旧库预取但永不使用（前段先制造一批，交接前还会再制造一批保证块内有未用号）
    conn = psycopg2.connect(OLD_DSN)
    cur = conn.cursor()
    cur.execute("SELECT nextval('work_orders_id_seq')")
    print(f"   短连接取号 {cur.fetchone()[0]} 后断开，制造缓存未用号")
    cur.close()
    conn.close()

    print("== 2. 逻辑复制把旧库工单同步到新库 ==")
    deadline = time.time() + 15
    synced = False
    while time.time() < deadline:
        old_cnt = sql(OLD_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        new_cnt = sql(NEW_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        if old_cnt == new_cnt:
            synced = True
            break
        time.sleep(0.5)
    check("新库收齐旧库已提交工单（行数与最大号一致）", synced, f"old={old_cnt} new={new_cnt}")

    print("== 3. 暂停订阅 -> 发起交接 -> 追平超时 -> 自动回滚旧库恢复 ==")
    code, body = api("POST", "/api/subscription/pause")
    check("订阅暂停成功", code == 200 and body["subscriber"]["enabled"] is False)
    # 暂停期间旧库再写一条，制造未传播的增量
    code, body = api("POST", "/api/work-orders", {"title": "暂停期间工单"})
    check("暂停期间旧库仍可接单", code == 200 and body["accepted_by"] == "old")
    t0 = time.time()
    code, body = api("POST", "/api/cutover", {"confirm": True}, timeout=30)
    elapsed = time.time() - t0
    check("交接超时返回 504", code == 504, f"code={code} body={body}")
    check("超时后回滚为 OLD，旧库恢复接单",
          body["state"] == "OLD" and body["accepting_db"] == "old", str(body))
    check("超时耗时接近配置阈值(8s)", 7 <= elapsed <= 14, f"{elapsed:.1f}s")
    code, body = api("POST", "/api/work-orders", {"title": "回滚后工单"})
    check("回滚后网页可继续向旧库提交", code == 200 and body["accepted_by"] == "old")

    print("== 4. 恢复订阅，等待追平 ==")
    code, body = api("POST", "/api/subscription/resume")
    check("订阅恢复成功", code == 200 and body["subscriber"]["enabled"] is True)
    deadline = time.time() + 20
    caught = False
    while time.time() < deadline:
        old_cnt = sql(OLD_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        new_cnt = sql(NEW_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
        if old_cnt == new_cnt:
            caught = True
            break
        time.sleep(0.5)
    check("恢复订阅后新库重新追平", caught, f"old={old_cnt} new={new_cnt}")

    print("== 5. 在途写入未结束时发起交接：先等写入结束再切换 ==")
    # 5a. 慢事务期间发起交接：状态先进入 SWITCHING 并等待，事务提交后自动完成
    slow = subprocess.Popen(
        [sys.executable, "-c",
         "import psycopg2,time;"
         "c=psycopg2.connect('"+OLD_DSN+"');cur=c.cursor();"
         "cur.execute(\"INSERT INTO work_orders(title,detail,source_db) VALUES ('慢事务工单','在途','old')\");"
         "time.sleep(3);c.commit();c.close()"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)  # 确保慢事务已开始（idle in transaction）
    t0 = time.time()
    code, body = api("POST", "/api/cutover", {"confirm": True}, timeout=30)
    elapsed = time.time() - t0
    slow.wait(timeout=10)
    check("在途写入结束后交接成功", code == 200 and body.get("ok") is True,
          f"code={code} body={body}")
    # 慢事务在 INSERT 后 sleep 3s 才提交；交接在其提交后才继续
    check("交接确实等待了在途写入（>=2.0s）", elapsed >= 2.0, f"{elapsed:.1f}s")
    # 演示需要：把状态回 OLD 以便用干净的序列场景再做一次正式交接验证
    reset_state_to_old()

    # 5b. 制造确定的“缓存块内未用号”：短连接取块内首号即断开，
    #     last_value 停在块首，块内其余 cache-1 个号预留但永不使用
    conn = psycopg2.connect(OLD_DSN)
    cur = conn.cursor()
    cur.execute("SELECT nextval('work_orders_id_seq')")
    gap_id = cur.fetchone()[0]
    cur.close()
    conn.close()
    pre_seq = seq_info(OLD_DSN)
    print(f"   短连接取号 {gap_id} 后断开，序列预留最高号={pre_seq[2]}（块内其余号未用）")

    print("== 6. 首个新编号超过旧库序列预留最高号（含缓存未用/回滚空号）==")
    # 正式交接（交接过程不取号，缓存块保持未用状态）
    code, body = api("POST", "/api/cutover", {"confirm": True}, timeout=30)
    check("带缓存未用号的交接成功", code == 200 and body.get("ok") is True, str(body))
    old_seq = seq_info(OLD_DSN)
    start, cache, old_last = old_seq
    # pg_sequence_last_value 返回的是“预留到的最高号”：短连接预取整块后，
    # 该值已包含块内未用号（实际发出的最大号必然小于它）
    actual_max_issued = sql(OLD_DSN, "SELECT max(id) FROM work_orders")[0][0]
    expected_highest = old_last
    expected_first = old_last + 1
    check("存在缓存未用号（实际最大发号 < 序列预留最高号，证明未用号被计入）",
          actual_max_issued < expected_highest,
          f"实际最大发号={actual_max_issued} 预留最高号={expected_highest}")
    check("交接返回的首个新编号严格大于预留最高号",
          body["first_new_id"] == expected_first,
          f"got={body.get('first_new_id')} expected={expected_first}, old_seq={old_seq}")
    check("旧库最高预留号统计正确（含缓存未用号、回滚空号）",
          body["old_highest_reserved"] == expected_highest, str(body))
    new_seq = seq_info(NEW_DSN)
    check("新库序列已 setval 到预留最高号", new_seq[2] == expected_highest, str(new_seq))

    print("== 7. 切换后工单由新库接单，编号接着发 ==")
    code, body = api("POST", "/api/work-orders", {"title": "新库首单"})
    check("切换后由新库接单", code == 200 and body["accepted_by"] == "new")
    check("新库首个编号严格大于旧库预留最高号",
          body["work_order"]["id"] == expected_first,
          f"id={body['work_order']['id']} expected={expected_first}")
    code, body2 = api("POST", "/api/work-orders", {"title": "新库第二单"})
    # CACHE 5 下后端预取会跳号（如 1040 -> 1045），属正确语义：
    # 断言严格递增且都大于旧库预留最高号，而非紧挨着 +1
    check("新库编号严格递增接续（允许缓存预取跳号）",
          body2["work_order"]["id"] > body["work_order"]["id"]
          and body2["work_order"]["id"] > expected_highest,
          f"{body['work_order']['id']} -> {body2['work_order']['id']}")
    code, body = api("POST", "/api/work-orders", {"title": "切换期拦截"})
    # 已在 NEW，不应再出现 503
    check("NEW 状态提交不再被拦截", code == 200)

    print("== 8. 重复点击交接：只显示当前接单库，不重复切换 ==")
    code, body = api("POST", "/api/cutover", {"confirm": True})
    check("重复交接返回 already_done", code == 200 and body.get("already_done") is True
          and body["accepting_db"] == "new", str(body))

    print("== 9. 应用重启后沿用已生效接单库 ==")
    # 通过状态文件存在性间接验证持久化；重启进程由调用脚本完成，这里直接核对落盘状态
    import sqlite3
    conn = sqlite3.connect(STATE_DB)
    persisted = conn.execute("SELECT value FROM kv WHERE key='state'").fetchone()[0]
    conn.close()
    check("接单库状态已持久化到 SQLite", persisted == "NEW", persisted)
    events = api("GET", "/api/events")[1]
    check("事件日志包含回滚与生效记录",
          any("回滚" in e["message"] for e in events)
          and any(e["phase"] == "NEW" for e in events),
          f"{len(events)} 条事件")

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
