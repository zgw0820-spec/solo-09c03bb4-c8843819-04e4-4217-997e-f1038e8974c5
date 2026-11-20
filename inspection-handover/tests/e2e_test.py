#!/usr/bin/env python3
"""端到端试跑：交接成功路径 + 暂停订阅追平超时回滚 + 在途写入 +
工单遇上交接（503/在途放行）+ 两次交接重叠 + 并发压测 + 重启保持 + 重复点击。

前置：scripts/local_pg.sh start 已启动两库，应用通过 scripts/local_app.sh 启动。
用 -I 运行（隔离模式），仅使用标准库 + 虚拟环境内的 psycopg2。
"""
import json
import os
import signal
import subprocess
import sys
import threading
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


def reset_demo_to_old() -> None:
    """测试辅助：把演示拨回 OLD 以便重复验证交接（生产路径不会回切）。

    交接生效后新库会受理旧库没有的工单（source_db='new'），直接拨回 OLD 会让
    下一次交接的“两库行数一致”校验失败，旧库序列也可能落到新库已发号段内。
    因此拨回时一并：删掉新库独有的工单、把旧库序列前推到两库序列已预留的
    最高号，模拟“从未交接”的干净起点。已复制到两库的工单数据本身不回滚。
    """
    import sqlite3
    conn = sqlite3.connect(STATE_DB)
    conn.execute("UPDATE kv SET value='OLD' WHERE key='state'")
    conn.commit()
    conn.close()
    sql(NEW_DSN, "DELETE FROM work_orders WHERE source_db='new'")
    old_last = seq_info(OLD_DSN)[2] or 0
    new_last = seq_info(NEW_DSN)[2] or 0
    sql(OLD_DSN, "SELECT setval('work_orders_id_seq', %s, true)",
        (max(old_last, new_last),))


def spawn_slow_transaction(seconds: int = 3) -> subprocess.Popen:
    """旧库上的慢写事务：INSERT 后 sleep 再提交，用于制造“在途写入”。"""
    return subprocess.Popen(
        [sys.executable, "-c",
         "import psycopg2,time;"
         "c=psycopg2.connect('" + OLD_DSN + "');cur=c.cursor();"
         "cur.execute(\"INSERT INTO work_orders(title,detail,source_db) "
         "VALUES ('慢事务工单','在途','old')\");"
         f"time.sleep({seconds});c.commit();c.close()"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def cutover_worker(results: dict, tag: str) -> None:
    """线程内发起一次交接，结果写入 results[tag] = (状态码, 响应体, 耗时)。"""
    t0 = time.time()
    code, body = api("POST", "/api/cutover", {"confirm": True}, timeout=60)
    results[tag] = (code, body, time.time() - t0)


def events_since(ts: float, needle: str) -> int:
    """统计 ts 之后消息中包含 needle 的事件条数。"""
    _, events = api("GET", "/api/events")
    return sum(1 for e in events if e["ts"] >= ts and needle in e["message"])


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

    print("== 3b. 两次交接重叠（前者超时回滚）：迟到请求只回报当前接单库 ==")
    code, body = api("POST", "/api/subscription/pause")
    check("3b 订阅暂停成功", code == 200 and body["subscriber"]["enabled"] is False)
    code, body = api("POST", "/api/work-orders", {"title": "3b 积压工单"})
    check("3b 暂停期间旧库仍可接单", code == 200 and body["accepted_by"] == "old")
    res3b: dict = {}
    mark = time.time()
    ta = threading.Thread(target=cutover_worker, args=(res3b, "A"))
    ta.start()
    time.sleep(0.5)  # 让 A 先进入交接（将因订阅暂停而追平超时）
    tb = threading.Thread(target=cutover_worker, args=(res3b, "B"))
    tb.start()
    ta.join(timeout=30)
    tb.join(timeout=30)
    code_a, body_a, el_a = res3b["A"]
    code_b, body_b, el_b = res3b["B"]
    check("3b 先到的交接超时返回 504 并回滚 OLD",
          code_a == 504 and body_a["state"] == "OLD", f"{code_a} {body_a}")
    check("3b 迟到的交接不报错、不重复切换，只回报当前接单库（旧库）",
          code_b == 200 and body_b.get("duplicate") is True
          and body_b["accepting_db"] == "old" and body_b["state"] == "OLD",
          f"{code_b} {body_b}")
    check("3b 迟到请求只等前者落定，没有自己再跑一次交接（耗时非两倍）",
          el_b < el_a + 3, f"A={el_a:.1f}s B={el_b:.1f}s")
    check("3b 回滚只发生一次（迟到请求未再次回滚）",
          events_since(mark, "交接回滚") == 1, f"{events_since(mark, '交接回滚')} 次")
    code, body = api("POST", "/api/work-orders", {"title": "3b 回滚后工单"})
    check("3b 回滚后旧库恢复接单", code == 200 and body["accepted_by"] == "old")
    code, body = api("POST", "/api/subscription/resume")
    check("3b 订阅恢复成功", code == 200 and body["subscriber"]["enabled"] is True)
    deadline = time.time() + 20
    caught = False
    while time.time() < deadline:
        if (sql(OLD_DSN, "SELECT count(*), max(id) FROM work_orders")[0]
                == sql(NEW_DSN, "SELECT count(*), max(id) FROM work_orders")[0]):
            caught = True
            break
        time.sleep(0.5)
    check("3b 恢复订阅后新库重新追平", caught)

    print("== 5. 在途写入未结束时发起交接：先等写入结束再切换 ==")
    # 5a. 慢事务期间发起交接：状态先进入 SWITCHING 并等待，事务提交后自动完成
    slow = spawn_slow_transaction(3)
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
    reset_demo_to_old()

    print("== 5c. 两次交接重叠（前者成功）：迟到请求只回报当前接单库，序列不重复对齐 ==")
    slow = spawn_slow_transaction(3)
    time.sleep(1.0)  # 慢事务在途，交接 A 将在 SWITCHING 停留约 2s
    res5c: dict = {}
    mark = time.time()
    ta = threading.Thread(target=cutover_worker, args=(res5c, "A"))
    ta.start()
    time.sleep(0.7)  # B 在 A 交接进行中到达（生效前到达、仍待处理）
    tb = threading.Thread(target=cutover_worker, args=(res5c, "B"))
    tb.start()
    ta.join(timeout=30)
    tb.join(timeout=30)
    slow.wait(timeout=10)
    code_a, body_a, el_a = res5c["A"]
    code_b, body_b, el_b = res5c["B"]
    check("5c 先到的交接成功", code_a == 200 and body_a.get("switched") is True,
          f"{code_a} {body_a}")
    first_5c = body_a["first_new_id"]
    check("5c 迟到的交接只回报当前接单库（新库），未重复切换",
          code_b == 200 and body_b.get("duplicate") is True
          and body_b["accepting_db"] == "new" and "first_new_id" not in body_b,
          f"{code_b} {body_b}")
    new_seq_5c = seq_info(NEW_DSN)[2]
    check("5c 新库序列只对齐一次（等于首个新编号-1，未被迟到请求拨动）",
          new_seq_5c == first_5c - 1, f"last_value={new_seq_5c} first_new={first_5c}")
    check("5c 交接生效事件只有一条", events_since(mark, "交接生效") == 1,
          f"{events_since(mark, '交接生效')} 条")
    check("5c 序列对齐事件只有一条", events_since(mark, "新库序列已对齐") == 1,
          f"{events_since(mark, '新库序列已对齐')} 条")
    old_max_5c = sql(OLD_DSN, "SELECT max(id) FROM work_orders")[0][0]
    check("5c 旧库没有交接后的迟到工单（最大号 < 首个新编号）",
          old_max_5c < first_5c, f"old_max={old_max_5c} first_new={first_5c}")
    code, body = api("POST", "/api/work-orders", {"title": "5c 生效后工单"})
    check("5c 生效后工单由新库接单且编号接续",
          code == 200 and body["accepted_by"] == "new"
          and body["work_order"]["id"] == first_5c, f"{code} {body}")
    reset_demo_to_old()

    print("== 5d. 工单遇上交接：在途写入放行完毕才切换，暂停期 503，生效后只写新库 ==")
    # 用 EXCLUSIVE 表锁挡住旧库 INSERT（SELECT 不受影响），让一个在途写请求
    # 确定性地停在“已获准、未提交”状态
    lock_conn = psycopg2.connect(OLD_DSN)
    lock_cur = lock_conn.cursor()
    lock_cur.execute("LOCK TABLE work_orders IN EXCLUSIVE MODE")
    res_submit: dict = {}

    def submit_worker(tag: str, title: str) -> None:
        c, b = api("POST", "/api/work-orders", {"title": title}, timeout=30)
        res_submit[tag] = (c, b)

    t_slow = threading.Thread(target=submit_worker, args=("slow", "5d 在途工单"))
    t_slow.start()
    deadline = time.time() + 10
    st = {}
    while time.time() < deadline:
        _, st = api("GET", "/api/status")
        if st.get("inflight_writes", 0) >= 1:
            break
        time.sleep(0.1)
    check("5d 暂停前获准的写请求已进入在途计数", st.get("inflight_writes", 0) >= 1, str(st))
    res5d: dict = {}
    ta = threading.Thread(target=cutover_worker, args=(res5d, "A"))
    ta.start()
    wait_status("SWITCHING", timeout=10)  # 交接已暂停接单，且在等在途写入
    code, body = api("POST", "/api/work-orders", {"title": "5d 暂停期探针"})
    check("5d 暂停期间提交工单直接 503", code == 503, f"{code} {body}")
    time.sleep(0.5)  # 让交接确实等过一段时间
    lock_conn.rollback()  # 释放在途写入（其事务随即提交）
    t_slow.join(timeout=15)
    ta.join(timeout=30)
    lock_cur.close()
    lock_conn.close()
    code_slow, body_slow = res_submit["slow"]
    code_a, body_a, _ = res5d["A"]
    first_5d = body_a["first_new_id"]
    check("5d 暂停前获准的写入完成提交并落入旧库",
          code_slow == 200 and body_slow["accepted_by"] == "old",
          f"{code_slow} {body_slow}")
    check("5d 在途工单编号小于首个新编号（交接等它提交后才对齐序列）",
          body_slow["work_order"]["id"] < first_5d,
          f"inflight_id={body_slow['work_order']['id']} first_new={first_5d}")
    check("5d 交接在在途写入结束后才成功",
          code_a == 200 and body_a.get("switched") is True, f"{code_a} {body_a}")
    _, w = api("GET", "/api/work-orders")
    new_ids = [r["id"] for r in w["new"]]
    check("5d 在途工单已随复制到达新库", body_slow["work_order"]["id"] in new_ids)
    code, body = api("POST", "/api/work-orders", {"title": "5d 生效后工单"})
    check("5d 生效后工单只写新库",
          code == 200 and body["accepted_by"] == "new"
          and body["work_order"]["id"] == first_5d, f"{code} {body}")
    old_max_5d = sql(OLD_DSN, "SELECT max(id) FROM work_orders")[0][0]
    check("5d 交接生效后旧库没有新工单", old_max_5d < first_5d,
          f"old_max={old_max_5d} first_new={first_5d}")
    reset_demo_to_old()

    print("== 5e. 并发提交压测遇上交接：无 500、暂停期 503、无迟到旧库写入 ==")
    slow = spawn_slow_transaction(3)
    time.sleep(1.0)
    stop_hammer = threading.Event()
    hammer_results: list = []

    def hammer() -> None:
        i = 0
        while not stop_hammer.is_set():
            i += 1
            try:
                c, b = api("POST", "/api/work-orders",
                           {"title": f"5e 压测工单{threading.get_ident()}-{i}"}, timeout=15)
                hammer_results.append((c, b))
            except Exception as exc:  # 连接异常也算失败样本
                hammer_results.append((0, {"error": str(exc)}))
            time.sleep(0.05)

    hammers = [threading.Thread(target=hammer) for _ in range(4)]
    for t in hammers:
        t.start()
    time.sleep(0.4)  # 先打出一批旧库受理的 200
    res5e: dict = {}
    ta = threading.Thread(target=cutover_worker, args=(res5e, "A"))
    ta.start()
    ta.join(timeout=30)
    slow.wait(timeout=10)
    time.sleep(0.5)  # 让压测线程继续打到 NEW 状态
    stop_hammer.set()
    for t in hammers:
        t.join(timeout=10)
    code_a, body_a, _ = res5e["A"]
    first_5e = body_a["first_new_id"]
    check("5e 压测期间交接成功", code_a == 200 and body_a.get("switched") is True,
          f"{code_a} {body_a}")
    codes = {c for c, _ in hammer_results}
    check("5e 压测响应只有 200/503，没有 500 或异常", codes <= {200, 503}, str(codes))
    n503 = sum(1 for c, _ in hammer_results if c == 503)
    check("5e 暂停期间确有提交被 503", n503 >= 1, f"503 数={n503}")
    old_ids = [b["work_order"]["id"] for c, b in hammer_results
               if c == 200 and b.get("accepted_by") == "old"]
    new_ids = [b["work_order"]["id"] for c, b in hammer_results
               if c == 200 and b.get("accepted_by") == "new"]
    check("5e 覆盖交接前旧库受理与生效后新库受理两类样本",
          len(old_ids) >= 1 and len(new_ids) >= 1, f"old={len(old_ids)} new={len(new_ids)}")
    check("5e 旧库受理编号都小于首个新编号（暂停前获准、交接前已提交）",
          all(i < first_5e for i in old_ids), f"max(old)={max(old_ids)} first_new={first_5e}")
    check("5e 新库受理编号都不小于首个新编号",
          all(i >= first_5e for i in new_ids), f"min(new)={min(new_ids)} first_new={first_5e}")
    old_max_5e = sql(OLD_DSN, "SELECT max(id) FROM work_orders")[0][0]
    check("5e 交接生效后旧库没有迟到工单", old_max_5e < first_5e,
          f"old_max={old_max_5e} first_new={first_5e}")
    reset_demo_to_old()


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
