"""单向交接状态机。

流程（OLD -> SWITCHING -> NEW）：
  1. 旧库暂停接单，等待应用在途写入结束（进程内计数器 + 旧库活动写事务双重确认）。
  2. 等待逻辑复制追平：发布端 current_lsn 与订阅端 replay_lsn 一致，
     且两库工单表行数、最大编号一致（确认旧库已提交的工单全部到达新库）。
  3. 读取旧库序列状态：缓存未用号、回滚空号都计入旧库序列预留的最高号，
     新库序列 setval 到该最高号，使首个新编号严格大于它。
  4. 提交切换到 NEW。任一步出错或超时则回滚，旧库恢复接单。

单进程并发约定：
  - 接单许可（begin_write）把“读状态 + 在途计数”放在 admit_lock 临界区内，
    交接把状态切到 SWITCHING 也走同一把锁。因此暂停前已获准写旧库的请求
    一定先被计数，交接会等它提交/回滚后才确认追平并对齐序列；
    暂停后到达的请求直接看到 SWITCHING，由路由返回 503。
  - 交接串行化：switch_lock 保证同一时刻只有一个交接在执行。拿锁后
    双重检查状态，防止等待期间另一交接已生效后又重复切换（重复对齐序列、
    超时回滚把旧库错误恢复接单）。重叠到达的请求等前者落定后只回报当前
    接单库，不再执行第二次切换。
"""
import threading
import time

from . import db
from .config import Settings
from .state import StateStore


class CutoverError(Exception):
    """交接失败（含超时）。触发后旧库恢复接单。"""


class CutoverManager:
    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store
        # 应用自身在途写事务计数；单进程 uvicorn 下即可反映网页提交的在途写入
        self.inflight = 0
        self.inflight_lock = threading.Lock()
        # 接单许可与状态切换互斥：获准写入一定先于暂停被计数
        self.admit_lock = threading.Lock()
        # 交接串行化：同一时刻只允许一个交接在执行
        self.switch_lock = threading.Lock()

    # ---- 在途写入计数 -------------------------------------------------
    def begin_write(self) -> str | None:
        """接单许可：返回受理库对应的状态（"OLD"/"NEW"）；SWITCHING 返回 None。

        状态判断与在途计数在同一个临界区完成：要么先于暂停被计数（交接会
        等它提交/回滚），要么直接看到 SWITCHING 被 503 拒绝。不会出现
        “读到 OLD 但还没计数，交接已切走，最后写进旧库”的窗口。
        """
        with self.admit_lock:
            state = self.store.get_state()
            if state == "SWITCHING":
                return None
            with self.inflight_lock:
                self.inflight += 1
            return state

    def exit_write(self) -> None:
        with self.inflight_lock:
            self.inflight = max(0, self.inflight - 1)

    def current_state(self) -> str:
        return self.store.get_state()

    def accepting_db(self) -> str:
        state = self.store.get_state()
        if state == "NEW":
            return "new"
        return "old"

    # ---- 交接 ---------------------------------------------------------
    def _already_done_result(self) -> dict:
        return {
            "already_done": True,
            "duplicate": False,
            "switched": False,
            "state": "NEW",
            "accepting_db": "new",
            "message": "交接已生效，当前由新库接单",
        }

    def start_cutover(self) -> dict:
        # 已生效：重复点击只回报当前接单库
        if self.store.get_state() == "NEW":
            return self._already_done_result()

        if self.switch_lock.acquire(blocking=False):
            try:
                # 双重检查：等锁/拿锁期间另一交接可能已生效，绝不能再次切换
                # （否则会重复对齐新库序列，超时回滚还会把旧库错误恢复接单）
                if self.store.get_state() == "NEW":
                    return self._already_done_result()
                return self._run_cutover()
            finally:
                self.switch_lock.release()

        # 另有交接正在进行（含生效前到达、仍待处理的请求）：
        # 等它落定，只回报当前接单库，不重复执行切换
        self._wait_ongoing_cutover()
        state = self.store.get_state()
        if state == "NEW":
            message = "交接请求重叠：另一交接已生效，当前由新库接单（本次未重复切换）"
        elif state == "SWITCHING":
            message = "交接请求重叠：另一交接仍在进行，当前接单库未变"
        else:
            message = "交接请求重叠：另一交接未生效（已回滚），当前仍由旧库接单"
        return {
            "already_done": state == "NEW",
            "duplicate": True,
            "switched": False,
            "state": state,
            "accepting_db": self.accepting_db(),
            "message": message,
        }

    def _wait_ongoing_cutover(self) -> None:
        """等进行中的交接落定；兜底超时（交接阈值 + 余量）后不再等，直接回报当前状态。"""
        timeout = self.settings.cutover_timeout_seconds + 10
        if self.switch_lock.acquire(timeout=timeout):
            self.switch_lock.release()

    def _run_cutover(self) -> dict:
        s = self.settings
        deadline = time.monotonic() + s.cutover_timeout_seconds
        summary: dict = {}

        def remaining() -> float:
            return deadline - time.monotonic()

        def sleep_with_timeout(seconds: float) -> None:
            if remaining() <= 0:
                raise CutoverError("交接超时")
            time.sleep(min(seconds, remaining()))

        try:
            # 1) 旧库暂停接单：与接单许可互斥，保证暂停前获准的写入都已被计数，
            #    暂停后到达的请求直接看到 SWITCHING（路由返回 503）
            with self.admit_lock:
                self.store.set_state("SWITCHING")
            self.store.log_event("SWITCHING", "旧库已暂停接单，开始单向交接")

            # 2) 等待在途写入结束（应用计数 + 数据库活动事务双重确认）
            self._wait_inflight(remaining, sleep_with_timeout)
            self.store.log_event("SWITCHING", "在途写入已全部结束")

            # 3) 等待逻辑复制追平
            self._wait_replication_caught_up(remaining, sleep_with_timeout)
            self.store.log_event("SWITCHING", "逻辑复制已追平，旧库已提交工单全部到达新库")

            # 4) 序列对齐：首个新编号必须超过旧库序列预留的最高号
            seq = db.sequence_snapshot(s.old_dsn)
            old_count, old_max = db.count_and_max(s.old_dsn)
            new_count, new_max = db.count_and_max(s.new_dsn)
            if old_count != new_count or old_max != new_max:
                raise CutoverError(
                    f"两库数据不一致: old(count={old_count},max={old_max}) "
                    f"new(count={new_count},max={new_max})"
                )

            # 旧库序列预留范围 = 已用号 + 缓存未用号 + 回滚空号；
            # pg_sequence_last_value 即预留最高号（CACHE 块上界会写入序列关系），
            # 首个新编号 = 预留最高号 + increment（见 db.sequence_snapshot）。
            seq = db.sequence_snapshot(s.old_dsn)
            first_new_id = seq["first_new_id"]
            db.align_sequence(s.new_dsn, first_new_id)
            summary = {
                "old_sequence": seq,
                "old_highest_reserved": seq["highest_reserved"],
                "first_new_id": first_new_id,
                "row_count": old_count,
            }
            self.store.log_event(
                "SWITCHING",
                f"新库序列已对齐，首个新编号 {first_new_id}（旧库预留最高号 "
                f"{seq['highest_reserved']}，含缓存未用号与回滚空号）",
                summary,
            )

            # 5) 提交切换
            self.store.set_state("NEW")
            self.store.log_event("NEW", "交接生效，新库开始接单", summary)
            return {
                "already_done": False,
                "duplicate": False,
                "switched": True,
                "state": "NEW",
                "accepting_db": "new",
                "message": "交接成功，新库开始接单",
                **summary,
            }
        except CutoverError as exc:
            self._rollback(str(exc))
            raise
        except Exception as exc:  # 任何意外错误同样回滚
            self._rollback(f"交接出错: {exc!r}")
            raise CutoverError(f"交接出错: {exc!r}") from exc

    def _wait_inflight(self, remaining, sleep) -> None:
        while True:
            app_inflight = self.inflight
            db_inflight = db.count_active_app_transactions(self.settings.old_dsn)
            if app_inflight == 0 and db_inflight == 0:
                return
            if remaining() <= 0:
                raise CutoverError(
                    f"等待在途写入结束超时（应用计数={app_inflight}，库内活动写事务={db_inflight}）"
                )
            self.store.log_event(
                "SWITCHING",
                f"等待在途写入结束（应用计数={app_inflight}，库内活动写事务={db_inflight}）",
            )
            sleep(self.settings.poll_interval_seconds)

    def _wait_replication_caught_up(self, remaining, sleep) -> None:
        s = self.settings
        while True:
            pub = db.publisher_wal_positions(s.old_dsn)
            sub = db.subscriber_status(s.new_dsn, s.subscription_name)
            replay = pub.get("replay_lsn")
            current = pub["current_lsn"]
            enabled = sub.get("enabled", False)
            # 订阅暂停时不满足追平条件，继续等待直到超时（用于演示追平超时）
            if (
                enabled
                and replay is not None
                and pub.get("sub_state") == "streaming"
                and str(replay) == str(current)
            ):
                # LSN 追平后再核对两库表数据，确保回放的数据可见
                old_count, old_max = db.count_and_max(s.old_dsn)
                new_count, new_max = db.count_and_max(s.new_dsn)
                if old_count == new_count and old_max == new_max:
                    return
            if remaining() <= 0:
                raise CutoverError(
                    f"等待复制追平超时（current={current}，replay={replay}，"
                    f"sub_state={pub.get('sub_state')}）"
                )
            self.store.log_event(
                "SWITCHING",
                f"等待复制追平（current={current}，replay={replay}，"
                f"sub_state={pub.get('sub_state')}）",
            )
            sleep(s.poll_interval_seconds)

    def _rollback(self, reason: str) -> None:
        # 切换生效前出错或超时：旧库恢复接单
        self.store.set_state("OLD")
        self.store.log_event("OLD", f"交接回滚，旧库恢复接单：{reason}")

    # ---- 订阅暂停/恢复 ------------------------------------------------
    def pause_subscription(self) -> dict:
        db.alter_subscription(
            self.settings.new_dsn, self.settings.subscription_name, enable=False
        )
        self.store.log_event("SUBSCRIPTION", "订阅已暂停（发布端 WAL 将保留，形成复制延迟）")
        return self.subscription_overview()

    def resume_subscription(self) -> dict:
        db.alter_subscription(
            self.settings.new_dsn, self.settings.subscription_name, enable=True
        )
        self.store.log_event("SUBSCRIPTION", "订阅已恢复")
        return self.subscription_overview()

    def subscription_overview(self) -> dict:
        sub = db.subscriber_status(self.settings.new_dsn, self.settings.subscription_name)
        pub = db.publisher_wal_positions(self.settings.old_dsn)
        return {"subscriber": sub, "publisher": pub}
