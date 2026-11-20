"""单向交接状态机。

流程（OLD -> SWITCHING -> NEW）：
  1. 旧库暂停接单，等待应用在途写入结束（进程内计数器 + 旧库活动写事务双重确认）。
  2. 等待逻辑复制追平：发布端 current_lsn 与订阅端 replay_lsn 一致，
     且两库工单表行数、最大编号一致（确认旧库已提交的工单全部到达新库）。
  3. 读取旧库序列状态：缓存未用号、回滚空号都计入旧库序列预留的最高号，
     新库序列 setval 到该最高号，使首个新编号严格大于它。
  4. 提交切换到 NEW。任一步出错或超时则回滚，旧库恢复接单。

重复触发：已在 NEW 时直接返回当前接单库，不再执行切换。
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
        # 交接串行化：交接进行中的再次触发返回 409
        self.switch_lock = threading.Lock()

    # ---- 在途写入计数 -------------------------------------------------
    def enter_write(self) -> None:
        with self.inflight_lock:
            self.inflight += 1

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
    def start_cutover(self) -> dict:
        if self.store.get_state() == "NEW":
            # 切换成功后重复点击：只显示当前接单库
            return {
                "already_done": True,
                "state": "NEW",
                "accepting_db": "new",
                "message": "交接已生效，当前由新库接单",
            }

        if not self.switch_lock.acquire(blocking=False):
            raise CutoverError("已有交接正在进行中")

        try:
            return self._run_cutover()
        finally:
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
            # 1) 旧库暂停接单
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
