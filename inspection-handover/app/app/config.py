"""应用配置：全部来自环境变量，便于容器与本地两种方式运行。"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    old_dsn: str
    new_dsn: str
    state_db_path: str
    cutover_timeout_seconds: float
    subscription_name: str
    poll_interval_seconds: float = 0.5


def get_settings() -> Settings:
    return Settings(
        old_dsn=os.environ["OLD_DSN"],
        new_dsn=os.environ["NEW_DSN"],
        state_db_path=os.environ.get("STATE_DB_PATH", "/data/handover_state.db"),
        cutover_timeout_seconds=float(os.environ.get("CUTOVER_TIMEOUT_SECONDS", "30")),
        subscription_name=os.environ.get("SUBSCRIPTION_NAME", "inspection_sub"),
    )
