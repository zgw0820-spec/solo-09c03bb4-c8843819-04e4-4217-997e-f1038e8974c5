-- 新库（订阅端）初始化：同名表/序列 + 逻辑复制订阅
CREATE TABLE IF NOT EXISTS work_orders (
    id          bigint PRIMARY KEY,
    title       text        NOT NULL,
    detail      text        NOT NULL DEFAULT '',
    source_db   text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS work_orders_id_seq
    AS bigint
    START WITH 1000
    CACHE 5
    OWNED BY work_orders.id;

ALTER TABLE work_orders ALTER COLUMN id
    SET DEFAULT nextval('work_orders_id_seq');

-- 订阅在 shell 脚本中创建（需要重试，等待发布端完全就绪），见 02_create_subscription.sh
