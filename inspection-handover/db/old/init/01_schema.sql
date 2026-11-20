-- 旧库（发布端）初始化：工单表 + 序列 + 逻辑复制发布
CREATE TABLE IF NOT EXISTS work_orders (
    id          bigint PRIMARY KEY,
    title       text        NOT NULL,
    detail      text        NOT NULL DEFAULT '',
    source_db   text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- 工单编号序列：cache 5。序列预留出的号即使未使用（缓存未用号、事务回滚空号）
-- 也不会再发给别人，交接时必须把这些预留号一并计入新库起点。
CREATE SEQUENCE IF NOT EXISTS work_orders_id_seq
    AS bigint
    START WITH 1000
    CACHE 5
    OWNED BY work_orders.id;

ALTER TABLE work_orders ALTER COLUMN id
    SET DEFAULT nextval('work_orders_id_seq');

-- 只发布表数据；序列状态不随逻辑复制传播，由应用在交接时显式对齐
-- PG15 的 CREATE PUBLICATION 不支持 IF NOT EXISTS，用 DO 块保证幂等
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'inspection_pub') THEN
        CREATE PUBLICATION inspection_pub FOR TABLE work_orders;
    END IF;
END
$$;
