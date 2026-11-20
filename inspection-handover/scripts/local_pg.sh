#!/bin/bash
# 无 Docker 环境下的本地两库编排：用解包的 PostgreSQL 15 启动两个实例，
# 并配置与 docker-compose 完全相同的逻辑复制拓扑。
# 用法: ./local_pg.sh init|start|stop|clean|status
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGHOME=/workspace/.local/pg/usr/lib/postgresql/15
PGBIN="$PGHOME/bin"
RUN="$ROOT/.localrun"
OLD_DATA="$RUN/olddata"
NEW_DATA="$RUN/newdata"
OLD_PORT=55433
NEW_PORT=55434
DBUSER=node
DBNAME=inspection

export LD_LIBRARY_PATH="$PGHOME/lib:${LD_LIBRARY_PATH:-}"
export PGSYSCONFDIR="$RUN/conf"

old_dsn="host=127.0.0.1 port=$OLD_PORT user=$DBUSER dbname=$DBNAME"
new_dsn="host=127.0.0.1 port=$NEW_PORT user=$DBUSER dbname=$DBNAME"

init_cluster() {
    mkdir -p "$RUN/conf" "$RUN/logs"
    for name in old new; do
        port="$OLD_PORT"; data="$OLD_DATA"
        [ "$name" = "new" ] && { port="$NEW_PORT"; data="$NEW_DATA"; }
        if [ ! -d "$data" ]; then
            "$PGBIN/initdb" -D "$data" -U "$DBUSER" --auth=trust --no-locale --encoding=UTF8 >/dev/null
            cat >> "$data/postgresql.conf" <<EOF
port = $port
unix_socket_directories = '$RUN'
listen_addresses = '127.0.0.1'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
fsync = off
synchronous_commit = off
full_page_writes = off
EOF
        fi
    done
}

start_cluster() {
    init_cluster
    "$PGBIN/pg_ctl" -D "$OLD_DATA" status >/dev/null 2>&1 \
        || "$PGBIN/pg_ctl" -D "$OLD_DATA" -l "$RUN/logs/old.log" -w start
    "$PGBIN/pg_ctl" -D "$NEW_DATA" status >/dev/null 2>&1 \
        || "$PGBIN/pg_ctl" -D "$NEW_DATA" -l "$RUN/logs/new.log" -w start
    # 建库（已存在则忽略）
    "$PGBIN/psql" -h 127.0.0.1 -p "$OLD_PORT" -U "$DBUSER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='$DBNAME'" | grep -q 1 \
        || "$PGBIN/createdb" -h 127.0.0.1 -p "$OLD_PORT" -U "$DBUSER" "$DBNAME"
    "$PGBIN/psql" -h 127.0.0.1 -p "$NEW_PORT" -U "$DBUSER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='$DBNAME'" | grep -q 1 \
        || "$PGBIN/createdb" -h 127.0.0.1 -p "$NEW_PORT" -U "$DBUSER" "$DBNAME"

    # 发布端 schema + publication（幂等）
    "$PGBIN/psql" "$old_dsn" -v ON_ERROR_STOP=1 -f "$ROOT/db/old/init/01_schema.sql"
    # 订阅端 schema
    "$PGBIN/psql" "$new_dsn" -v ON_ERROR_STOP=1 -f "$ROOT/db/new/init/01_schema.sql"
    # 订阅（幂等，带重试）。CREATE SUBSCRIPTION 会内部提交，不能放在 DO 块里。
    for i in $(seq 1 30); do
        exists=$("$PGBIN/psql" "$new_dsn" -tAc "SELECT count(*) FROM pg_subscription WHERE subname='inspection_sub'")
        if [ "$exists" = "1" ]; then echo "subscription already exists"; break; fi
        if "$PGBIN/psql" "$new_dsn" -v ON_ERROR_STOP=1 -c \
            "CREATE SUBSCRIPTION inspection_sub \
             CONNECTION 'host=127.0.0.1 port=$OLD_PORT user=$DBUSER dbname=$DBNAME' \
             PUBLICATION inspection_pub \
             WITH (create_slot = true, enabled = true, copy_data = true)"; then
            echo "subscription ready"; break
        fi
        sleep 1
    done
    echo "old: $old_dsn"
    echo "new: $new_dsn"
}

stop_cluster() {
    "$PGBIN/pg_ctl" -D "$OLD_DATA" stop -m fast 2>/dev/null || true
    "$PGBIN/pg_ctl" -D "$NEW_DATA" stop -m fast 2>/dev/null || true
}

case "${1:-}" in
    init)  init_cluster ;;
    start) start_cluster ;;
    stop)  stop_cluster ;;
    clean) stop_cluster; rm -rf "$RUN" ;;
    status)
        "$PGBIN/psql" "$old_dsn" -c "SELECT 'old' AS db, count(*) FROM work_orders"
        "$PGBIN/psql" "$new_dsn" -c "SELECT 'new' AS db, count(*) FROM work_orders"
        "$PGBIN/psql" "$new_dsn" -c "SELECT subname, subenabled FROM pg_subscription"
        ;;
    *) echo "usage: $0 init|start|stop|clean|status"; exit 1 ;;
esac
