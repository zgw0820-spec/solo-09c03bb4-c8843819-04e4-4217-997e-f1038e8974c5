#!/bin/bash
# 在新库上创建逻辑复制订阅。容器首次初始化时执行；订阅已存在则跳过。
# 发布端可能尚未完全就绪，因此带重试。
# 注意：CREATE SUBSCRIPTION 会内部提交，不能放在 PL/pgSQL DO 块中。
set -euo pipefail

: "${PUBLISHER_DSN:?PUBLISHER_DSN is required}"

for i in $(seq 1 60); do
    exists=$(psql -tAc "SELECT count(*) FROM pg_subscription WHERE subname='inspection_sub'" \
        --username "$POSTGRES_USER" --dbname "$POSTGRES_DB")
    if [ "$exists" = "1" ]; then
        echo "subscription inspection_sub already exists"
        exit 0
    fi
    if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "CREATE SUBSCRIPTION inspection_sub CONNECTION '${PUBLISHER_DSN}' \
            PUBLICATION inspection_pub \
            WITH (create_slot = true, enabled = true, copy_data = true)"; then
        echo "subscription inspection_sub is ready"
        exit 0
    fi
    echo "publisher not ready, retry ${i}/60..."
    sleep 2
done

echo "failed to create subscription after retries" >&2
exit 1
