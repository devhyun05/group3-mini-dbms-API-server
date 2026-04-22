#!/bin/sh
set -eu

PORT=18080
DATA_DIR="$(mktemp -d)"
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    rm -rf "$DATA_DIR"
}

trap cleanup EXIT INT TERM

cat >"$DATA_DIR/users.csv" <<'CSV'
id(PK),email(UK),name(NN)
1,alice@test.com,Alice
2,bob@test.com,Bob
CSV

./mini_dbms_api_server "$PORT" "$DATA_DIR" >/tmp/mini_dbms_api_server.log 2>&1 &
SERVER_PID=$!

i=0
while [ "$i" -lt 50 ]; do
    if curl -s -o /dev/null "http://127.0.0.1:$PORT/query" 2>/dev/null; then
        break
    fi
    i=$((i + 1))
    sleep 0.1
done

post_query() {
    curl -s -X POST "http://127.0.0.1:$PORT/query" \
        -H "Content-Type: application/json" \
        -d "{\"sql\":\"$1\"}"
}

SELECT_RESPONSE="$(post_query "SELECT * FROM users WHERE id = 1;")"
echo "$SELECT_RESPONSE" | grep '"ok":true' >/dev/null
echo "$SELECT_RESPONSE" | grep '"access_path":"pk_index"' >/dev/null
echo "$SELECT_RESPONSE" | grep '"Alice"' >/dev/null

INSERT_RESPONSE="$(post_query "INSERT INTO users VALUES ('carol@test.com','Carol');")"
echo "$INSERT_RESPONSE" | grep '"affected_rows":1' >/dev/null

GET_QUERY_CODE="$(curl -s -o /tmp/test_api_get_query.out -w '%{http_code}' "http://127.0.0.1:$PORT/query")"
[ "$GET_QUERY_CODE" = "405" ]

GET_HEALTH_CODE="$(curl -s -o /tmp/test_api_get_health.out -w '%{http_code}' "http://127.0.0.1:$PORT/health")"
[ "$GET_HEALTH_CODE" = "404" ]

INVALID_JSON_CODE="$(curl -s -o /tmp/test_api_invalid_json.out -w '%{http_code}' \
    -X POST "http://127.0.0.1:$PORT/query" \
    -H "Content-Type: application/json" \
    -d '{"bad":"body"}')"
[ "$INVALID_JSON_CODE" = "400" ]

python3 - <<'PY' &
import socket
import time

holders = []
for _ in range(400):
    s = socket.socket()
    s.connect(("127.0.0.1", 18080))
    holders.append(s)
time.sleep(4)
for s in holders:
    s.close()
PY
FLOOD_PID=$!
sleep 1
QUEUE_FULL_CODE="$(curl -s -o /tmp/test_api_queue_full.out -w '%{http_code}' \
    -X POST "http://127.0.0.1:$PORT/query" \
    -H "Content-Type: application/json" \
    -d '{"sql":"SELECT * FROM users WHERE id = 1;"}')"
wait "$FLOOD_PID"
[ "$QUEUE_FULL_CODE" = "503" ]
