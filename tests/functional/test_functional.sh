#!/bin/sh
set -eu

. "$(dirname "$0")/../server_test_lib.sh"

PORT="$(pick_port)"
DATA_DIR="$(mktemp -d)"
LOG_FILE="/tmp/mini_dbms_functional.log"
SERVER_PID=""

cleanup() {
    stop_server
    rm -rf "$DATA_DIR"
}

trap cleanup EXIT INT TERM

cat >"$DATA_DIR/users.csv" <<'CSV'
id(PK),email(UK),name(NN)
1,alice@test.com,Alice
2,bob@test.com,Bob
CSV

start_server "$PORT" "$DATA_DIR" "$LOG_FILE"
wait_for_server "$PORT"

GET_ROOT_CODE="$(curl -s -o /tmp/test_functional_root.out -w '%{http_code}' "http://127.0.0.1:$PORT/")"
[ "$GET_ROOT_CODE" = "200" ]
grep 'SQL Console' /tmp/test_functional_root.out >/dev/null

SELECT_RESPONSE="$(post_query "$PORT" "SELECT * FROM users WHERE id = 1;")"
echo "$SELECT_RESPONSE" | grep '"ok":true' >/dev/null
echo "$SELECT_RESPONSE" | grep '"access_path":"pk_index"' >/dev/null
echo "$SELECT_RESPONSE" | grep '"Alice"' >/dev/null

INSERT_RESPONSE="$(post_query "$PORT" "INSERT INTO users VALUES ('carol@test.com','Carol');")"
echo "$INSERT_RESPONSE" | grep '"affected_rows":1' >/dev/null

SELECT_UK_RESPONSE="$(post_query "$PORT" "SELECT * FROM users WHERE email = 'carol@test.com';")"
echo "$SELECT_UK_RESPONSE" | grep '"ok":true' >/dev/null
echo "$SELECT_UK_RESPONSE" | grep '"access_path":"uk_index"' >/dev/null
echo "$SELECT_UK_RESPONSE" | grep '"Carol"' >/dev/null

UPDATE_RESPONSE="$(post_query "$PORT" "UPDATE users SET name = 'Carolyn' WHERE email = 'carol@test.com';")"
echo "$UPDATE_RESPONSE" | grep '"ok":true' >/dev/null
echo "$UPDATE_RESPONSE" | grep '"affected_rows":1' >/dev/null
echo "$UPDATE_RESPONSE" | grep '"access_path":"uk_index"' >/dev/null

DELETE_RESPONSE="$(post_query "$PORT" "DELETE FROM users WHERE id = 3;")"
echo "$DELETE_RESPONSE" | grep '"ok":true' >/dev/null
echo "$DELETE_RESPONSE" | grep '"affected_rows":1' >/dev/null
echo "$DELETE_RESPONSE" | grep '"access_path":"pk_index"' >/dev/null

i=0
PIDS=""
while [ "$i" -lt 8 ]; do
    OUT_FILE="$DATA_DIR/concurrent_$i.out"
    post_query "$PORT" "SELECT * FROM users WHERE id = 1;" >"$OUT_FILE" &
    PIDS="$PIDS $!"
    i=$((i + 1))
done

for PID in $PIDS; do
    wait "$PID"
done

for OUT_FILE in "$DATA_DIR"/concurrent_*.out; do
    grep '"ok":true' "$OUT_FILE" >/dev/null
    grep '"queue_wait_ms":' "$OUT_FILE" >/dev/null
done

echo "test_functional: ok"
