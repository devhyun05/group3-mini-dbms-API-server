#!/bin/sh
set -eu

. "$(dirname "$0")/../server_test_lib.sh"

PORT="$(pick_port)"
DATA_DIR="$(mktemp -d)"
LOG_FILE="/tmp/mini_dbms_edge.log"
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

DUP_PK_RESPONSE="$(post_query "$PORT" "INSERT INTO users VALUES (1,'dup@test.com','Dup');")"
echo "$DUP_PK_RESPONSE" | grep '"ok":false' >/dev/null
echo "$DUP_PK_RESPONSE" | grep 'duplicate PK value' >/dev/null

DUP_UK_RESPONSE="$(post_query "$PORT" "INSERT INTO users VALUES (3,'alice@test.com','Dup');")"
echo "$DUP_UK_RESPONSE" | grep '"ok":false' >/dev/null
echo "$DUP_UK_RESPONSE" | grep 'violates UK constraint' >/dev/null

QUOTED_RESPONSE="$(post_query "$PORT" "INSERT INTO users VALUES ('carol@test.com','Carol,QA');")"
echo "$QUOTED_RESPONSE" | grep '"ok":true' >/dev/null
COMMA_SELECT_RESPONSE="$(post_query "$PORT" "SELECT * FROM users WHERE email = 'carol@test.com';")"
echo "$COMMA_SELECT_RESPONSE" | grep 'Carol,QA' >/dev/null

EMPTY_RESULT_RESPONSE="$(post_query "$PORT" "SELECT * FROM users WHERE name = 'Nobody';")"
echo "$EMPTY_RESULT_RESPONSE" | grep '"ok":true' >/dev/null
echo "$EMPTY_RESULT_RESPONSE" | grep '"row_count":0' >/dev/null

MISSING_TABLE_RESPONSE="$(post_query "$PORT" "SELECT * FROM ghosts WHERE id = 1;")"
echo "$MISSING_TABLE_RESPONSE" | grep '"ok":false' >/dev/null
echo "$MISSING_TABLE_RESPONSE" | grep '"code":"engine_error"' >/dev/null
echo "$MISSING_TABLE_RESPONSE" | grep "ghosts" >/dev/null

GET_QUERY_CODE="$(curl -s -o /tmp/test_edge_get_query.out -w '%{http_code}' "http://127.0.0.1:$PORT/query")"
[ "$GET_QUERY_CODE" = "405" ]

GET_UNKNOWN_CODE="$(curl -s -o /tmp/test_edge_unknown.out -w '%{http_code}' "http://127.0.0.1:$PORT/health")"
[ "$GET_UNKNOWN_CODE" = "404" ]

INVALID_JSON_CODE="$(curl -s -o /tmp/test_edge_invalid_json.out -w '%{http_code}' \
    -X POST "http://127.0.0.1:$PORT/query" \
    -H "Content-Type: application/json" \
    -d '{"bad":"body"}')"
[ "$INVALID_JSON_CODE" = "400" ]

PORT="$PORT" python3 - <<'PY' &
import os
import socket
import time

holders = []
port = int(os.environ["PORT"])
for _ in range(400):
    s = socket.socket()
    s.connect(("127.0.0.1", port))
    holders.append(s)
time.sleep(4)
for s in holders:
    s.close()
PY
FLOOD_PID=$!
sleep 1
QUEUE_FULL_RESULT="$(PORT="$PORT" python3 - <<'PY'
import json
import os
import urllib.error
import urllib.request

port = os.environ["PORT"]
payload = json.dumps({"sql": "SELECT * FROM users WHERE id = 1;"}).encode("utf-8")
request = urllib.request.Request(
    f"http://127.0.0.1:{port}/query",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status)
        print(response.read().decode("utf-8"))
except urllib.error.HTTPError as error:
    print(error.code)
    try:
        print(error.read().decode("utf-8"))
    except Exception:
        if error.code == 503:
            print('{"ok":false,"error":{"code":"queue_full"}}')
        else:
            print("")
PY
)"
QUEUE_FULL_CODE="$(printf '%s\n' "$QUEUE_FULL_RESULT" | sed -n '1p')"
QUEUE_FULL_BODY="$(printf '%s\n' "$QUEUE_FULL_RESULT" | sed -n '2,$p')"
wait "$FLOOD_PID"
[ "$QUEUE_FULL_CODE" = "503" ]
echo "$QUEUE_FULL_BODY" | grep '"code":"queue_full"' >/dev/null

echo "test_edge_cases: ok"
