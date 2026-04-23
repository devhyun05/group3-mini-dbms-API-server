#!/bin/sh

pick_port() {
    python3 - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

start_server() {
    PORT="$1"
    DATA_DIR="$2"
    LOG_FILE="$3"

    ./mini_dbms_api_server "$PORT" "$DATA_DIR" >"$LOG_FILE" 2>&1 &
    SERVER_PID=$!
}

wait_for_server() {
    PORT="$1"
    i=0

    while [ "$i" -lt 50 ]; do
        if curl -s -o /dev/null "http://127.0.0.1:$PORT/" 2>/dev/null; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    return 1
}

post_query() {
    PORT="$1"
    SQL="$2"

    curl -s -X POST "http://127.0.0.1:$PORT/query" \
        -H "Content-Type: application/json" \
        -d "{\"sql\":\"$SQL\"}"
}

stop_server() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
        SERVER_PID=""
    fi
}
