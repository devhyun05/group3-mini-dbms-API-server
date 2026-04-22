import concurrent.futures
import http.client
import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "api_server"


def reserve_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request_json(port, method, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {}
    payload = None
    if body is not None:
        payload = json.dumps(body)
        headers["Content-Type"] = "application/json"
    conn.request(method, path, body=payload, headers=headers)
    response = conn.getresponse()
    raw = response.read().decode("utf-8")
    conn.close()
    return response.status, json.loads(raw)


def query(port, sql):
    return request_json(port, "POST", "/query", {"sql": sql})


def wait_for_health(port, timeout=5):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            status, body = request_json(port, "GET", "/health")
            if status == 200 and body == {"ok": True}:
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


def build_temp_data_dir():
    temp_dir = Path(tempfile.mkdtemp(prefix="api-server-test-"))
    (temp_dir / "users.csv").write_text(
        "id(PK),name,email(UK)\n"
        "1,Alice,alice@example.com\n"
        "2,Bob,bob@example.com\n",
        encoding="utf-8",
    )
    return temp_dir


def main():
    data_dir = build_temp_data_dir()
    port = reserve_port()
    env = os.environ.copy()
    process = subprocess.Popen(
        [str(SERVER), "--port", str(port), "--workers", "4", "--data-dir", str(data_dir)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        wait_for_health(port)

        status, body = request_json(port, "GET", "/health")
        assert status == 200
        assert body == {"ok": True}

        status, body = query(port, "SELECT * FROM users WHERE id = 1;")
        assert status == 200
        assert body["ok"] is True
        assert body["type"] == "select"
        assert body["row_count"] == 1
        assert body["rows"] == ["1,Alice,alice@example.com"]

        status, body = query(port, "INSERT INTO users VALUES ('Charlie','charlie@example.com');")
        assert status == 200
        assert body == {"ok": True, "type": "insert", "affected_rows": 1}

        status, body = query(port, "SELECT * FROM users WHERE id = 3;")
        assert status == 200
        assert body["rows"] == ["3,Charlie,charlie@example.com"]

        status, body = query(port, "INSERT INTO users VALUES (1,'Dup','dup@example.com');")
        assert status == 422
        assert body["ok"] is False
        assert "duplicate PK value" in body["error"]

        status, body = query(port, "SELEC * FROM users;")
        assert status == 422
        assert body["ok"] is False
        assert "invalid SQL statement" in body["error"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(query, port, "SELECT * FROM users WHERE id = 1;") for _ in range(8)]
            results = [future.result() for future in futures]
        for status, body in results:
            assert status == 200
            assert body["rows"] == ["1,Alice,alice@example.com"]

        insert_sqls = [
            f"INSERT INTO users VALUES ({100 + i},'Parallel{i}','parallel{i}@example.com');"
            for i in range(5)
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(query, port, sql) for sql in insert_sqls]
            results = [future.result() for future in futures]
        for status, body in results:
            assert status == 200
            assert body["affected_rows"] == 1

        for i in range(5):
            status, body = query(port, f"SELECT * FROM users WHERE id = {100 + i};")
            assert status == 200
            assert body["row_count"] == 1

        mixed_sqls = [
            "SELECT * FROM users WHERE id = 1;",
            "SELECT * FROM users WHERE id = 2;",
            "INSERT INTO users VALUES (200,'Mixer','mixer@example.com');",
            "SELECT * FROM users WHERE id = 3;",
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(query, port, sql) for sql in mixed_sqls]
            results = [future.result() for future in futures]
        assert any(body.get("type") == "insert" for _, body in results)
        for status, body in results:
            assert status == 200
            assert body["ok"] is True

        status, body = query(port, "SELECT * FROM users WHERE id = 200;")
        assert status == 200
        assert body["rows"] == ["200,Mixer,mixer@example.com"]

        print("all tests passed")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    main()
