# Minimal C API Server

단일 프로세스 안에 `SQL-B-Tree` 엔진을 내장한 최소 스펙 API 서버입니다.

구조는 아래처럼 단순하게 고정했습니다.

```text
Client -> HTTP API Server -> SQL-B-Tree DB module
```

핵심 의도:

- 외부 인터페이스는 `HTTP + JSON`
- SQL 범위는 `SELECT`, `INSERT`
- API 서버 내부에 thread pool 구성
- 동시성은 `SELECT 병렬`, `INSERT 직렬`

## Build

```bash
make
```

## Run

기본 데이터 디렉터리는 `./data` 입니다.

```bash
./api_server --port 8080 --workers 4 --data-dir ./data
```

## Endpoints

### Health check

```bash
curl http://127.0.0.1:8080/health
```

### Query

```bash
curl -X POST http://127.0.0.1:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM users WHERE id = 1;"}'
```

```bash
curl -X POST http://127.0.0.1:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"INSERT INTO users VALUES (3, '\''Charlie'\'', '\''charlie@example.com'\'');"}'
```

## Test

```bash
make test
```

테스트는 임시 데이터 디렉터리를 만들고 서버를 띄운 뒤 아래를 확인합니다.

- `/health`
- `INSERT -> SELECT`
- duplicate PK 에러
- 잘못된 SQL 에러
- 동시 `SELECT`
- 동시 `INSERT`
- `SELECT` 와 `INSERT` 혼합 요청
