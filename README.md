# Mini DBMS API Server

Tiny Web Server 스타일의 `listen -> accept -> task queue -> worker -> doit(fd)` 흐름 위에,
`SQL-B-Tree` 기반 SQL 엔진을 함수 호출형 모듈로 재구성한 멀티스레드 미니 DBMS API 서버입니다.

## 핵심 포인트

- 구현 언어: C
- 서버 구조: Tiny Web Server 스타일
- 동시성 모델: 고정 크기 스레드 풀 + bounded FIFO linked-list queue
- 스레드 수: 실행 머신의 `CPU online core` 수
- 작업 깨우기 규칙:
  - 요청 1개당 worker 1개만 `pthread_cond_signal()`
  - 종료 시에만 전체 `pthread_cond_broadcast()`
- DB 엔진: `SQL-B-Tree`의 `lexer/parser/B+Tree/executor`를 서버 호출형 API로 재구성
- 엔진 실행 전략: DB 엔진은 전역 `pthread_mutex_t`로 직렬화해 원래 단일 스레드 구조에 가깝게 유지
  - API 서버는 멀티스레드로 요청을 받음
  - DB 엔진 내부 실행은 한 번에 한 요청만 처리

## 디렉터리 구조

```text
src/
  engine/
    db_engine.[ch]         # 서버가 호출하는 엔진 API
    lexer.[ch]             # SQL 토크나이저
    parser.[ch]            # SQL 파서
    bptree.[ch]            # PK/UK 인덱스용 B+ Tree
    executor.[ch]          # CSV/인덱스 실행기
  server/
    csapp.[ch]             # Tiny 스타일 소켓/RIO 유틸
    task_queue.[ch]        # fd 기반 linked-list task queue
    http_server.[ch]       # 스레드 풀 + HTTP API 서버
  main.c                   # 서버 엔트리 포인트
tests/
  test_engine.c
  test_queue.c
  test_api.sh
data/
  *.csv                    # 테이블 파일
  *.delta / *.idx          # 엔진 보조 파일
```

## 지원 API

### `GET /`

브라우저용 SQL 콘솔 페이지를 반환합니다.

- 큰 입력창에 SQL을 적고 실행할 수 있습니다.
- `SELECT`는 결과를 HTML 테이블로 보여줍니다.
- `INSERT/UPDATE/DELETE`는 성공/실패 메시지를 보여줍니다.
- 실제 실행은 같은 서버의 `POST /query`를 호출합니다.

### `POST /query`

요청 본문:

```json
{
  "sql": "SELECT * FROM users WHERE id = 1;"
}
```

성공 응답 예시:

```json
{
  "ok": true,
  "statement": "SELECT",
  "row_count": 1,
  "affected_rows": 0,
  "columns": ["id", "email", "name"],
  "rows": [["1", "alice@test.com", "Alice"]],
  "access_path": "pk_index",
  "message": "ok",
  "queue_wait_ms": 0.012,
  "execute_ms": 0.451
}
```

실패 응답 예시:

```json
{
  "ok": false,
  "error": {
    "code": "queue_full",
    "message": "Request queue is full. Try again later."
  },
  "queue_wait_ms": 0.0,
  "execute_ms": 0.0
}
```

## 큐 정책

- `Task` 구조체는 두 필드만 사용합니다.
  - `int fd`
  - `struct Task *next`
- 큐는 bounded FIFO linked-list입니다.
- 큐 용량은 `max(32, worker_count * 8)`입니다.
- 큐가 가득 차면 accept 스레드는 대기하지 않고 즉시 `503 Service Unavailable`을 반환합니다.

## 빌드

```bash
make
```

실행:

```bash
./mini_dbms_api_server 18080 data
```

브라우저에서 열기:

```bash
open http://127.0.0.1:18080/
```

또는 데이터 디렉터리를 별도로 줄 수 있습니다.

```bash
./mini_dbms_api_server 18080 /tmp/mini-db-data
```

## 데이터 파일 규칙

- 테이블 본문: `data/<table>.csv`
- delta 로그: `data/<table>.delta`
- index snapshot: `data/<table>.idx`

예시 테이블:

```csv
id(PK),email(UK),name(NN)
1,alice@test.com,Alice
2,bob@test.com,Bob
```

파일명은 곧 테이블명입니다. 위 파일은 `users` 테이블로 조회합니다.

## 테스트

엔진/큐/API 테스트를 한 번에 실행:

```bash
make test
```

테스트 구성:

- `tests/test_engine.c`
  - `SELECT/INSERT/UPDATE/DELETE`
  - PK/UK 인덱스 경로 검증
- `tests/test_queue.c`
  - FIFO, full, shutdown 동작 검증
- `tests/test_api.sh`
  - `GET /` UI 진입 검증
  - `POST /query` 통합 테스트
  - `GET /query` 405 검증
  - `GET /health` 404 검증
  - invalid JSON 400 검증
  - queue full 503 검증

## 시연 포인트

1. 여러 클라이언트가 동시에 요청해도 스레드 풀이 병렬 처리합니다.
2. `SELECT * FROM users WHERE id = ...` 응답에서 `access_path: "pk_index"`가 보입니다.
3. `SELECT * FROM users WHERE email = ...` 응답에서 `access_path: "uk_index"`가 보입니다.
4. 인덱스를 못 쓰는 조건은 `access_path: "linear_scan"`으로 내려옵니다.
5. queue가 꽉 차면 즉시 `503 queue_full`을 반환합니다.
