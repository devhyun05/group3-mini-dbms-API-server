# group3-mini-dbms-API-server

Tiny Web Server 스타일의 API 서버 위에 `SQL-B-Tree` 기반 DB 엔진을 함수 호출형 모듈로 재구성한 미니 DBMS 프로젝트입니다.  
클라이언트 요청은 스레드 풀로 병렬 처리하지만, DB 엔진 내부 실행은 전역 mutex로 보호해 데이터 일관성을 유지합니다.  
즉, **서버 레벨에서는 동시성**을 확보하고, **DB 레벨에서는 안전한 직렬 실행**을 선택한 구조입니다.

## 시연

브라우저에서 SQL을 입력하면 서버가 요청을 받아 DB 엔진을 실행하고, 결과를 JSON으로 반환합니다.  
`SELECT`는 테이블 형태로 확인할 수 있고, `INSERT`, `UPDATE`, `DELETE`는 성공 여부와 영향받은 행 수를 바로 확인할 수 있습니다.

```sql
SELECT * FROM users WHERE id = 1;
INSERT INTO users VALUES ('carol@test.com','Carol');
SELECT * FROM users WHERE email = 'carol@test.com';
UPDATE users SET name = 'Carolyn' WHERE email = 'carol@test.com';
DELETE FROM users WHERE id = 3;
```



## 흐름도

<img width="769" height="636" alt="image" src="https://github.com/user-attachments/assets/55831449-8118-41d5-9f46-45e4e2bafe95" />

## 스레드 풀 설계

<img width="1600" height="823" alt="thread_pool" src="https://github.com/user-attachments/assets/2dc88aa3-cb59-478c-ba4d-0b039e788d5c" />

### 구조 개요

```text
Client
  -> Application (accept loop)
  -> Task Queue
  -> Worker Threads (N = CPU cores)
  -> DB Engine (global mutex)
```

서버는 멀티스레드 구조로 여러 클라이언트 요청을 동시에 받아들이고 처리할 수 있습니다.  
다만, **DB 엔진 내부는 전역 mutex로 보호**되기 때문에 실제 SQL 실행은 한 번에 하나씩만 수행됩니다.

즉, 이 프로젝트는 다음 두 가지를 분리해서 설계했습니다.

- **네트워크 요청 처리의 동시성**
- **DB 실행 구간의 일관성 보장**

### 스레드 수를 코어 수로 제한한 이유

DB 실행 구간은 mutex로 직렬화되어 있기 때문에, 특정 시점에 실제로 SQL을 실행 중인 워커는 항상 한 명뿐입니다.  
나머지 워커는 HTTP 파싱, 응답 생성, 연결 종료 같은 mutex 바깥 작업을 수행하거나, DB 락 해제를 기다리는 상태가 됩니다.

이 구조에서 워커를 CPU 코어 수보다 더 많이 늘리면 다음 문제가 생길 수 있습니다.

- DB 처리량은 크게 증가하지 않습니다.
- 대기 스레드가 많아질수록 컨텍스트 스위칭 비용이 증가합니다.
- 락 대기열이 길어져 tail latency가 악화될 수 있습니다.

그래서 스레드 풀 크기는 하드웨어 병렬성의 상한인 **CPU 코어 수**에 맞춰 고정했습니다.

### 구성 값

| 항목 | 값 | 비고 |
| --- | --- | --- |
| 워커 스레드 수 | `sysconf(_SC_NPROCESSORS_ONLN)` | 실행 환경의 CPU 코어 수에 자동 맞춤 |
| Task Queue 용량 | `max(32, worker_count * 8)` | Bounded queue, 초과 시 즉시 거절 |
| 큐 동기화 | `pthread_mutex_t + pthread_cond_t` | 큐 보호 및 빈 큐 대기 처리 |
| 깨우기 방식 | `pthread_cond_signal()` | 요청 1개당 worker 1개만 깨움 |
| 종료 방식 | `pthread_cond_broadcast()` | 서버 종료 시 전체 worker 깨움 |

## 락 방식

| 구간 | 락 종류 | 목적 | 동작 방식 |
| --- | --- | --- | --- |
| 작업 큐 접근 | `queue->mutex` | 큐 데이터 경쟁 방지 | `push/pop` 시 mutex 잠금 |
| DB 실행 구간 | `engine->execute_mutex` | 파일/테이블 상태 일관성 유지 | `db_engine_execute()` 전체를 단일 mutex로 보호 |

정리하면 다음과 같습니다.

1. **큐 락**은 여러 스레드가 동시에 요청 큐를 건드려도 안전하게 요청을 분배하기 위한 락입니다.
2. **DB 실행 락**은 CSV 파일과 테이블 상태를 안전하게 유지하기 위해 SQL 실행 전체를 직렬화하는 락입니다.

즉, 이 프로젝트는 **서버의 동시성**과 **DB의 일관성** 사이에서 균형을 맞춘 설계입니다.

## 벤치마크 

<img width="1408" height="768" alt="image" src="https://github.com/user-attachments/assets/8cf55770-261e-48e3-ac54-ed86465d342f" />


대표 쿼리 5개를 기준으로 worker thread 수를 `4 / 8 / 12 / 16 / 20`으로 바꿔가며 처리량을 비교했습니다.

| 쿼리 | 4개 | 8개 | 12개 | 16개 | 20개 | 최고 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| id 기준 인덱스 조회 | 10131.10 | 10031.13 | 10017.39 | 9315.40 | 8625.70 | 4개 |
| email 기준 인덱스 조회 | 9753.92 | 9798.40 | 9916.73 | 9373.75 | 9545.42 | 12개 |
| name 기준 전체 탐색 조회 | 2729.07 | 2808.83 | 2884.94 | 3005.28 | 3055.08 | 20개 |
| id 기준 인덱스 업데이트 | 9868.68 | 9970.53 | 10003.74 | 9909.00 | 9936.59 | 12개 |
| name 기준 전체 탐색 업데이트 | 4608.14 | 4718.80 | 4657.82 | 4720.67 | 4713.28 | 16개 |

평균 처리량 기준으로는 **12개 스레드**가 가장 높은 성능을 보였습니다.

| 스레드 수 | 평균 처리량 |
| --- | ---: |
| 4개 | 7418.18 |
| 8개 | 7465.54 |
| 12개 | 7496.12 |
| 16개 | 7264.82 |
| 20개 | 7175.21 |

### 해석

이번 결과에서 하나의 스레드 수가 모든 쿼리에서 항상 가장 좋지는 않았습니다.  
다만 전체 5개 대표 쿼리를 평균적으로 비교했을 때는 **12개 스레드가 가장 높은 처리량**을 보여 주었습니다.

즉, 현재 프로젝트 구조에서는 다음과 같이 해석할 수 있습니다.

- **12개 스레드**는 전체 workload 관점에서 가장 균형 잡힌 설정이었습니다.
- **4개 스레드**는 `id 기준 인덱스 조회`처럼 빠른 인덱스 조회에서 가장 좋은 결과를 보였습니다.
- **16개 / 20개 스레드**는 일부 전체 탐색 쿼리에서 더 높은 처리량이 나오기도 했지만, 전체 평균에서는 오히려 떨어졌습니다.
- 따라서 현재 구조에서는 스레드를 무조건 많이 늘리는 것이 항상 성능 향상으로 이어지지는 않았습니다.

## 테스트

테스트는 네 단계로 나누어 구성했습니다.

| 분류 | 목적 |
| --- | --- |
| Unit Test | 토크나이저, 파서, 작업 큐 같은 개별 모듈이 독립적으로 올바르게 동작하는지 검증 |
| Integration Test | SQL 실행 경로와 락 동작이 엔진 단위에서 연결되어 정상 동작하는지 검증 |
| Functional Test | 실제 API 서버에 요청을 보내 `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `WHERE` 흐름이 정상 수행되는지 검증 |
| Edge Case Test | 중복 PK/UK, 잘못된 메서드, 잘못된 JSON, 존재하지 않는 테이블, 빈 결과, `queue_full` 같은 예외 상황을 검증 |

각 테스트는 다음 관점에 초점을 맞췄습니다.

- **Unit Test**
  - lexer / parser 기본 동작 검증
  - `TaskQueue`의 FIFO, full, blocking, shutdown 동작 검증
  - 스레드 풀의 기본 동기화 단위를 작은 범위에서 확인

- **Integration Test**
  - `db_engine_execute()` 기준 end-to-end SQL 실행 검증
  - 엔진 재시작 후 데이터 유지 여부 검증
  - `execute_mutex`를 통한 DB 실행 직렬화 동작 검증

- **Functional Test**
  - `GET /` 페이지 진입 확인
  - `POST /query` 기반 CRUD 시나리오 검증
  - 여러 요청을 동시에 보내도 서버가 정상 응답하는지 검증

- **Edge Case Test**
  - duplicate PK / duplicate UK 검증
  - quoted string / empty result / missing table 검증
  - `405`, `404`, `400`, `503 queue_full` 응답 검증

전체 테스트는 아래 명령으로 실행할 수 있습니다.

```bash
make test

### 관찰 포인트

- **쿼리 접근 방식의 영향이 스레드 수보다 더 컸습니다.**  
  인덱스를 사용하는 조회/업데이트 쿼리는 대체로 `9,000 ~ 10,000 req/s` 수준을 유지했지만, 전체 탐색 쿼리는 `2,700 ~ 4,700 req/s` 수준으로 크게 낮아졌습니다.

- **인덱스를 사용하는 쿼리는 적은 수의 스레드에서도 충분히 높은 성능을 보였습니다.**  
  특히 `id 기준 인덱스 조회`는 `4개 스레드`에서 가장 높았고, `12개 이상`으로 늘렸을 때 오히려 성능이 감소했습니다.


## 마무리

이 프로젝트는 **완전한 병렬 DB 엔진**을 목표로 하기보다,  
**멀티스레드 API 서버 + 안전한 단일 실행 DB 엔진** 구조를 구현하는 데 초점을 맞췄습니다.

이를 통해 다음을 직접 구현하고 검증했습니다.

- Tiny 스타일의 API 서버 구조
- 고정 크기 스레드 풀과 bounded task queue
- producer-consumer 기반 동기화
- DB 실행 직렬화와 데이터 일관성 유지
- 인덱스 경로와 선형 스캔 경로의 성능 차이 측정

결과적으로 이 프로젝트는  
**"서버는 동시성을 제공하고, DB는 안전하게 직렬 실행한다"**  
라는 아키텍처를 실제 코드와 벤치마크로 보여주는 미니 DBMS API 서버입니다.
