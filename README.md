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

### 측정 조건

- 시나리오 수: 10개
- worker 수: `4 / 8 / 12 / 16 / 20`
- 시나리오당 요청 수: `5,000`
- 동시성: `32`
- 데이터셋: `users` 테이블 12,000행

### 평균 처리량 요약

| worker | 평균 처리량 (req/s) |
| --- | ---: |
| 4 | 7453.93 |
| 8 | 7241.26 |
| 12 | 7336.55 |
| 16 | 7280.99 |
| 20 | 6941.53 |

### 해석

- 전체 10개 시나리오 평균 기준으로는 **worker 4개**가 가장 효율적이었습니다.
- `pk_index`, `uk_index`를 사용하는 쿼리는 대체로 **1만 req/s 전후**의 높은 처리량을 보였습니다.
- 반면 `linear_scan` 경로로 내려가는 쿼리는 처리량이 크게 떨어졌습니다.
- 즉, 이 프로젝트에서 성능을 결정하는 핵심은 단순히 스레드 수보다도 **인덱스를 타는지 여부**와 **DB 실행 직렬화 구조**입니다.

### 관찰 포인트

- `SELECT * FROM users WHERE id = ...` 같은 PK 조회는 빠르게 처리됩니다.
- `SELECT * FROM users WHERE email = ...` 같은 UK 조회도 높은 처리량을 보입니다.
- `WHERE name = ...`처럼 인덱스를 타지 못하는 조건은 선형 스캔으로 내려가면서 성능 차이가 크게 벌어집니다.
- worker를 과도하게 늘려도 DB 실행 구간이 직렬화되어 있기 때문에 성능이 비례해서 증가하지는 않습니다.

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
