# Per-Message Async 처리 + Redis Cluster Read Replica 구현 계획

## 배경

현재 Profit Worker Webflux는 Kafka 배치 리스너로 최대 50건을 모아서 벌크 파이프라인으로 처리한다.
Redis Cluster(마스터 3 + 레플리카 3) 환경에서 메시지별 독립 처리로 전환하면:

- 포트폴리오 fanout이 서로 다른 해시 슬롯 → 다른 Redis 노드에서 병렬 처리
- 읽기 작업은 레플리카, 쓰기 작업은 마스터로 분산
- Phase 간 barrier 제거 → 파이프라인 활용도 향상

---

## 현재 구조 vs 목표 구조

```
현재 (배치 벌크):
  50건 수신 → 중복제거 → [Phase1 전체완료] → [Phase2 전체완료] → ... → commit

목표 (메시지별 순차 + 포트폴리오 병렬):
  파티션 A: msg1 완료 → msg2 완료 → msg3 완료  (순차)
  파티션 B: msg4 완료 → msg5 완료 → msg6 완료  (순차)
                ↑ 파티션 간 병렬 (concurrency=3)

  msg1 내부:
    SMEMBERS (레플리카) → portfolio 1 ─┐
                          portfolio 2 ─┼─ 병렬 fanout (다른 해시 슬롯 → 다른 Redis 노드)
                          portfolio 3 ─┘
```

---

## 변경 내역

### 1. Redis Cluster ReadFrom 설정

**신규 파일**: `config/RedisConfig.java`

- `ReadFrom.REPLICA_PREFERRED` 설정
- 읽기 커맨드(SMEMBERS, GET, HMGET) → 레플리카 노드
- 쓰기 커맨드(HINCRBYFLOAT, SET, HMSET, SADD) → 마스터 노드
- Lettuce가 커맨드 타입을 자동 판별하므로 비즈니스 코드 변경 불필요

### 2. application.yaml 변경

- `spring.kafka.listener.type: batch` 제거 (단건 리스너 전환)
- `max.poll.records` 설정 제거 (배치 불필요)
- Redis cluster 설정 확인

### 3. StockPriceEventConsumer — 단건 리스너

**변경 전**:
```java
@KafkaListener(topics = "...")
public Mono<Void> consume(List<String> payloads) {
    // 배치 수신 → stockId 중복제거 → 벌크 처리
}
```

**변경 후**:
```java
@KafkaListener(topics = "...")
public Mono<Void> consume(String payload) {
    StockPriceUpdatedEvent event = read(payload, StockPriceUpdatedEvent.class);
    return profitCalculationUseCase.updateProfitByStockPriceChange(toRequest(event));
}
```

- 배치 수신 제거, 중복 제거 제거
- 메시지 하나당 독립 reactive chain

### 4. ProfitCalculateService — 단건 처리 재작성

**변경 전**: `updateProfitsByStockPriceChanges(List<Request>)` → 벌크 6-phase 파이프라인

**변경 후**: `updateProfitByStockPriceChange(Request)` → 단건 처리, 포트폴리오 병렬 fanout

```
1. findPortfolioIdsByStockId(stockId)          — SMEMBERS (레플리카 읽기)
2. 포트폴리오별 병렬 flatMap:
   2-1. recalculateCurrentValue(pid, stockId, newPrice)
        → GET quantity (레플리카)
        → GET oldStockCV (레플리카)
        → delta 계산 (인메모리)
        → HINCRBYFLOAT portfolio CV (마스터, 원자적)
        → SET newStockCV (마스터)
   2-2. findPurchasedValue + findAssetCount     — HMGET (레플리카)
   2-3. savePortfolioValuation                  — HMSET + SADD (마스터)
   2-4. 유저 fanout:
        → findUserIdByPortfolioId (레플리카)
        → HINCRBYFLOAT user CV (마스터, 원자적)
        → findPurchasedValue + findPortfolioCount (레플리카)
        → saveUserValuation (마스터)
```

- 기존 `recalculateCurrentValue()` 재활용 (이미 단건 처리용으로 구현됨)
- 벌크 메서드(`bulkFetch*`, `bulkIncrement*`) 호출 제거

### 5. ProfitCalculationUseCase 인터페이스

- `updateProfitsByStockPriceChanges(List)` 제거
- `updateProfitByStockPriceChange(Request)` 단건만 유지

### 6. 메트릭스 정리

- `BATCH_SIZE`, `BATCH_DEDUPLICATED_SIZE` 제거
- phase duration 메트릭 제거 (phase 구분이 없어짐)
- 기존 단건 메트릭(`hash_get`, `hash_increment_float` 등) 활용

---

## 동시성 안전성

| 연산 | 타입 | 안전? | 이유 |
|------|------|-------|------|
| SMEMBERS | 읽기 | O | 레플리카 읽기 전용 |
| GET quantity / stockCV | 읽기 | O | 레플리카 읽기 전용 |
| HINCRBYFLOAT portfolio CV | 쓰기 | O | Redis 원자적 연산, 델타 기반 |
| SET stock CV | 쓰기 | O | 같은 파티션 내 순차 처리로 race 없음 |
| HINCRBYFLOAT user CV | 쓰기 | O | Redis 원자적 연산, 델타 기반 |

같은 stockId 이벤트는 같은 파티션에서 순차 도착 → 순차 처리되므로 lost update 없음.

---

## 수정 대상 파일

| 파일 | 변경 |
|------|------|
| `config/RedisConfig.java` | **신규** — ReadFrom.REPLICA_PREFERRED |
| `application.yaml` | batch listener 제거 |
| `StockPriceEventConsumer.java` | 단건 리스너로 전환 |
| `ProfitCalculateService.java` | 벌크 → 단건 재작성 |
| `ProfitCalculationUseCase.java` | 벌크 메서드 시그니처 제거 |
| `ProfitWorkerMetrics.java` | 배치 메트릭 제거 |
| 테스트 코드 | 서비스/컨슈머 변경에 맞게 수정 |

인프라 계층(`RedisPortfolioStateStore`, `RedisUserStateStore`, `RedisValuationRepositoryAdapter`)은 변경 없음.
단건 메서드가 이미 모두 존재함.
