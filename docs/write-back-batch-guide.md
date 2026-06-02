# Write-back Batch 구현 가이드

이 문서는 profit worker가 Redis에 저장한 valuation snapshot과 dirty marker를 외부 batch 서버가 DB에 반영하는 방법을 정리한다.
기준 코드는 `RedisValuationRepositoryAdapter`, `RedisPortfolioStateStore`, `RedisUserStateStore`이다.

## Batch 서버 책임

write-back batch 서버는 Redis를 source로 보고 DB를 최종 조회 저장소로 갱신한다.

| 책임 | 설명 |
| --- | --- |
| Dirty 대상 조회 | Redis dirty set에서 write-back 대상 portfolio/user를 조회한다. |
| Snapshot 읽기 | 대상 ID별 Redis valuation snapshot을 읽는다. |
| DB upsert | 일반 dirty 대상은 DB valuation row에 upsert 한다. |
| DB soft delete | 포트폴리오 삭제 dirty 대상은 DB portfolio valuation row를 soft delete 한다. |
| Dirty 제거 | DB 반영에 성공한 대상만 Redis dirty set에서 제거한다. |
| 재시도 보장 | 실패한 대상은 dirty set에 남겨 다음 batch에서 재처리한다. |

## Redis Dirty Sets

batch 서버는 다음 set을 처리한다.

| Redis set | 값 타입 | 처리 |
| --- | --- | --- |
| `dirty:portfolio-valuations` | `Long` portfolioId | portfolio valuation upsert |
| `dirty:user-valuations` | `String` userId | user valuation upsert |
| `dirty:portfolio-valuation-deletions` | `Long` portfolioId | portfolio valuation soft delete |

처리 원칙:

- dirty set은 queue처럼 취급하되, 실패 시 유실되면 안 된다.
- 단순 `SPOP`은 처리 중 장애가 나면 대상이 유실될 수 있다.
- 초기 구현은 `SSCAN` 또는 `SMEMBERS`로 읽고, DB 성공 후 `SREM` 하는 방식이 안전하다.
- 대상 수가 많아지면 `SSCAN` + batch size 제한을 사용한다.

## Redis Snapshot Keys

현재 worker가 쓰는 Redis key는 다음과 같다.

### Portfolio Valuation

| 필드 | Redis key | 타입 | 필수 |
| --- | --- | --- | --- |
| purchasedValue | `portfolio:{portfolioId}:purchased-value` | Long | upsert 시 필수 |
| currentValue | `portfolio:{portfolioId}:current-value` | Long | upsert 시 필수 |
| profitRate | `portfolio:{portfolioId}:profit-rate` | Double | upsert 시 필수 |
| assetCount | `portfolio:{portfolioId}:asset-count` | Long | upsert 시 필수 |
| updatedAt | `portfolio:{portfolioId}:updated-at` | Instant string | upsert 시 권장 |
| deleted | `portfolio:{portfolioId}:deleted` | Boolean string | soft delete 시 사용 |
| deletedAt | `portfolio:{portfolioId}:deleted-at` | Instant string | soft delete 시 필수 |

DB row 기준 필드:

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| portfolioId | Long | PK |
| purchasedValue | Long | 총 구매액 |
| currentValue | Long | 현재 총 평가액 |
| profitRate | Double | 수익률 |
| assetCount | Long | 보유 종목 수 |
| updatedAt | Instant | snapshot 갱신 시각. DB schema에 추가 권장 |
| deleted | Boolean | soft delete 여부. DB schema에 추가 필요 |
| deletedAt | Instant | soft delete 시각. DB schema에 추가 필요 |

### User Valuation

| 필드 | Redis key | 타입 | 필수 |
| --- | --- | --- | --- |
| purchasedValue | `user:{userId}:purchased-value` | Long | 필수 |
| currentValue | `user:{userId}:current-value` | Long | 필수 |
| profitRate | `user:{userId}:profit-rate` | Double | 필수 |
| portfolioCount | `user:{userId}:portfolio-count` | Long | 필수 |
| updatedAt | `user:{userId}:updated-at` | Instant string | 권장 |

DB row 기준 필드:

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| userId | String | PK. 현재 worker는 String userId를 사용한다. |
| purchasedValue | Long | 유저 전체 총 구매액 |
| currentValue | Long | 유저 전체 현재 평가액 |
| profitRate | Double | 수익률 |
| portfolioCount | Long | 보유 포트폴리오 수 |
| updatedAt | Instant | snapshot 갱신 시각. DB schema에 추가 권장 |

## 처리 순서

한 번의 batch tick에서 다음 순서를 권장한다.

1. Portfolio deletion dirty 대상을 처리한다.
2. Portfolio valuation dirty 대상을 처리한다.
3. User valuation dirty 대상을 처리한다.

이유:

- 삭제된 포트폴리오가 일반 portfolio dirty set에도 남아 있을 수 있다.
- 삭제 처리를 먼저 하면 DB에서 삭제 상태가 우선 적용된다.
- portfolio valuation 반영 후 user valuation을 반영하면 유저 snapshot이 더 최신 portfolio 상태를 기준으로 저장될 가능성이 높다.

## Portfolio Upsert 절차

대상: `dirty:portfolio-valuations`의 portfolioId.

절차:

1. portfolioId를 dirty set에서 조회한다.
2. `portfolio:{portfolioId}:deleted`가 `true`이면 일반 upsert를 건너뛰고 삭제 처리 대상으로 본다.
3. 필수 snapshot 필드를 읽는다.
4. 필수 필드가 누락되었으면 dirty set에서 제거하지 않는다.
5. DB의 기존 row와 Redis `updatedAt`을 비교한다.
6. Redis snapshot이 더 최신이면 upsert 한다.
7. DB upsert가 성공하면 `dirty:portfolio-valuations`에서 portfolioId를 제거한다.

필수 필드 누락 처리:

- Redis에 해당 portfolio snapshot이 없으면 hydration 대상 로그를 남긴다.
- dirty set은 유지한다.
- 반복 실패가 누적되면 dead-letter 또는 manual repair 대상으로 분리한다.

## User Upsert 절차

대상: `dirty:user-valuations`의 userId.

절차:

1. userId를 dirty set에서 조회한다.
2. 필수 snapshot 필드를 읽는다.
3. 필수 필드가 누락되었으면 dirty set에서 제거하지 않는다.
4. DB의 기존 row와 Redis `updatedAt`을 비교한다.
5. Redis snapshot이 더 최신이면 upsert 한다.
6. DB upsert가 성공하면 `dirty:user-valuations`에서 userId를 제거한다.

주의:

- 현재 userId는 `String`이다.
- 추후 정수 기반 userId로 변경되면 Redis key와 DB PK 타입 변경이 함께 필요하다.

## Portfolio Soft Delete 절차

대상: `dirty:portfolio-valuation-deletions`의 portfolioId.

포트폴리오 삭제 이벤트를 처리하면 worker는 다음 Redis 값을 남긴다.

| Redis key | 값 |
| --- | --- |
| `portfolio:{portfolioId}:deleted` | `true` |
| `portfolio:{portfolioId}:deleted-at` | 삭제 marker 생성 시각 |
| `dirty:portfolio-valuation-deletions` | portfolioId |

절차:

1. portfolioId를 삭제 dirty set에서 조회한다.
2. `portfolio:{portfolioId}:deleted-at`을 읽는다.
3. DB의 portfolio valuation row를 `deleted = true`, `deletedAt = Redis deletedAt`으로 갱신한다.
4. DB row가 없어도 성공으로 처리할 수 있어야 한다.
5. soft delete 성공 후 `dirty:portfolio-valuation-deletions`에서 portfolioId를 제거한다.
6. 필요하면 `dirty:portfolio-valuations`에서도 같은 portfolioId를 제거한다.

DB row가 없는 경우:

- 삭제 이벤트가 먼저 도착했거나 이미 삭제 처리된 상태일 수 있다.
- soft delete는 idempotent 해야 하므로 성공으로 처리하는 것을 권장한다.
- 필요하면 tombstone row를 insert할 수 있지만, 현재 구조에서는 필수는 아니다.

## 최신성 정책

Redis snapshot에는 `updatedAt`이 저장된다.
삭제 marker에는 `deletedAt`이 저장된다.

권장 정책:

- 일반 upsert는 Redis `updatedAt`이 DB `updatedAt`보다 최신일 때만 반영한다.
- DB row의 `updatedAt`이 없으면 Redis snapshot을 반영한다.
- soft delete는 DB `deletedAt`이 없거나 Redis `deletedAt`이 더 최신일 때 반영한다.
- soft delete된 row에는 일반 upsert가 다시 적용되지 않도록 `deleted` 여부를 확인한다.

주의:

- 현재 worker의 `updatedAt`은 worker 처리 시각이다.
- 이벤트 원본 시각과 완전히 같지는 않다.
- 이벤트 순서 보장이 더 중요해지면 event version 또는 source event timestamp를 snapshot에 저장하도록 worker를 확장한다.

## Dirty 제거 기준

dirty set 제거는 DB 반영 성공 이후에만 수행한다.

| 상황 | 처리 |
| --- | --- |
| DB upsert 성공 | 해당 dirty set에서 대상 제거 |
| DB soft delete 성공 | 삭제 dirty set에서 대상 제거, 필요 시 portfolio dirty set에서도 제거 |
| DB 반영 실패 | dirty set 유지 |
| Redis 필수 필드 누락 | dirty set 유지, hydration 대상 기록 |
| Redis deletedAt 누락 | 삭제 dirty set 유지, repair 대상 기록 |

## Batch Size와 재시도

권장 초기값:

| 항목 | 권장값 |
| --- | --- |
| portfolio upsert batch size | 500 |
| user upsert batch size | 500 |
| portfolio deletion batch size | 500 |
| schedule interval | 1초에서 10초 사이 |

운영 중 조정 기준:

- dirty backlog가 계속 증가하면 batch size 또는 실행 빈도를 늘린다.
- DB 부하가 높으면 batch size를 줄인다.
- Kafka consumer lag와 dirty backlog를 함께 본다.

## Pseudocode

```java
void runWriteBackBatch() {
    processPortfolioDeletes(500);
    processPortfolioUpserts(500);
    processUserUpserts(500);
}

void processPortfolioDeletes(int limit) {
    for (Long portfolioId : scan("dirty:portfolio-valuation-deletions", limit)) {
        Instant deletedAt = getInstant("portfolio:" + portfolioId + ":deleted-at");
        if (deletedAt == null) {
            recordRepairTarget(portfolioId);
            continue;
        }

        softDeletePortfolioValuation(portfolioId, deletedAt);
        srem("dirty:portfolio-valuation-deletions", portfolioId);
        srem("dirty:portfolio-valuations", portfolioId);
    }
}

void processPortfolioUpserts(int limit) {
    for (Long portfolioId : scan("dirty:portfolio-valuations", limit)) {
        if (isDeleted(portfolioId)) {
            continue;
        }

        PortfolioSnapshot snapshot = readPortfolioSnapshot(portfolioId);
        if (snapshot.hasMissingRequiredField()) {
            recordHydrationTarget(portfolioId);
            continue;
        }

        upsertPortfolioValuationIfNewer(snapshot);
        srem("dirty:portfolio-valuations", portfolioId);
    }
}

void processUserUpserts(int limit) {
    for (String userId : scan("dirty:user-valuations", limit)) {
        UserSnapshot snapshot = readUserSnapshot(userId);
        if (snapshot.hasMissingRequiredField()) {
            recordHydrationTarget(userId);
            continue;
        }

        upsertUserValuationIfNewer(snapshot);
        srem("dirty:user-valuations", userId);
    }
}
```

## DB Schema 권장 사항

현재 worker domain class에는 `updatedAt`, `deleted`, `deletedAt` 필드가 없다.
하지만 batch DB에는 다음 컬럼을 추가하는 것을 권장한다.

Portfolio valuation:

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| portfolio_id | BIGINT | PK |
| purchased_value | BIGINT | 총 구매액 |
| current_value | BIGINT | 현재 평가액 |
| profit_rate | DOUBLE | 수익률 |
| asset_count | BIGINT | 보유 종목 수 |
| updated_at | TIMESTAMP | 마지막 write-back snapshot 시각 |
| deleted | BOOLEAN | soft delete 여부 |
| deleted_at | TIMESTAMP | soft delete 시각 |

User valuation:

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| user_id | VARCHAR | PK |
| purchased_value | BIGINT | 총 구매액 |
| current_value | BIGINT | 현재 평가액 |
| profit_rate | DOUBLE | 수익률 |
| portfolio_count | BIGINT | 보유 포트폴리오 수 |
| updated_at | TIMESTAMP | 마지막 write-back snapshot 시각 |

## 모니터링 지표

batch 서버는 다음 지표를 노출해야 한다.

| 지표 | 설명 |
| --- | --- |
| dirty portfolio backlog | `dirty:portfolio-valuations` 크기 |
| dirty user backlog | `dirty:user-valuations` 크기 |
| dirty deletion backlog | `dirty:portfolio-valuation-deletions` 크기 |
| write-back success count | DB 반영 성공 수 |
| write-back failure count | DB 반영 실패 수 |
| snapshot missing count | Redis 필수 필드 누락 수 |
| batch duration | batch tick 처리 시간 |

## 구현 시 주의 사항

- Redis dirty set을 DB 성공 전에 제거하지 않는다.
- 삭제 대상은 일반 upsert보다 먼저 처리한다.
- userId는 현재 String이다.
- soft delete된 portfolio valuation은 일반 upsert로 되살리지 않는다.
- Redis 필드 값은 문자열로 저장되므로 batch 서버에서 Long, Double, Boolean, Instant로 변환해야 한다.
- Redis 장애 또는 flush 이후에는 별도 hydration job으로 상태를 복구해야 한다.
