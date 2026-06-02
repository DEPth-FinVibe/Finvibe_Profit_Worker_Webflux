# Redis 데이터 모델

이 문서는 profit worker가 Redis에서 읽고 쓰는 데이터를 외부 서비스 관점에서 정리한다.
구체적인 Redis key 문자열은 코드의 adapter 구현에 맡기고, 여기서는 어떤 데이터를 지속적으로 관리해야 하는지만 설명한다.

## 전체 흐름

profit worker는 Redis를 계산용 상태 저장소로 사용한다.

1. 외부 서비스가 매수, 매도, 포트폴리오 생성, 포트폴리오 삭제 이벤트를 기반으로 Redis 상태를 최신화한다.
2. profit worker는 주가 변경 이벤트를 받으면 Redis 상태를 읽어 포트폴리오/유저 수익률을 계산한다.
3. 계산된 valuation snapshot은 Redis에 저장되고 dirty 대상으로 표시된다.
4. 외부 batch service가 dirty 대상을 읽어 DB에 write-back 한다.

## 관리 대상 요약

| 모델 | 설명 | 주요 사용자 |
| --- | --- | --- |
| 종목 보유 포트폴리오 인덱스 | 특정 종목을 보유한 포트폴리오 목록 | profit worker |
| 포트폴리오 종목 보유 상태 | 포트폴리오가 특정 종목을 얼마나 보유하고 있고 현재 얼마로 평가되는지 | profit worker |
| 포트폴리오 집계 상태 | 포트폴리오 단위 구매액, 평가액, 종목 수, 수익률 | profit worker, batch service |
| 포트폴리오-유저 관계 | 포트폴리오가 어떤 유저에게 속하는지 | profit worker |
| 유저 포트폴리오 목록 | 유저가 보유한 포트폴리오 목록 | profit worker |
| 유저 집계 상태 | 유저 단위 구매액, 평가액, 포트폴리오 수, 수익률 | profit worker, batch service |
| dirty valuation 목록 | DB write-back이 필요한 포트폴리오/유저 목록 | batch service |

## 종목 보유 포트폴리오 인덱스

특정 종목 가격이 바뀌었을 때 재계산 대상 포트폴리오를 찾기 위한 역인덱스다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| stockId | Long | 종목 ID |
| portfolioIds | Set<Long> | 해당 종목을 보유한 포트폴리오 ID 목록 |

관리 규칙:

- 매수 후 해당 종목 보유 수량이 0에서 양수가 되면 포트폴리오를 추가한다.
- 매도 후 해당 종목 보유 수량이 0이 되면 포트폴리오를 제거한다.
- 부분 매도 후 수량이 남아 있으면 제거하지 않는다.

## 포트폴리오 종목 보유 상태

포트폴리오 내부의 종목별 상태다. 주가 변경 시 포트폴리오 평가액 delta를 계산하는 데 필요하다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| portfolioId | Long | 포트폴리오 ID |
| stockId | Long | 종목 ID |
| quantity | Long | 현재 보유 수량 |
| currentValue | Long | 해당 종목의 현재 평가액 |

관리 규칙:

- 매수 시 `quantity`와 `currentValue`를 증가시킨다.
- 매도 시 `quantity`와 `currentValue`를 감소시킨다.
- 전량 매도 시 해당 종목 보유 상태를 제거한다.
- 주가 변경 시 worker가 `quantity`를 기준으로 새 종목 평가액을 계산하고 `currentValue`를 갱신한다.

## 포트폴리오 집계 상태

포트폴리오 단위 valuation 계산 결과와 중간 상태다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| portfolioId | Long | 포트폴리오 ID |
| purchasedValue | Long | 총 구매액 |
| currentValue | Long | 현재 총 평가액 |
| assetCount | Long | 보유 종목 수 |
| profitRate | Double | 수익률 |
| updatedAt | Instant | valuation snapshot 갱신 시각 |

관리 규칙:

- 매수 시 `purchasedValue`와 `currentValue`를 증가시킨다.
- 매도 시 `purchasedValue`와 `currentValue`를 감소시킨다.
- 새 종목 보유가 시작되면 `assetCount`를 증가시킨다.
- 전량 매도되어 종목 보유가 끝나면 `assetCount`를 감소시킨다.
- 주가 변경 시 worker가 `currentValue`와 `profitRate`를 재계산한다.
- batch service는 이 snapshot을 읽어 DB에 반영한다.

## 포트폴리오-유저 관계

포트폴리오 valuation 변경이 어떤 유저 valuation에 영향을 주는지 찾기 위한 관계 데이터다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| portfolioId | Long | 포트폴리오 ID |
| userId | String | 포트폴리오 소유 유저 ID. 추후 정수 기반 UserID로 변경 예정 |

관리 규칙:

- 포트폴리오 생성 시 관계를 저장한다.
- 포트폴리오 삭제 시 관계를 제거한다.
- 주가 변경으로 포트폴리오가 재계산되면 worker가 이 관계로 affected user를 찾는다.

## 유저 포트폴리오 목록

유저의 현재 평가액을 포트폴리오 평가액 합산으로 계산하기 위한 목록이다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| userId | String | 유저 ID. 추후 정수 기반 UserID로 변경 예정 |
| portfolioIds | Set<Long> | 유저가 보유한 포트폴리오 ID 목록 |

관리 규칙:

- 포트폴리오 생성 시 목록에 추가한다.
- 포트폴리오 삭제 시 목록에서 제거한다.
- worker는 이 목록을 기준으로 유저 현재 평가액을 계산한다.

## 유저 집계 상태

유저 단위 valuation 계산 결과와 중간 상태다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| userId | String | 유저 ID. 추후 정수 기반 UserID로 변경 예정 |
| purchasedValue | Long | 유저 전체 총 구매액 |
| currentValue | Long | 유저 전체 현재 평가액 |
| portfolioCount | Long | 보유 포트폴리오 수 |
| profitRate | Double | 수익률 |
| updatedAt | Instant | valuation snapshot 갱신 시각 |

관리 규칙:

- 포트폴리오 생성 시 포트폴리오 구매액만큼 `purchasedValue`를 증가시키고 `portfolioCount`를 증가시킨다.
- 포트폴리오 삭제 시 포트폴리오 구매액만큼 `purchasedValue`를 감소시키고 `portfolioCount`를 감소시킨다.
- 주가 변경 시 worker가 affected user의 `currentValue`와 `profitRate`를 재계산한다.
- batch service는 이 snapshot을 읽어 DB에 반영한다.

## Dirty Valuation 목록

DB write-back이 필요한 대상을 표시하는 목록이다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| portfolioIds | Set<Long> | DB 반영이 필요한 포트폴리오 ID 목록 |
| userIds | Set<Long> | DB 반영이 필요한 유저 ID 목록 |
| deletedPortfolioIds | Set<Long> | DB에서 soft delete 처리해야 하는 포트폴리오 ID 목록 |

관리 규칙:

- worker가 포트폴리오 valuation snapshot을 저장하면 해당 포트폴리오를 dirty 대상으로 표시한다.
- worker가 포트폴리오 삭제 이벤트를 처리하면 해당 포트폴리오를 삭제 dirty 대상으로 표시한다.
- worker가 유저 valuation snapshot을 저장하면 해당 유저를 dirty 대상으로 표시한다.
- batch service는 dirty 대상을 읽고 DB upsert 성공 후 제거한다.
- batch service는 삭제 dirty 대상을 읽고 DB soft delete 성공 후 제거한다.
- DB 반영 실패 시 dirty 대상은 유지하거나 재등록해야 한다.

## 외부 서비스 책임

Redis 상태는 batch sync와 이벤트 기반 갱신을 함께 사용해 관리한다.

| 책임 | 담당 | 설명 |
| --- | --- | --- |
| Redis 초기 적재 | batch service | DB 또는 원천 데이터 기준으로 계산에 필요한 상태를 Redis에 적재한다. |
| Redis 정합성 보정 | batch service | Redis 유실, 누락, 불일치가 발생했을 때 DB 또는 원천 데이터 기준으로 복구한다. |
| 매수/매도 반영 | CacheUpdateService | 체결 이벤트를 기반으로 포트폴리오 종목 보유 상태와 집계 상태를 실시간 갱신한다. |
| 포트폴리오 생성/삭제 반영 | CacheUpdateService | 포트폴리오-유저 관계와 유저 집계 상태를 실시간 갱신한다. |
| 주가 변경 valuation 계산 | profit worker | Redis 상태를 읽어 포트폴리오/유저 평가액과 수익률을 계산한다. |
| DB write-back | batch service | dirty valuation 목록을 기준으로 Redis snapshot을 DB에 반영하고, 삭제 dirty 대상은 DB soft delete 처리한다. |

event consumer는 Kafka 등 외부 이벤트를 받아 `CacheUpdateService`를 호출한다.
Redis 상태를 직접 갱신하는 application 로직은 `CacheUpdateService`가 담당한다.

| 이벤트 | 호출 use case | 갱신 대상 |
| --- | --- | --- |
| 종목 매수 | `updatePortfolioCache` | 종목 보유 포트폴리오 인덱스, 포트폴리오 종목 보유 상태, 포트폴리오 집계 상태 |
| 종목 매도 | `updatePortfolioCache` | 종목 보유 포트폴리오 인덱스, 포트폴리오 종목 보유 상태, 포트폴리오 집계 상태 |
| 포트폴리오 생성 | `updateUserCache` | 포트폴리오-유저 관계, 유저 포트폴리오 목록, 유저 집계 상태 |
| 포트폴리오 삭제 | `updateUserCache` | 포트폴리오-유저 관계, 유저 포트폴리오 목록, 유저 집계 상태 |
| DB write-back batch | 별도 batch | dirty valuation 목록, 포트폴리오/유저 valuation snapshot, 포트폴리오 soft delete marker |

## Batch Sync 책임

batch service는 Redis를 주기적으로 DB 또는 원천 데이터와 동기화한다.

주요 작업:

- 서비스 시작 전 또는 Redis 장애 복구 후 필요한 Redis 상태를 warm-up 한다.
- DB 또는 원천 이벤트 로그 기준으로 Redis 누락 데이터를 보정한다.
- 오래된 관계 데이터, orphan 데이터, 전량 매도 후 남은 종목 상태를 정리한다.
- dirty valuation 목록을 읽어 DB에 upsert 한다.
- 포트폴리오 삭제 dirty 목록을 읽어 DB에 soft delete를 반영한다.
- DB write-back 성공 후 dirty 대상을 제거한다.
- DB write-back 실패 시 dirty 대상을 유지하거나 재등록한다.

권장 방식:

- write-back은 idempotent upsert로 구현한다.
- 대량 재적재는 트래픽이 낮은 시간대에 수행하거나 별도 namespace에 적재 후 전환한다.
- 이벤트 처리와 batch sync가 동시에 같은 데이터를 갱신할 수 있으므로 updatedAt, event version, snapshot version 중 하나를 두는 것을 고려한다.
- batch sync는 Redis를 무조건 덮어쓰기보다 더 최신 이벤트가 반영된 값을 보호할 수 있어야 한다.

## Event Consumer 책임

event consumer는 도메인 이벤트를 받아 `CacheUpdateService`에 전달한다.
Redis 상태 갱신 로직은 consumer에 두지 않는다.

매수 이벤트:

- consumer는 체결가와 체결 수량을 포함해 `updatePortfolioCache`를 호출한다.
- `CacheUpdateService`가 포트폴리오 종목 보유 수량을 증가시킨다.
- `CacheUpdateService`가 포트폴리오 종목 현재 평가액을 증가시킨다.
- `CacheUpdateService`가 포트폴리오 총 구매액과 현재 평가액을 증가시킨다.
- `CacheUpdateService`가 새로 보유하게 된 종목이면 종목 보유 포트폴리오 인덱스에 추가하고 보유 종목 수를 증가시킨다.

매도 이벤트:

- consumer는 체결가와 체결 수량을 포함해 `updatePortfolioCache`를 호출한다.
- `CacheUpdateService`가 포트폴리오 종목 보유 수량을 감소시킨다.
- `CacheUpdateService`가 포트폴리오 종목 현재 평가액을 감소시킨다.
- `CacheUpdateService`가 포트폴리오 총 구매액과 현재 평가액을 감소시킨다.
- `CacheUpdateService`가 전량 매도라면 종목 보유 포트폴리오 인덱스에서 제거하고 보유 종목 수를 감소시킨다.

포트폴리오 생성 이벤트:

- consumer는 유저 ID와 포트폴리오 ID를 포함해 `updateUserCache`를 호출한다.
- `CacheUpdateService`가 포트폴리오-유저 관계를 저장한다.
- `CacheUpdateService`가 유저 포트폴리오 목록에 포트폴리오를 추가한다.
- `CacheUpdateService`가 유저 포트폴리오 수를 증가시킨다.

포트폴리오 삭제 이벤트:

- consumer는 유저 ID와 포트폴리오 ID를 포함해 `updateUserCache`를 호출한다.
- `CacheUpdateService`가 포트폴리오-유저 관계를 제거한다.
- `CacheUpdateService`가 유저 포트폴리오 목록에서 포트폴리오를 제거한다.
- `CacheUpdateService`가 유저 구매액과 포트폴리오 수를 감소시킨다.
- `CacheUpdateService`가 삭제된 포트폴리오의 종목 보유 상태와 집계 상태를 정리한다.

## 현재 누락 또는 결정 필요 사항

현재 결정된 방향은 다음과 같다.

- 종목 매수/매도 시 `CacheUpdateService`가 유저 총 구매액도 즉시 갱신한다.
- 종목 매수/매도 시 `CacheUpdateService`가 포트폴리오/유저 valuation snapshot을 저장하고 dirty 대상으로 표시한다.
- 포트폴리오 삭제 시 `CacheUpdateService`가 포트폴리오 종목별 보유 상태와 집계 상태를 즉시 정리한다.
- Kafka inbound adapter는 `CacheUpdateEventConsumer`와 `StockPriceEventConsumer`로 구현한다.
- 유저 현재 평가액은 우선 포트폴리오 평가액 합산 방식으로 계산한다.
- Redis 원자성은 현재 Java adapter 구조를 유지하고, 추후 `calculateCurrentValue`부터 Lua script 적용을 검토한다.
- batch sync 최신성 기준은 우선 `updatedAt` 도입을 권장하고, 필요 시 event version 또는 snapshot version으로 전환한다.
- DB write-back은 외부 batch service가 dirty set 기반으로 수행한다. 구현 기준은 `docs/write-back-batch-guide.md`를 따른다.

## 주의 사항

- 수량이 남아 있는 부분 매도에서는 종목 보유 포트폴리오 인덱스를 제거하면 안 된다.
- 주가 변경 delta 계산은 포트폴리오 종목 보유 수량과 종목별 현재 평가액이 모두 있어야 정확하다.
- dirty 목록은 DB write-back 성공 여부와 함께 관리해야 한다.
- Redis 데이터가 유실되면 DB 또는 원천 이벤트로 hydration할 수 있는 별도 절차가 필요하다.
- batch sync와 event sync가 같은 데이터를 동시에 갱신할 수 있으므로 최신성 판단 기준이 필요하다.
