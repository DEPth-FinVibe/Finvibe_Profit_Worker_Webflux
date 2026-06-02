# Profit Worker WebFlux Renewal

## Existing worker shape

- The original Profit Worker is not a Spring MVC controller-oriented service. It is a Kafka event worker.
- Main flow:
  - stock price event -> affected portfolio lookup -> Redis valuation update -> dirty valuation set update
  - trade event -> portfolio/user cache update -> valuation snapshot update
  - portfolio owner event -> user mapping/cache update -> deletion or snapshot update
- Original blocking points:
  - `StringRedisTemplate` and `executePipelined`
  - `java.net.http.HttpClient`
  - JPA/QueryDSL dependencies that are not on the worker hot path
  - synchronous application ports returning `void`, `List`, or `Map`

## Renewal direction

- Keep the event-driven worker behavior and Redis key model.
- Make hot-path I/O reactive:
  - `WebClient` for monolith calls
  - `ReactiveStringRedisTemplate` for Redis state and dirty-set writes
  - `Mono`-based input/output ports
- Remove unused JPA/QueryDSL runtime surface from this worker.
- Keep Kafka listener entrypoints, but expose reactive `Mono<Void>` processing so Spring Kafka can treat listener completion asynchronously.

## Current implementation

- `build.gradle`
  - uses `spring-boot-starter-webflux`
  - removes JPA, QueryDSL, JDBC drivers
- `application*.yaml`
  - sets `spring.main.web-application-type=reactive`
  - removes datasource/JPA settings
- Application ports
  - `ProfitCalculationUseCase`, `CacheUpdateUseCase`, `PortfolioStateStore`, `UserStateStore`, `ValuationRepository`, `MonolithClient` now return `Mono`
- Infrastructure
  - Redis adapters use `ReactiveStringRedisTemplate`
  - Monolith client uses `WebClient`
- Tests
  - service and Kafka tests subscribe to reactive flows
  - Redis metrics tests mock reactive Redis operations
- Metrics
  - all Micrometer meters include the common tag `worker_runtime=webflux`
  - existing metric names and business tags are preserved for dashboard compatibility

## Remaining performance follow-ups

- If Kafka becomes the next bottleneck, evaluate replacing `@KafkaListener` with Reactor Kafka for end-to-end backpressure control.
- Benchmark Redis fan-out concurrency values in `flatMap(..., concurrency)` against production Redis latency and connection pool settings.
- Consider Lua scripts for multi-key trade update atomicity if strict consistency under concurrent trade events becomes more important than command-level parallelism.
