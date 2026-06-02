package depth.finvibe.profit.worker.infrastructure.redis;

import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class RedisPortfolioStateStore implements PortfolioStateStore {

    private static final String PRECISE_CURRENT_VALUE_FIELD = "cvp";

    private final ReactiveStringRedisTemplate redisTemplate;
    private final ProfitWorkerMetrics metrics;

    @Override
    public Mono<List<Long>> findPortfolioIdsByStockId(Long stockId) {
        return redisTemplate.opsForSet()
                .members(stockPortfoliosKey(stockId))
                .map(Long::valueOf)
                .collectList();
    }

    @Override
    public Mono<Long> findPurchasedValue(Long portfolioId) {
        return getHashLong(portfolioHashKey(portfolioId), "pv");
    }

    @Override
    public Mono<BigDecimal> findCurrentValue(Long portfolioId) {
        return getPortfolioCurrentValue(portfolioId);
    }

    @Override
    public Mono<BigDecimal> calculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice) {
        return recalculateCurrentValue(portfolioId, changedStockId, newPrice).map(PortfolioCurrentValueUpdate::currentValue);
    }

    @Override
    public Mono<PortfolioCurrentValueUpdate> recalculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

        return getDecimal(portfolioStockQuantityKey(portfolioId, changedStockId))
                .flatMap(quantity -> {
                    if (quantity.signum() == 0) {
                        return getPortfolioCurrentValue(portfolioId)
                                .map(currentValue -> new PortfolioCurrentValueUpdate(currentValue, currentValue, BigDecimal.ZERO));
                    }

                    String stockCurrentValueKey = portfolioStockCurrentValueKey(portfolioId, changedStockId);
                    return getDecimal(stockCurrentValueKey)
                            .flatMap(oldStockCurrentValue -> {
                                BigDecimal newStockCurrentValue = BigDecimal.valueOf(newPrice).multiply(quantity);
                                BigDecimal delta = newStockCurrentValue.subtract(oldStockCurrentValue);
                                return hashIncrementFloat(portfolioHashKey(portfolioId), PRECISE_CURRENT_VALUE_FIELD, delta.doubleValue())
                                        .map(BigDecimal::valueOf)
                                        .flatMap(nextPortfolioCurrentValue -> setDecimal(stockCurrentValueKey, newStockCurrentValue)
                                                .thenReturn(new PortfolioCurrentValueUpdate(
                                                        nextPortfolioCurrentValue.subtract(delta),
                                                        nextPortfolioCurrentValue,
                                                        delta)));
                            });
                })
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisDuration(
                        ProfitWorkerMetrics.OPERATION_PORTFOLIO_CURRENT_VALUE,
                        result[0],
                        sample));
    }

    @Override
    public Mono<Long> findAssetCount(Long portfolioId) {
        return getHashLong(portfolioHashKey(portfolioId), "ac");
    }

    @Override
    public Mono<Boolean> increaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity) {
        String quantityKey = portfolioStockQuantityKey(portfolioId, stockId);
        return getDecimal(quantityKey)
                .flatMap(previousQuantity -> setDecimal(quantityKey, previousQuantity.add(quantity))
                        .then(redisTemplate.opsForSet().add(stockPortfoliosKey(stockId), String.valueOf(portfolioId)))
                        .then(redisTemplate.opsForSet().add(portfolioStocksKey(portfolioId), String.valueOf(stockId)))
                        .thenReturn(previousQuantity.signum() == 0));
    }

    @Override
    public Mono<Boolean> decreaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity) {
        String quantityKey = portfolioStockQuantityKey(portfolioId, stockId);
        return getDecimal(quantityKey)
                .flatMap(currentQuantity -> {
                    BigDecimal nextQuantity = currentQuantity.subtract(quantity);
                    if (nextQuantity.signum() > 0) {
                        return setDecimal(quantityKey, nextQuantity).thenReturn(false);
                    }

                    return redisTemplate.delete(quantityKey)
                            .then(redisTemplate.opsForSet().remove(stockPortfoliosKey(stockId), String.valueOf(portfolioId)))
                            .then(redisTemplate.opsForSet().remove(portfolioStocksKey(portfolioId), String.valueOf(stockId)))
                            .thenReturn(true);
                });
    }

    @Override
    public Mono<Long> addPurchasedValue(Long portfolioId, Long amount) {
        return incrementHash(portfolioHashKey(portfolioId), "pv", amount);
    }

    @Override
    public Mono<Long> subtractPurchasedValue(Long portfolioId, Long amount) {
        return incrementHash(portfolioHashKey(portfolioId), "pv", -amount);
    }

    @Override
    public Mono<BigDecimal> addCurrentValue(Long portfolioId, BigDecimal amount) {
        return hashIncrementFloat(portfolioHashKey(portfolioId), PRECISE_CURRENT_VALUE_FIELD, amount.doubleValue())
                .map(BigDecimal::valueOf);
    }

    @Override
    public Mono<BigDecimal> subtractCurrentValue(Long portfolioId, BigDecimal amount) {
        return hashIncrementFloat(portfolioHashKey(portfolioId), PRECISE_CURRENT_VALUE_FIELD, -amount.doubleValue())
                .map(BigDecimal::valueOf);
    }

    @Override
    public Mono<Void> addStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount) {
        String key = portfolioStockCurrentValueKey(portfolioId, stockId);
        return getDecimal(key)
                .flatMap(currentValue -> setDecimal(key, currentValue.add(amount)))
                .then();
    }

    @Override
    public Mono<Void> subtractStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount) {
        String key = portfolioStockCurrentValueKey(portfolioId, stockId);
        return getDecimal(key)
                .flatMap(currentValue -> {
                    BigDecimal nextValue = currentValue.subtract(amount);
                    if (nextValue.signum() <= 0) {
                        return redisTemplate.delete(key).then();
                    }
                    return setDecimal(key, nextValue);
                });
    }

    @Override
    public Mono<Long> increaseAssetCount(Long portfolioId) {
        return incrementHash(portfolioHashKey(portfolioId), "ac", 1L);
    }

    @Override
    public Mono<Long> decreaseAssetCount(Long portfolioId) {
        return incrementHash(portfolioHashKey(portfolioId), "ac", -1L);
    }

    @Override
    public Mono<Void> deletePortfolioState(Long portfolioId) {
        return redisTemplate.opsForSet()
                .members(portfolioStocksKey(portfolioId))
                .flatMap(stockId -> redisTemplate.delete(
                                portfolioStockQuantityKey(portfolioId, Long.valueOf(stockId)),
                                portfolioStockCurrentValueKey(portfolioId, Long.valueOf(stockId)))
                        .then(redisTemplate.opsForSet().remove(stockPortfoliosKey(Long.valueOf(stockId)), String.valueOf(portfolioId))))
                .then(redisTemplate.delete(portfolioStocksKey(portfolioId), portfolioHashKey(portfolioId)))
                .then();
    }

    @Override
    public Mono<Map<Long, PortfolioMetadata>> bulkFetchPortfolioMetadata(List<Long> portfolioIds) {
        Timer.Sample sample = metrics.startSample();
        return Flux.fromIterable(portfolioIds)
                .flatMap(portfolioId -> redisTemplate.opsForHash()
                        .multiGet(portfolioHashKey(portfolioId), List.of("pv", "ac", "u", "cvp", "cv"))
                        .map(fields -> Map.entry(portfolioId, parsePortfolioMetadata(fields))), 64)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_hmget_portfolio",
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        sample));
    }

    @Override
    public Mono<Map<String, StockHolding>> bulkFetchStockHoldings(List<StockHoldingKey> tasks) {
        Timer.Sample sample = metrics.startSample();
        return Flux.fromIterable(tasks)
                .flatMap(task -> Mono.zip(
                                getDecimal(portfolioStockQuantityKey(task.portfolioId(), task.stockId())),
                                getDecimal(portfolioStockCurrentValueKey(task.portfolioId(), task.stockId()))
                        )
                        .map(tuple -> Map.entry(task.toKey(), new StockHolding(tuple.getT1(), tuple.getT2()))), 128)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_get_stock_holdings",
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        sample));
    }

    @Override
    public Mono<Void> bulkSetStockCurrentValues(Map<String, BigDecimal> updates) {
        Timer.Sample sample = metrics.startSample();
        return Flux.fromIterable(updates.entrySet())
                .flatMap(entry -> redisTemplate.opsForValue().set(entry.getKey(), toPlainString(entry.getValue())), 128)
                .then()
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_set_stock_cvs",
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        sample));
    }

    @Override
    public Mono<Map<Long, BigDecimal>> bulkIncrementCurrentValues(Map<Long, BigDecimal> deltasByPortfolioId) {
        Timer.Sample sample = metrics.startSample();
        return Flux.fromIterable(deltasByPortfolioId.entrySet())
                .flatMap(entry -> hashIncrementFloat(
                                portfolioHashKey(entry.getKey()),
                                PRECISE_CURRENT_VALUE_FIELD,
                                entry.getValue().doubleValue())
                        .map(BigDecimal::valueOf)
                        .map(value -> Map.entry(entry.getKey(), value)), 128)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_hincrbyfloat_portfolio_cv",
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        sample));
    }

    @Override
    public String stockCurrentValueKey(Long portfolioId, Long stockId) {
        return portfolioStockCurrentValueKey(portfolioId, stockId);
    }

    private PortfolioMetadata parsePortfolioMetadata(List<Object> fields) {
        Long pv = parseNullableLong(fields.get(0));
        Long ac = parseNullableLong(fields.get(1));
        String userId = parseNullableString(fields.get(2));
        BigDecimal cv;
        if (fields.get(3) != null) {
            cv = new BigDecimal(fields.get(3).toString());
        } else if (fields.get(4) != null) {
            cv = BigDecimal.valueOf(Long.parseLong(fields.get(4).toString()));
        } else {
            cv = BigDecimal.ZERO;
        }
        return new PortfolioMetadata(pv, ac, userId, cv);
    }

    private Long parseNullableLong(Object value) {
        if (value == null) {
            return 0L;
        }
        return Long.valueOf(value.toString());
    }

    private String parseNullableString(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    private Mono<Long> getHashLong(String key, String field) {
        return hashGet(key, field).map(Long::valueOf).defaultIfEmpty(0L);
    }

    private Mono<BigDecimal> getDecimal(String key) {
        return valueGet(key).map(BigDecimal::new).defaultIfEmpty(BigDecimal.ZERO);
    }

    private Mono<Long> incrementHash(String key, String field, Long delta) {
        return hashIncrement(key, field, delta).defaultIfEmpty(0L);
    }

    private Mono<BigDecimal> getPortfolioCurrentValue(Long portfolioId) {
        String key = portfolioHashKey(portfolioId);
        return hashGet(key, PRECISE_CURRENT_VALUE_FIELD)
                .map(BigDecimal::new)
                .switchIfEmpty(getHashLong(key, "cv").map(BigDecimal::valueOf));
    }

    private Mono<Void> setDecimal(String key, BigDecimal value) {
        return redisTemplate.opsForValue().set(key, toPlainString(value)).then();
    }

    private Mono<String> hashGet(String key, String field) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        return redisTemplate.opsForHash()
                .get(key, field)
                .cast(String.class)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration("hash_get", result[0], sample));
    }

    private Mono<Long> hashIncrement(String key, String field, Long delta) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        return redisTemplate.opsForHash()
                .increment(key, field, delta)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration("hash_increment", result[0], sample));
    }

    private Mono<Double> hashIncrementFloat(String key, String field, double delta) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        return redisTemplate.opsForHash()
                .increment(key, field, delta)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration("hash_increment_float", result[0], sample));
    }

    private Mono<String> valueGet(String key) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        return redisTemplate.opsForValue()
                .get(key)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration("value_get", result[0], sample));
    }

    private String stockPortfoliosKey(Long stockId) {
        return "stock:" + stockId + ":portfolios";
    }

    private String portfolioStocksKey(Long portfolioId) {
        return "portfolio:" + portfolioId + ":stocks";
    }

    private String portfolioHashKey(Long portfolioId) {
        return "pf:" + portfolioId;
    }

    private String portfolioStockQuantityKey(Long portfolioId, Long stockId) {
        return "portfolio:" + portfolioId + ":stock:" + stockId + ":quantity";
    }

    private String portfolioStockCurrentValueKey(Long portfolioId, Long stockId) {
        return "portfolio:" + portfolioId + ":stock:" + stockId + ":current-value";
    }

    private String toPlainString(BigDecimal value) {
        return value.stripTrailingZeros().toPlainString();
    }
}
