package depth.finvibe.profit.worker.infrastructure.redis;

import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class RedisUserStateStore implements UserStateStore {

    private final ReactiveStringRedisTemplate redisTemplate;
    private final ProfitWorkerMetrics metrics;

    @Override
    public Mono<String> findUserIdByPortfolioId(Long portfolioId) {
        return hashGet(portfolioHashKey(portfolioId), "u");
    }

    @Override
    public Mono<Long> findPurchasedValue(String userId) {
        return getHashLong(userHashKey(userId), "pv");
    }

    @Override
    public Mono<BigDecimal> calculateCurrentValue(String userId) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

        return redisTemplate.opsForSet()
                .members(userPortfoliosKey(userId))
                .map(Long::valueOf)
                .flatMap(this::getPortfolioCurrentValue, 64)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisDuration(
                        ProfitWorkerMetrics.OPERATION_USER_CURRENT_VALUE,
                        result[0],
                        sample));
    }

    @Override
    public Mono<BigDecimal> findCurrentValue(String userId) {
        return getUserCurrentValue(userId);
    }

    @Override
    public Mono<BigDecimal> addCurrentValue(String userId, BigDecimal delta) {
        return hashIncrementFloat(userHashKey(userId), "cvp", delta.doubleValue()).map(BigDecimal::valueOf);
    }

    @Override
    public Mono<Long> findPortfolioCount(String userId) {
        return getHashLong(userHashKey(userId), "pc");
    }

    @Override
    public Mono<Void> mapPortfolioToUser(Long portfolioId, String userId) {
        return redisTemplate.opsForHash()
                .put(portfolioHashKey(portfolioId), "u", userId)
                .then(redisTemplate.opsForSet().add(userPortfoliosKey(userId), String.valueOf(portfolioId)))
                .then();
    }

    @Override
    public Mono<Void> removePortfolioUserMapping(Long portfolioId) {
        return findUserIdByPortfolioId(portfolioId)
                .flatMap(userId -> redisTemplate.opsForHash()
                        .remove(portfolioHashKey(portfolioId), "u")
                        .then(redisTemplate.opsForSet().remove(userPortfoliosKey(userId), String.valueOf(portfolioId))))
                .switchIfEmpty(redisTemplate.opsForHash().remove(portfolioHashKey(portfolioId), "u"))
                .then();
    }

    @Override
    public Mono<Long> addPurchasedValue(String userId, Long amount) {
        return incrementHash(userHashKey(userId), "pv", amount);
    }

    @Override
    public Mono<Long> subtractPurchasedValue(String userId, Long amount) {
        return incrementHash(userHashKey(userId), "pv", -amount);
    }

    @Override
    public Mono<Long> increasePortfolioCount(String userId) {
        return incrementHash(userHashKey(userId), "pc", 1L);
    }

    @Override
    public Mono<Long> decreasePortfolioCount(String userId) {
        return incrementHash(userHashKey(userId), "pc", -1L);
    }

    @Override
    public Mono<Map<String, UserMetadata>> bulkFetchUserMetadata(List<String> userIds) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

        return Flux.fromIterable(userIds)
                .flatMap(userId -> redisTemplate.opsForHash()
                        .multiGet(userHashKey(userId), List.of("pv", "pc"))
                        .map(fields -> Map.entry(userId, parseUserMetadata(fields))), 64)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_hmget_user",
                        result[0],
                        sample));
    }

    @Override
    public Mono<Map<String, BigDecimal>> bulkIncrementCurrentValues(Map<String, BigDecimal> deltasByUserId) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

        return Flux.fromIterable(deltasByUserId.entrySet())
                .flatMap(entry -> hashIncrementFloat(userHashKey(entry.getKey()), "cvp", entry.getValue().doubleValue())
                        .map(BigDecimal::valueOf)
                        .map(value -> Map.entry(entry.getKey(), value)), 128)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_hincrbyfloat_user_cv",
                        result[0],
                        sample));
    }

    private UserMetadata parseUserMetadata(List<Object> fields) {
        return new UserMetadata(parseNullableLong(fields.get(0)), parseNullableLong(fields.get(1)));
    }

    private Long parseNullableLong(Object value) {
        if (value == null) {
            return 0L;
        }
        return Long.valueOf(value.toString());
    }

    private Mono<Long> getHashLong(String key, String field) {
        return hashGet(key, field).map(Long::valueOf).defaultIfEmpty(0L);
    }

    private Mono<Long> incrementHash(String key, String field, Long delta) {
        return hashIncrement(key, field, delta).defaultIfEmpty(0L);
    }

    private Mono<BigDecimal> getPortfolioCurrentValue(Long portfolioId) {
        String key = portfolioHashKey(portfolioId);
        return hashGet(key, "cvp")
                .map(BigDecimal::new)
                .switchIfEmpty(getHashLong(key, "cv").map(BigDecimal::valueOf));
    }

    private Mono<BigDecimal> getUserCurrentValue(String userId) {
        String key = userHashKey(userId);
        return hashGet(key, "cvp")
                .map(BigDecimal::new)
                .switchIfEmpty(getHashLong(key, "cv").map(BigDecimal::valueOf));
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

    private String portfolioHashKey(Long portfolioId) {
        return "pf:" + portfolioId;
    }

    private String userHashKey(String userId) {
        return "usr:" + userId;
    }

    private String userPortfoliosKey(String userId) {
        return "user:" + userId + ":portfolios";
    }
}
