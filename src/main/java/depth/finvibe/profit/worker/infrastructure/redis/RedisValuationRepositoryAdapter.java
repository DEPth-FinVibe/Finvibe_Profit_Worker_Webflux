package depth.finvibe.profit.worker.infrastructure.redis;

import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class RedisValuationRepositoryAdapter implements ValuationRepository {

    private final ReactiveStringRedisTemplate redisTemplate;
    private final ProfitWorkerMetrics metrics;

    @Override
    public Mono<Void> savePortfolioValuation(PortfolioValuation valuation) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

        Long portfolioId = valuation.getPortfolioId();
        Instant updatedAt = Instant.now();

        return hashPutAll(portfolioHashKey(portfolioId), Map.of(
                "pv", String.valueOf(valuation.getPurchasedValue()),
                "cv", String.valueOf(valuation.getCurrentValue()),
                "pr", String.valueOf(valuation.getProfitRate()),
                "ac", String.valueOf(valuation.getAssetCount()),
                "del", "0",
                "ua", updatedAt.toString()
        ))
                .then(setAdd(dirtyPortfolioValuationsKey(), String.valueOf(portfolioId)))
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisDuration(
                        ProfitWorkerMetrics.OPERATION_PORTFOLIO_VALUATION_SAVE,
                        result[0],
                        sample));
    }

    @Override
    public Mono<Void> markPortfolioValuationDeleted(Long portfolioId) {
        Instant deletedAt = Instant.now();
        return hashPutAll(portfolioHashKey(portfolioId), Map.of(
                "del", "1",
                "da", deletedAt.toString()
        ))
                .then(setAdd(dirtyPortfolioValuationDeletionsKey(), String.valueOf(portfolioId)));
    }

    @Override
    public Mono<Void> saveUserValuation(UserValuation valuation) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

        String userId = valuation.getUserId();
        Instant updatedAt = Instant.now();

        return hashPutAll(userHashKey(userId), Map.of(
                "pv", String.valueOf(valuation.getPurchasedValue()),
                "cv", String.valueOf(valuation.getCurrentValue()),
                "pr", String.valueOf(valuation.getProfitRate()),
                "pc", String.valueOf(valuation.getPortfolioCount()),
                "ua", updatedAt.toString()
        ))
                .then(setAdd(dirtyUserValuationsKey(), userId))
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisDuration(
                        ProfitWorkerMetrics.OPERATION_USER_VALUATION_SAVE,
                        result[0],
                        sample));
    }

    @Override
    public Mono<Void> bulkSavePortfolioValuations(List<PortfolioValuation> valuations) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        String updatedAtStr = Instant.now().toString();
        return Flux.fromIterable(valuations)
                .flatMap(valuation -> hashPutAll(portfolioHashKey(valuation.getPortfolioId()), Map.of(
                                "pv", String.valueOf(valuation.getPurchasedValue()),
                                "cv", String.valueOf(valuation.getCurrentValue()),
                                "pr", String.valueOf(valuation.getProfitRate()),
                                "ac", String.valueOf(valuation.getAssetCount()),
                                "del", "0",
                                "ua", updatedAtStr
                        ))
                        .then(setAdd(dirtyPortfolioValuationsKey(), String.valueOf(valuation.getPortfolioId()))), 128)
                .then()
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_save_portfolio_valuations",
                        result[0],
                        sample));
    }

    @Override
    public Mono<Void> bulkSaveUserValuations(List<UserValuation> valuations) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        String updatedAtStr = Instant.now().toString();
        return Flux.fromIterable(valuations)
                .flatMap(valuation -> hashPutAll(userHashKey(valuation.getUserId()), Map.of(
                                "pv", String.valueOf(valuation.getPurchasedValue()),
                                "cv", String.valueOf(valuation.getCurrentValue()),
                                "pr", String.valueOf(valuation.getProfitRate()),
                                "pc", String.valueOf(valuation.getPortfolioCount()),
                                "ua", updatedAtStr
                        ))
                        .then(setAdd(dirtyUserValuationsKey(), valuation.getUserId())), 128)
                .then()
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .doFinally(ignored -> metrics.recordRedisCommandDuration(
                        "reactive_save_user_valuations",
                        result[0],
                        sample));
    }

    private Mono<Void> hashPutAll(String key, Map<String, String> values) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        return redisTemplate.opsForHash()
                .putAll(key, values)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .then()
                .doFinally(ignored -> metrics.recordRedisCommandDuration("hash_put_all", result[0], sample));
    }

    private Mono<Void> setAdd(String key, String member) {
        Timer.Sample sample = metrics.startSample();
        String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
        return redisTemplate.opsForSet()
                .add(key, member)
                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                .then()
                .doFinally(ignored -> metrics.recordRedisCommandDuration("set_add", result[0], sample));
    }

    private String dirtyPortfolioValuationsKey() {
        return "dirty:portfolio-valuations";
    }

    private String dirtyPortfolioValuationDeletionsKey() {
        return "dirty:portfolio-valuation-deletions";
    }

    private String dirtyUserValuationsKey() {
        return "dirty:user-valuations";
    }

    private String portfolioHashKey(Long portfolioId) {
        return "pf:" + portfolioId;
    }

    private String userHashKey(String userId) {
        return "usr:" + userId;
    }
}
