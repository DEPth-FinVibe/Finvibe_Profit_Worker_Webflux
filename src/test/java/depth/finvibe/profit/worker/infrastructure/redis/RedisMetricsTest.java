package depth.finvibe.profit.worker.infrastructure.redis;

import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import depth.finvibe.profit.worker.support.TestMetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisMetricsTest {

    @Test
    void recordsPortfolioCurrentValueMetric() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        ReactiveStringRedisTemplate redisTemplate = mock(ReactiveStringRedisTemplate.class);
        ReactiveValueOperations<String, String> valueOperations = mock(ReactiveValueOperations.class);
        ReactiveHashOperations<String, Object, Object> hashOperations = mock(ReactiveHashOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(valueOperations.get(anyString())).thenReturn(Mono.empty());
        when(hashOperations.get(anyString(), anyString())).thenReturn(Mono.just("100"));

        RedisPortfolioStateStore store = new RedisPortfolioStateStore(redisTemplate, fixture.metrics());

        assertThat(store.calculateCurrentValue(1L, 10L, 200L).block()).isEqualByComparingTo(BigDecimal.valueOf(100L));
        assertThat(registry.find(ProfitWorkerMetrics.REDIS_OPERATION_DURATION)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_PORTFOLIO_CURRENT_VALUE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SUCCESS)
                .timer().count()).isEqualTo(1);
    }

    @Test
    void recordsUserCurrentValueMetric() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        ReactiveStringRedisTemplate redisTemplate = mock(ReactiveStringRedisTemplate.class);
        ReactiveSetOperations<String, String> setOperations = mock(ReactiveSetOperations.class);

        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(setOperations.members(anyString())).thenReturn(Flux.empty());

        RedisUserStateStore store = new RedisUserStateStore(redisTemplate, fixture.metrics());

        assertThat(store.calculateCurrentValue("user-1").block()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(registry.find(ProfitWorkerMetrics.REDIS_OPERATION_DURATION)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_USER_CURRENT_VALUE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SUCCESS)
                .timer().count()).isEqualTo(1);
    }

    @Test
    void recordsPortfolioValuationSaveMetric() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        ReactiveStringRedisTemplate redisTemplate = valuationTemplate(Mono.just(true), Mono.just(1L));

        RedisValuationRepositoryAdapter repository = new RedisValuationRepositoryAdapter(redisTemplate, fixture.metrics());
        repository.savePortfolioValuation(PortfolioValuation.builder()
                .portfolioId(1L)
                .purchasedValue(100L)
                .currentValue(150L)
                .profitRate(50.0)
                .assetCount(2L)
                .build()).block();

        assertThat(registry.find(ProfitWorkerMetrics.REDIS_OPERATION_DURATION)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_PORTFOLIO_VALUATION_SAVE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SUCCESS)
                .timer().count()).isEqualTo(1);
    }

    @Test
    void recordsUserValuationSaveMetric() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        ReactiveStringRedisTemplate redisTemplate = valuationTemplate(Mono.just(true), Mono.just(1L));

        RedisValuationRepositoryAdapter repository = new RedisValuationRepositoryAdapter(redisTemplate, fixture.metrics());
        repository.saveUserValuation(UserValuation.builder()
                .userId("user-1")
                .purchasedValue(100L)
                .currentValue(150L)
                .profitRate(50.0)
                .portfolioCount(2L)
                .build()).block();

        assertThat(registry.find(ProfitWorkerMetrics.REDIS_OPERATION_DURATION)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_USER_VALUATION_SAVE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SUCCESS)
                .timer().count()).isEqualTo(1);
    }

    @Test
    void recordsPortfolioValuationFailureMetric() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        ReactiveStringRedisTemplate redisTemplate = valuationTemplate(Mono.error(new IllegalStateException("boom")), Mono.just(1L));
        RedisValuationRepositoryAdapter repository = new RedisValuationRepositoryAdapter(redisTemplate, fixture.metrics());

        assertThatThrownBy(() -> repository.savePortfolioValuation(PortfolioValuation.builder()
                .portfolioId(1L)
                .purchasedValue(100L)
                .currentValue(150L)
                .profitRate(50.0)
                .assetCount(2L)
                .build()).block()).isInstanceOf(IllegalStateException.class);

        assertThat(registry.find(ProfitWorkerMetrics.REDIS_OPERATION_DURATION)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_PORTFOLIO_VALUATION_SAVE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_FAILURE)
                .timer().count()).isEqualTo(1);
    }

    private ReactiveStringRedisTemplate valuationTemplate(Mono<Boolean> hashPutAllResult, Mono<Long> setAddResult) {
        ReactiveStringRedisTemplate redisTemplate = mock(ReactiveStringRedisTemplate.class);
        ReactiveHashOperations<String, Object, Object> hashOperations = mock(ReactiveHashOperations.class);
        ReactiveSetOperations<String, String> setOperations = mock(ReactiveSetOperations.class);

        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(hashOperations.putAll(anyString(), any(Map.class))).thenReturn(hashPutAllResult);
        when(setOperations.add(anyString(), anyString())).thenReturn(setAddResult);
        return redisTemplate;
    }
}
