package depth.finvibe.profit.worker.application;

import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import depth.finvibe.profit.worker.dto.ProfitCalculationDto;
import depth.finvibe.profit.worker.support.TestMetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class ProfitCalculateServiceTest {

    @Test
    void updatesPortfolioAndUserValuationByStockPriceChange() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        FakeValuationRepository valuationRepository = new FakeValuationRepository();
        FakePortfolioStateStore portfolioStateStore = new FakePortfolioStateStore();
        FakeUserStateStore userStateStore = new FakeUserStateStore();
        ProfitCalculateService service = new ProfitCalculateService(
                portfolioStateStore,
                userStateStore,
                valuationRepository,
                fixture.metrics()
        );

        portfolioStateStore.portfolioIdsByStockId.put(10L, List.of(1L, 2L));
        portfolioStateStore.portfolioMetadata.put(1L, new PortfolioStateStore.PortfolioMetadata(1_000L, 2L, "100", new BigDecimal("1000")));
        portfolioStateStore.portfolioMetadata.put(2L, new PortfolioStateStore.PortfolioMetadata(1_000L, 1L, "100", new BigDecimal("1000")));
        portfolioStateStore.stockHoldings.put("1:10", new PortfolioStateStore.StockHolding(BigDecimal.TEN, new BigDecimal("1000")));
        portfolioStateStore.stockHoldings.put("2:10", new PortfolioStateStore.StockHolding(new BigDecimal("2"), new BigDecimal("600")));
        userStateStore.userMetadata.put("100", new UserStateStore.UserMetadata(2_000L, 2L));
        userStateStore.currentValues.put("100", new BigDecimal("2000"));

        service.updateProfitByStockPriceChange(ProfitCalculationDto.ProfitCalculationRequest.builder()
                .stockId(10L)
                .newPrice(150L)
                .build()).block();

        PortfolioValuation firstPortfolio = valuationRepository.portfolioValuations.get(1L);
        assertThat(firstPortfolio.getCurrentValue()).isEqualTo(1_500L);
        assertThat(firstPortfolio.getProfitRate()).isEqualTo(50.0);

        PortfolioValuation secondPortfolio = valuationRepository.portfolioValuations.get(2L);
        assertThat(secondPortfolio.getCurrentValue()).isEqualTo(700L);
        assertThat(secondPortfolio.getProfitRate()).isEqualTo(-30.0);

        UserValuation userValuation = valuationRepository.userValuations.get("100");
        assertThat(userValuation.getCurrentValue()).isEqualTo(2_200L);
        assertThat(userValuation.getProfitRate()).isEqualTo(10.0);
        assertThat(registry.find(ProfitWorkerMetrics.AFFECTED_PORTFOLIOS)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION)
                .summary().totalAmount()).isEqualTo(2.0);
    }

    @Test
    void skipsWhenNoPortfolioIsAffected() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        FakeValuationRepository valuationRepository = new FakeValuationRepository();
        FakePortfolioStateStore portfolioStateStore = new FakePortfolioStateStore();
        FakeUserStateStore userStateStore = new FakeUserStateStore();
        ProfitCalculateService service = new ProfitCalculateService(
                portfolioStateStore,
                userStateStore,
                valuationRepository,
                fixture.metrics()
        );

        portfolioStateStore.portfolioIdsByStockId.put(999L, List.of());

        service.updateProfitByStockPriceChange(ProfitCalculationDto.ProfitCalculationRequest.builder()
                .stockId(999L)
                .newPrice(150L)
                .build()).block();

        assertThat(valuationRepository.portfolioValuations).isEmpty();
        assertThat(valuationRepository.userValuations).isEmpty();
    }

    private static class FakePortfolioStateStore implements PortfolioStateStore {
        final Map<Long, List<Long>> portfolioIdsByStockId = new HashMap<>();
        final Map<Long, PortfolioMetadata> portfolioMetadata = new HashMap<>();
        final Map<String, StockHolding> stockHoldings = new HashMap<>();
        final Map<Long, BigDecimal> currentValues = new HashMap<>();

        @Override
        public Mono<List<Long>> findPortfolioIdsByStockId(Long stockId) {
            return Mono.just(portfolioIdsByStockId.getOrDefault(stockId, List.of()));
        }

        @Override
        public Mono<Map<Long, List<Long>>> bulkFindPortfolioIdsByStockIds(List<Long> stockIds) {
            Map<Long, List<Long>> result = new HashMap<>();
            for (Long stockId : stockIds) {
                result.put(stockId, portfolioIdsByStockId.getOrDefault(stockId, List.of()));
            }
            return Mono.just(result);
        }

        @Override
        public Mono<Long> findPurchasedValue(Long portfolioId) {
            PortfolioMetadata meta = portfolioMetadata.get(portfolioId);
            return Mono.just(meta != null ? meta.purchasedValue() : 0L);
        }

        @Override
        public Mono<BigDecimal> findCurrentValue(Long portfolioId) {
            return Mono.just(currentValues.getOrDefault(portfolioId, BigDecimal.ZERO));
        }

        @Override
        public Mono<BigDecimal> calculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice) {
            return findCurrentValue(portfolioId);
        }

        @Override
        public Mono<PortfolioCurrentValueUpdate> recalculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice) {
            return findCurrentValue(portfolioId).map(cv -> new PortfolioCurrentValueUpdate(cv, cv, BigDecimal.ZERO));
        }

        @Override
        public Mono<Long> findAssetCount(Long portfolioId) {
            PortfolioMetadata meta = portfolioMetadata.get(portfolioId);
            return Mono.just(meta != null ? meta.assetCount() : 0L);
        }

        @Override
        public Mono<Map<Long, PortfolioMetadata>> bulkFetchPortfolioMetadata(List<Long> portfolioIds) {
            Map<Long, PortfolioMetadata> result = new HashMap<>();
            for (Long id : portfolioIds) {
                PortfolioMetadata meta = portfolioMetadata.get(id);
                if (meta != null) {
                    result.put(id, meta);
                }
            }
            return Mono.just(result);
        }

        @Override
        public Mono<Map<String, StockHolding>> bulkFetchStockHoldings(List<StockHoldingKey> tasks) {
            Map<String, StockHolding> result = new HashMap<>();
            for (StockHoldingKey key : tasks) {
                StockHolding holding = stockHoldings.get(key.toKey());
                if (holding != null) {
                    result.put(key.toKey(), holding);
                }
            }
            return Mono.just(result);
        }

        @Override
        public Mono<Map<Long, BigDecimal>> bulkIncrementCurrentValues(Map<Long, BigDecimal> deltasByPortfolioId) {
            Map<Long, BigDecimal> result = new HashMap<>();
            for (var entry : deltasByPortfolioId.entrySet()) {
                Long portfolioId = entry.getKey();
                BigDecimal oldCV = portfolioMetadata.getOrDefault(
                        portfolioId,
                        new PortfolioMetadata(0L, 0L, null, BigDecimal.ZERO)
                ).currentValue();
                BigDecimal newCV = oldCV.add(entry.getValue());
                currentValues.put(portfolioId, newCV);
                result.put(portfolioId, newCV);
            }
            return Mono.just(result);
        }

        @Override
        public String stockCurrentValueKey(Long portfolioId, Long stockId) {
            return "portfolio:" + portfolioId + ":stock:" + stockId + ":current-value";
        }

        @Override
        public Mono<Void> bulkSetStockCurrentValues(Map<String, BigDecimal> updates) {
            return Mono.empty();
        }

        @Override public Mono<Boolean> increaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity) { return unsupported(); }
        @Override public Mono<Boolean> decreaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity) { return unsupported(); }
        @Override public Mono<Long> addPurchasedValue(Long portfolioId, Long amount) { return unsupported(); }
        @Override public Mono<Long> subtractPurchasedValue(Long portfolioId, Long amount) { return unsupported(); }
        @Override public Mono<BigDecimal> addCurrentValue(Long portfolioId, BigDecimal amount) { return unsupported(); }
        @Override public Mono<BigDecimal> subtractCurrentValue(Long portfolioId, BigDecimal amount) { return unsupported(); }
        @Override public Mono<Void> addStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount) { return unsupported(); }
        @Override public Mono<Void> subtractStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount) { return unsupported(); }
        @Override public Mono<Long> increaseAssetCount(Long portfolioId) { return unsupported(); }
        @Override public Mono<Long> decreaseAssetCount(Long portfolioId) { return unsupported(); }
        @Override public Mono<Void> deletePortfolioState(Long portfolioId) { return unsupported(); }
    }

    private static class FakeUserStateStore implements UserStateStore {
        final Map<String, UserMetadata> userMetadata = new HashMap<>();
        final Map<String, BigDecimal> currentValues = new HashMap<>();

        @Override public Mono<String> findUserIdByPortfolioId(Long portfolioId) { return unsupported(); }
        @Override public Mono<Long> findPurchasedValue(String userId) { return Mono.just(userMetadata.getOrDefault(userId, new UserMetadata(0L, 0L)).purchasedValue()); }
        @Override public Mono<BigDecimal> calculateCurrentValue(String userId) { return Mono.just(currentValues.getOrDefault(userId, BigDecimal.ZERO)); }
        @Override public Mono<BigDecimal> findCurrentValue(String userId) { return calculateCurrentValue(userId); }
        @Override public Mono<BigDecimal> addCurrentValue(String userId, BigDecimal delta) { return unsupported(); }
        @Override public Mono<Long> findPortfolioCount(String userId) { return Mono.just(userMetadata.getOrDefault(userId, new UserMetadata(0L, 0L)).portfolioCount()); }
        @Override public Mono<Map<String, UserMetadata>> bulkFetchUserMetadata(List<String> userIds) { return Mono.just(userMetadata); }
        @Override public Mono<Map<String, BigDecimal>> bulkIncrementCurrentValues(Map<String, BigDecimal> deltasByUserId) {
            Map<String, BigDecimal> result = new HashMap<>();
            for (var entry : deltasByUserId.entrySet()) {
                BigDecimal next = currentValues.getOrDefault(entry.getKey(), BigDecimal.ZERO).add(entry.getValue());
                currentValues.put(entry.getKey(), next);
                result.put(entry.getKey(), next);
            }
            return Mono.just(result);
        }
        @Override public Mono<Void> mapPortfolioToUser(Long portfolioId, String userId) { return unsupported(); }
        @Override public Mono<Void> removePortfolioUserMapping(Long portfolioId) { return unsupported(); }
        @Override public Mono<Long> addPurchasedValue(String userId, Long amount) { return unsupported(); }
        @Override public Mono<Long> subtractPurchasedValue(String userId, Long amount) { return unsupported(); }
        @Override public Mono<Long> increasePortfolioCount(String userId) { return unsupported(); }
        @Override public Mono<Long> decreasePortfolioCount(String userId) { return unsupported(); }
    }

    private static class FakeValuationRepository implements ValuationRepository {
        final Map<Long, PortfolioValuation> portfolioValuations = new ConcurrentHashMap<>();
        final Map<String, UserValuation> userValuations = new ConcurrentHashMap<>();
        final List<UserValuation> savedUserValuations = new ArrayList<>();

        @Override
        public Mono<Void> savePortfolioValuation(PortfolioValuation valuation) {
            return Mono.fromRunnable(() -> portfolioValuations.put(valuation.getPortfolioId(), valuation));
        }

        @Override
        public Mono<Void> markPortfolioValuationDeleted(Long portfolioId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> saveUserValuation(UserValuation valuation) {
            return Mono.fromRunnable(() -> {
                userValuations.put(valuation.getUserId(), valuation);
                savedUserValuations.add(valuation);
            });
        }

        @Override
        public Mono<Void> bulkSavePortfolioValuations(List<PortfolioValuation> valuations) {
            return Mono.fromRunnable(() -> valuations.forEach(v -> portfolioValuations.put(v.getPortfolioId(), v)));
        }

        @Override
        public Mono<Void> bulkSaveUserValuations(List<UserValuation> valuations) {
            return Mono.fromRunnable(() -> valuations.forEach(v -> {
                userValuations.put(v.getUserId(), v);
                savedUserValuations.add(v);
            }));
        }
    }

    private static <T> Mono<T> unsupported() {
        return Mono.error(new UnsupportedOperationException());
    }
}
