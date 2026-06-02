package depth.finvibe.profit.worker.application;

import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import depth.finvibe.profit.worker.dto.CacheUpdateDto;
import depth.finvibe.profit.worker.support.TestMetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CacheUpdateServiceTest {

    @Test
    void updatesPortfolioCacheByStockBuy() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        FakePortfolioStateStore portfolioStateStore = new FakePortfolioStateStore();
        FakeUserStateStore userStateStore = new FakeUserStateStore(portfolioStateStore);
        userStateStore.userIdsByPortfolioId.put(1L, "100");
        userStateStore.portfolioIdsByUserId.put("100", new HashSet<>(Set.of(1L)));
        FakeValuationRepository valuationRepository = new FakeValuationRepository();
        CacheUpdateService service = new CacheUpdateService(portfolioStateStore, userStateStore, valuationRepository, fixture.metrics());

        service.updatePortfolioCache(CacheUpdateDto.PortfolioCacheUpdateRequest.builder()
                .portfolioId(1L)
                .stockId(10L)
                .type(CacheUpdateDto.PortfolioCacheUpdateRequest.TradeType.STOCK_BUY)
                .price(100L)
                .quantity(new BigDecimal("10.5"))
                .build()).block();

        assertThat(portfolioStateStore.stockQuantities.get("1:10")).isEqualByComparingTo("10.5");
        assertThat(portfolioStateStore.stockCurrentValues.get("1:10")).isEqualByComparingTo("1050");
        assertThat(portfolioStateStore.purchasedValues.get(1L)).isEqualTo(1_050L);
        assertThat(userStateStore.purchasedValues.get("100")).isEqualTo(1_050L);
        assertThat(valuationRepository.portfolioValuations.get(1L).getCurrentValue()).isEqualTo(1_050L);
        assertThat(valuationRepository.userValuations.get("100").getCurrentValue()).isEqualTo(1_050L);
        assertThat(registry.find(ProfitWorkerMetrics.AFFECTED_USERS)
                .tags(ProfitWorkerMetrics.TAG_OPERATION, ProfitWorkerMetrics.OPERATION_PORTFOLIO_CACHE_UPDATE)
                .summary().totalAmount()).isEqualTo(1.0);
    }

    @Test
    void updatesUserCacheByPortfolioCreatedAndDeleted() {
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        FakePortfolioStateStore portfolioStateStore = new FakePortfolioStateStore();
        portfolioStateStore.purchasedValues.put(1L, 1_000L);
        portfolioStateStore.currentValues.put(1L, new BigDecimal("1200.5"));
        portfolioStateStore.stockIdsByPortfolioId.put(1L, new HashSet<>(Set.of(10L)));
        portfolioStateStore.stockQuantities.put("1:10", BigDecimal.TEN);
        portfolioStateStore.stockCurrentValues.put("1:10", new BigDecimal("1200.5"));
        FakeUserStateStore userStateStore = new FakeUserStateStore(portfolioStateStore);
        FakeValuationRepository valuationRepository = new FakeValuationRepository();
        CacheUpdateService service = new CacheUpdateService(portfolioStateStore, userStateStore, valuationRepository, fixture.metrics());

        service.updateUserCache(CacheUpdateDto.UserCacheUpdateRequest.builder()
                .userId("100")
                .portfolioId(1L)
                .type(CacheUpdateDto.UserCacheUpdateRequest.ChangeType.CREATED)
                .build()).block();

        assertThat(userStateStore.userIdsByPortfolioId.get(1L)).isEqualTo("100");
        assertThat(userStateStore.purchasedValues.get("100")).isEqualTo(1_000L);
        assertThat(userStateStore.portfolioCounts.get("100")).isEqualTo(1L);
        assertThat(valuationRepository.userValuations.get("100").getCurrentValue()).isEqualTo(1_201L);

        service.updateUserCache(CacheUpdateDto.UserCacheUpdateRequest.builder()
                .userId("100")
                .portfolioId(1L)
                .type(CacheUpdateDto.UserCacheUpdateRequest.ChangeType.DELETED)
                .build()).block();

        assertThat(userStateStore.userIdsByPortfolioId).doesNotContainKey(1L);
        assertThat(userStateStore.purchasedValues.get("100")).isZero();
        assertThat(userStateStore.portfolioCounts.get("100")).isZero();
        assertThat(portfolioStateStore.purchasedValues).doesNotContainKey(1L);
        assertThat(valuationRepository.deletedPortfolioIds).contains(1L);
    }

    private static class FakePortfolioStateStore implements PortfolioStateStore {
        private final Map<Long, Set<Long>> stockIdsByPortfolioId = new HashMap<>();
        private final Map<String, BigDecimal> stockQuantities = new HashMap<>();
        private final Map<String, BigDecimal> stockCurrentValues = new HashMap<>();
        private final Map<Long, Long> purchasedValues = new HashMap<>();
        private final Map<Long, BigDecimal> currentValues = new HashMap<>();
        private final Map<Long, Long> assetCounts = new HashMap<>();

        @Override
        public Mono<List<Long>> findPortfolioIdsByStockId(Long stockId) {
            return Mono.just(stockIdsByPortfolioId.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(stockId))
                    .map(Map.Entry::getKey)
                    .toList());
        }

        @Override
        public Mono<Long> findPurchasedValue(Long portfolioId) {
            return Mono.just(purchasedValues.getOrDefault(portfolioId, 0L));
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
            return Mono.just(assetCounts.getOrDefault(portfolioId, 0L));
        }

        @Override
        public Mono<Boolean> increaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity) {
            String key = key(portfolioId, stockId);
            BigDecimal previous = stockQuantities.getOrDefault(key, BigDecimal.ZERO);
            stockQuantities.put(key, previous.add(quantity));
            stockIdsByPortfolioId.computeIfAbsent(portfolioId, ignored -> new HashSet<>()).add(stockId);
            return Mono.just(previous.signum() == 0);
        }

        @Override
        public Mono<Boolean> decreaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity) {
            String key = key(portfolioId, stockId);
            BigDecimal next = stockQuantities.getOrDefault(key, BigDecimal.ZERO).subtract(quantity);
            if (next.signum() > 0) {
                stockQuantities.put(key, next);
                return Mono.just(false);
            }
            stockQuantities.remove(key);
            stockIdsByPortfolioId.getOrDefault(portfolioId, Set.of()).remove(stockId);
            return Mono.just(true);
        }

        @Override public Mono<Long> addPurchasedValue(Long portfolioId, Long amount) { return addLong(purchasedValues, portfolioId, amount); }
        @Override public Mono<Long> subtractPurchasedValue(Long portfolioId, Long amount) { return addLong(purchasedValues, portfolioId, -amount); }
        @Override public Mono<BigDecimal> addCurrentValue(Long portfolioId, BigDecimal amount) { return addDecimal(currentValues, portfolioId, amount); }
        @Override public Mono<BigDecimal> subtractCurrentValue(Long portfolioId, BigDecimal amount) { return addDecimal(currentValues, portfolioId, amount.negate()); }

        @Override
        public Mono<Void> addStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount) {
            return addDecimal(stockCurrentValues, key(portfolioId, stockId), amount).then();
        }

        @Override
        public Mono<Void> subtractStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount) {
            return addDecimal(stockCurrentValues, key(portfolioId, stockId), amount.negate()).then();
        }

        @Override public Mono<Long> increaseAssetCount(Long portfolioId) { return addLong(assetCounts, portfolioId, 1L); }
        @Override public Mono<Long> decreaseAssetCount(Long portfolioId) { return addLong(assetCounts, portfolioId, -1L); }

        @Override
        public Mono<Void> deletePortfolioState(Long portfolioId) {
            Set<Long> stockIds = stockIdsByPortfolioId.remove(portfolioId);
            if (stockIds != null) {
                stockIds.forEach(stockId -> {
                    stockQuantities.remove(key(portfolioId, stockId));
                    stockCurrentValues.remove(key(portfolioId, stockId));
                });
            }
            purchasedValues.remove(portfolioId);
            currentValues.remove(portfolioId);
            assetCounts.remove(portfolioId);
            return Mono.empty();
        }

        @Override public Mono<Map<Long, PortfolioMetadata>> bulkFetchPortfolioMetadata(List<Long> portfolioIds) { return unsupported(); }
        @Override public Mono<Map<String, StockHolding>> bulkFetchStockHoldings(List<StockHoldingKey> tasks) { return unsupported(); }
        @Override public Mono<Void> bulkSetStockCurrentValues(Map<String, BigDecimal> updates) { return unsupported(); }
        @Override public Mono<Map<Long, BigDecimal>> bulkIncrementCurrentValues(Map<Long, BigDecimal> deltasByPortfolioId) { return unsupported(); }
        @Override public String stockCurrentValueKey(Long portfolioId, Long stockId) { return key(portfolioId, stockId) + ":current-value"; }

        private String key(Long portfolioId, Long stockId) {
            return portfolioId + ":" + stockId;
        }
    }

    private static class FakeUserStateStore implements UserStateStore {
        private final FakePortfolioStateStore portfolioStateStore;
        private final Map<Long, String> userIdsByPortfolioId = new HashMap<>();
        private final Map<String, Long> purchasedValues = new HashMap<>();
        private final Map<String, Long> portfolioCounts = new HashMap<>();
        private final Map<String, Set<Long>> portfolioIdsByUserId = new HashMap<>();

        FakeUserStateStore(FakePortfolioStateStore portfolioStateStore) {
            this.portfolioStateStore = portfolioStateStore;
        }

        @Override public Mono<String> findUserIdByPortfolioId(Long portfolioId) { return Mono.justOrEmpty(userIdsByPortfolioId.get(portfolioId)); }
        @Override public Mono<Long> findPurchasedValue(String userId) { return Mono.just(purchasedValues.getOrDefault(userId, 0L)); }
        @Override public Mono<BigDecimal> calculateCurrentValue(String userId) {
            BigDecimal sum = portfolioIdsByUserId.getOrDefault(userId, Set.of()).stream()
                    .map(portfolioId -> portfolioStateStore.currentValues.getOrDefault(portfolioId, BigDecimal.ZERO))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            return Mono.just(sum);
        }
        @Override public Mono<BigDecimal> findCurrentValue(String userId) { return calculateCurrentValue(userId); }
        @Override public Mono<BigDecimal> addCurrentValue(String userId, BigDecimal delta) { return unsupported(); }
        @Override public Mono<Long> findPortfolioCount(String userId) { return Mono.just(portfolioCounts.getOrDefault(userId, 0L)); }

        @Override
        public Mono<Void> mapPortfolioToUser(Long portfolioId, String userId) {
            userIdsByPortfolioId.put(portfolioId, userId);
            portfolioIdsByUserId.computeIfAbsent(userId, ignored -> new HashSet<>()).add(portfolioId);
            return Mono.empty();
        }

        @Override
        public Mono<Void> removePortfolioUserMapping(Long portfolioId) {
            String userId = userIdsByPortfolioId.remove(portfolioId);
            if (userId != null) {
                portfolioIdsByUserId.getOrDefault(userId, Set.of()).remove(portfolioId);
            }
            return Mono.empty();
        }

        @Override public Mono<Long> addPurchasedValue(String userId, Long amount) { return addLong(purchasedValues, userId, amount); }
        @Override public Mono<Long> subtractPurchasedValue(String userId, Long amount) { return addLong(purchasedValues, userId, -amount); }
        @Override public Mono<Long> increasePortfolioCount(String userId) { return addLong(portfolioCounts, userId, 1L); }
        @Override public Mono<Long> decreasePortfolioCount(String userId) { return addLong(portfolioCounts, userId, -1L); }
        @Override public Mono<Map<String, UserMetadata>> bulkFetchUserMetadata(List<String> userIds) { return unsupported(); }
        @Override public Mono<Map<String, BigDecimal>> bulkIncrementCurrentValues(Map<String, BigDecimal> deltasByUserId) { return unsupported(); }
    }

    private static class FakeValuationRepository implements ValuationRepository {
        private final Map<Long, PortfolioValuation> portfolioValuations = new HashMap<>();
        private final Map<String, UserValuation> userValuations = new HashMap<>();
        private final Set<Long> deletedPortfolioIds = new HashSet<>();

        @Override public Mono<Void> savePortfolioValuation(PortfolioValuation valuation) { portfolioValuations.put(valuation.getPortfolioId(), valuation); return Mono.empty(); }
        @Override public Mono<Void> markPortfolioValuationDeleted(Long portfolioId) { deletedPortfolioIds.add(portfolioId); return Mono.empty(); }
        @Override public Mono<Void> saveUserValuation(UserValuation valuation) { userValuations.put(valuation.getUserId(), valuation); return Mono.empty(); }
        @Override public Mono<Void> bulkSavePortfolioValuations(List<PortfolioValuation> valuations) { return unsupported(); }
        @Override public Mono<Void> bulkSaveUserValuations(List<UserValuation> valuations) { return unsupported(); }
    }

    private static <K> Mono<Long> addLong(Map<K, Long> values, K key, Long delta) {
        Long next = values.getOrDefault(key, 0L) + delta;
        values.put(key, next);
        return Mono.just(next);
    }

    private static <K> Mono<BigDecimal> addDecimal(Map<K, BigDecimal> values, K key, BigDecimal delta) {
        BigDecimal next = values.getOrDefault(key, BigDecimal.ZERO).add(delta);
        values.put(key, next);
        return Mono.just(next);
    }

    private static <T> Mono<T> unsupported() {
        return Mono.error(new UnsupportedOperationException());
    }
}
