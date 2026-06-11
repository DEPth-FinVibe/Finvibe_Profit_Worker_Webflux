package depth.finvibe.profit.worker.application;

import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import depth.finvibe.profit.worker.dto.ProfitCalculationDto;
import depth.finvibe.profit.worker.support.TestMetricsFactory;
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
        FakeValuationRepository valuationRepository = new FakeValuationRepository();
        FakePortfolioStateStore portfolioStateStore = new FakePortfolioStateStore();
        FakeUserStateStore userStateStore = new FakeUserStateStore();
        ProfitCalculateService service = new ProfitCalculateService(
                portfolioStateStore,
                userStateStore,
                valuationRepository,
                fixture.metrics()
        );

        // stock 10을 보유한 포트폴리오: 1, 2
        portfolioStateStore.portfolioIdsByStockId.put(10L, List.of(1L, 2L));

        // 포트폴리오 1: 주식10 수량 10, 기존 평가액 1000, 구매액 1000, 종목수 2, 유저 "100"
        portfolioStateStore.purchasedValues.put(1L, 1_000L);
        portfolioStateStore.assetCounts.put(1L, 2L);
        portfolioStateStore.quantities.put("1:10", BigDecimal.TEN);
        portfolioStateStore.stockCurrentValues.put("1:10", new BigDecimal("1000"));
        portfolioStateStore.portfolioCurrentValues.put(1L, new BigDecimal("1000"));

        // 포트폴리오 2: 주식10 수량 2, 기존 평가액 1000, 구매액 1000, 종목수 1, 유저 "100"
        portfolioStateStore.purchasedValues.put(2L, 1_000L);
        portfolioStateStore.assetCounts.put(2L, 1L);
        portfolioStateStore.quantities.put("2:10", new BigDecimal("2"));
        portfolioStateStore.stockCurrentValues.put("2:10", new BigDecimal("600"));
        portfolioStateStore.portfolioCurrentValues.put(2L, new BigDecimal("1000"));

        // 유저 "100": 포트폴리오 1, 2 소유
        userStateStore.userIdByPortfolioId.put(1L, "100");
        userStateStore.userIdByPortfolioId.put(2L, "100");
        userStateStore.currentValues.put("100", new BigDecimal("2000"));
        userStateStore.purchasedValues.put("100", 2_000L);
        userStateStore.portfolioCounts.put("100", 2L);

        service.updateProfitByStockPriceChange(ProfitCalculationDto.ProfitCalculationRequest.builder()
                .stockId(10L)
                .newPrice(150L)
                .build()).block();

        PortfolioValuation firstPortfolio = valuationRepository.portfolioValuations.get(1L);
        assertThat(firstPortfolio.getCurrentValue()).isEqualTo(1_500L);
        assertThat(firstPortfolio.getProfitRate()).isEqualTo(50.0);

        PortfolioValuation secondPortfolio = valuationRepository.portfolioValuations.get(2L);
        assertThat(secondPortfolio.getCurrentValue()).isEqualTo(700L);  // 1000 - 600 + 300 = 700? No: delta = 150*2 - 600 = -300, newCV = 1000 + (-300) = 700? Wait...
        // delta = 150*2 - 600 = -300. But we need to check: newPrice=150, quantity=2 → newStockCV = 300, oldStockCV = 600, delta = 300 - 600 = -300
        // portfolioCV = 1000 + (-300) = 700
        assertThat(secondPortfolio.getProfitRate()).isEqualTo(-30.0);

        UserValuation userValuation = valuationRepository.userValuations.get("100");
        // user delta = portfolio1 delta + portfolio2 delta = 500 + (-300) = 200
        // userCV = 2000 + 200 = 2200
        assertThat(userValuation.getCurrentValue()).isEqualTo(2_200L);
        assertThat(userValuation.getProfitRate()).isEqualTo(10.0);
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
        final Map<Long, Long> purchasedValues = new HashMap<>();
        final Map<Long, Long> assetCounts = new HashMap<>();
        final Map<Long, BigDecimal> portfolioCurrentValues = new ConcurrentHashMap<>();
        final Map<String, BigDecimal> quantities = new HashMap<>();
        final Map<String, BigDecimal> stockCurrentValues = new ConcurrentHashMap<>();

        @Override
        public Mono<List<Long>> findPortfolioIdsByStockId(Long stockId) {
            return Mono.just(portfolioIdsByStockId.getOrDefault(stockId, List.of()));
        }

        @Override
        public Mono<Long> findPurchasedValue(Long portfolioId) {
            return Mono.just(purchasedValues.getOrDefault(portfolioId, 0L));
        }

        @Override
        public Mono<BigDecimal> findCurrentValue(Long portfolioId) {
            return Mono.just(portfolioCurrentValues.getOrDefault(portfolioId, BigDecimal.ZERO));
        }

        @Override
        public Mono<BigDecimal> calculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice) {
            return recalculateCurrentValue(portfolioId, changedStockId, newPrice)
                    .map(PortfolioCurrentValueUpdate::currentValue);
        }

        @Override
        public Mono<PortfolioCurrentValueUpdate> recalculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice) {
            String key = portfolioId + ":" + changedStockId;
            BigDecimal quantity = quantities.getOrDefault(key, BigDecimal.ZERO);
            if (quantity.signum() == 0) {
                BigDecimal cv = portfolioCurrentValues.getOrDefault(portfolioId, BigDecimal.ZERO);
                return Mono.just(new PortfolioCurrentValueUpdate(cv, cv, BigDecimal.ZERO));
            }
            BigDecimal newStockCV = BigDecimal.valueOf(newPrice).multiply(quantity);
            BigDecimal oldStockCV = stockCurrentValues.getOrDefault(key, BigDecimal.ZERO);
            BigDecimal delta = newStockCV.subtract(oldStockCV);
            BigDecimal oldPortfolioCV = portfolioCurrentValues.getOrDefault(portfolioId, BigDecimal.ZERO);
            BigDecimal newPortfolioCV = oldPortfolioCV.add(delta);
            portfolioCurrentValues.put(portfolioId, newPortfolioCV);
            stockCurrentValues.put(key, newStockCV);
            return Mono.just(new PortfolioCurrentValueUpdate(oldPortfolioCV, newPortfolioCV, delta));
        }

        @Override
        public Mono<Long> findAssetCount(Long portfolioId) {
            return Mono.just(assetCounts.getOrDefault(portfolioId, 0L));
        }

        @Override
        public String stockCurrentValueKey(Long portfolioId, Long stockId) {
            return "portfolio:" + portfolioId + ":stock:" + stockId + ":current-value";
        }

        @Override public Mono<Map<Long, List<Long>>> bulkFindPortfolioIdsByStockIds(List<Long> stockIds) { return unsupported(); }
        @Override public Mono<Map<Long, PortfolioMetadata>> bulkFetchPortfolioMetadata(List<Long> portfolioIds) { return unsupported(); }
        @Override public Mono<Map<String, StockHolding>> bulkFetchStockHoldings(List<StockHoldingKey> tasks) { return unsupported(); }
        @Override public Mono<Map<Long, BigDecimal>> bulkIncrementCurrentValues(Map<Long, BigDecimal> deltasByPortfolioId) { return unsupported(); }
        @Override public Mono<Void> bulkSetStockCurrentValues(Map<String, BigDecimal> updates) { return unsupported(); }
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
        final Map<Long, String> userIdByPortfolioId = new HashMap<>();
        final Map<String, BigDecimal> currentValues = new ConcurrentHashMap<>();
        final Map<String, Long> purchasedValues = new HashMap<>();
        final Map<String, Long> portfolioCounts = new HashMap<>();

        @Override
        public Mono<String> findUserIdByPortfolioId(Long portfolioId) {
            String userId = userIdByPortfolioId.get(portfolioId);
            return userId != null ? Mono.just(userId) : Mono.empty();
        }

        @Override
        public Mono<Long> findPurchasedValue(String userId) {
            return Mono.just(purchasedValues.getOrDefault(userId, 0L));
        }

        @Override
        public Mono<BigDecimal> calculateCurrentValue(String userId) {
            return Mono.just(currentValues.getOrDefault(userId, BigDecimal.ZERO));
        }

        @Override
        public Mono<BigDecimal> findCurrentValue(String userId) {
            return calculateCurrentValue(userId);
        }

        @Override
        public Mono<BigDecimal> addCurrentValue(String userId, BigDecimal delta) {
            BigDecimal newValue = currentValues.getOrDefault(userId, BigDecimal.ZERO).add(delta);
            currentValues.put(userId, newValue);
            return Mono.just(newValue);
        }

        @Override
        public Mono<Long> findPortfolioCount(String userId) {
            return Mono.just(portfolioCounts.getOrDefault(userId, 0L));
        }

        @Override public Mono<Map<String, UserMetadata>> bulkFetchUserMetadata(List<String> userIds) { return unsupported(); }
        @Override public Mono<Map<String, BigDecimal>> bulkIncrementCurrentValues(Map<String, BigDecimal> deltasByUserId) { return unsupported(); }
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
            return Mono.fromRunnable(() -> userValuations.put(valuation.getUserId(), valuation));
        }

        @Override
        public Mono<Void> bulkSavePortfolioValuations(List<PortfolioValuation> valuations) {
            return Mono.fromRunnable(() -> valuations.forEach(v -> portfolioValuations.put(v.getPortfolioId(), v)));
        }

        @Override
        public Mono<Void> bulkSaveUserValuations(List<UserValuation> valuations) {
            return Mono.fromRunnable(() -> valuations.forEach(v -> userValuations.put(v.getUserId(), v)));
        }
    }

    private static <T> Mono<T> unsupported() {
        return Mono.error(new UnsupportedOperationException());
    }
}
