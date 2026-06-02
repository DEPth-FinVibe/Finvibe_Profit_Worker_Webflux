package depth.finvibe.profit.worker.application;

import depth.finvibe.profit.worker.application.port.in.CacheUpdateUseCase;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import depth.finvibe.profit.worker.dto.CacheUpdateDto;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class CacheUpdateService implements CacheUpdateUseCase {

    private final PortfolioStateStore portfolioStateStore;
    private final UserStateStore userStateStore;
    private final ValuationRepository valuationRepository;
    private final ProfitWorkerMetrics metrics;

    @Override
    public Mono<Void> updatePortfolioCache(CacheUpdateDto.PortfolioCacheUpdateRequest req) {
        return Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
            Long portfolioId = Objects.requireNonNull(req.getPortfolioId(), "portfolioId must not be null");
            Long stockId = Objects.requireNonNull(req.getStockId(), "stockId must not be null");
            Long price = Objects.requireNonNull(req.getPrice(), "price must not be null");
            BigDecimal quantity = Objects.requireNonNull(req.getQuantity(), "quantity must not be null");
            CacheUpdateDto.PortfolioCacheUpdateRequest.TradeType type =
                    Objects.requireNonNull(req.getType(), "type must not be null");

            BigDecimal amount = ValuationDecimalSupport.decimalOf(price).multiply(quantity);
            Mono<Long> update = switch (type) {
                case STOCK_BUY -> updatePortfolioCacheByStockBuy(portfolioId, stockId, quantity, amount);
                case STOCK_SELL -> updatePortfolioCacheByStockSell(portfolioId, stockId, quantity, amount);
            };

            return update
                    .doOnSuccess(affectedUsers -> {
                        metrics.recordAffectedPortfolios(ProfitWorkerMetrics.OPERATION_PORTFOLIO_CACHE_UPDATE, 1);
                        metrics.recordAffectedUsers(ProfitWorkerMetrics.OPERATION_PORTFOLIO_CACHE_UPDATE, affectedUsers);
                        result[0] = ProfitWorkerMetrics.RESULT_SUCCESS;
                    })
                    .then()
                    .doFinally(ignored -> metrics.recordServiceDuration(
                            ProfitWorkerMetrics.OPERATION_PORTFOLIO_CACHE_UPDATE,
                            result[0],
                            sample));
        });
    }

    @Override
    public Mono<Void> updateUserCache(CacheUpdateDto.UserCacheUpdateRequest request) {
        return Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};
            String userId = Objects.requireNonNull(request.getUserId(), "userId must not be null");
            Long portfolioId = Objects.requireNonNull(request.getPortfolioId(), "portfolioId must not be null");
            CacheUpdateDto.UserCacheUpdateRequest.ChangeType type =
                    Objects.requireNonNull(request.getType(), "type must not be null");

            return portfolioStateStore.findPurchasedValue(portfolioId)
                    .flatMap(portfolioPurchasedValue -> switch (type) {
                        case CREATED -> updateUserCacheByPortfolioCreated(userId, portfolioId, portfolioPurchasedValue);
                        case DELETED -> updateUserCacheByPortfolioDeleted(userId, portfolioId, portfolioPurchasedValue);
                    })
                    .doOnSuccess(ignored -> {
                        metrics.recordAffectedPortfolios(ProfitWorkerMetrics.OPERATION_USER_CACHE_UPDATE, 1);
                        metrics.recordAffectedUsers(ProfitWorkerMetrics.OPERATION_USER_CACHE_UPDATE, 1);
                        result[0] = ProfitWorkerMetrics.RESULT_SUCCESS;
                    })
                    .doFinally(ignored -> metrics.recordServiceDuration(
                            ProfitWorkerMetrics.OPERATION_USER_CACHE_UPDATE,
                            result[0],
                            sample));
        });
    }

    private Mono<Long> updatePortfolioCacheByStockBuy(Long portfolioId, Long stockId, BigDecimal quantity, BigDecimal amount) {
        return portfolioStateStore.increaseStockQuantity(stockId, portfolioId, quantity)
                .flatMap(added -> portfolioStateStore.addPurchasedValue(portfolioId, roundToLong(amount))
                        .then(portfolioStateStore.addCurrentValue(portfolioId, amount))
                        .then(portfolioStateStore.addStockCurrentValue(stockId, portfolioId, amount))
                        .then(userStateStore.findUserIdByPortfolioId(portfolioId))
                        .defaultIfEmpty("")
                        .flatMap(userId -> {
                            String mappedUserId = userId.isBlank() ? null : userId;
                            Mono<Void> userUpdate = mappedUserId == null
                                    ? Mono.empty()
                                    : userStateStore.addPurchasedValue(mappedUserId, roundToLong(amount)).then();
                            Mono<Void> assetCountUpdate = added
                                    ? portfolioStateStore.increaseAssetCount(portfolioId).then()
                                    : Mono.empty();
                            return userUpdate
                                    .then(assetCountUpdate)
                                    .then(saveValuationSnapshot(portfolioId, mappedUserId))
                                    .thenReturn(mappedUserId == null ? 0L : 1L);
                        }));
    }

    private Mono<Long> updatePortfolioCacheByStockSell(Long portfolioId, Long stockId, BigDecimal quantity, BigDecimal amount) {
        return portfolioStateStore.decreaseStockQuantity(stockId, portfolioId, quantity)
                .flatMap(removed -> portfolioStateStore.subtractPurchasedValue(portfolioId, roundToLong(amount))
                        .then(portfolioStateStore.subtractCurrentValue(portfolioId, amount))
                        .then(portfolioStateStore.subtractStockCurrentValue(stockId, portfolioId, amount))
                        .then(userStateStore.findUserIdByPortfolioId(portfolioId))
                        .defaultIfEmpty("")
                        .flatMap(userId -> {
                            String mappedUserId = userId.isBlank() ? null : userId;
                            Mono<Void> userUpdate = mappedUserId == null
                                    ? Mono.empty()
                                    : userStateStore.subtractPurchasedValue(mappedUserId, roundToLong(amount)).then();
                            Mono<Void> assetCountUpdate = removed
                                    ? portfolioStateStore.decreaseAssetCount(portfolioId).then()
                                    : Mono.empty();
                            return userUpdate
                                    .then(assetCountUpdate)
                                    .then(saveValuationSnapshot(portfolioId, mappedUserId))
                                    .thenReturn(mappedUserId == null ? 0L : 1L);
                        }));
    }

    private Mono<Void> updateUserCacheByPortfolioCreated(String userId, Long portfolioId, Long portfolioPurchasedValue) {
        return userStateStore.mapPortfolioToUser(portfolioId, userId)
                .then(userStateStore.addPurchasedValue(userId, portfolioPurchasedValue))
                .then(userStateStore.increasePortfolioCount(userId))
                .then(saveUserValuationSnapshot(userId));
    }

    private Mono<Void> updateUserCacheByPortfolioDeleted(String userId, Long portfolioId, Long portfolioPurchasedValue) {
        return userStateStore.removePortfolioUserMapping(portfolioId)
                .then(userStateStore.subtractPurchasedValue(userId, portfolioPurchasedValue))
                .then(userStateStore.decreasePortfolioCount(userId))
                .then(valuationRepository.markPortfolioValuationDeleted(portfolioId))
                .then(portfolioStateStore.deletePortfolioState(portfolioId))
                .then(saveUserValuationSnapshot(userId));
    }

    private Mono<Void> saveValuationSnapshot(Long portfolioId, String userId) {
        Mono<Void> userSnapshot = userId == null ? Mono.empty() : saveUserValuationSnapshot(userId);
        return savePortfolioValuationSnapshot(portfolioId).then(userSnapshot);
    }

    private Mono<Void> savePortfolioValuationSnapshot(Long portfolioId) {
        return Mono.zip(
                        portfolioStateStore.findPurchasedValue(portfolioId),
                        portfolioStateStore.findCurrentValue(portfolioId),
                        portfolioStateStore.findAssetCount(portfolioId)
                )
                .flatMap(tuple -> {
                    Long purchasedValue = tuple.getT1();
                    BigDecimal currentValue = tuple.getT2();
                    Long assetCount = tuple.getT3();
                    return valuationRepository.savePortfolioValuation(PortfolioValuation.builder()
                            .portfolioId(portfolioId)
                            .purchasedValue(purchasedValue)
                            .currentValue(roundToLong(currentValue))
                            .profitRate(calculateProfitRate(purchasedValue, currentValue))
                            .assetCount(assetCount)
                            .build());
                });
    }

    private Mono<Void> saveUserValuationSnapshot(String userId) {
        return Mono.zip(
                        userStateStore.findPurchasedValue(userId),
                        userStateStore.calculateCurrentValue(userId),
                        userStateStore.findPortfolioCount(userId)
                )
                .flatMap(tuple -> {
                    Long purchasedValue = tuple.getT1();
                    BigDecimal currentValue = tuple.getT2();
                    Long portfolioCount = tuple.getT3();
                    return valuationRepository.saveUserValuation(UserValuation.builder()
                            .userId(userId)
                            .purchasedValue(purchasedValue)
                            .currentValue(roundToLong(currentValue))
                            .profitRate(calculateProfitRate(purchasedValue, currentValue))
                            .portfolioCount(portfolioCount)
                            .build());
                });
    }

    private Double calculateProfitRate(Long purchasedValue, BigDecimal currentValue) {
        if (purchasedValue == 0L) {
            return 0.0;
        }

        return currentValue
                .subtract(ValuationDecimalSupport.decimalOf(purchasedValue))
                .divide(ValuationDecimalSupport.decimalOf(purchasedValue), 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .doubleValue();
    }

    private Long roundToLong(BigDecimal value) {
        return ValuationDecimalSupport.toWholeNumber(value);
    }
}
