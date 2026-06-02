package depth.finvibe.profit.worker.application;

import depth.finvibe.profit.worker.application.port.in.ProfitCalculationUseCase;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore.PortfolioMetadata;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore.StockHolding;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore.StockHoldingKey;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import depth.finvibe.profit.worker.application.port.out.UserStateStore.UserMetadata;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import depth.finvibe.profit.worker.dto.ProfitCalculationDto;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class ProfitCalculateService implements ProfitCalculationUseCase {

    private final PortfolioStateStore portfolioStateStore;
    private final UserStateStore userStateStore;
    private final ValuationRepository valuationRepository;
    private final ProfitWorkerMetrics metrics;

    @Override
    public Mono<Void> updateProfitByStockPriceChange(ProfitCalculationDto.ProfitCalculationRequest request) {
        return updateProfitsByStockPriceChanges(List.of(request));
    }

    @Override
    public Mono<Void> updateProfitsByStockPriceChanges(List<ProfitCalculationDto.ProfitCalculationRequest> requests) {
        return Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

            Timer.Sample reverseIndexSample = metrics.startSample();
            List<Long> stockIds = new ArrayList<>();
            Map<Long, Long> priceByStockId = new HashMap<>();
            for (ProfitCalculationDto.ProfitCalculationRequest request : requests) {
                Long stockId = Objects.requireNonNull(request.getStockId());
                Long newPrice = Objects.requireNonNull(request.getNewPrice());
                stockIds.add(stockId);
                priceByStockId.put(stockId, newPrice);
            }

            return portfolioStateStore.bulkFindPortfolioIdsByStockIds(stockIds)
                    .map(portfolioIdsByStockId -> {
                        List<PortfolioRecalculationTask> tasks = new ArrayList<>();
                        for (Long stockId : stockIds) {
                            Long newPrice = priceByStockId.get(stockId);
                            List<Long> portfolioIds = portfolioIdsByStockId.getOrDefault(stockId, List.of());
                            for (Long portfolioId : portfolioIds) {
                                tasks.add(new PortfolioRecalculationTask(portfolioId, stockId, newPrice));
                            }
                        }
                        return tasks;
                    })
                    .doOnSuccess(ignored -> metrics.recordPhaseDuration(
                            ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                            ProfitWorkerMetrics.PHASE_REVERSE_INDEX_LOOKUP,
                            ProfitWorkerMetrics.RESULT_SUCCESS,
                            reverseIndexSample))
                    .flatMap(tasks -> {
                        if (tasks.isEmpty()) {
                            result[0] = ProfitWorkerMetrics.RESULT_SUCCESS;
                            return Mono.<Void>empty();
                        }
                        return recalculatePortfolios(tasks)
                                .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS);
                    })
                    .doFinally(ignored -> metrics.recordServiceDuration(
                            ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                            result[0],
                            sample));
        });
    }

    private Mono<Void> recalculatePortfolios(List<PortfolioRecalculationTask> tasks) {
        Timer.Sample prefetchSample = metrics.startSample();
        List<Long> portfolioIds = tasks.stream().map(PortfolioRecalculationTask::portfolioId).distinct().toList();
        List<StockHoldingKey> holdingKeys = tasks.stream()
                .map(t -> new StockHoldingKey(t.portfolioId(), t.stockId()))
                .toList();

        return Mono.zip(
                        portfolioStateStore.bulkFetchPortfolioMetadata(portfolioIds),
                        portfolioStateStore.bulkFetchStockHoldings(holdingKeys)
                )
                .doOnSuccess(ignored -> metrics.recordPhaseDuration(
                        ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                        "bulk_prefetch",
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        prefetchSample))
                .flatMap(tuple -> {
                    Timer.Sample computeSample = metrics.startSample();
                    RecalculationPlan plan = buildRecalculationPlan(tasks, tuple.getT1(), tuple.getT2());
                    metrics.recordPhaseDuration(
                            ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                            "in_memory_compute",
                            ProfitWorkerMetrics.RESULT_SUCCESS,
                            computeSample);
                    return applyRecalculationPlan(tasks, tuple.getT1(), plan);
                });
    }

    private RecalculationPlan buildRecalculationPlan(
            List<PortfolioRecalculationTask> tasks,
            Map<Long, PortfolioMetadata> portfolioMetadata,
            Map<String, StockHolding> stockHoldings
    ) {
        Map<Long, BigDecimal> portfolioDeltaSum = new HashMap<>();
        Map<String, BigDecimal> newStockCurrentValues = new HashMap<>();

        for (PortfolioRecalculationTask task : tasks) {
            String holdingKey = task.portfolioId() + ":" + task.stockId();
            StockHolding holding = stockHoldings.getOrDefault(holdingKey, new StockHolding(BigDecimal.ZERO, BigDecimal.ZERO));

            if (holding.quantity().signum() == 0) {
                continue;
            }

            BigDecimal newStockCV = BigDecimal.valueOf(task.newPrice()).multiply(holding.quantity());
            BigDecimal delta = newStockCV.subtract(holding.currentValue());

            portfolioDeltaSum.merge(task.portfolioId(), delta, BigDecimal::add);
            String stockCVKey = portfolioStateStore.stockCurrentValueKey(task.portfolioId(), task.stockId());
            newStockCurrentValues.put(stockCVKey, newStockCV);
        }

        return new RecalculationPlan(portfolioMetadata, portfolioDeltaSum, newStockCurrentValues);
    }

    private Mono<Void> applyRecalculationPlan(
            List<PortfolioRecalculationTask> tasks,
            Map<Long, PortfolioMetadata> portfolioMetadata,
            RecalculationPlan plan
    ) {
        Timer.Sample portfolioIncrSample = metrics.startSample();
        Mono<Map<Long, BigDecimal>> newPortfolioCVsMono = plan.portfolioDeltaSum().isEmpty()
                ? Mono.just(Map.of())
                : portfolioStateStore.bulkIncrementCurrentValues(plan.portfolioDeltaSum());

        return newPortfolioCVsMono
                .doOnSuccess(ignored -> metrics.recordPhaseDuration(
                        ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                        "pipeline_portfolio_incr",
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        portfolioIncrSample))
                .flatMap(newPortfolioCVs -> {
                    Timer.Sample stockCVSample = metrics.startSample();
                    Mono<Void> saveStockCVs = plan.newStockCurrentValues().isEmpty()
                            ? Mono.empty()
                            : portfolioStateStore.bulkSetStockCurrentValues(plan.newStockCurrentValues());
                    return saveStockCVs
                            .doOnSuccess(ignored -> metrics.recordPhaseDuration(
                                    ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                                    "pipeline_stock_cv_set",
                                    ProfitWorkerMetrics.RESULT_SUCCESS,
                                    stockCVSample))
                            .then(savePortfolioAndUserValuations(tasks, portfolioMetadata, plan, newPortfolioCVs));
                });
    }

    private Mono<Void> savePortfolioAndUserValuations(
            List<PortfolioRecalculationTask> tasks,
            Map<Long, PortfolioMetadata> portfolioMetadata,
            RecalculationPlan plan,
            Map<Long, BigDecimal> newPortfolioCVs
    ) {
        Timer.Sample portfolioValSample = metrics.startSample();
        Map<String, BigDecimal> userDeltaByUserId = new HashMap<>();
        List<PortfolioValuation> portfolioValuations = new ArrayList<>();

        for (Long portfolioId : plan.portfolioDeltaSum().keySet()) {
            PortfolioMetadata meta = portfolioMetadata.get(portfolioId);
            BigDecimal newCV = newPortfolioCVs.getOrDefault(portfolioId, meta != null ? meta.currentValue() : BigDecimal.ZERO);
            Long purchasedValue = meta != null ? meta.purchasedValue() : 0L;
            Long assetCount = meta != null ? meta.assetCount() : 0L;
            String userId = meta != null ? meta.userId() : null;

            portfolioValuations.add(PortfolioValuation.builder()
                    .portfolioId(portfolioId)
                    .purchasedValue(purchasedValue)
                    .currentValue(roundToLong(newCV))
                    .profitRate(calculateProfitRate(purchasedValue, newCV))
                    .assetCount(assetCount)
                    .build());

            if (userId != null) {
                BigDecimal delta = plan.portfolioDeltaSum().get(portfolioId);
                userDeltaByUserId.merge(userId, delta, BigDecimal::add);
            }
        }

        return valuationRepository.bulkSavePortfolioValuations(portfolioValuations)
                .doOnSuccess(ignored -> metrics.recordPhaseDuration(
                        ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                        ProfitWorkerMetrics.PHASE_PORTFOLIO_FANOUT,
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        portfolioValSample))
                .then(recalculateUsersBulk(userDeltaByUserId))
                .doOnSuccess(ignored -> {
                    metrics.recordAffectedPortfolios(ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION, tasks.size());
                    metrics.recordAffectedUsers(ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION, userDeltaByUserId.size());
                });
    }

    private Mono<Void> recalculateUsersBulk(Map<String, BigDecimal> userDeltaByUserId) {
        Timer.Sample userFanoutSample = metrics.startSample();
        if (userDeltaByUserId.isEmpty()) {
            metrics.recordPhaseDuration(
                    ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                    ProfitWorkerMetrics.PHASE_USER_FANOUT,
                    ProfitWorkerMetrics.RESULT_SUCCESS,
                    userFanoutSample);
            return Mono.empty();
        }

        List<String> userIds = new ArrayList<>(userDeltaByUserId.keySet());
        return Mono.zip(
                        userStateStore.bulkIncrementCurrentValues(userDeltaByUserId),
                        userStateStore.bulkFetchUserMetadata(userIds)
                )
                .flatMap(tuple -> {
                    Map<String, BigDecimal> newUserCVs = tuple.getT1();
                    Map<String, UserMetadata> userMetadata = tuple.getT2();
                    List<UserValuation> userValuations = new ArrayList<>();

                    for (String userId : userIds) {
                        UserMetadata meta = userMetadata.get(userId);
                        BigDecimal currentValue = newUserCVs.getOrDefault(userId, BigDecimal.ZERO);
                        Long purchasedValue = meta != null ? meta.purchasedValue() : 0L;
                        Long portfolioCount = meta != null ? meta.portfolioCount() : 0L;

                        userValuations.add(UserValuation.builder()
                                .userId(userId)
                                .purchasedValue(purchasedValue)
                                .currentValue(roundToLong(currentValue))
                                .profitRate(calculateProfitRate(purchasedValue, currentValue))
                                .portfolioCount(portfolioCount)
                                .build());
                    }
                    return valuationRepository.bulkSaveUserValuations(userValuations);
                })
                .doOnSuccess(ignored -> metrics.recordPhaseDuration(
                        ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                        ProfitWorkerMetrics.PHASE_USER_FANOUT,
                        ProfitWorkerMetrics.RESULT_SUCCESS,
                        userFanoutSample));
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

    private record PortfolioRecalculationTask(Long portfolioId, Long stockId, Long newPrice) {
    }

    private record RecalculationPlan(
            Map<Long, PortfolioMetadata> portfolioMetadata,
            Map<Long, BigDecimal> portfolioDeltaSum,
            Map<String, BigDecimal> newStockCurrentValues
    ) {
    }
}
