package depth.finvibe.profit.worker.application;

import depth.finvibe.profit.worker.application.port.in.ProfitCalculationUseCase;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore.PortfolioCurrentValueUpdate;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.dto.ProfitCalculationDto;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class ProfitCalculateService implements ProfitCalculationUseCase {

    private final PortfolioStateStore portfolioStateStore;
    private final ValuationRepository valuationRepository;
    private final ProfitWorkerMetrics metrics;

    @Override
    public Mono<Void> updateProfitByStockPriceChange(ProfitCalculationDto.ProfitCalculationRequest request) {
        return Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

            Long stockId = Objects.requireNonNull(request.getStockId());
            Long newPrice = Objects.requireNonNull(request.getNewPrice());

            Timer.Sample reverseIndexSample = metrics.startSample();

            return portfolioStateStore.findPortfolioIdsByStockId(stockId)
                    .flatMap(portfolioIds -> {
                        metrics.recordPhaseDuration(
                                ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                                ProfitWorkerMetrics.PHASE_REVERSE_INDEX_LOOKUP,
                                ProfitWorkerMetrics.RESULT_SUCCESS,
                                reverseIndexSample
                        );
                        metrics.recordAffectedPortfolios(
                                ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                                portfolioIds.size()
                        );

                        Timer.Sample portfolioFanoutSample = metrics.startSample();

                                return Flux.fromIterable(portfolioIds)
                                        .flatMap(portfolioId -> recalculatePortfolio(
                                                portfolioId,
                                                stockId,
                                                newPrice
                                        ),
                                        Integer.MAX_VALUE)
                                .then()
                                .doFinally(ignored -> metrics.recordPhaseDuration(
                                        ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                                        ProfitWorkerMetrics.PHASE_PORTFOLIO_FANOUT,
                                        result[0],
                                        portfolioFanoutSample
                                ));
                    })
                    .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                    .doFinally(ignored -> metrics.recordServiceDuration(
                            ProfitWorkerMetrics.OPERATION_STOCK_PRICE_RECALCULATION,
                            result[0],
                            sample
                    ));
        });
    }

    private Mono<Void> recalculatePortfolio(
            Long portfolioId,
            Long stockId,
            Long newPrice
    ) {
        return portfolioStateStore.recalculateCurrentValue(portfolioId, stockId, newPrice)
                .flatMap(update -> {
                    if (update.delta().signum() == 0) {
                        return Mono.empty();
                    }

                    return Mono.zip(
                                    portfolioStateStore.findPurchasedValue(portfolioId),
                                    portfolioStateStore.findAssetCount(portfolioId)
                            )
                            .flatMap(tuple -> {
                                Long purchasedValue = tuple.getT1();
                                Long assetCount = tuple.getT2();
                                BigDecimal newCV = update.currentValue();

                                PortfolioValuation valuation = PortfolioValuation.builder()
                                        .portfolioId(portfolioId)
                                        .purchasedValue(purchasedValue)
                                        .currentValue(roundToLong(newCV))
                                        .profitRate(calculateProfitRate(purchasedValue, newCV))
                                        .assetCount(assetCount)
                                        .build();

                                return valuationRepository.savePortfolioValuation(valuation);
                            });
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
