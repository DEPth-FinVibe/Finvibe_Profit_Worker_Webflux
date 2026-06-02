package depth.finvibe.profit.worker.application.port.out;

import depth.finvibe.profit.worker.dto.PortfolioAggregateData;
import depth.finvibe.profit.worker.dto.PortfolioHoldingData;
import depth.finvibe.profit.worker.dto.PortfolioOwnerData;
import depth.finvibe.profit.worker.dto.StockPortfolioMappingData;
import depth.finvibe.profit.worker.dto.UserAggregateData;
import reactor.core.publisher.Mono;

public interface MonolithClient {
    Mono<StockPortfolioMappingData> getStockPortfolioMapping(Long stockId);

    Mono<PortfolioOwnerData> getPortfolioOwner(Long portfolioId);

    Mono<PortfolioHoldingData> getPortfolioHolding(Long portfolioId, Long stockId);

    Mono<PortfolioAggregateData> getPortfolioAggregate(Long portfolioId);

    Mono<UserAggregateData> getUserAggregate(Long userId);
}
