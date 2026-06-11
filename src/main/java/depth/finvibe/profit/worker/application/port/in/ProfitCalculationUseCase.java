package depth.finvibe.profit.worker.application.port.in;

import depth.finvibe.profit.worker.dto.ProfitCalculationDto;
import reactor.core.publisher.Mono;

public interface ProfitCalculationUseCase {
    Mono<Void> updateProfitByStockPriceChange(ProfitCalculationDto.ProfitCalculationRequest request);
}
