package depth.finvibe.profit.worker.application.port.in;

import depth.finvibe.profit.worker.dto.CacheUpdateDto;
import reactor.core.publisher.Mono;

public interface CacheUpdateUseCase {
    Mono<Void> updatePortfolioCache(CacheUpdateDto.PortfolioCacheUpdateRequest req);

    Mono<Void> updateUserCache(CacheUpdateDto.UserCacheUpdateRequest request);
}
