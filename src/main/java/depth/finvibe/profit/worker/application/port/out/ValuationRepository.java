package depth.finvibe.profit.worker.application.port.out;

import depth.finvibe.profit.worker.domain.PortfolioValuation;
import depth.finvibe.profit.worker.domain.UserValuation;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 계산된 포트폴리오/유저 평가 snapshot을 저장하는 포트.
 */
public interface ValuationRepository {

    /**
     * 포트폴리오 평가 snapshot을 저장한다.
     *
     * @param valuation 저장할 포트폴리오 평가 정보
     */
    Mono<Void> savePortfolioValuation(PortfolioValuation valuation);

    /**
     * 포트폴리오 평가 snapshot을 soft delete 대상으로 표시한다.
     *
     * @param portfolioId 삭제된 포트폴리오 ID
     */
    Mono<Void> markPortfolioValuationDeleted(Long portfolioId);

    /**
     * 유저 평가 snapshot을 저장한다.
     *
     * @param valuation 저장할 유저 평가 정보
     */
    Mono<Void> saveUserValuation(UserValuation valuation);

    /**
     * 여러 포트폴리오 평가 snapshot을 일괄 저장한다 (pipeline).
     */
    Mono<Void> bulkSavePortfolioValuations(List<PortfolioValuation> valuations);

    /**
     * 여러 유저 평가 snapshot을 일괄 저장한다 (pipeline).
     */
    Mono<Void> bulkSaveUserValuations(List<UserValuation> valuations);
}
