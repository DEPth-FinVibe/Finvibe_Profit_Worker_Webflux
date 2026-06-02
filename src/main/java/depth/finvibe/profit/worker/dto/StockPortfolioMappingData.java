package depth.finvibe.profit.worker.dto;

import java.util.List;

/**
 * 특정 종목을 보유한 포트폴리오 목록 캐시를 복구하기 위한 Monolith 응답 데이터다.
 *
 * <p>{@code profit:stock:{stockId}:portfolios}와
 * {@code profit:stock:{stockId}:portfolios:initialized}를 구성하는 데 사용한다.</p>
 *
 * @param stockId 종목 ID
 * @param initialized Monolith 기준으로 해당 종목의 보유 포트폴리오 목록 조회가 완료되었는지 여부
 * @param portfolioIds 해당 종목을 보유한 포트폴리오 ID 목록
 */
public record StockPortfolioMappingData(
        Long stockId,
        boolean initialized,
        List<Long> portfolioIds
) {
}
