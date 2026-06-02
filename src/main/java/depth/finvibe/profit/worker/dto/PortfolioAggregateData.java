package depth.finvibe.profit.worker.dto;

import java.math.BigDecimal;

/**
 * 포트폴리오 집계 캐시를 복구하기 위한 Monolith 응답 데이터다.
 *
 * <p>{@code profit:portfolio:{portfolioId}} hash를 구성하는 데 사용한다.</p>
 *
 * @param portfolioId 포트폴리오 ID
 * @param totalPurchaseAmount 포트폴리오 전체 매입 금액
 * @param unrealizedProfit 포트폴리오 평가 손익
 * @param returnRate 포트폴리오 수익률
 * @param realizedProfit 포트폴리오 실현 손익
 * @param totalStockCount 포트폴리오 내 보유 종목 수
 */
public record PortfolioAggregateData(
        Long portfolioId,
        BigDecimal totalPurchaseAmount,
        BigDecimal unrealizedProfit,
        BigDecimal returnRate,
        BigDecimal realizedProfit,
        Integer totalStockCount
) {
}
