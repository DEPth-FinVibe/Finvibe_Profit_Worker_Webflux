package depth.finvibe.profit.worker.dto;

import java.math.BigDecimal;

/**
 * 포트폴리오 내 특정 종목 보유 상태 캐시를 복구하기 위한 Monolith 응답 데이터다.
 *
 * <p>{@code profit:portfolio:{portfolioId}:stock:{stockId}} hash를 구성하는 데 사용한다.</p>
 *
 * @param portfolioId 포트폴리오 ID
 * @param stockId 종목 ID
 * @param quantity 보유 수량
 * @param averagePurchasePrice 평균 매입가
 * @param purchaseAmount 해당 종목 총 매입 금액
 * @param currentPrice 현재가
 * @param unrealizedProfit 해당 종목 평가 손익
 * @param returnRate 해당 종목 수익률
 */
public record PortfolioHoldingData(
        Long portfolioId,
        Long stockId,
        BigDecimal quantity,
        BigDecimal averagePurchasePrice,
        BigDecimal purchaseAmount,
        BigDecimal currentPrice,
        BigDecimal unrealizedProfit,
        BigDecimal returnRate
) {
}
