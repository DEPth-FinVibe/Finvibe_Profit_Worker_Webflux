package depth.finvibe.profit.worker.dto;

import java.math.BigDecimal;

/**
 * 유저 집계 캐시를 복구하기 위한 Monolith 응답 데이터다.
 *
 * <p>{@code profit:user:{userId}} hash를 구성하는 데 사용한다.
 * 랭킹 ZSet은 가격 이벤트 재처리 시 {@code ZADD}로 self-heal 된다.</p>
 *
 * @param userId 유저 ID. Monolith의 Long 사용자 ID를 전달한다.
 * @param totalPurchaseAmount 유저 전체 포트폴리오의 총 매입 금액
 * @param unrealizedProfit 유저 전체 평가 손익
 * @param returnRate 유저 전체 수익률
 * @param realizedProfit 유저 전체 실현 손익
 * @param totalPortfolioCount 유저의 전체 포트폴리오 수
 */
public record UserAggregateData(
        Long userId,
        BigDecimal totalPurchaseAmount,
        BigDecimal unrealizedProfit,
        BigDecimal returnRate,
        BigDecimal realizedProfit,
        Integer totalPortfolioCount
) {
}
