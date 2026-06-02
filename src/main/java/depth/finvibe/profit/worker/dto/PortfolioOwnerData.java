package depth.finvibe.profit.worker.dto;

/**
 * 포트폴리오 소유자 매핑 캐시를 복구하기 위한 Monolith 응답 데이터다.
 *
 * <p>{@code profit:portfolio-user:{portfolioId}}를 구성하는 데 사용한다.</p>
 *
 * @param portfolioId 포트폴리오 ID
 * @param userId 포트폴리오 소유 유저 ID. Monolith의 Long 사용자 ID를 전달한다.
 */
public record PortfolioOwnerData(
        Long portfolioId,
        Long userId
) {
}
