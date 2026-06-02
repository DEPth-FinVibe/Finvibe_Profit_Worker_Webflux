package depth.finvibe.profit.worker.application.port.out;

import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 유저 단위 수익률 계산과 캐시 갱신에 필요한 상태 저장소 포트.
 */
public interface UserStateStore {

    /**
     * 포트폴리오를 소유한 유저 ID를 조회한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @return 포트폴리오 소유 유저 ID
     */
    Mono<String> findUserIdByPortfolioId(Long portfolioId);

    /**
     * 유저의 총 구매액을 조회한다.
     *
     * @param userId 유저 ID
     * @return 유저 총 구매액
     */
    Mono<Long> findPurchasedValue(String userId);

    /**
     * 유저가 보유한 포트폴리오들의 현재 평가액 합계를 계산한다.
     *
     * @param userId 유저 ID
     * @return 유저 현재 평가액
     */
    Mono<BigDecimal> calculateCurrentValue(String userId);

    /**
     * 유저 현재 평가액을 조회한다.
     *
     * @param userId 유저 ID
     * @return 유저 현재 평가액
     */
    Mono<BigDecimal> findCurrentValue(String userId);

    /**
     * 유저 현재 평가액에 delta를 누적 반영한다.
     *
     * @param userId 유저 ID
     * @param delta 반영할 평가액 변화량
     * @return 반영 후 유저 현재 평가액
     */
    Mono<BigDecimal> addCurrentValue(String userId, BigDecimal delta);

    /**
     * 유저의 보유 포트폴리오 수를 조회한다.
     *
     * @param userId 유저 ID
     * @return 보유 포트폴리오 수
     */
    Mono<Long> findPortfolioCount(String userId);

    /**
     * 포트폴리오와 유저의 소유 관계를 저장한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param userId 유저 ID
     */
    Mono<Void> mapPortfolioToUser(Long portfolioId, String userId); // 추후 정수 기반 UserID로 변경

    /**
     * 포트폴리오와 유저의 소유 관계를 제거한다.
     *
     * @param portfolioId 포트폴리오 ID
     */
    Mono<Void> removePortfolioUserMapping(Long portfolioId);

    /**
     * 유저 총 구매액에 금액을 더한다.
     *
     * @param userId 유저 ID
     * @param amount 더할 원화 금액
     */
    Mono<Long> addPurchasedValue(String userId, Long amount);

    /**
     * 유저 총 구매액에서 금액을 뺀다.
     *
     * @param userId 유저 ID
     * @param amount 뺄 원화 금액
     */
    Mono<Long> subtractPurchasedValue(String userId, Long amount);

    /**
     * 유저 보유 포트폴리오 수를 1 증가시킨다.
     *
     * @param userId 유저 ID
     */
    Mono<Long> increasePortfolioCount(String userId);

    /**
     * 유저 보유 포트폴리오 수를 1 감소시킨다.
     *
     * @param userId 유저 ID
     */
    Mono<Long> decreasePortfolioCount(String userId);

    /**
     * 여러 유저의 메타데이터(구매액, 포트폴리오수)를 일괄 조회한다.
     *
     * @param userIds 유저 ID 목록
     * @return userId → UserMetadata 매핑
     */
    Mono<Map<String, UserMetadata>> bulkFetchUserMetadata(List<String> userIds);

    /**
     * 여러 유저의 현재 평가액에 delta를 원자적으로 누적 반영한다 (pipeline).
     *
     * @param deltasByUserId userId → delta 매핑
     * @return userId → 반영 후 평가액 매핑
     */
    Mono<Map<String, BigDecimal>> bulkIncrementCurrentValues(Map<String, BigDecimal> deltasByUserId);

    record UserMetadata(Long purchasedValue, Long portfolioCount) {
    }
}
