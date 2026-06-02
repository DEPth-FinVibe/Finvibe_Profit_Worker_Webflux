package depth.finvibe.profit.worker.application.port.out;

import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 포트폴리오 단위 수익률 계산과 캐시 갱신에 필요한 상태 저장소 포트.
 */
public interface PortfolioStateStore {

    /**
     * 특정 종목을 보유한 포트폴리오 ID 목록을 조회한다.
     *
     * @param stockId 종목 ID
     * @return 종목을 보유한 포트폴리오 ID 목록
     */
    Mono<List<Long>> findPortfolioIdsByStockId(Long stockId);

    /**
     * 포트폴리오의 총 구매액을 조회한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @return 포트폴리오 총 구매액
     */
    Mono<Long> findPurchasedValue(Long portfolioId);

    /**
     * 포트폴리오의 현재 평가액을 조회한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @return 포트폴리오 현재 평가액
     */
    Mono<BigDecimal> findCurrentValue(Long portfolioId);

    /**
     * 변경된 종목 가격을 반영한 포트폴리오 평가액을 계산한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param changedStockId 가격이 변경된 종목 ID
     * @param newPrice 변경된 종목의 신규 가격
     * @return 변경 가격이 반영된 포트폴리오 평가액
     */
    Mono<BigDecimal> calculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice);

    /**
     * 변경된 종목 가격을 반영하고, 포트폴리오 현재 평가액의 delta를 함께 반환한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param changedStockId 가격이 변경된 종목 ID
     * @param newPrice 변경된 종목의 신규 가격
     * @return 이전/이후 평가액과 delta
     */
    Mono<PortfolioCurrentValueUpdate> recalculateCurrentValue(Long portfolioId, Long changedStockId, Long newPrice);

    /**
     * 포트폴리오의 보유 종목 수를 조회한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @return 보유 종목 수
     */
    Mono<Long> findAssetCount(Long portfolioId);

    /**
     * 포트폴리오의 종목 보유 수량을 증가시킨다.
     *
     * @param stockId 종목 ID
     * @param portfolioId 포트폴리오 ID
     * @param quantity 증가시킬 수량
     * @return 기존 수량이 0이었다가 새로 보유하게 되었으면 true
     */
    Mono<Boolean> increaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity);

    /**
     * 포트폴리오의 종목 보유 수량을 감소시킨다.
     *
     * @param stockId 종목 ID
     * @param portfolioId 포트폴리오 ID
     * @param quantity 감소시킬 수량
     * @return 감소 후 수량이 0이 되어 보유 관계가 제거되었으면 true
     */
    Mono<Boolean> decreaseStockQuantity(Long stockId, Long portfolioId, BigDecimal quantity);

    /**
     * 포트폴리오 총 구매액에 금액을 더한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param amount 더할 원화 금액
     */
    Mono<Long> addPurchasedValue(Long portfolioId, Long amount);

    /**
     * 포트폴리오 총 구매액에서 금액을 뺀다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param amount 뺄 원화 금액
     */
    Mono<Long> subtractPurchasedValue(Long portfolioId, Long amount);

    /**
     * 포트폴리오 평가액에 금액을 더한다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param amount 더할 원화 금액
     */
    Mono<BigDecimal> addCurrentValue(Long portfolioId, BigDecimal amount);

    /**
     * 포트폴리오 평가액에서 금액을 뺀다.
     *
     * @param portfolioId 포트폴리오 ID
     * @param amount 뺄 원화 금액
     */
    Mono<BigDecimal> subtractCurrentValue(Long portfolioId, BigDecimal amount);

    /**
     * 포트폴리오 내 특정 종목 평가액에 금액을 더한다.
     *
     * @param stockId 종목 ID
     * @param portfolioId 포트폴리오 ID
     * @param amount 더할 원화 금액
     */
    Mono<Void> addStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount);

    /**
     * 포트폴리오 내 특정 종목 평가액에서 금액을 뺀다.
     *
     * @param stockId 종목 ID
     * @param portfolioId 포트폴리오 ID
     * @param amount 뺄 원화 금액
     */
    Mono<Void> subtractStockCurrentValue(Long stockId, Long portfolioId, BigDecimal amount);

    /**
     * 포트폴리오 보유 종목 수를 1 증가시킨다.
     *
     * @param portfolioId 포트폴리오 ID
     */
    Mono<Long> increaseAssetCount(Long portfolioId);

    /**
     * 포트폴리오 보유 종목 수를 1 감소시킨다.
     *
     * @param portfolioId 포트폴리오 ID
     */
    Mono<Long> decreaseAssetCount(Long portfolioId);

    /**
     * 포트폴리오 삭제 시 Redis에 남은 포트폴리오 상태를 정리한다.
     *
     * @param portfolioId 포트폴리오 ID
     */
    Mono<Void> deletePortfolioState(Long portfolioId);

    /**
     * 여러 포트폴리오의 메타데이터(구매액, 종목수, 소유유저, 평가액)를 일괄 조회한다.
     *
     * @param portfolioIds 포트폴리오 ID 목록
     * @return portfolioId → PortfolioMetadata 매핑
     */
    Mono<Map<Long, PortfolioMetadata>> bulkFetchPortfolioMetadata(List<Long> portfolioIds);

    /**
     * 여러 포트폴리오-종목 조합의 보유수량과 현재평가액을 일괄 조회한다.
     *
     * @param tasks portfolioId+stockId 조합 목록
     * @return "portfolioId:stockId" → StockHolding 매핑
     */
    Mono<Map<String, StockHolding>> bulkFetchStockHoldings(List<StockHoldingKey> tasks);

    /**
     * 여러 포트폴리오-종목의 현재평가액을 일괄 저장한다.
     *
     * @param updates "portfolio:{id}:stock:{stockId}:current-value" → 새 값 매핑
     */
    Mono<Void> bulkSetStockCurrentValues(Map<String, BigDecimal> updates);

    /**
     * 여러 포트폴리오의 현재 평가액에 delta를 원자적으로 누적 반영한다 (pipeline).
     *
     * @param deltasByPortfolioId portfolioId → delta 매핑
     * @return portfolioId → 반영 후 평가액 매핑
     */
    Mono<Map<Long, BigDecimal>> bulkIncrementCurrentValues(Map<Long, BigDecimal> deltasByPortfolioId);

    /**
     * 포트폴리오-종목의 현재평가액 키를 생성한다.
     */
    String stockCurrentValueKey(Long portfolioId, Long stockId);

    record PortfolioCurrentValueUpdate(
            BigDecimal previousCurrentValue,
            BigDecimal currentValue,
            BigDecimal delta
    ) {
    }

    record PortfolioMetadata(Long purchasedValue, Long assetCount, String userId, BigDecimal currentValue) {
    }

    record StockHolding(BigDecimal quantity, BigDecimal currentValue) {
    }

    record StockHoldingKey(Long portfolioId, Long stockId) {
        public String toKey() {
            return portfolioId + ":" + stockId;
        }
    }
}
