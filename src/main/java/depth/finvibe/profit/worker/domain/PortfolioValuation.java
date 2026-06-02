package depth.finvibe.profit.worker.domain;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class PortfolioValuation {
    private Long portfolioId;

    private Long purchasedValue; // 구매액

    private Long currentValue; // 평가액

    private Double profitRate; // 수익률

    private Long assetCount; // 종목 개수
}
