package depth.finvibe.profit.worker.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class UserValuation {
    private String userId; // 추후 정수 기반 UserID로 변경

    private Long purchasedValue; // 구매액

    private Long currentValue; // 평가액

    private Double profitRate; // 수익률

    private Long portfolioCount; // 포트폴리오 개수
}
