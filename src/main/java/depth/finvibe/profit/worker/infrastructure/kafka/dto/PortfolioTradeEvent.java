package depth.finvibe.profit.worker.infrastructure.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PortfolioTradeEvent {

    private Long tradeId;
    private String userId; // 추후 정수 기반 UserID로 변경
    private String type;
    private BigDecimal amount;
    private Long price;
    private Long portfolioId;
    private Long stockId;
    private String name;
    private String currency;
}
