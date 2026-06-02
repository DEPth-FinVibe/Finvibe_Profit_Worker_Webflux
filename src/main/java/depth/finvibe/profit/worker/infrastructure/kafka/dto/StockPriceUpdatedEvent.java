package depth.finvibe.profit.worker.infrastructure.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StockPriceUpdatedEvent {

    private Long stockId;
    private BigDecimal price;
    private LocalDateTime updatedAt;
}
