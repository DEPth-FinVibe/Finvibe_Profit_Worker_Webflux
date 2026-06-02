package depth.finvibe.profit.worker.infrastructure.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PortfolioUserEvent {

    private EventType eventType;
    private String userId; // 추후 정수 기반 UserID로 변경
    private Long portfolioId;
    private Long targetPortfolioId;
    private Instant occurredAt;

    public enum EventType {
        CREATED,
        UPDATED,
        DELETED
    }
}
