package depth.finvibe.profit.worker.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

public class ProfitCalculationDto {

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class ProfitCalculationRequest {
        private Long stockId;
        private Long newPrice;
        private Instant timestamp;
    }
}
