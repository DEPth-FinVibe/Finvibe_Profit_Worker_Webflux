package depth.finvibe.profit.worker.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

public class CacheUpdateDto {

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class PortfolioCacheUpdateRequest {
        private Long portfolioId;

        private Long stockId;

        private TradeType type;

        private Long price; // 원화 단위

        private BigDecimal quantity;

        public enum TradeType {
            STOCK_BUY,
            STOCK_SELL
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class UserCacheUpdateRequest {
        private String userId; // 추후 정수 기반 UserID로 변경

        private Long portfolioId;

        private ChangeType type;

        public enum ChangeType {
            CREATED,
            DELETED
        }
    }
}
