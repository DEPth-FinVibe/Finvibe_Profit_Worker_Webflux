package depth.finvibe.profit.worker.application.exception;

public class ProfitCacheMissException extends RuntimeException {
    private final Long stockId;
    private final Long portfolioId;
    private final Long userId;
    private final ProfitCacheMissReason reason;

    public ProfitCacheMissException(Long stockId, Long portfolioId, Long userId, ProfitCacheMissReason reason) {
        super(message(stockId, portfolioId, userId, reason));
        this.stockId = stockId;
        this.portfolioId = portfolioId;
        this.userId = userId;
        this.reason = reason;
    }

    public Long getStockId() {
        return stockId;
    }

    public Long getPortfolioId() {
        return portfolioId;
    }

    public Long getUserId() {
        return userId;
    }

    public ProfitCacheMissReason getReason() {
        return reason;
    }

    private static String message(Long stockId, Long portfolioId, Long userId, ProfitCacheMissReason reason) {
        return "Profit cache miss: stockId=" + stockId
                + ", portfolioId=" + portfolioId
                + ", userId=" + userId
                + ", reason=" + reason;
    }
}
