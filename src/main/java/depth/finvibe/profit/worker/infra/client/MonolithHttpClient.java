package depth.finvibe.profit.worker.infra.client;

import depth.finvibe.profit.worker.application.port.out.MonolithClient;
import depth.finvibe.profit.worker.dto.PortfolioAggregateData;
import depth.finvibe.profit.worker.dto.PortfolioHoldingData;
import depth.finvibe.profit.worker.dto.PortfolioOwnerData;
import depth.finvibe.profit.worker.dto.StockPortfolioMappingData;
import depth.finvibe.profit.worker.dto.UserAggregateData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
public class MonolithHttpClient implements MonolithClient {
    private static final String STOCK_PORTFOLIO_MAPPING_PATH = "/internal/profit-cache/stocks/%d/portfolios";
    private static final String PORTFOLIO_OWNER_PATH = "/internal/profit-cache/portfolios/%d/owner";
    private static final String PORTFOLIO_HOLDING_PATH = "/internal/profit-cache/portfolios/%d/stocks/%d/holding";
    private static final String PORTFOLIO_AGGREGATE_PATH = "/internal/profit-cache/portfolios/%d/aggregate";
    private static final String USER_AGGREGATE_PATH = "/internal/profit-cache/users/%s/aggregate";

    private final WebClient webClient;
    private final Duration requestTimeout;

    public MonolithHttpClient(
            WebClient.Builder webClientBuilder,
            @Value("${worker.monolith.base-url:http://localhost:8080}") String baseUrl,
            @Value("${worker.monolith.read-timeout-ms:3000}") long readTimeoutMillis
    ) {
        this.webClient = webClientBuilder
                .baseUrl(normalizeBaseUrl(baseUrl))
                .build();
        this.requestTimeout = Duration.ofMillis(readTimeoutMillis);
    }

    @Override
    public Mono<StockPortfolioMappingData> getStockPortfolioMapping(Long stockId) {
        if (stockId == null) {
            throw new IllegalArgumentException("stockId must not be null");
        }
        return get(STOCK_PORTFOLIO_MAPPING_PATH.formatted(stockId), StockPortfolioMappingData.class, "stockId=" + stockId);
    }

    @Override
    public Mono<PortfolioOwnerData> getPortfolioOwner(Long portfolioId) {
        if (portfolioId == null) {
            throw new IllegalArgumentException("portfolioId must not be null");
        }
        return get(PORTFOLIO_OWNER_PATH.formatted(portfolioId), PortfolioOwnerData.class, "portfolioId=" + portfolioId);
    }

    @Override
    public Mono<PortfolioHoldingData> getPortfolioHolding(Long portfolioId, Long stockId) {
        if (portfolioId == null) {
            throw new IllegalArgumentException("portfolioId must not be null");
        }
        if (stockId == null) {
            throw new IllegalArgumentException("stockId must not be null");
        }
        return get(
                PORTFOLIO_HOLDING_PATH.formatted(portfolioId, stockId),
                PortfolioHoldingData.class,
                "portfolioId=" + portfolioId + ", stockId=" + stockId
        );
    }

    @Override
    public Mono<PortfolioAggregateData> getPortfolioAggregate(Long portfolioId) {
        if (portfolioId == null) {
            throw new IllegalArgumentException("portfolioId must not be null");
        }
        return get(PORTFOLIO_AGGREGATE_PATH.formatted(portfolioId), PortfolioAggregateData.class, "portfolioId=" + portfolioId);
    }

    @Override
    public Mono<UserAggregateData> getUserAggregate(Long userId) {
        if (userId == null) {
            throw new IllegalArgumentException("userId must not be null");
        }
        return get(USER_AGGREGATE_PATH.formatted(userId), UserAggregateData.class, "userId=" + userId);
    }

    private <T> Mono<T> get(String path, Class<T> responseType, String context) {
        return webClient.get()
                .uri(path)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response -> response.createException()
                        .map(exception -> new IllegalStateException(
                                "Failed to fetch profit cache data from monolith: " + context
                                        + ", statusCode=" + exception.getStatusCode().value(),
                                exception)))
                .bodyToMono(responseType)
                .timeout(requestTimeout)
                .onErrorMap(WebClientResponseException.class, exception -> new IllegalStateException(
                        "Failed to fetch profit cache data from monolith: " + context
                                + ", statusCode=" + exception.getStatusCode().value(),
                        exception))
                .onErrorMap(throwable -> !(throwable instanceof IllegalStateException),
                        throwable -> new IllegalStateException("Failed to fetch profit cache data from monolith: " + context, throwable));
    }

    private String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("worker.monolith.base-url must not be blank");
        }
        if (baseUrl.endsWith("/")) {
            return baseUrl.substring(0, baseUrl.length() - 1);
        }
        return baseUrl;
    }
}
