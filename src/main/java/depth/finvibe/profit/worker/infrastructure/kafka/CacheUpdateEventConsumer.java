package depth.finvibe.profit.worker.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.application.port.in.CacheUpdateUseCase;
import depth.finvibe.profit.worker.dto.CacheUpdateDto;
import depth.finvibe.profit.worker.infrastructure.kafka.dto.PortfolioTradeEvent;
import depth.finvibe.profit.worker.infrastructure.kafka.dto.PortfolioUserEvent;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
public class CacheUpdateEventConsumer {

    private final CacheUpdateUseCase cacheUpdateUseCase;
    private final ProfitWorkerMetrics metrics;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @KafkaListener(topics = "${app.kafka.topics.portfolio-trade:trade.trade-executed.v1}")
    public Mono<Void> consumePortfolioTradeEvents(List<String> payloads) {
        return Flux.fromIterable(payloads)
                .concatMap(payload -> Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

                PortfolioTradeEvent event = read(payload, PortfolioTradeEvent.class);

            return cacheUpdateUseCase.updatePortfolioCache(CacheUpdateDto.PortfolioCacheUpdateRequest.builder()
                        .portfolioId(event.getPortfolioId())
                        .stockId(event.getStockId())
                        .type(toTradeType(event.getType()))
                        .price(event.getPrice())
                        .quantity(toQuantity(event))
                        .build())
                    .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                    .doFinally(ignored -> {
                        metrics.recordConsumed(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_TRADE, result[0]);
                        metrics.recordListenerDuration(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_TRADE, result[0], sample);
                    });
                }))
                .then();
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-user:asset.portfolio-group-changed.v1}")
    public Mono<Void> consumePortfolioUserEvents(List<String> payloads) {
        return Flux.fromIterable(payloads)
                .concatMap(payload -> Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

                PortfolioUserEvent event = read(payload, PortfolioUserEvent.class);
                Instant occurredAt = event.getOccurredAt();
                if (occurredAt != null) {
                    metrics.recordEventAge(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER, Duration.between(occurredAt, Instant.now()));
                }

                if (event.getEventType() == PortfolioUserEvent.EventType.UPDATED) {
                    result[0] = ProfitWorkerMetrics.RESULT_SKIPPED;
                    metrics.recordSkipped(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER, ProfitWorkerMetrics.REASON_UPDATED_EVENT_IGNORED);
                    return Mono.<Void>empty()
                            .doFinally(ignored -> {
                                metrics.recordConsumed(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER, result[0]);
                                metrics.recordListenerDuration(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER, result[0], sample);
                            });
                }

            return cacheUpdateUseCase.updateUserCache(CacheUpdateDto.UserCacheUpdateRequest.builder()
                        .userId(event.getUserId())
                        .portfolioId(event.getPortfolioId())
                        .type(toChangeType(event.getEventType()))
                        .build())
                    .doOnSuccess(ignored -> result[0] = ProfitWorkerMetrics.RESULT_SUCCESS)
                    .doFinally(ignored -> {
                        metrics.recordConsumed(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER, result[0]);
                        metrics.recordListenerDuration(ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER, result[0], sample);
                    });
                }))
                .then();
    }

    private <T> T read(String payload, Class<T> type) {
        try {
            return objectMapper.readValue(payload, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid Kafka event payload", e);
        }
    }

    private CacheUpdateDto.PortfolioCacheUpdateRequest.TradeType toTradeType(String type) {
        return switch (type) {
            case "BUY" -> CacheUpdateDto.PortfolioCacheUpdateRequest.TradeType.STOCK_BUY;
            case "SELL" -> CacheUpdateDto.PortfolioCacheUpdateRequest.TradeType.STOCK_SELL;
            default -> throw new IllegalArgumentException("Unsupported trade event type: " + type);
        };
    }

    private BigDecimal toQuantity(PortfolioTradeEvent event) {
        BigDecimal amount = event.getAmount();
        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException("Trade event amount must be positive: " + amount);
        }
        return amount.stripTrailingZeros();
    }

    private CacheUpdateDto.UserCacheUpdateRequest.ChangeType toChangeType(PortfolioUserEvent.EventType eventType) {
        return switch (eventType) {
            case CREATED -> CacheUpdateDto.UserCacheUpdateRequest.ChangeType.CREATED;
            case DELETED -> CacheUpdateDto.UserCacheUpdateRequest.ChangeType.DELETED;
            case UPDATED -> throw new IllegalArgumentException("Updated portfolio event does not change user cache");
        };
    }
}
