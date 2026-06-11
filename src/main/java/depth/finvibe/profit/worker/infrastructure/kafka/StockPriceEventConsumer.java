package depth.finvibe.profit.worker.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.application.port.in.ProfitCalculationUseCase;
import depth.finvibe.profit.worker.dto.ProfitCalculationDto;
import depth.finvibe.profit.worker.infrastructure.kafka.dto.StockPriceUpdatedEvent;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

@Component
@RequiredArgsConstructor
public class StockPriceEventConsumer {

    private final ProfitCalculationUseCase profitCalculationUseCase;
    private final ProfitWorkerMetrics metrics;
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @KafkaListener(topics = "${app.kafka.topics.stock-price-updated:market.stock-price-updated.v1}")
    public Mono<Void> consumeStockPriceUpdatedEvent(String payload) {
        return Mono.defer(() -> {
            Timer.Sample sample = metrics.startSample();
            String[] result = {ProfitWorkerMetrics.RESULT_FAILURE};

            StockPriceUpdatedEvent event = read(payload, StockPriceUpdatedEvent.class);
            Instant updatedAt = event.getUpdatedAt().atZone(ZoneId.systemDefault()).toInstant();
            metrics.recordEventAge(ProfitWorkerMetrics.EVENT_TYPE_STOCK_PRICE_UPDATED,
                    Duration.between(updatedAt, Instant.now()));

            ProfitCalculationDto.ProfitCalculationRequest request =
                    ProfitCalculationDto.ProfitCalculationRequest.builder()
                            .stockId(event.getStockId())
                            .newPrice(toPrice(event))
                            .timestamp(updatedAt)
                            .build();

            return profitCalculationUseCase.updateProfitByStockPriceChange(request)
                    .doOnSuccess(ignored -> {
                        metrics.recordConsumed(
                                ProfitWorkerMetrics.EVENT_TYPE_STOCK_PRICE_UPDATED,
                                ProfitWorkerMetrics.RESULT_SUCCESS);
                        result[0] = ProfitWorkerMetrics.RESULT_SUCCESS;
                    })
                    .doFinally(ignored -> metrics.recordListenerDuration(
                            ProfitWorkerMetrics.EVENT_TYPE_STOCK_PRICE_UPDATED,
                            result[0],
                            sample));
        });
    }

    private <T> T read(String payload, Class<T> type) {
        try {
            return objectMapper.readValue(payload, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid Kafka event payload", e);
        }
    }

    private Long toPrice(StockPriceUpdatedEvent event) {
        try {
            return event.getPrice().longValueExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Stock price event price must be an integer: " + event.getPrice(), e);
        }
    }
}
