package depth.finvibe.profit.worker;

import depth.finvibe.profit.worker.application.port.in.ProfitCalculationUseCase;
import depth.finvibe.profit.worker.infrastructure.kafka.StockPriceEventConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;
import reactor.core.publisher.Mono;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.listener.auto-startup=false"
)
class MetricsActuatorIntegrationTest {

    @MockitoBean
    private ProfitCalculationUseCase profitCalculationUseCase;

    @Autowired
    private StockPriceEventConsumer stockPriceEventConsumer;

    @LocalServerPort
    private int port;

    @Test
    void exposesProfitWorkerMetricsOnPrometheusEndpoint() throws IOException, InterruptedException {
        when(profitCalculationUseCase.updateProfitsByStockPriceChanges(anyList())).thenReturn(Mono.empty());
        stockPriceEventConsumer.consumeStockPriceUpdatedEvents(java.util.List.of("""
                {
                  "stockId": 123,
                  "price": 72000,
                  "updatedAt": "2026-05-13T13:00:00"
                }
                """)).block();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/actuator/prometheus"))
                .GET()
                .build();
        String response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString())
                .body();

        assertThat(response).contains("profit_worker_events_consumed_total");
        assertThat(response).contains("profit_worker_listener_duration_seconds_count");
    }
}
