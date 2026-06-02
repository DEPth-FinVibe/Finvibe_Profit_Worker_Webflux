package depth.finvibe.profit.worker.infrastructure.kafka;

import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import depth.finvibe.profit.worker.application.port.in.CacheUpdateUseCase;
import depth.finvibe.profit.worker.dto.CacheUpdateDto;
import depth.finvibe.profit.worker.support.TestMetricsFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CacheUpdateEventConsumerTest {

    @Test
    void consumesMonolithTradeExecutedEvent() {
        CacheUpdateUseCase cacheUpdateUseCase = mock(CacheUpdateUseCase.class);
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        CacheUpdateEventConsumer consumer = new CacheUpdateEventConsumer(cacheUpdateUseCase, fixture.metrics());
        when(cacheUpdateUseCase.updatePortfolioCache(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.empty());

        consumer.consumePortfolioTradeEvents(java.util.List.of("""
                {
                  "tradeId": 1,
                  "userId": "7a22103f-1d1c-4ab4-9c47-4040c3a46964",
                  "type": "BUY",
                  "amount": 10,
                  "price": 50000,
                  "stockId": 123,
                  "name": "Samsung Electronics",
                  "currency": "KRW",
                  "portfolioId": 456
                }
                """)).block();

        ArgumentCaptor<CacheUpdateDto.PortfolioCacheUpdateRequest> captor =
                ArgumentCaptor.forClass(CacheUpdateDto.PortfolioCacheUpdateRequest.class);
        verify(cacheUpdateUseCase).updatePortfolioCache(captor.capture());

        CacheUpdateDto.PortfolioCacheUpdateRequest request = captor.getValue();
        assertThat(request.getPortfolioId()).isEqualTo(456L);
        assertThat(request.getStockId()).isEqualTo(123L);
        assertThat(request.getType()).isEqualTo(CacheUpdateDto.PortfolioCacheUpdateRequest.TradeType.STOCK_BUY);
        assertThat(request.getPrice()).isEqualTo(50000L);
        assertThat(request.getQuantity()).isEqualByComparingTo(BigDecimal.TEN);
        assertThat(registry.find(ProfitWorkerMetrics.EVENTS_CONSUMED)
                .tags(ProfitWorkerMetrics.TAG_EVENT_TYPE, ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_TRADE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SUCCESS)
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void acceptsFractionalTradeAmount() {
        CacheUpdateUseCase cacheUpdateUseCase = mock(CacheUpdateUseCase.class);
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        CacheUpdateEventConsumer consumer = new CacheUpdateEventConsumer(cacheUpdateUseCase, fixture.metrics());
        when(cacheUpdateUseCase.updatePortfolioCache(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.empty());

        consumer.consumePortfolioTradeEvents(java.util.List.of("""
                {
                  "type": "SELL",
                  "amount": 10.5,
                  "price": 50000,
                  "stockId": 123,
                  "portfolioId": 456
                }
                """)).block();

        ArgumentCaptor<CacheUpdateDto.PortfolioCacheUpdateRequest> captor =
                ArgumentCaptor.forClass(CacheUpdateDto.PortfolioCacheUpdateRequest.class);
        verify(cacheUpdateUseCase).updatePortfolioCache(captor.capture());
        assertThat(captor.getValue().getQuantity()).isEqualByComparingTo("10.5");
        assertThat(registry.find(ProfitWorkerMetrics.EVENTS_CONSUMED)
                .tags(ProfitWorkerMetrics.TAG_EVENT_TYPE, ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_TRADE,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SUCCESS)
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void consumesPortfolioGroupCreatedEvent() {
        CacheUpdateUseCase cacheUpdateUseCase = mock(CacheUpdateUseCase.class);
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        CacheUpdateEventConsumer consumer = new CacheUpdateEventConsumer(cacheUpdateUseCase, fixture.metrics());
        when(cacheUpdateUseCase.updateUserCache(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.empty());

        consumer.consumePortfolioUserEvents(java.util.List.of("""
                {
                  "eventType": "CREATED",
                  "userId": "7a22103f-1d1c-4ab4-9c47-4040c3a46964",
                  "portfolioId": 456,
                  "occurredAt": "2026-05-13T13:00:00Z"
                }
                """)).block();

        ArgumentCaptor<CacheUpdateDto.UserCacheUpdateRequest> captor =
                ArgumentCaptor.forClass(CacheUpdateDto.UserCacheUpdateRequest.class);
        verify(cacheUpdateUseCase).updateUserCache(captor.capture());

        CacheUpdateDto.UserCacheUpdateRequest request = captor.getValue();
        assertThat(request.getUserId()).isEqualTo("7a22103f-1d1c-4ab4-9c47-4040c3a46964");
        assertThat(request.getPortfolioId()).isEqualTo(456L);
        assertThat(request.getType()).isEqualTo(CacheUpdateDto.UserCacheUpdateRequest.ChangeType.CREATED);
        assertThat(registry.find(ProfitWorkerMetrics.EVENT_AGE)
                .tags(ProfitWorkerMetrics.TAG_EVENT_TYPE, ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER)
                .timer().count()).isEqualTo(1);
    }

    @Test
    void ignoresPortfolioGroupUpdatedEvent() {
        CacheUpdateUseCase cacheUpdateUseCase = mock(CacheUpdateUseCase.class);
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        CacheUpdateEventConsumer consumer = new CacheUpdateEventConsumer(cacheUpdateUseCase, fixture.metrics());

        consumer.consumePortfolioUserEvents(java.util.List.of("""
                {
                  "eventType": "UPDATED",
                  "userId": "7a22103f-1d1c-4ab4-9c47-4040c3a46964",
                  "portfolioId": 456,
                  "occurredAt": "2026-05-13T13:00:00Z"
                }
                """)).block();

        verifyNoInteractions(cacheUpdateUseCase);
        assertThat(registry.find(ProfitWorkerMetrics.EVENTS_CONSUMED)
                .tags(ProfitWorkerMetrics.TAG_EVENT_TYPE, ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER,
                        ProfitWorkerMetrics.TAG_RESULT, ProfitWorkerMetrics.RESULT_SKIPPED)
                .counter().count()).isEqualTo(1.0);
        assertThat(registry.find(ProfitWorkerMetrics.EVENTS_SKIPPED)
                .tags(ProfitWorkerMetrics.TAG_EVENT_TYPE, ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER,
                        ProfitWorkerMetrics.TAG_REASON, ProfitWorkerMetrics.REASON_UPDATED_EVENT_IGNORED)
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void skipsNegativePortfolioUserEventAgeMetric() {
        CacheUpdateUseCase cacheUpdateUseCase = mock(CacheUpdateUseCase.class);
        TestMetricsFactory.MetricsFixture fixture = TestMetricsFactory.create();
        SimpleMeterRegistry registry = fixture.registry();
        CacheUpdateEventConsumer consumer = new CacheUpdateEventConsumer(cacheUpdateUseCase, fixture.metrics());
        when(cacheUpdateUseCase.updateUserCache(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.empty());

        consumer.consumePortfolioUserEvents(java.util.List.of("""
                {
                  "eventType": "CREATED",
                  "userId": "7a22103f-1d1c-4ab4-9c47-4040c3a46964",
                  "portfolioId": 456,
                  "occurredAt": "3026-05-13T13:00:00Z"
                }
                """)).block();

        assertThat(registry.find(ProfitWorkerMetrics.EVENT_AGE)
                .tags(ProfitWorkerMetrics.TAG_EVENT_TYPE, ProfitWorkerMetrics.EVENT_TYPE_PORTFOLIO_USER)
                .timer()).isNull();
    }
}
