package depth.finvibe.profit.worker.support;

import depth.finvibe.profit.worker.application.ProfitWorkerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public final class TestMetricsFactory {

    private TestMetricsFactory() {
    }

    public static MetricsFixture create() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        return new MetricsFixture(registry, new ProfitWorkerMetrics(registry));
    }

    public record MetricsFixture(SimpleMeterRegistry registry, ProfitWorkerMetrics metrics) {
    }
}
