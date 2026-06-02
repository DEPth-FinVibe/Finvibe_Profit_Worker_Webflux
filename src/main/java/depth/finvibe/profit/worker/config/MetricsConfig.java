package depth.finvibe.profit.worker.config;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public MeterFilter profitWorkerRuntimeTagFilter(
            @Value("${app.metrics.worker-runtime:webflux}") String workerRuntime
    ) {
        return MeterFilter.commonTags(Tags.of("worker_runtime", workerRuntime));
    }
}
