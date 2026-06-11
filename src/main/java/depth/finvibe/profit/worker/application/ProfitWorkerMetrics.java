package depth.finvibe.profit.worker.application;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ProfitWorkerMetrics {

    public static final String EVENTS_CONSUMED = "profit.worker.events.consumed";
    public static final String EVENTS_SKIPPED = "profit.worker.events.skipped";
    public static final String LISTENER_DURATION = "profit.worker.listener.duration";
    public static final String SERVICE_DURATION = "profit.worker.service.duration";
    public static final String REDIS_OPERATION_DURATION = "profit.worker.redis.operation.duration";
    public static final String REDIS_COMMAND_DURATION = "profit.worker.redis.command.duration";
    public static final String AFFECTED_PORTFOLIOS = "profit.worker.affected.portfolios";
    public static final String AFFECTED_USERS = "profit.worker.affected.users";
    public static final String EVENT_AGE = "profit.worker.event.age";
    public static final String LAST_LISTENER_DURATION = "profit.worker.listener.last.duration";
    public static final String LAST_SERVICE_DURATION = "profit.worker.service.last.duration";
    public static final String LAST_EVENT_AGE = "profit.worker.event.last.age";

    public static final String TAG_EVENT_TYPE = "event_type";
    public static final String TAG_RESULT = "result";
    public static final String TAG_REASON = "reason";
    public static final String TAG_OPERATION = "operation";
    public static final String TAG_COMMAND = "command";

    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_FAILURE = "failure";
    public static final String RESULT_SKIPPED = "skipped";

    public static final String EVENT_TYPE_STOCK_PRICE_UPDATED = "stock_price_updated";
    public static final String EVENT_TYPE_PORTFOLIO_TRADE = "portfolio_trade";
    public static final String EVENT_TYPE_PORTFOLIO_USER = "portfolio_user";

    public static final String REASON_UPDATED_EVENT_IGNORED = "updated_event_ignored";

    public static final String OPERATION_STOCK_PRICE_RECALCULATION = "stock_price_recalculation";
    public static final String OPERATION_PORTFOLIO_CACHE_UPDATE = "portfolio_cache_update";
    public static final String OPERATION_USER_CACHE_UPDATE = "user_cache_update";
    public static final String OPERATION_PORTFOLIO_CURRENT_VALUE = "portfolio_current_value";
    public static final String OPERATION_USER_CURRENT_VALUE = "user_current_value";
    public static final String OPERATION_PORTFOLIO_VALUATION_SAVE = "portfolio_valuation_save";
    public static final String OPERATION_USER_VALUATION_SAVE = "user_valuation_save";

    private final MeterRegistry meterRegistry;
    private final Map<String, AtomicLong> lastListenerDurationNanos = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> lastServiceDurationNanos = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> lastEventAgeNanos = new ConcurrentHashMap<>();

    public ProfitWorkerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public Timer.Sample startSample() {
        return Timer.start(meterRegistry);
    }

    public void recordConsumed(String eventType, String result) {
        safeRecord(() -> meterRegistry.counter(EVENTS_CONSUMED,
                TAG_EVENT_TYPE, eventType,
                TAG_RESULT, result).increment());
    }

    public void recordSkipped(String eventType, String reason) {
        safeRecord(() -> meterRegistry.counter(EVENTS_SKIPPED,
                TAG_EVENT_TYPE, eventType,
                TAG_REASON, reason).increment());
    }

    public void recordListenerDuration(String eventType, String result, Timer.Sample sample) {
        safeRecord(() -> {
            long nanos = sample.stop(Timer.builder(LISTENER_DURATION)
                .tag(TAG_EVENT_TYPE, eventType)
                .tag(TAG_RESULT, result)
                .register(meterRegistry));
            updateLastDurationGauge(lastListenerDurationNanos, LAST_LISTENER_DURATION,
                    Tags.of(TAG_EVENT_TYPE, eventType, TAG_RESULT, result), nanos);
        });
    }

    public void recordServiceDuration(String operation, String result, Timer.Sample sample) {
        safeRecord(() -> {
            long nanos = sample.stop(Timer.builder(SERVICE_DURATION)
                .tag(TAG_OPERATION, operation)
                .tag(TAG_RESULT, result)
                .register(meterRegistry));
            updateLastDurationGauge(lastServiceDurationNanos, LAST_SERVICE_DURATION,
                    Tags.of(TAG_OPERATION, operation, TAG_RESULT, result), nanos);
        });
    }

    public void recordRedisDuration(String operation, String result, Timer.Sample sample) {
        safeRecord(() -> sample.stop(Timer.builder(REDIS_OPERATION_DURATION)
                .tag(TAG_OPERATION, operation)
                .tag(TAG_RESULT, result)
                .register(meterRegistry)));
    }

    public void recordRedisCommandDuration(String command, String result, Timer.Sample sample) {
        safeRecord(() -> sample.stop(Timer.builder(REDIS_COMMAND_DURATION)
                .tag(TAG_COMMAND, command)
                .tag(TAG_RESULT, result)
                .register(meterRegistry)));
    }

    public void recordAffectedPortfolios(String operation, long count) {
        safeRecord(() -> DistributionSummary.builder(AFFECTED_PORTFOLIOS)
                .tag(TAG_OPERATION, operation)
                .register(meterRegistry)
                .record(count));
    }

    public void recordAffectedUsers(String operation, long count) {
        safeRecord(() -> DistributionSummary.builder(AFFECTED_USERS)
                .tag(TAG_OPERATION, operation)
                .register(meterRegistry)
                .record(count));
    }

    public void recordEventAge(String eventType, Duration duration) {
        if (duration == null || duration.isNegative()) {
            return;
        }

        safeRecord(() -> {
            Timer.builder(EVENT_AGE)
                .tag(TAG_EVENT_TYPE, eventType)
                .register(meterRegistry)
                .record(duration);
            updateLastDurationGauge(lastEventAgeNanos, LAST_EVENT_AGE,
                    Tags.of(TAG_EVENT_TYPE, eventType), duration.toNanos());
        });
    }

    private void updateLastDurationGauge(
            Map<String, AtomicLong> values,
            String meterName,
            Tags tags,
            long nanos
    ) {
        String key = meterName + tags.stream()
                .map(tag -> tag.getKey() + "=" + tag.getValue())
                .reduce("", (left, right) -> left + "|" + right);
        AtomicLong holder = values.computeIfAbsent(key, ignored -> registerDurationGauge(meterName, tags));
        holder.set(nanos);
    }

    private AtomicLong registerDurationGauge(String meterName, Tags tags) {
        AtomicLong holder = new AtomicLong();
        Gauge.builder(meterName, holder, value -> value.get() / 1_000_000_000.0)
                .tags(tags)
                .register(meterRegistry);
        return holder;
    }

    private void safeRecord(Runnable recording) {
        try {
            recording.run();
        } catch (RuntimeException ignored) {
            // Metrics must never affect worker flow.
        }
    }
}
