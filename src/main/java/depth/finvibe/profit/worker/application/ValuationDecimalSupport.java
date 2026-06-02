package depth.finvibe.profit.worker.application;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class ValuationDecimalSupport {

    private ValuationDecimalSupport() {
    }

    public static BigDecimal decimalOf(Long value) {
        return value == null ? BigDecimal.ZERO : BigDecimal.valueOf(value);
    }

    public static long toWholeNumber(BigDecimal value) {
        return normalized(value).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    public static BigDecimal normalized(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value.stripTrailingZeros();
    }
}
