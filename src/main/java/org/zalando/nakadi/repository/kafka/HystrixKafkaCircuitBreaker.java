package org.zalando.nakadi.repository.kafka;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class wraps Hystrix API to count success/failuresCount without using Hystrix command. In general we just use Hystrix to
 * collect metrics and check for opened circuit.
 */
public class HystrixKafkaCircuitBreaker {

    private static final int THRESHOLD_PERCENT = 20;
    private static final long REQUEST_THRESHOLD = 20;
    private static final long ROLLING_WINDOW = 10000;
    private static final long SLEEPING_WINDOW_MS = 5000;

    private final AtomicBoolean circuitOpen;
    private final AtomicLong totalCount;
    private final AtomicLong failuresCount;
    private final AtomicLong lastTimeOpened;
    private final AtomicLong totalRollingWindowCount;
    private final AtomicLong rollingWindowLastUpdate;

    public HystrixKafkaCircuitBreaker() {
        this.circuitOpen = new AtomicBoolean(false);
        this.totalCount = new AtomicLong(0);
        this.failuresCount = new AtomicLong(0);
        this.lastTimeOpened = new AtomicLong(0);
        this.totalRollingWindowCount = new AtomicLong(0);
        this.rollingWindowLastUpdate = new AtomicLong(0);
    }

    public boolean allowRequest() {
        return !isCircuitOpened() || allowSingleTest();
    }

    private boolean isCircuitOpened() {
        if (circuitOpen.get()) {
            return true;
        }

        final long currentTotalCount = totalCount.get();
        final long currentFailuresCount = failuresCount.get();

        if (currentTotalCount < REQUEST_THRESHOLD) {
            return false;
        }

        final int errorPercent = (int) ((double) currentFailuresCount / currentTotalCount * 100);
        if (errorPercent < THRESHOLD_PERCENT) {
            return false;
        }

        if (circuitOpen.compareAndSet(false, true)) {
            lastTimeOpened.set(System.currentTimeMillis());
            return true;
        }

        return true;
    }

    private boolean allowSingleTest() {
        final long currentLastTimeOpened = lastTimeOpened.get();
        if (circuitOpen.get() && System.currentTimeMillis() > currentLastTimeOpened + SLEEPING_WINDOW_MS) {
            if (lastTimeOpened.compareAndSet(currentLastTimeOpened, System.currentTimeMillis())) {
                return true;
            }
        }

        return false;
    }

    public void markSuccessfully() {
        circuitOpen.compareAndSet(true, false);
        long currentRollingWindowLast = rollingWindowLastUpdate.get();
        if (System.currentTimeMillis() > currentRollingWindowLast + ROLLING_WINDOW) {
            rollingWindowLastUpdate.compareAndSet(currentRollingWindowLast, System.currentTimeMillis());
            final long currentTotal = totalCount.addAndGet(-totalRollingWindowCount.get());
            if (currentTotal < 0) {
                totalCount.set(0);
            }
            totalRollingWindowCount.set(currentTotal);
        } else {
            totalCount.incrementAndGet();
        }
        failuresCount.set(0);
    }

    public void markFailure() {
        totalCount.incrementAndGet();
        failuresCount.incrementAndGet();
    }

    public String getMetrics() {
        final long currentTotal = totalCount.get();
        final long currentFailures = failuresCount.get();
        final StringBuilder sb = new StringBuilder("HystrixKafkaCircuitBreaker{");
        sb.append("totalCount=").append(currentTotal);
        sb.append(", failuresCount=").append(currentFailures);
        sb.append(", error=").append(currentTotal == 0 ? 0 : (int) ((double) currentFailures / currentTotal * 100));
        sb.append("%}");
        return sb.toString();
    }
}
