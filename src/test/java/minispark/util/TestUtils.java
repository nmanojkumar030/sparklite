package minispark.util;

import minispark.network.MessageBus;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Test helper utilities shared across MiniSpark test suites.
 */
public final class TestUtils {
    private TestUtils() {}

    /**
     * Drives the {@link MessageBus} until the supplied {@code condition} becomes {@code true} or the
     * {@code timeout} elapses. Mimics TigerBeetle's approach of advancing the event-loop instead of
     * sleeping for a fixed interval.
     *
     * @param bus      message bus whose {@code tick()} method advances simulated time
     * @param condition predicate to satisfy
     * @param timeout  maximum wall-clock duration to wait
     * @throws IllegalStateException if the timeout expires before the condition is met
     */
    public static void runUntil(MessageBus bus,
                                BooleanSupplier condition,
                                Duration timeout) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            bus.tick();
            // Yield briefly to avoid tight CPU loop.
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Condition not met within " + timeout);
            }
        }
    }

    /** Convenience overload with a 30-second timeout. */
    public static void runUntil(MessageBus bus, BooleanSupplier condition) {
        runUntil(bus, condition, Duration.ofSeconds(30));
    }
}
