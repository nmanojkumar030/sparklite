package minispark.util;

import minispark.distributed.network.MessageBus;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Deterministic simulation runner using TigerBeetle's tick-based pattern.
 *
 * <p>This class implements TigerBeetle's approach to distributed system testing,
 * where time advances in discrete, deterministic steps called "ticks" rather
 * than relying on wall-clock time or traditional event loops.
 *
 * <h3>What happens in each tick:</h3>
 * <ul>
 *   <li><strong>Time Progression:</strong> Logical time advances by one unit</li>
 *   <li><strong>Message Delivery:</strong> Network messages are sent and delivered</li>
 *   <li><strong>Message Processing:</strong> Server nodes process delivered messages</li>
 *   <li><strong>Response Generation:</strong> Servers put response messages on the network</li>
 *   <li><strong>State Consistency:</strong> System reaches a consistent state</li>
 * </ul>
 *
 * <p>This tick-based approach ensures that distributed system interactions happen
 * in a predictable, reproducible sequence. Each tick represents a complete
 * message exchange cycle in the simulated network.
 *
 * <h3>Advantages of Tick-Based Testing</h3>
 * <ul>
 *   <li><strong>Deterministic:</strong> Same sequence of ticks produces identical results</li>
 *   <li><strong>Fast:</strong> No waiting for real network delays or timeouts</li>
 *   <li><strong>Controllable:</strong> Tests precisely control when interactions occur</li>
 *   <li><strong>Reproducible:</strong> Results independent of system load or timing</li>
 * </ul>
 */
public final class EventLoopRunner {
    private EventLoopRunner() {}

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
