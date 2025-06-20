package minispark.util;

import minispark.network.MessageBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

/**
 * Utility methods for testing with deterministic simulation.
 * These utilities help ensure proper tick progression and future completion.
 */
public class TestUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);
    
    /**
     * Runs the simulation until a condition is met or timeout occurs.
     * This drives messageBus.tick() in a loop to ensure deterministic execution.
     * 
     * @param messageBus The message bus to drive
     * @param condition The condition to wait for
     * @param timeout Maximum time to wait
     * @return true if condition was met, false if timeout occurred
     */
    public static boolean runUntil(MessageBus messageBus, BooleanSupplier condition, Duration timeout) {
        Instant start = Instant.now();
        
        while (!condition.getAsBoolean()) {
            if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                logger.warn("TestUtils.runUntil timed out after {}", timeout);
                return false;
            }
            
            messageBus.tick();
            
            // Small yield to prevent tight loops in tests
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Convenience method with default 30-second timeout.
     */
    public static boolean runUntil(MessageBus messageBus, BooleanSupplier condition) {
        return runUntil(messageBus, condition, Duration.ofSeconds(30));
    }
    
    /**
     * BLOCKING FIX: Combines multiple futures using the recommended allOf() + join() pattern.
     * This is safe because allOf() guarantees completion before join() is called.
     * 
     * @param futures List of futures to combine
     * @return CompletableFuture containing list of results
     */
    public static <T> CompletableFuture<List<T>> combineAllOf(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                // join() is non-blocking here because allOf() guarantees completion
                // This also avoids checked exceptions compared to get()
                return futures.stream()
                    .map(CompletableFuture::join)
                    .toList();
            });
    }
    
    /**
     * BLOCKING FIX: Alternative version that filters out null results.
     * Useful when some futures might fail and return null.
     */
    public static <T> CompletableFuture<List<T>> combineAllOfNonNull(List<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                return futures.stream()
                    .map(CompletableFuture::join)
                    .filter(result -> result != null)
                    .toList();
            });
    }
} 