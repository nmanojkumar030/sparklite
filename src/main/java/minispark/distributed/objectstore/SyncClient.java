package minispark.distributed.objectstore;

import minispark.distributed.network.MessageBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.TimeUnit;

/**
 * Synchronous wrapper around the async Client that uses MessageBus tick progression
 * to complete futures. This allows RDD compute methods to use clean sequential code
 * while maintaining deterministic execution through the tick system.
 */
public class SyncClient {
    private static final Logger logger = LoggerFactory.getLogger(SyncClient.class);
    
    private final Client asyncClient;
    private final MessageBus messageBus;
    
    public SyncClient(Client asyncClient, MessageBus messageBus) {
        this.asyncClient = asyncClient;
        this.messageBus = messageBus;
    }
    
    /**
     * Synchronously puts an object, progressing ticks until completion.
     */
    public void putObject(String key, byte[] data) {
        CompletableFuture<Void> future = asyncClient.putObject(key, data);
        awaitCompletion(future);
        logger.debug("PUT_OBJECT completed synchronously for key {}", key);
    }
    
    /**
     * Synchronously gets an object, progressing ticks until completion.
     */
    public byte[] getObject(String key) {
        CompletableFuture<byte[]> future = asyncClient.getObject(key);
        byte[] result = awaitCompletion(future);
        logger.debug("GET_OBJECT completed synchronously for key {}", key);
        return result;
    }
    
    /**
     * Synchronously gets an object range, progressing ticks until completion.
     */
    public byte[] getObjectRange(String key, long startByte, long endByte) {
        CompletableFuture<byte[]> future = asyncClient.getObjectRange(key, startByte, endByte);
        byte[] result = awaitCompletion(future);
        logger.debug("GET_OBJECT_RANGE completed synchronously for key {} range {}-{}", 
            key, startByte, endByte);
        return result;
    }
    
    /**
     * Synchronously gets object size, progressing ticks until completion.
     */
    public long getObjectSize(String key) {
        CompletableFuture<Long> future = asyncClient.getObjectSize(key);
        Long result = awaitCompletion(future);
        logger.debug("GET_OBJECT_SIZE completed synchronously for key {}", key);
        return result;
    }
    
    /**
     * Synchronously deletes an object, progressing ticks until completion.
     */
    public void deleteObject(String key) {
        CompletableFuture<Void> future = asyncClient.deleteObject(key);
        awaitCompletion(future);
        logger.debug("DELETE_OBJECT completed synchronously for key {}", key);
    }
    
    /**
     * Synchronously lists objects, progressing ticks until completion.
     */
    public List<String> listObjects(String prefix) {
        CompletableFuture<List<String>> future = asyncClient.listObjects(prefix);
        List<String> result = awaitCompletion(future);
        logger.debug("LIST_OBJECTS completed synchronously for prefix {}", prefix);
        return result;
    }
    
    /**
     * Core method that drives tick progression until a future completes.
     * This is where the "synchronous" behavior is implemented through
     * deterministic tick advancement.
     */
    private <T> T awaitCompletion(CompletableFuture<T> future) {
        logger.debug("Awaiting completion of future with tick progression");
        
        while (!future.isDone()) {
            messageBus.tick();
            
            // Brief yield to avoid tight CPU loop - this is optional
            // and can be removed for maximum determinism
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        
        try {
            T result = future.join();
            logger.debug("Future completed successfully");
            return result;
        } catch (Exception e) {
            logger.error("Future completed with exception: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * Get access to the underlying async client if needed.
     */
    public Client getAsyncClient() {
        return asyncClient;
    }
    
    /**
     * Get access to the MessageBus if needed.
     */
    public MessageBus getMessageBus() {
        return messageBus;
    }
} 