package minispark.network;

import minispark.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message bus for communication between nodes in the MiniSpark cluster.
 * Handles message routing between the scheduler and workers.
 */
public class MessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);

    private final Map<NetworkEndpoint, MessageHandler> handlers = new ConcurrentHashMap<>();
    private final Map<NetworkEndpoint, Queue<MessageEnvelope>> messageQueues = new ConcurrentHashMap<>();
    private final Map<NetworkEndpoint, ExecutorService> executors = new ConcurrentHashMap<>();
    private final AtomicLong messageIdGenerator = new AtomicLong(0);
    private volatile boolean isRunning = false;

    public interface MessageHandler {
        void handleMessage(Message message, NetworkEndpoint sender);
    }

    private static class MessageEnvelope {
        final long messageId;
        final Message message;
        final NetworkEndpoint source;
        final NetworkEndpoint destination;

        MessageEnvelope(long messageId, Message message, NetworkEndpoint source, NetworkEndpoint destination) {
            this.messageId = messageId;
            this.message = message;
            this.source = source;
            this.destination = destination;
        }
    }

    public void start() {
        isRunning = true;
        for (NetworkEndpoint endpoint : handlers.keySet()) {
            startProcessingForEndpoint(endpoint);
        }
        logger.info("MessageBus started");
    }

    public void stop() {
        isRunning = false;
        for (ExecutorService executor : executors.values()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        executors.clear();
        logger.info("MessageBus stopped");
    }

    public void registerHandler(NetworkEndpoint endpoint, MessageHandler handler) {
        logger.info("Registering handler for endpoint: {}", endpoint);
        handlers.put(endpoint, handler);
        messageQueues.put(endpoint, new ConcurrentLinkedQueue<>());
        if (isRunning) {
            startProcessingForEndpoint(endpoint);
        }
    }

    public void unregisterHandler(NetworkEndpoint endpoint) {
        handlers.remove(endpoint);
        messageQueues.remove(endpoint);
        ExecutorService executor = executors.remove(endpoint);
        if (executor != null) {
            executor.shutdown();
        }
        logger.info("Unregistered handler for endpoint: {}", endpoint);
    }

    public void send(Message message, NetworkEndpoint source, NetworkEndpoint destination) {
        logger.debug("Queued message {} from {} to {}", message.getType(), source, destination);
        Queue<MessageEnvelope> queue = messageQueues.get(destination);
        if (queue != null) {
            queue.offer(new MessageEnvelope(messageIdGenerator.incrementAndGet(), message, source, destination));
        } else {
            logger.warn("No message queue found for endpoint: {}", destination);
        }
    }

    private void startProcessingForEndpoint(NetworkEndpoint endpoint) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executors.put(endpoint, executor);
        executor.submit(() -> processMessagesForEndpoint(endpoint));
    }

    private void processMessagesForEndpoint(NetworkEndpoint endpoint) {
        Queue<MessageEnvelope> queue = messageQueues.get(endpoint);
        while (isRunning) {
            MessageEnvelope envelope = queue.poll();
            if (envelope != null) {
                logger.debug("Processing message {} from {} to {}", 
                    envelope.message.getType(), envelope.source, envelope.destination);

                MessageHandler handler = handlers.get(endpoint);
                if (handler != null && endpoint.equals(envelope.destination)) {
                    try {
                        handler.handleMessage(envelope.message, envelope.source);
                        logger.debug("Successfully delivered message {} from {} to {}", 
                            envelope.message.getType(), envelope.source, envelope.destination);
                    } catch (Exception e) {
                        logger.error("Error processing message {} from {} to {}: {}", 
                            envelope.message.getType(), envelope.source, envelope.destination, e.getMessage(), e);
                    }
                } else {
                    Queue<MessageEnvelope> destinationQueue = messageQueues.get(envelope.destination);
                    if (destinationQueue != null) {
                        destinationQueue.offer(envelope);
                        logger.debug("Forwarded message {} from {} to {}", 
                            envelope.message.getType(), envelope.source, envelope.destination);
                    } else {
                        logger.warn("No message queue found for endpoint: {}", envelope.destination);
                    }
                }
            }
        }
    }
} 