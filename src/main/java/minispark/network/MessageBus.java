package minispark.network;

import minispark.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message bus for communication between nodes in the MiniSpark cluster.
 * Handles message routing between the scheduler and workers.
 */
public class MessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);

    private final Map<NetworkEndpoint, MessageHandler> handlers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final BlockingQueue<MessageEnvelope> messageQueue = new LinkedBlockingQueue<>();
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
        executor.submit(this::processMessages);
        logger.info("MessageBus started");
    }

    public void stop() {
        isRunning = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("MessageBus stopped");
    }

    public void registerHandler(NetworkEndpoint endpoint, MessageHandler handler) {
        handlers.put(endpoint, handler);
        logger.info("Registered handler for endpoint: {}", endpoint);
    }

    public void unregisterHandler(NetworkEndpoint endpoint) {
        handlers.remove(endpoint);
        logger.info("Unregistered handler for endpoint: {}", endpoint);
    }

    public void send(Message message, NetworkEndpoint source, NetworkEndpoint destination) {
        long messageId = messageIdGenerator.incrementAndGet();
        MessageEnvelope envelope = new MessageEnvelope(messageId, message, source, destination);
        messageQueue.offer(envelope);
        logger.debug("Queued message {} from {} to {}", messageId, source, destination);
    }

    private void processMessages() {
        while (isRunning) {
            try {
                MessageEnvelope envelope = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (envelope != null) {
                    MessageHandler handler = handlers.get(envelope.destination);
                    if (handler != null) {
                        try {
                            handler.handleMessage(envelope.message, envelope.source);
                            logger.debug("Delivered message {} from {} to {}", 
                                envelope.messageId, envelope.source, envelope.destination);
                        } catch (Exception e) {
                            logger.error("Error processing message {} from {} to {}: {}", 
                                envelope.messageId, envelope.source, envelope.destination, e.getMessage());
                        }
                    } else {
                        logger.warn("No handler found for message {} to {}", 
                            envelope.messageId, envelope.destination);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
} 