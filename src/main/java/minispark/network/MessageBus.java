package minispark.network;

import minispark.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message bus for communication between nodes in the MiniSpark cluster.
 * Uses SimulatedNetwork to provide configurable network conditions for testing.
 */
public class MessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);

    private final Map<NetworkEndpoint, MessageHandler> handlers = new ConcurrentHashMap<>();
    private final AtomicLong messageIdGenerator = new AtomicLong(0);
    private volatile boolean isRunning = false;
    
    // Network simulator for realistic network behavior
    private final SimulatedNetwork network;
    
    // Scheduler for ticking the network simulator
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> tickTask;
    
    // Tick interval in milliseconds
    private static final long DEFAULT_TICK_INTERVAL_MS = 100;
    private long tickIntervalMs = DEFAULT_TICK_INTERVAL_MS;

    public interface MessageHandler {
        void handleMessage(Message message, NetworkEndpoint sender);
    }

    /**
     * Creates a MessageBus with default network settings.
     */
    public MessageBus() {
        this.network = new SimulatedNetwork(this::deliverMessage);
    }

    /**
     * Sets the network tick interval.
     * 
     * @param intervalMs The interval between ticks in milliseconds
     */
    public void setTickInterval(long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("Tick interval must be positive");
        }
        this.tickIntervalMs = intervalMs;
        
        // Restart tick task if running
        if (isRunning && tickTask != null) {
            tickTask.cancel(false);
            startTickTask();
        }
    }
    
    /**
     * Configures the message loss rate for the network.
     *
     * @param rate A value between 0.0 (no loss) and 1.0 (all messages lost)
     */
    public void setMessageLossRate(double rate) {
        network.withMessageLossRate(rate);
    }

    /**
     * Configures the latency range for message delivery.
     *
     * @param minTicks Minimum latency in ticks
     * @param maxTicks Maximum latency in ticks
     */
    public void setNetworkLatency(int minTicks, int maxTicks) {
        network.withLatency(minTicks, maxTicks);
    }

    /**
     * Creates a network partition between two endpoints.
     * Messages sent between these endpoints will be dropped.
     *
     * @param endpoint1 First endpoint
     * @param endpoint2 Second endpoint
     */
    public void disconnectEndpoints(NetworkEndpoint endpoint1, NetworkEndpoint endpoint2) {
        network.disconnectEndpointsBidirectional(endpoint1, endpoint2);
    }

    /**
     * Removes all network partitions.
     */
    public void reconnectAllEndpoints() {
        network.reconnectAll();
    }

    /**
     * Starts the message bus and the network simulator.
     */
    public void start() {
        isRunning = true;
        startTickTask();
        logger.info("MessageBus started");
    }

    private void startTickTask() {
        tickTask = scheduler.scheduleAtFixedRate(
            () -> {
                try {
                    int delivered = network.tick();
                    if (delivered > 0) {
                        logger.debug("Network tick delivered {} messages", delivered);
                    }
                } catch (Exception e) {
                    logger.error("Error during network tick", e);
                }
            },
            tickIntervalMs, tickIntervalMs, TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stops the message bus and the network simulator.
     */
    public void stop() {
        isRunning = false;
        if (tickTask != null) {
            tickTask.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("MessageBus stopped");
    }

    /**
     * Registers a handler for a specific endpoint.
     */
    public void registerHandler(NetworkEndpoint endpoint, MessageHandler handler) {
        logger.info("Registering handler for endpoint: {}", endpoint);
        handlers.put(endpoint, handler);
    }

    /**
     * Unregisters a handler for a specific endpoint.
     */
    public void unregisterHandler(NetworkEndpoint endpoint) {
        handlers.remove(endpoint);
        logger.info("Unregistered handler for endpoint: {}", endpoint);
    }

    /**
     * Sends a message from a source endpoint to a destination endpoint.
     */
    public void send(Message message, NetworkEndpoint source, NetworkEndpoint destination) {
        if (!isRunning) {
            logger.warn("Cannot send message when MessageBus is not running");
            return;
        }
        
        long messageId = messageIdGenerator.incrementAndGet();
        MessageEnvelope envelope = new MessageEnvelope(messageId, message, source, destination);
        
        boolean scheduled = network.sendMessage(envelope);
        if (scheduled) {
            logger.debug("Queued message {} from {} to {}", message.getType(), source, destination);
        } else {
            logger.debug("Message {} from {} to {} was dropped", message.getType(), source, destination);
        }
    }

    /**
     * Delivers a message to its destination handler.
     * This method is called by the SimulatedNetwork when a message is ready to be delivered.
     */
    private void deliverMessage(MessageEnvelope envelope, SimulatedNetwork.DeliveryContext context) {
        NetworkEndpoint destination = envelope.destination;
        MessageHandler handler = handlers.get(destination);
        
        if (handler != null) {
            try {
                handler.handleMessage(envelope.message, envelope.source);
                logger.debug("Successfully delivered message {} from {} to {}", 
                    envelope.message.getType(), envelope.source, envelope.destination);
            } catch (Exception e) {
                logger.error("Error processing message {} from {} to {}: {}", 
                    envelope.message.getType(), envelope.source, envelope.destination, e.getMessage(), e);
            }
        } else {
            logger.warn("No handler found for endpoint: {}", destination);
        }
    }
    
    /**
     * Resets the network state and message bus.
     */
    public void reset() {
        network.reset();
        messageIdGenerator.set(0);
        logger.info("MessageBus reset");
    }
    
    /**
     * Gets the current network tick.
     */
    public long getCurrentTick() {
        return network.getCurrentTick();
    }
    
    /**
     * Gets the number of messages currently in the queue.
     */
    public int getQueueSize() {
        return network.getQueueSize();
    }
} 