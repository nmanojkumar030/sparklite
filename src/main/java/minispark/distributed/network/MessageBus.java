package minispark.distributed.network;

import minispark.distributed.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Message bus for communication between nodes in the MiniSpark cluster.
 * Uses SimulatedNetwork to provide configurable network conditions for testing.
 */
public class MessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);

    private final Map<NetworkEndpoint, MessageHandler> handlers = new LinkedHashMap<>();
    private long messageIdGenerator = 0;
    // Network simulator for realistic network behavior
    private final SimulatedNetwork network;
    private boolean started = false;
    private final long seed;

    public void tick() {
        if (!started) {
            return;
        }
        
        network.tick();
    }

    public interface MessageHandler {
        void handleMessage(Message message, NetworkEndpoint sender);
    }

    /**
     * Creates a MessageBus with default network settings.
     */
    public MessageBus() {
        this(0L);
    }

    /**
     * SEED CONFIGURATION: Constructor with configurable seed for fuzz testing.
     * This allows fuzz tests to supply any seed and store it with their history
     * for reproducible test failures.
     * 
     * @param seed Random seed for deterministic but varied behavior
     */
    public MessageBus(long seed) {
        this.seed = seed;
        this.network = new SimulatedNetwork(this::deliverMessage, seed);
        logger.info("MessageBus initialized with seed: {}", seed);
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
        started = true;
        logger.info("MessageBus started with seed {}", seed);
    }


    /**
     * Stops the message bus and the network simulator.
     */
    public void stop() {
        started = false;
        handlers.clear();
        logger.info("MessageBus stopped");
    }

    /**
     * Registers a handler for a specific endpoint.
     */
    public void registerHandler(NetworkEndpoint endpoint, MessageHandler handler) {
        handlers.put(endpoint, handler);
        logger.debug("Registered handler for endpoint: {}", endpoint);
    }

    /**
     * Unregisters a handler for a specific endpoint.
     */
    public void unregisterHandler(NetworkEndpoint endpoint) {
        handlers.remove(endpoint);
        logger.debug("Unregistered handler for endpoint: {}", endpoint);
    }

    /**
     * Sends a message from a source endpoint to a destination endpoint.
     */
    public void send(Message message, NetworkEndpoint source, NetworkEndpoint destination) {
        if (!started) {
            throw new IllegalStateException("MessageBus not started");
        }
        
        long messageId = ++messageIdGenerator;
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
        messageIdGenerator = 0;
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
    
    /**
     * SEED CONFIGURATION: Get the seed used by this MessageBus.
     * Useful for logging and test result correlation.
     */
    public long getSeed() {
        return seed;
    }
    
    /**
     * SEED CONFIGURATION: Builder pattern for easy configuration.
     */
    public static class Builder {
        private long seed = 0L;
        
        public Builder withSeed(long seed) {
            this.seed = seed;
            return this;
        }
        
        public Builder withRandomSeed() {
            this.seed = System.nanoTime();
            return this;
        }
        
        public MessageBus build() {
            return new MessageBus(seed);
        }
    }
    
    /**
     * SEED CONFIGURATION: Create a builder for fluent configuration.
     */
    public static Builder builder() {
        return new Builder();
    }
} 