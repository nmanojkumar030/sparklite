package minispark.network;

import minispark.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Simulates a network with configurable conditions like message loss, latency, and partitioning.
 * This can be used to test the system under various network conditions.
 */
public class SimulatedNetwork {
    private static final Logger logger = LoggerFactory.getLogger(SimulatedNetwork.class);

    // Default values
    private static final double DEFAULT_MESSAGE_LOSS_RATE = 0.0;
    private static final int DEFAULT_MIN_LATENCY = 0;
    private static final int DEFAULT_MAX_LATENCY = 0;
    private static final int DEFAULT_MAX_MESSAGES_PER_TICK = Integer.MAX_VALUE;

    // Configuration
    private volatile double messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
    private volatile int minLatencyTicks = DEFAULT_MIN_LATENCY;
    private volatile int maxLatencyTicks = DEFAULT_MAX_LATENCY;
    private volatile int maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;

    // Current simulation tick
    private long currentTick = 0;

    // Message queue for scheduling and delivering messages
    private final PriorityBlockingQueue<ScheduledMessage> messageQueue;
    
    // Message sequence counter for FIFO ordering when messages have the same delivery tick
    private final AtomicLong messageSequence = new AtomicLong(0);

    // Map of disconnected endpoints - use LinkedHashMap for deterministic iteration
    private final Map<NetworkEndpoint, Set<NetworkEndpoint>> disconnectedEndpoints = new LinkedHashMap<>();

    // Message delivery callback
    private final BiConsumer<MessageEnvelope, DeliveryContext> messageDeliveryCallback;

    // Random number generator - seeded for deterministic behavior
    private final Random random;

    /**
     * Delivery context provided when delivering a message.
     */
    public static class DeliveryContext {
        private final NetworkEndpoint source;
        private final NetworkEndpoint destination;

        public DeliveryContext(NetworkEndpoint source, NetworkEndpoint destination) {
            this.source = source;
            this.destination = destination;
        }

        public NetworkEndpoint getSource() {
            return source;
        }

        public NetworkEndpoint getDestination() {
            return destination;
        }
    }

    /**
     * Class representing a scheduled message.
     */
    private static class ScheduledMessage implements Comparable<ScheduledMessage> {
        final MessageEnvelope envelope;
        final long deliveryTick;
        final long sequenceNumber;

        public ScheduledMessage(MessageEnvelope envelope, long deliveryTick, long sequenceNumber) {
            this.envelope = envelope;
            this.deliveryTick = deliveryTick;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public int compareTo(ScheduledMessage other) {
            int tickCompare = Long.compare(this.deliveryTick, other.deliveryTick);
            if (tickCompare != 0) {
                return tickCompare;
            }
            return Long.compare(this.sequenceNumber, other.sequenceNumber);
        }
    }

    /**
     * Creates a new SimulatedNetwork with the specified message delivery callback.
     * Uses a default seed of 0 for deterministic behavior.
     *
     * @param messageDeliveryCallback The callback to invoke when messages are delivered
     */
    public SimulatedNetwork(BiConsumer<MessageEnvelope, DeliveryContext> messageDeliveryCallback) {
        this(messageDeliveryCallback, 0L);
    }

    /**
     * Creates a new SimulatedNetwork with the specified message delivery callback and random seed.
     *
     * @param messageDeliveryCallback The callback to invoke when messages are delivered
     * @param randomSeed The seed for the random number generator to ensure deterministic behavior
     */
    public SimulatedNetwork(BiConsumer<MessageEnvelope, DeliveryContext> messageDeliveryCallback, long randomSeed) {
        if (messageDeliveryCallback == null) {
            throw new IllegalArgumentException("Message delivery callback cannot be null");
        }
        this.messageDeliveryCallback = messageDeliveryCallback;
        this.messageQueue = new PriorityBlockingQueue<>();
        this.random = new Random(randomSeed);
        logger.info("SimulatedNetwork initialized with random seed: {}", randomSeed);
    }

    /**
     * Configures the message loss rate for the network.
     *
     * @param rate A value between 0.0 (no loss) and 1.0 (all messages lost)
     * @return This SimulatedNetwork instance for method chaining
     */
    public SimulatedNetwork withMessageLossRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException("Message loss rate must be between 0.0 and 1.0");
        }
        messageLossRate = rate;
        return this;
    }

    /**
     * Configures the latency range for message delivery in ticks.
     *
     * @param minTicks Minimum latency in ticks
     * @param maxTicks Maximum latency in ticks
     * @return This SimulatedNetwork instance for method chaining
     */
    public SimulatedNetwork withLatency(int minTicks, int maxTicks) {
        if (minTicks < 0 || maxTicks < 0) {
            throw new IllegalArgumentException("Latency ticks cannot be negative");
        }
        if (minTicks > maxTicks) {
            throw new IllegalArgumentException("Minimum latency cannot be greater than maximum latency");
        }
        minLatencyTicks = minTicks;
        maxLatencyTicks = maxTicks;
        return this;
    }

    /**
     * Configures the maximum number of messages that can be processed per tick.
     *
     * @param maxMessages Maximum messages per tick
     * @return This SimulatedNetwork instance for method chaining
     */
    public SimulatedNetwork withBandwidthLimit(int maxMessages) {
        if (maxMessages < 0) {
            throw new IllegalArgumentException("Maximum messages per tick cannot be negative");
        }
        maxMessagesPerTick = maxMessages;
        return this;
    }

    /**
     * Resets the simulator to default settings.
     */
    public synchronized SimulatedNetwork reset() {
        currentTick = 0;
        messageLossRate = DEFAULT_MESSAGE_LOSS_RATE;
        minLatencyTicks = DEFAULT_MIN_LATENCY;
        maxLatencyTicks = DEFAULT_MAX_LATENCY;
        maxMessagesPerTick = DEFAULT_MAX_MESSAGES_PER_TICK;
        messageQueue.clear();
        disconnectedEndpoints.clear();
        logger.info("Reset network state");
        return this;
    }

    /**
     * Creates a bidirectional disconnect between two endpoints.
     * Messages sent between these endpoints in either direction will be dropped.
     *
     * @param endpoint1 First endpoint
     * @param endpoint2 Second endpoint
     */
    public void disconnectEndpointsBidirectional(NetworkEndpoint endpoint1, NetworkEndpoint endpoint2) {
        disconnectedEndpoints.computeIfAbsent(endpoint1, k -> new LinkedHashSet<>()).add(endpoint2);
        disconnectedEndpoints.computeIfAbsent(endpoint2, k -> new LinkedHashSet<>()).add(endpoint1);
        logger.info("Disconnected endpoints bidirectionally: {} <-> {}", endpoint1, endpoint2);
    }

    /**
     * Removes all network partitions, effectively reconnecting all endpoints.
     */
    public void reconnectAll() {
        disconnectedEndpoints.clear();
        logger.info("Cleared all network disconnections");
    }

    /**
     * Checks if two endpoints can communicate.
     *
     * @param source      Source endpoint
     * @param destination Target endpoint
     * @return true if messages can be delivered from source to destination
     */
    public boolean canCommunicate(NetworkEndpoint source, NetworkEndpoint destination) {
        Set<NetworkEndpoint> disconnectedFromSource = disconnectedEndpoints.getOrDefault(source, Collections.emptySet());
        return !disconnectedFromSource.contains(destination);
    }

    /**
     * Sends a message through the simulated network.
     *
     * @param envelope The message envelope to send
     * @return true if the message will be delivered (now or later), false if it was dropped
     */
    public boolean sendMessage(MessageEnvelope envelope) {
        if (envelope == null) {
            throw new IllegalArgumentException("Message envelope cannot be null");
        }
        
        NetworkEndpoint source = envelope.source;
        NetworkEndpoint destination = envelope.destination;
        
        // Check if endpoints can communicate
        if (!canCommunicate(source, destination)) {
            logger.debug("Message dropped due to network partition: {} -> {}, type: {}", 
                source, destination, envelope.message.getType());
            return false;
        }
        
        // Apply message loss based on configured rate
        if (messageLossRate > 0 && random.nextDouble() < messageLossRate) {
            logger.debug("Message dropped due to random loss: {} -> {}, type: {}, loss rate: {}", 
                source, destination, envelope.message.getType(), messageLossRate);
            return false;
        }
        
        // Calculate delivery tick with latency
        long deliveryTick = calculateDeliveryTick();
        
        // Schedule the message
        messageQueue.add(new ScheduledMessage(envelope, deliveryTick, messageSequence.getAndIncrement()));
        
        logger.debug("Message from {} to {} scheduled for delivery at tick {}, type: {}", 
            source, destination, deliveryTick, envelope.message.getType());
        return true;
    }

    /**
     * Calculates the delivery tick for a message based on the current tick and latency settings.
     * Uses seeded random for deterministic behavior.
     *
     * @return The tick at which the message should be delivered
     */
    private long calculateDeliveryTick() {
        int delay;
        
        if (minLatencyTicks == maxLatencyTicks) {
            delay = minLatencyTicks;
        } else {
            // Use seeded random instead of ThreadLocalRandom for deterministic behavior
            delay = minLatencyTicks + random.nextInt(maxLatencyTicks - minLatencyTicks + 1);
        }
        
        return currentTick + Math.max(delay, 1); // At minimum, deliver on next tick
    }

    /**
     * Advances the simulation by one tick and processes any messages due for delivery.
     *
     * @return The number of messages delivered during this tick
     */
    public int tick() {
        currentTick++;
        
        logger.debug("SimulatedNetwork advanced to tick {}", currentTick);
        
        // Get messages scheduled for this tick
        List<ScheduledMessage> messagesForThisTick = new ArrayList<>();
        while (!messageQueue.isEmpty() && messageQueue.peek().deliveryTick <= currentTick) {
            ScheduledMessage message = messageQueue.poll();
            if (messagesForThisTick.size() < maxMessagesPerTick) {
                messagesForThisTick.add(message);
            } else {
                // Reschedule for next tick if bandwidth limit reached
                messageQueue.add(new ScheduledMessage(message.envelope, currentTick + 1, messageSequence.getAndIncrement()));
            }
        }
        
        // Process messages
        int messagesDelivered = 0;
        for (ScheduledMessage scheduledMessage : messagesForThisTick) {
            MessageEnvelope envelope = scheduledMessage.envelope;
            
            // Check if endpoints can still communicate (partitions might have changed)
            if (!canCommunicate(envelope.source, envelope.destination)) {
                logger.debug("Delayed message dropped due to network partition: {} -> {}, type: {}", 
                    envelope.source, envelope.destination, envelope.message.getType());
                continue;
            }
            
            // Deliver the message via callback
            DeliveryContext context = new DeliveryContext(envelope.source, envelope.destination);
            logger.debug("Delivering message from {} to {} at tick {}, type: {}", 
                envelope.source, envelope.destination, currentTick, envelope.message.getType());
            
            messageDeliveryCallback.accept(envelope, context);
            messagesDelivered++;
        }
        
        return messagesDelivered;
    }

    /**
     * Gets the current value of the tick counter.
     *
     * @return The current tick
     */
    public long getCurrentTick() {
        return currentTick;
    }
    
    /**
     * Gets the current size of the message queue.
     *
     * @return The number of messages in the queue
     */
    public int getQueueSize() {
        return messageQueue.size();
    }
} 