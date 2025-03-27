package minispark.network;

import minispark.messages.Message;

/**
 * Represents a message envelope containing a message along with source and destination information.
 */
public class MessageEnvelope {
    public final long messageId;
    public final Message message;
    public final NetworkEndpoint source;
    public final NetworkEndpoint destination;

    public MessageEnvelope(long messageId, Message message, NetworkEndpoint source, NetworkEndpoint destination) {
        this.messageId = messageId;
        this.message = message;
        this.source = source;
        this.destination = destination;
    }
} 