package minispark.distributed.messages;

import java.io.Serializable;

public abstract class Message implements Serializable {
    private final MessageType type;
    private final String correlationId;

    protected Message(MessageType type) {
        this.type = type;
        this.correlationId = null;
    }
    
    protected Message(MessageType type, String correlationId) {
        this.type = type;
        this.correlationId = correlationId;
    }

    public MessageType getType() {
        return type;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
} 