package minispark.distributed.messages;

public class ListObjectsMessage extends Message {
    private final String prefix;
    private final String correlationId;

    public ListObjectsMessage(String prefix) {
        super(MessageType.LIST_OBJECTS);
        this.prefix = prefix;
        this.correlationId = null;
    }

    public ListObjectsMessage(String prefix, String correlationId) {
        super(MessageType.LIST_OBJECTS);
        this.prefix = prefix;
        this.correlationId = correlationId;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getCorrelationId() {
        return correlationId;
    }
} 