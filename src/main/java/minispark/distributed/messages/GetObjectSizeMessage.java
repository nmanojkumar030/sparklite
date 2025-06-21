package minispark.distributed.messages;

/**
 * Message for requesting the size of an object without downloading content.
 * Equivalent to HTTP HEAD request.
 */
public class GetObjectSizeMessage extends Message {
    private final String key;

    public GetObjectSizeMessage(String key, String correlationId) {
        super(MessageType.GET_OBJECT_SIZE, correlationId);
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return String.format("GetObjectSizeMessage{key='%s', correlationId='%s'}", 
            key, getCorrelationId());
    }
} 