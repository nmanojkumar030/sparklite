package minispark.messages;

/**
 * Message for requesting a specific byte range from an object store.
 * Similar to HTTP Range requests (Range: bytes=start-end).
 */
public class GetObjectRangeMessage extends Message {
    private final String key;
    private final long startByte;
    private final long endByte;

    /**
     * Create a range request message.
     * 
     * @param key The object key
     * @param startByte Starting byte position (inclusive, 0-based)
     * @param endByte Ending byte position (inclusive), or -1 for end of file
     * @param correlationId Unique request identifier
     */
    public GetObjectRangeMessage(String key, long startByte, long endByte, String correlationId) {
        super(MessageType.GET_OBJECT_RANGE, correlationId);
        this.key = key;
        this.startByte = startByte;
        this.endByte = endByte;
    }

    public String getKey() {
        return key;
    }

    public long getStartByte() {
        return startByte;
    }

    public long getEndByte() {
        return endByte;
    }

    @Override
    public String toString() {
        return String.format("GetObjectRangeMessage{key='%s', range=%d-%d, correlationId='%s'}", 
            key, startByte, endByte, getCorrelationId());
    }
} 