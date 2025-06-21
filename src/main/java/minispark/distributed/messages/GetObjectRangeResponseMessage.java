package minispark.distributed.messages;

/**
 * Response message for object range requests.
 */
public class GetObjectRangeResponseMessage extends Message {
    private final String key;
    private final byte[] data;
    private final boolean success;
    private final String errorMessage;
    private final long startByte;
    private final long endByte;

    /**
     * Create a successful range response.
     */
    public GetObjectRangeResponseMessage(String key, byte[] data, long startByte, long endByte, String correlationId) {
        super(MessageType.GET_OBJECT_RANGE_RESPONSE, correlationId);
        this.key = key;
        this.data = data;
        this.startByte = startByte;
        this.endByte = endByte;
        this.success = true;
        this.errorMessage = null;
    }

    /**
     * Create an error range response.
     */
    public GetObjectRangeResponseMessage(String key, String errorMessage, long startByte, long endByte, String correlationId) {
        super(MessageType.GET_OBJECT_RANGE_RESPONSE, correlationId);
        this.key = key;
        this.data = null;
        this.startByte = startByte;
        this.endByte = endByte;
        this.success = false;
        this.errorMessage = errorMessage;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getStartByte() {
        return startByte;
    }

    public long getEndByte() {
        return endByte;
    }

    @Override
    public String toString() {
        return String.format("GetObjectRangeResponseMessage{key='%s', range=%d-%d, success=%s, dataSize=%d}", 
            key, startByte, endByte, success, data != null ? data.length : 0);
    }
} 