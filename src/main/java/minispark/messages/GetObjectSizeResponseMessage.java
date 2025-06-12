package minispark.messages;

/**
 * Response message for object size requests.
 */
public class GetObjectSizeResponseMessage extends Message {
    private final String key;
    private final long size;
    private final boolean success;
    private final String errorMessage;

    /**
     * Create a successful size response.
     */
    public GetObjectSizeResponseMessage(String key, long size, String correlationId) {
        super(MessageType.GET_OBJECT_SIZE_RESPONSE, correlationId);
        this.key = key;
        this.size = size;
        this.success = true;
        this.errorMessage = null;
    }

    /**
     * Create an error size response.
     */
    public GetObjectSizeResponseMessage(String key, String errorMessage, String correlationId) {
        super(MessageType.GET_OBJECT_SIZE_RESPONSE, correlationId);
        this.key = key;
        this.size = -1;
        this.success = false;
        this.errorMessage = errorMessage;
    }

    public String getKey() {
        return key;
    }

    public long getSize() {
        return size;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return String.format("GetObjectSizeResponseMessage{key='%s', success=%s, size=%d}", 
            key, success, size);
    }
} 