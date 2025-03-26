package minispark.messages;

public class GetObjectResponseMessage extends Message {
    private final String key;
    private final byte[] data;
    private final boolean success;
    private final String errorMessage;

    public GetObjectResponseMessage(String key, byte[] data, boolean success, String errorMessage) {
        super(MessageType.GET_OBJECT_RESPONSE);
        this.key = key;
        this.data = data;
        this.success = success;
        this.errorMessage = errorMessage;
    }
    
    public GetObjectResponseMessage(String key, byte[] data, boolean success, String errorMessage, String correlationId) {
        super(MessageType.GET_OBJECT_RESPONSE, correlationId);
        this.key = key;
        this.data = data;
        this.success = success;
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
} 