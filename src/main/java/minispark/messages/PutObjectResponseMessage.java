package minispark.messages;

public class PutObjectResponseMessage extends Message {
    private final String key;
    private final boolean success;
    private final String errorMessage;

    public PutObjectResponseMessage(String key, boolean success, String errorMessage) {
        super(MessageType.PUT_OBJECT_RESPONSE);
        this.key = key;
        this.success = success;
        this.errorMessage = errorMessage;
    }
    
    public PutObjectResponseMessage(String key, boolean success, String errorMessage, String correlationId) {
        super(MessageType.PUT_OBJECT_RESPONSE, correlationId);
        this.key = key;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public String getKey() {
        return key;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
} 