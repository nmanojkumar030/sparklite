package minispark.distributed.messages;

public class DeleteObjectMessage extends Message {
    private final String key;

    public DeleteObjectMessage(String key) {
        super(MessageType.DELETE_OBJECT);
        this.key = key;
    }
    
    public DeleteObjectMessage(String key, String correlationId) {
        super(MessageType.DELETE_OBJECT, correlationId);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
} 