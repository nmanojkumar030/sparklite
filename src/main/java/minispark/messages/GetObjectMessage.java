package minispark.messages;

public class GetObjectMessage extends Message {
    private final String key;

    public GetObjectMessage(String key) {
        super(MessageType.GET_OBJECT);
        this.key = key;
    }
    
    public GetObjectMessage(String key, String correlationId) {
        super(MessageType.GET_OBJECT, correlationId);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
} 