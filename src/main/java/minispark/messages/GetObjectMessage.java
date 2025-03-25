package minispark.messages;

public class GetObjectMessage extends Message {
    private final String key;

    public GetObjectMessage(String key) {
        super(MessageType.GET_OBJECT);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
} 