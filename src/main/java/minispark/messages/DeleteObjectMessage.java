package minispark.messages;

public class DeleteObjectMessage extends Message {
    private final String key;

    public DeleteObjectMessage(String key) {
        super(MessageType.DELETE_OBJECT);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
} 