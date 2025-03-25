package minispark.messages;

public class PutObjectMessage extends Message {
    private final String key;
    private final byte[] data;

    public PutObjectMessage(String key, byte[] data) {
        super(MessageType.PUT_OBJECT);
        this.key = key;
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }
} 