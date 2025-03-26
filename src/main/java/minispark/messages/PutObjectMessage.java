package minispark.messages;

public class PutObjectMessage extends Message {
    private final String key;
    private final byte[] data;
    private final boolean overwrite;

    public PutObjectMessage(String key, byte[] data, boolean overwrite) {
        super(MessageType.PUT_OBJECT);
        this.key = key;
        this.data = data;
        this.overwrite = overwrite;
    }
    
    public PutObjectMessage(String key, byte[] data, boolean overwrite, String correlationId) {
        super(MessageType.PUT_OBJECT, correlationId);
        this.key = key;
        this.data = data;
        this.overwrite = overwrite;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isOverwrite() {
        return overwrite;
    }
} 