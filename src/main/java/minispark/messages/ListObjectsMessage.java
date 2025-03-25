package minispark.messages;

public class ListObjectsMessage extends Message {
    private final String prefix;

    public ListObjectsMessage(String prefix) {
        super(MessageType.LIST_OBJECTS);
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
} 