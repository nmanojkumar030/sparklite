package minispark.distributed.messages;

public class ListObjectsWithMetadataMessage extends Message {
    private final String prefix;

    public ListObjectsWithMetadataMessage(String prefix) {
        super(MessageType.LIST_OBJECTS_WITH_METADATA);
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
} 