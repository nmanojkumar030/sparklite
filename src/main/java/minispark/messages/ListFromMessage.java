package minispark.messages;

public class ListFromMessage extends Message {
    private final String prefix;
    private final boolean recursive;

    public ListFromMessage(String prefix, boolean recursive) {
        super(MessageType.LIST_FROM);
        this.prefix = prefix;
        this.recursive = recursive;
    }

    public String getPrefix() {
        return prefix;
    }

    public boolean isRecursive() {
        return recursive;
    }
} 