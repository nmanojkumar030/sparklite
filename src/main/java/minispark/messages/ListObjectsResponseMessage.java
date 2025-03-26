package minispark.messages;

import java.util.List;

public class ListObjectsResponseMessage extends Message {
    private final List<String> objects;
    private final boolean success;
    private final String errorMessage;
    private final String prefix;
    private final String correlationId;

    public ListObjectsResponseMessage(List<String> objects, boolean success, String errorMessage, String prefix) {
        super(MessageType.LIST_OBJECTS_RESPONSE);
        this.objects = objects;
        this.success = success;
        this.errorMessage = errorMessage;
        this.prefix = prefix;
        this.correlationId = null;
    }

    public ListObjectsResponseMessage(List<String> objects, boolean success, String errorMessage, String prefix, String correlationId) {
        super(MessageType.LIST_OBJECTS_RESPONSE);
        this.objects = objects;
        this.success = success;
        this.errorMessage = errorMessage;
        this.prefix = prefix;
        this.correlationId = correlationId;
    }

    public List<String> getObjects() {
        return objects;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getCorrelationId() {
        return correlationId;
    }
} 