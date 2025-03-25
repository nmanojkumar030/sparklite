package minispark.messages;

import java.util.List;

public class ListObjectsResponseMessage extends Message {
    private final List<String> objects;
    private final boolean success;
    private final String errorMessage;

    public ListObjectsResponseMessage(List<String> objects, boolean success, String errorMessage) {
        super(MessageType.LIST_OBJECTS_RESPONSE);
        this.objects = objects;
        this.success = success;
        this.errorMessage = errorMessage;
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
} 