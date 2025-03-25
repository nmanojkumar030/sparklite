package minispark.messages;

import minispark.objectstore.FileStatus;
import java.util.List;

public class ListFromResponseMessage extends Message {
    private final List<FileStatus> objects;
    private final boolean success;
    private final String errorMessage;

    public ListFromResponseMessage(List<FileStatus> objects, boolean success, String errorMessage) {
        super(MessageType.LIST_FROM_RESPONSE);
        this.objects = objects;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public List<FileStatus> getObjects() {
        return objects;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
} 