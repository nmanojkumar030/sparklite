package minispark.distributed.messages;

import minispark.distributed.objectstore.FileStatus;
import java.util.List;

public class ListObjectsWithMetadataResponseMessage extends Message {
    private final List<FileStatus> objects;
    private final boolean success;
    private final String errorMessage;

    public ListObjectsWithMetadataResponseMessage(List<FileStatus> objects, boolean success, String errorMessage) {
        super(MessageType.LIST_OBJECTS_WITH_METADATA_RESPONSE);
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