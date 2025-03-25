package minispark.deltalite.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Action that represents removing a file from the table.
 */
public class RemoveFile implements Action {
    private final String path;
    private final long deletionTimestamp;

    @JsonCreator
    public RemoveFile(
        @JsonProperty("path") String path,
        @JsonProperty("deletionTimestamp") long deletionTimestamp
    ) {
        this.path = path;
        this.deletionTimestamp = deletionTimestamp;
    }

    @Override
    @JsonProperty("type")
    public String getType() {
        return "remove";
    }

    /**
     * Gets the path of the removed file.
     *
     * @return the file path
     */
    @JsonProperty("path")
    public String getPath() {
        return path;
    }
    
    /**
     * Gets the deletion timestamp of the file.
     *
     * @return the deletion timestamp
     */
    @JsonProperty("deletionTimestamp")
    public long getDeletionTimestamp() {
        return deletionTimestamp;
    }
} 