package minispark.deltalite.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Action that represents adding a new file to a Delta table.
 */
public class AddFile implements Action {
    private final String path;
    private final long size;
    private final long modificationTime;
    private final boolean dataChange;

    @JsonCreator
    public AddFile(
        @JsonProperty("path") String path,
        @JsonProperty("size") long size,
        @JsonProperty("modificationTime") long modificationTime,
        @JsonProperty("dataChange") boolean dataChange
    ) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
    }

    @Override
    @JsonProperty("type")
    public String getType() {
        return "add";
    }

    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    @JsonProperty("size")
    public long getSize() {
        return size;
    }

    @JsonProperty("modificationTime")
    public long getModificationTime() {
        return modificationTime;
    }

    @JsonProperty("dataChange")
    public boolean isDataChange() {
        return dataChange;
    }
} 