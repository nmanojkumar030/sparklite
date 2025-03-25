package minispark.deltalite;

import minispark.deltalite.actions.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the state of a Delta table at a specific version.
 * Contains all actions that have been applied to the table up to that version.
 */
public class Snapshot {
    private final DeltaLog deltaLog;
    private final long version;
    private final List<Action> actions;
    
    public Snapshot(DeltaLog deltaLog, long version, List<Action> actions) {
        this.deltaLog = deltaLog;
        this.version = version;
        this.actions = new ArrayList<>(actions);
    }
    
    /**
     * Gets the version number of this snapshot.
     *
     * @return the version number
     */
    public long getVersion() {
        return version;
    }
    
    /**
     * Gets all actions in this snapshot.
     *
     * @return the list of actions
     */
    public List<Action> getActions() {
        return new ArrayList<>(actions);
    }
    
    /**
     * Gets all actions of a specific type in this snapshot.
     *
     * @param type the type of actions to get
     * @return the list of actions of the specified type
     */
    public <T extends Action> List<T> getActions(Class<T> type) {
        return actions.stream()
                .filter(type::isInstance)
                .map(type::cast)
                .collect(Collectors.toList());
    }
    
    /**
     * Gets the DeltaLog associated with this snapshot.
     *
     * @return the DeltaLog
     */
    public DeltaLog getDeltaLog() {
        return deltaLog;
    }
    
    /**
     * Gets all files in this snapshot.
     *
     * @return the list of file paths
     */
    public List<String> getFiles() {
        return getActions(AddFile.class).stream()
                .map(AddFile::getPath)
                .collect(Collectors.toList());
    }
    
    /**
     * Gets all metadata in this snapshot.
     *
     * @return the map of metadata key-value pairs
     */
    public Map<String, String> getMetadata() {
        return getActions(Metadata.class).stream()
                .collect(Collectors.toMap(
                    Metadata::getKey,
                    Metadata::getValue,
                    (v1, v2) -> v1 // Keep the first value if there are duplicates
                ));
    }
} 