package minispark.deltalite;

import minispark.deltalite.actions.Action;
import minispark.deltalite.util.JsonUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * DeltaLog is responsible for managing the transaction log of a Delta table.
 * It handles reading and writing log files, maintaining table state, and
 * providing the necessary infrastructure for optimistic concurrency control.
 */
public class DeltaLog {
    
    /** Path to the table's log directory */
    private final Path logPath;
    
    /** Path to the table's data directory */
    private final Path dataPath;
    
    /** Lock for synchronizing access to the log */
    private final ReentrantLock deltaLogLock;
    
    /** Current snapshot of the table state */
    private volatile Snapshot currentSnapshot;
    
    /** Interval for creating checkpoints */
    private int checkpointInterval;
    
    private DeltaLog(String tablePath) {
        Path tableDir = Paths.get(tablePath);
        this.logPath = tableDir.resolve("_delta_log");
        this.dataPath = tableDir.resolve("_data");
        this.deltaLogLock = new ReentrantLock();
        this.checkpointInterval = 10;
    }
    
    /**
     * Creates or gets a DeltaLog for the specified table path.
     *
     * @param tablePath the path to the table
     * @return the DeltaLog instance
     * @throws IOException if an I/O error occurs
     */
    public static DeltaLog forTable(String tablePath) throws IOException {
        DeltaLog deltaLog = new DeltaLog(tablePath);
        
        // Create directories if they don't exist
        Files.createDirectories(deltaLog.logPath);
        Files.createDirectories(deltaLog.dataPath);
        
        return deltaLog;
    }
    
    /**
     * Updates the current snapshot by reading all actions from the log.
     *
     * @return the updated snapshot
     * @throws IOException if an I/O error occurs
     */
    public Snapshot update() throws IOException {
        List<Long> versions = listVersions();
        if (versions.isEmpty()) {
            currentSnapshot = new Snapshot(this, -1, new ArrayList<>());
            return currentSnapshot;
        }
        
        long latestVersion = versions.get(versions.size() - 1);
        List<Action> actions = readVersion(latestVersion);
        currentSnapshot = new Snapshot(this, latestVersion, actions);
        return currentSnapshot;
    }
    
    /**
     * Reads all actions from a specific version file.
     *
     * @param version the version to read
     * @return the list of actions
     * @throws IOException if an I/O error occurs
     */
    private List<Action> readVersion(long version) throws IOException {
        Path versionFile = logPath.resolve(String.format("%020d.json", version));
        
        if (!Files.exists(versionFile)) {
            return new ArrayList<>();
        }
        
        String json = Files.readString(versionFile);
        try {
            return JsonUtil.fromJsonArray(json);
        } catch (Exception e) {
            throw new IOException("Failed to parse actions from file: " + versionFile, e);
        }
    }
    
    /**
     * Lists all versions in the log directory.
     *
     * @return a list of version numbers
     * @throws IOException if an I/O error occurs
     */
    public List<Long> listVersions() throws IOException {
        if (!Files.exists(logPath)) {
            return new ArrayList<>();
        }
        
        return Files.list(logPath)
                .filter(file -> file.getFileName().toString().matches("\\d{20}\\.json"))
                .map(file -> {
                    String fileName = file.getFileName().toString();
                    String versionStr = fileName.substring(0, fileName.indexOf('.'));
                    return Long.parseLong(versionStr);
                })
                .sorted()
                .collect(Collectors.toList());
    }
    
    /**
     * Gets the latest version in the log.
     *
     * @return the latest version, or -1 if no versions exist
     * @throws IOException if an I/O error occurs
     */
    public long getLatestVersion() throws IOException {
        List<Long> versions = listVersions();
        return versions.isEmpty() ? -1 : versions.get(versions.size() - 1);
    }
    
    /**
     * Writes actions to a new version file.
     *
     * @param version the version to write
     * @param actions the actions to write
     * @throws IOException if an I/O error occurs
     */
    public void write(long version, List<Action> actions) throws IOException {
        try {
            deltaLogLock.lock();
            
            // Ensure both log and data directories exist
            Files.createDirectories(logPath);
            Files.createDirectories(dataPath);
            
            // Create the log file for this version
            Path versionFile = logPath.resolve(String.format("%020d.json", version));
            
            // Write all actions as a single JSON array
            try (var writer = Files.newBufferedWriter(versionFile)) {
                String json = JsonUtil.toJson(actions);
                System.out.println("Writing actions: " + json); // Debug log
                writer.write(json);
                writer.flush();
            }
            
            // Update the snapshot after writing
            update();
        } finally {
            deltaLogLock.unlock();
        }
    }
    
    /**
     * Gets the checkpoint interval for this log.
     *
     * @return the checkpoint interval
     */
    public int getCheckpointInterval() {
        return checkpointInterval;
    }
    
    /**
     * Sets the checkpoint interval for this log.
     *
     * @param interval the new checkpoint interval
     * @return this DeltaLog for chaining
     */
    public DeltaLog setCheckpointInterval(int interval) {
        this.checkpointInterval = interval;
        return this;
    }
    
    /**
     * Gets the path to the log directory.
     *
     * @return the log directory path
     */
    public Path getLogPath() {
        return logPath;
    }
    
    /**
     * Gets the path to the data directory.
     *
     * @return the data directory path
     */
    public Path getDataPath() {
        return dataPath;
    }
    
    /**
     * Gets the path to the table.
     *
     * @return the table path
     */
    public String getTablePath() {
        return dataPath.getParent().toString();
    }
    
    /**
     * Checks if the table exists.
     *
     * @return true if the table exists, false otherwise
     */
    public boolean tableExists() {
        return Files.exists(logPath) && Files.exists(dataPath);
    }
    
    /**
     * Starts a new optimistic transaction.
     *
     * @return a new OptimisticTransaction
     * @throws IOException if an I/O error occurs
     */
    public OptimisticTransaction startTransaction() throws IOException {
        return new OptimisticTransaction(getTablePath());
    }
    
    /**
     * Gets the current snapshot of the table state.
     *
     * @return the current snapshot
     * @throws IOException if an I/O error occurs
     */
    public Snapshot snapshot() throws IOException {
        return update();
    }
} 