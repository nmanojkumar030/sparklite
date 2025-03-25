package minispark.deltalite;

import minispark.objectstore.Client;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DeltaTableUtils {
    private static final String DELTA_LOG_FOLDER = "_delta_log";
    
    /**
     * Check if the given path is a Delta table.
     * A directory is a Delta table if it contains the _delta_log subdirectory.
     */
    public static boolean isDeltaTable(Client client, String path) {
        try {
            String deltaLogPath = path + "/" + DELTA_LOG_FOLDER;
            CompletableFuture<List<String>> future = client.listObjects(deltaLogPath);
            List<String> objects = future.join();
            return !objects.isEmpty();
        } catch (Exception e) {
            return false;
        }
    }
} 