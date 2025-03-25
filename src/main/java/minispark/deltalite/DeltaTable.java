package minispark.deltalite;

import minispark.objectstore.Client;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DeltaTable {
    private final String path;
    private final Client client;

    private DeltaTable(Client client, String path) {
        this.client = client;
        this.path = path;
    }

    public static DeltaTable forPath(Client client, String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be null or empty");
        }
        
        // Check if path exists and is a Delta table
        if (!DeltaTableUtils.isDeltaTable(client, path)) {
            throw new IllegalArgumentException("Path is not a Delta table: " + path);
        }
        
        return new DeltaTable(client, path);
    }

    public String getPath() {
        return path;
    }

    public Client getClient() {
        return client;
    }
} 