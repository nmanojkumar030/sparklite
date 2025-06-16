package minispark.storage.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * EDUCATIONAL: Manages Parquet file operations and versioning.
 * 
 * Demonstrates file management strategies for immutable formats:
 * - File versioning for updates/deletes
 * - Directory structure organization
 * - Cleanup and maintenance operations
 */
public class ParquetFileManager {
    
    private final String basePath;
    private final Set<byte[]> deletedRecords;
    private int currentVersion;
    
    /**
     * Creates file manager for the specified base path.
     * 
     * @param basePath Base directory for Parquet files
     */
    public ParquetFileManager(String basePath) {
        this.basePath = basePath;
        this.deletedRecords = new HashSet<>();
        this.currentVersion = 0;
        
        initializeDirectory();
    }
    
    /**
     * Gets the next filename for a new Parquet file.
     * 
     * @return Filename for next Parquet file
     */
    public String getNextFileName() {
        currentVersion++;
        String filename = String.format("part-%05d-%d.parquet", 
                                       currentVersion, System.currentTimeMillis());
        String fullPath = Paths.get(basePath, filename).toString();
        
        logFileCreation(filename);
        return fullPath;
    }
    
    /**
     * Gets all existing Parquet files in the directory.
     * 
     * @return List of Parquet file paths
     */
    public List<String> getAllParquetFiles() {
        List<String> parquetFiles = new ArrayList<>();
        File directory = new File(basePath);
        
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles((dir, name) -> name.endsWith(".parquet"));
            if (files != null) {
                for (File file : files) {
                    parquetFiles.add(file.getAbsolutePath());
                }
            }
        }
        
        logFileDiscovery(parquetFiles.size());
        return parquetFiles;
    }

    /**
     * Closes the file manager and cleans up resources.
     * 
     * @throws IOException If cleanup fails
     */
    public void close() throws IOException {
        logManagerClose();
    }
    
    // Private helper methods
    
    private void initializeDirectory() {
        try {
            Path path = Paths.get(basePath);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
                logDirectoryCreated();
            } else {
                discoverExistingFiles();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize directory: " + basePath, e);
        }
    }
    
    private void discoverExistingFiles() {
        List<String> existingFiles = getAllParquetFiles();
        if (!existingFiles.isEmpty()) {
            // Extract highest version number from existing files
            currentVersion = extractHighestVersion(existingFiles);
            logExistingFilesDiscovered(existingFiles.size(), currentVersion);
        }
    }
    
    private int extractHighestVersion(List<String> filenames) {
        int maxVersion = 0;
        for (String filename : filenames) {
            String name = Paths.get(filename).getFileName().toString();
            if (name.startsWith("part-") && name.endsWith(".parquet")) {
                try {
                    String versionStr = name.substring(5, 10); // Extract 5-digit version
                    int version = Integer.parseInt(versionStr);
                    maxVersion = Math.max(maxVersion, version);
                } catch (NumberFormatException e) {
                    // Skip files with invalid naming
                }
            }
        }
        return maxVersion;
    }

    private void logDirectoryCreated() {
        System.out.println("   📁 Created directory: " + basePath);
    }
    
    private void logFileCreation(String filename) {
        System.out.println("   📝 Next file: " + filename + " (version " + currentVersion + ")");
    }
    
    private void logFileDiscovery(int fileCount) {
        System.out.println("   🔍 Found " + fileCount + " existing Parquet files");
    }
    
    private void logRecordDeleted(byte[] key) {
        System.out.println("   🗑️ Marked as deleted: " + new String(key));
    }
    
    private void logExistingFilesDiscovered(int fileCount, int maxVersion) {
        System.out.println("   📂 Discovered " + fileCount + " files, max version: " + maxVersion);
    }
    
    private void logManagerClose() {
        System.out.println("   🔒 Closing file manager, " + deletedRecords.size() + " deleted records tracked");
    }
} 