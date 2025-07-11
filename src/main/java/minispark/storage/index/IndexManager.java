package minispark.storage.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IndexManager - Manages multiple generic indexes at the storage layer.
 * 
 * This is a minimal implementation to make the test pass.
 * The manager is completely generic and has no domain-specific knowledge.
 */
public class IndexManager {
    
    private final String tableName;
    private final Path indexBaseDirectory;
    
    // Map of index name -> Index instance
    private final Map<String, Index<String, String>> indexes = new ConcurrentHashMap<>();
    
    /**
     * Creates a new index manager for a table.
     * 
     * @param tableName Name of the table this manager serves
     * @param indexBaseDirectory Base directory where index files will be stored
     */
    public IndexManager(String tableName, Path indexBaseDirectory) {
        this.tableName = tableName;
        this.indexBaseDirectory = indexBaseDirectory;
        
        // Ensure the index directory exists
        indexBaseDirectory.toFile().mkdirs();
    }
    
    /**
     * Registers a new secondary index.
     * 
     * @param indexName Name of the index
     * @return The created index instance
     */
    public Index<String, String> registerIndex(String indexName) {
        if (indexes.containsKey(indexName)) {
            return indexes.get(indexName);
        }
        
        // Create index file path
        Path indexPath = indexBaseDirectory.resolve(indexName + ".btree");
        
        // Create new index
        Index<String, String> index = new Index<>(indexName, indexPath);
        indexes.put(indexName, index);
        
        return index;
    }
    
    /**
     * Adds a mapping to a specific index.
     * 
     * @param indexName Name of the index
     * @param key The index key
     * @param value The value to associate with the key
     */
    public void addIndexMapping(String indexName, String key, String value) {
        Index<String, String> index = indexes.get(indexName);
        if (index != null) {
            index.addMapping(key, value);
        }
    }
    
    /**
     * Builds a specific index from collected mappings.
     * 
     * @param indexName Name of the index to build
     * @return true if the index was built successfully
     * @throws IOException If an I/O error occurs
     */
    public boolean buildIndex(String indexName) throws IOException {
        Index<String, String> index = indexes.get(indexName);
        if (index == null) {
            return false;
        }
        
        return index.buildIndex();
    }
    
    /**
     * Looks up values for a key in a specific index.
     * 
     * @param indexName Name of the index
     * @param key The key to look up
     * @return List of values associated with the key
     * @throws IOException If an I/O error occurs
     */
    public List<String> lookup(String indexName, String key) throws IOException {
        Index<String, String> index = indexes.get(indexName);
        if (index == null) {
            return new ArrayList<>();
        }
        
        return index.lookup(key);
    }
    
    /**
     * Closes all indexes and releases resources.
     * 
     * @throws IOException If an I/O error occurs
     */
    public void close() throws IOException {
        for (Index<String, String> index : indexes.values()) {
            index.close();
        }
    }
} 