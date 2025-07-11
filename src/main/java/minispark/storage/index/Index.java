package minispark.storage.index;

import minispark.storage.btree.BTree;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Generic secondary index implementation using B+Tree for efficient lookups.
 * 
 * EDUCATIONAL DESIGN DECISION: Two-Phase Index Building
 * =====================================================
 * 
 * This implementation uses a two-phase approach instead of directly writing to B+Tree:
 * 
 * Phase 1: Collection (addMapping) - Collect key-value pairs in memory
 * Phase 2: Index Building (buildIndex) - Create B+Tree from complete collection
 * 
 * WHY NOT DIRECT B+TREE WRITES?
 * -----------------------------
 * Our B+Tree implementation only supports INSERT operations, not UPDATE operations.
 * 
 * If we tried to write directly to B+Tree during addMapping():
 * 1. First mapping: key="city1", value="customer1" → Works (insert)
 * 2. Second mapping: key="city1", value="customer2" → Problem!
 *    - We'd need to UPDATE the existing entry to append "customer2"
 *    - But B+Tree.write() only supports INSERT (overwrites existing)
 *    - We'd lose "customer1" and only keep "customer2"
 * 
 * SOLUTION: Two-Phase Approach
 * ----------------------------
 * 1. Collect all mappings in memory (supports multiple values per key)
 * 2. Build B+Tree index once with complete data (one INSERT per unique key)
 * 
 * REAL-WORLD RELEVANCE:
 * ---------------------
 * This mirrors real database systems:
 * - Batch index building in data warehouses
 * - Log-structured storage engines
 * - ETL processes that build indexes after data loading
 * 
 * EDUCATIONAL VALUE:
 * ------------------
 * - Demonstrates constraint-driven design
 * - Shows working around storage engine limitations
 * - Illustrates batch vs incremental index building strategies
 */
public class Index<K, V> {
    
    private final String indexName;
    private final Path indexPath;
    
    // In-memory collection of mappings before index is built
    // NOTE: This allows multiple values per key, which B+Tree INSERT-only doesn't support directly
    private final Map<K, Queue<V>> keyToValues = new ConcurrentHashMap<>();
    
    // B+Tree index for efficient lookups
    private BTree indexBTree;
    private boolean indexBuilt = false;
    
    /**
     * Creates a new generic index.
     * 
     * @param indexName Name of the index
     * @param indexPath Path where the B+Tree index will be stored
     */
    public Index(String indexName, Path indexPath) {
        this.indexName = indexName;
        this.indexPath = indexPath;
    }
    
    /**
     * Adds a key-value mapping to the index.
     * 
     * PHASE 1: Collection Phase
     * Mappings are collected in memory until buildIndex() is called.
     * This allows multiple values per key, which our INSERT-only B+Tree cannot handle directly.
     * 
     * @param key The index key
     * @param value The value to associate with the key
     */
    public void addMapping(K key, V value) {
        keyToValues.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>())
                   .add(value);
    }
    
    /**
     * Builds the B+Tree index from collected mappings.
     * 
     * PHASE 2: Index Building Phase
     * Creates the persistent B+Tree index from the complete in-memory collection.
     * Each unique key gets ONE INSERT operation with all its values.
     * 
     * @return true if the index was built successfully
     * @throws IOException If an I/O error occurs
     */
    public boolean buildIndex() throws IOException {
        if (keyToValues.isEmpty()) {
            return false;
        }
        
        // Create B+Tree for the index
        indexBTree = new BTree(indexPath);
        
        // Insert each key and its values into the index
        // NOTE: This is why we collect in memory first - we can INSERT each key only once
        for (Map.Entry<K, Queue<V>> entry : keyToValues.entrySet()) {
            K key = entry.getKey();
            Queue<V> values = entry.getValue();
            
            // Serialize values as index entry
            Map<String, Object> indexValue = new HashMap<>();
            indexValue.put("key", key.toString());
            indexValue.put("values", String.join(",", values.stream()
                .map(Object::toString)
                .toArray(String[]::new)));
            
            // Insert into B+Tree index (one INSERT per unique key)
            indexBTree.write(key.toString().getBytes(), indexValue);
        }
        
        indexBuilt = true;
        return true;
    }
    
    /**
     * Looks up values for a specific key.
     * 
     * @param key The key to look up
     * @return List of values associated with the key
     * @throws IOException If an I/O error occurs
     */
    public List<V> lookup(K key) throws IOException {
        if (key == null) {
            return new ArrayList<>();
        }
        
        if (indexBuilt && indexBTree != null) {
            // Use B+Tree index lookup
            Optional<Map<String, Object>> result = indexBTree.read(key.toString().getBytes());
            
            if (result.isPresent()) {
                String valuesString = (String) result.get().get("values");
                return parseValues(valuesString);
            }
        } else {
            // Fall back to in-memory lookup
            Queue<V> values = keyToValues.get(key);
            if (values != null) {
                return new ArrayList<>(values);
            }
        }
        
        return new ArrayList<>();
    }
    
    /**
     * Closes the index and releases resources.
     * 
     * @throws IOException If an I/O error occurs
     */
    public void close() throws IOException {
        if (indexBTree != null) {
            indexBTree.close();
        }
    }
    
    /**
     * Parses comma-separated values string back to list.
     * This is a simple implementation for the minimal case.
     */
    @SuppressWarnings("unchecked")
    private List<V> parseValues(String valuesString) {
        if (valuesString == null || valuesString.isEmpty()) {
            return new ArrayList<>();
        }
        
        String[] valueArray = valuesString.split(",");
        List<V> values = new ArrayList<>();
        
        for (String value : valueArray) {
            // Simple string casting - works for String values
            values.add((V) value.trim());
        }
        
        return values;
    }
} 