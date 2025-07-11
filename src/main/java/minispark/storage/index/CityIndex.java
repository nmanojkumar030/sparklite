package minispark.storage.index;

import minispark.storage.table.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * CityLookupService - Domain-specific service for city-based customer lookups.
 * 
 * This is a minimal implementation that uses the generic IndexManager underneath.
 * The service provides city-specific methods while being decoupled from B+Tree internals.
 */
public class CityIndex {
    
    private final Table table;
    private final IndexManager indexManager;
    
    private static final String CITY_INDEX_NAME = "city_index";
    
    /**
     * Creates a new city lookup service.
     * 
     * @param table The table to provide city lookups for
     * @param indexBaseDirectory Base directory for index files
     */
    public CityIndex(Table table, Path indexBaseDirectory) {
        this.table = table;
        this.indexManager = new IndexManager(
            table.getTableName() + "_indexes",
            indexBaseDirectory
        );
        
        // Register the city index
        indexManager.registerIndex(CITY_INDEX_NAME);
    }
    
    /**
     * Adds a city mapping (should be called during data insertion).
     * 
     * @param city The city name
     * @param customerId The customer ID
     */
    public void addCityMapping(String city, String customerId) {
        if (city != null && !city.trim().isEmpty()) {
            // Normalize city name
            String normalizedCity = city.trim().toLowerCase();
            indexManager.addIndexMapping(CITY_INDEX_NAME, normalizedCity, customerId);
        }
    }
    
    /**
     * Builds the city index from collected mappings.
     * 
     * @return true if the index was built successfully
     * @throws IOException If an I/O error occurs
     */
    public boolean buildCityIndex() throws IOException {
        return indexManager.buildIndex(CITY_INDEX_NAME);
    }
    
    /**
     * Gets all customer IDs for a specific city.
     * 
     * @param city The city name
     * @return List of customer IDs in that city
     * @throws IOException If an I/O error occurs
     */
    public List<String> getCustomerIdsByCity(String city) throws IOException {
        if (city == null || city.trim().isEmpty()) {
            return List.of();
        }
        
        // Normalize city name for lookup
        String normalizedCity = city.trim().toLowerCase();
        
        // Get customer IDs from the index
        return indexManager.lookup(CITY_INDEX_NAME, normalizedCity);
    }
    
    /**
     * Closes the service and releases resources.
     * 
     * @throws IOException If an I/O error occurs
     */
    public void close() throws IOException {
        indexManager.close();
    }
} 