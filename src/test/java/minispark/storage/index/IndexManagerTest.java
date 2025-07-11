package minispark.storage.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for IndexManager class following TDD principles.
 * 
 * This test demonstrates the desired architecture where:
 * - IndexManager manages multiple generic indexes
 * - IndexManager provides a unified interface for index operations
 * - IndexManager has no domain-specific knowledge
 */
public class IndexManagerTest {
    
    private Path indexDirectory;
    private IndexManager indexManager;
    
    @BeforeEach
    public void setUp() throws IOException {
        indexDirectory = Files.createTempDirectory("index_manager_test");
        indexManager = new IndexManager("test_table", indexDirectory);
    }
    
    @AfterEach
    public void tearDown() throws IOException {
        if (indexManager != null) {
            indexManager.close();
        }
        
        // Clean up temporary directory
        Files.walk(indexDirectory)
            .sorted(java.util.Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }
    
    @Test
    public void shouldManageMultipleIndexes() throws IOException {
        // Register two different indexes
        Index<String, String> cityIndex = indexManager.registerIndex("city_index");
        Index<String, String> categoryIndex = indexManager.registerIndex("category_index");
        
        assertNotNull(cityIndex);
        assertNotNull(categoryIndex);
        
        // Add mappings to both indexes
        indexManager.addIndexMapping("city_index", "new_york", "customer_1");
        indexManager.addIndexMapping("city_index", "new_york", "customer_2");
        indexManager.addIndexMapping("category_index", "electronics", "product_1");
        
        // Build indexes
        boolean cityBuilt = indexManager.buildIndex("city_index");
        boolean categoryBuilt = indexManager.buildIndex("category_index");
        
        assertTrue(cityBuilt);
        assertTrue(categoryBuilt);
        
        // Lookup through IndexManager
        List<String> newYorkCustomers = indexManager.lookup("city_index", "new_york");
        assertEquals(2, newYorkCustomers.size());
        
        List<String> electronics = indexManager.lookup("category_index", "electronics");
        assertEquals(1, electronics.size());
    }
} 