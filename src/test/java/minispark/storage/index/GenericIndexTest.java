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
 * Test for generic Index class following TDD principles.
 * 
 * This test demonstrates the desired generic architecture where:
 * - Index is completely generic (no domain-specific knowledge)
 * - Index can handle any key-value mappings
 * - Index uses B+Tree for efficient lookups
 */
public class GenericIndexTest {
    
    private Path indexDirectory;
    private Index<String, String> cityIndex;
    
    @BeforeEach
    public void setUp() throws IOException {
        indexDirectory = Files.createTempDirectory("generic_index_test");
        Path indexPath = indexDirectory.resolve("city_index.btree");
        
        // This should create a generic index that can handle city -> customer_id mappings
        cityIndex = new Index<>("city_index", indexPath);
    }
    
    @AfterEach
    public void tearDown() throws IOException {
        if (cityIndex != null) {
            cityIndex.close();
        }
        
        // Clean up temporary directory
        Files.walk(indexDirectory)
            .sorted(java.util.Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }
    
    @Test
    public void shouldCreateGenericIndexAndPerformBasicOperations() throws IOException {
        // Phase 1: Add mappings (collect phase)
        cityIndex.addMapping("new_york", "customer_1");
        cityIndex.addMapping("new_york", "customer_2");
        cityIndex.addMapping("los_angeles", "customer_3");
        cityIndex.addMapping("new_york", "customer_4");
        
        // Phase 2: Build index
        boolean built = cityIndex.buildIndex();
        assertTrue(built, "Index should be built successfully");
        
        // Phase 3: Lookup values
        List<String> newYorkCustomers = cityIndex.lookup("new_york");
        assertEquals(3, newYorkCustomers.size(), "Should find 3 customers in New York");
        assertTrue(newYorkCustomers.contains("customer_1"));
        assertTrue(newYorkCustomers.contains("customer_2"));
        assertTrue(newYorkCustomers.contains("customer_4"));
        
        List<String> laCustomers = cityIndex.lookup("los_angeles");
        assertEquals(1, laCustomers.size(), "Should find 1 customer in Los Angeles");
        assertTrue(laCustomers.contains("customer_3"));
        
        // Phase 4: Test empty lookup
        List<String> emptyResult = cityIndex.lookup("nonexistent_city");
        assertTrue(emptyResult.isEmpty(), "Should return empty list for non-existent city");
    }
} 