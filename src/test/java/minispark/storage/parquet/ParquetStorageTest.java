package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.table.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ParquetStorage implementation.
 * These tests verify the ParquetStorage skeleton works correctly
 */
public class ParquetStorageTest {
    
    @TempDir
    Path tempDir;
    
    private ParquetStorage storage;
    private TableSchema schema;
    
    @BeforeEach
    void setUp() {
        schema = TableSchema.createCustomerSchema();
        String basePath = tempDir.resolve("parquet_test").toString();
        storage = new ParquetStorage(basePath, schema);
    }
    
    /**
     * EDUCATIONAL: Test basic storage creation and initialization.
     */
    @Test
    void testStorageCreation() {
        System.out.println("\nTesting ParquetStorage Creation");
        System.out.println("=" .repeat(50));
        
        assertNotNull(storage);
        
        // Verify storage was created with correct configuration
        // (This demonstrates how to test object initialization)
        System.out.println("ParquetStorage created successfully");
    }
    
    /**
     * EDUCATIONAL: Test single record write with buffering.
     */
    @Test
    void testSingleRecordWrite() throws IOException {
        System.out.println("\nTesting Single Record Write");
        System.out.println("=" .repeat(50));
        
        // Create a test record
        Map<String, Object> customerData = createCustomerData("CUST001", "Alice Johnson", "alice@example.com", 28, "New York");
        byte[] key = "CUST001".getBytes();
        
        // Test write operation
        assertDoesNotThrow(() -> storage.write(key, customerData));
        
        System.out.println("Single record write completed");
    }
    
    /**
     * EDUCATIONAL: Test batch write operation.
     */
    @Test
    void testBatchWrite() throws IOException {
        System.out.println("\nTesting Batch Write");
        System.out.println("=" .repeat(50));
        
        // Create multiple test records
        List<Record> records = createTestRecords();
        
        // Test batch write
        assertDoesNotThrow(() -> storage.writeBatch(records));
        
        System.out.println("Batch write completed for " + records.size() + " records");
    }
    
    /**
     * EDUCATIONAL: Test point lookup operation.
     */
    @Test
    void testPointLookup() throws IOException {
        System.out.println("\nTesting Point Lookup");
        System.out.println("=" .repeat(50));
        
        byte[] key = "CUST001".getBytes();
        
        // Test read operation (should return empty since no actual files yet)
        Optional<Map<String, Object>> result = storage.read(key);
        
        // For now, this should return empty since we haven't implemented actual Parquet I/O
        assertFalse(result.isPresent());
        
        System.out.println("Point lookup completed (no data found as expected)");
    }
    
    /**
     * EDUCATIONAL: Test range scan operation.
     */
    @Test
    void testRangeScan() throws IOException {
        System.out.println("\nTesting Range Scan");
        System.out.println("=" .repeat(50));
        
        byte[] startKey = "CUST001".getBytes();
        byte[] endKey = "CUST999".getBytes();
        List<String> columns = Arrays.asList("name", "city");
        
        // Test scan operation
        List<Record> results = storage.scan(startKey, endKey, columns);
        
        // Should return empty list since no actual files yet
        assertNotNull(results);
        assertTrue(results.isEmpty());
        
        System.out.println("Range scan completed (no data found as expected)");
    }

    @Test
    void testErrorHandling() {
        System.out.println("\nTesting Error Handling");
        System.out.println("=" .repeat(50));
        
        // Test null records
        assertThrows(IllegalArgumentException.class, () -> storage.writeBatch(null));
        
        // Test empty records
        assertThrows(IllegalArgumentException.class, () -> storage.writeBatch(new ArrayList<>()));
        
        System.out.println("Error handling tests completed");
    }
    
    /**
     * EDUCATIONAL: Test storage cleanup.
     */
    @Test
    void testStorageClose() throws IOException {
        System.out.println("\nTesting Storage Close");
        System.out.println("=" .repeat(50));
        
        // Test close operation
        assertDoesNotThrow(() -> storage.close());
        
        System.out.println("Storage close completed");
    }
    
    // Helper methods for creating test data
    
    private List<Record> createTestRecords() {
        List<Record> records = new ArrayList<>();
        
        records.add(new Record("CUST001".getBytes(), 
            createCustomerData("CUST001", "Alice Johnson", "alice@example.com", 28, "New York")));
        
        records.add(new Record("CUST002".getBytes(), 
            createCustomerData("CUST002", "Bob Smith", "bob@example.com", 35, "Los Angeles")));
        
        records.add(new Record("CUST003".getBytes(), 
            createCustomerData("CUST003", "Carol Davis", "carol@example.com", 42, "Chicago")));
        
        return records;
    }
    
    private Map<String, Object> createCustomerData(String id, String name, String email, Integer age, String city) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", id);
        data.put("name", name);
        data.put("email", email);
        data.put("age", age);
        data.put("city", city);
        return data;
    }
} 