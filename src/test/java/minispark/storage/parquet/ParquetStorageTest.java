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
     * EDUCATIONAL: Test range scan operation with performance demonstration.
     * Creates enough data to show Parquet's columnar storage and row group filtering benefits.
     */
    @Test
    void testRangeScanPerformance() throws IOException {
        System.out.println("\nTesting Range Scan Performance");
        System.out.println("=" .repeat(50));
        
        // Create substantial test data to demonstrate performance benefits
        List<Record> largeDataset = createLargeTestDataset(10000); // 10K records
        
        System.out.println("Created " + largeDataset.size() + " records for performance testing");
        
        // Write the large dataset
        long writeStart = System.currentTimeMillis();
        assertDoesNotThrow(() -> storage.writeBatch(largeDataset));
        long writeTime = System.currentTimeMillis() - writeStart;
        System.out.println("Batch write completed in " + writeTime + "ms");
        
        // Test 1: Full scan (all columns)
        byte[] startKey = "CUST0001".getBytes();
        byte[] endKey = "CUST9999".getBytes();
        
        long fullScanStart = System.currentTimeMillis();
        List<Record> fullResults = storage.scan(startKey, endKey, null); // null = all columns
        long fullScanTime = System.currentTimeMillis() - fullScanStart;
        
        System.out.println("Full scan returned " + fullResults.size() + " records in " + fullScanTime + "ms");
        
        // Test 2: Columnar projection (only 2 columns)
        List<String> projectedColumns = Arrays.asList("name", "city");
        
        long projectionStart = System.currentTimeMillis();
        List<Record> projectedResults = storage.scan(startKey, endKey, projectedColumns);
        long projectionTime = System.currentTimeMillis() - projectionStart;
        
        System.out.println("Columnar projection returned " + projectedResults.size() + " records in " + projectionTime + "ms");
        
        // Test 3: Selective range scan (smaller range)
        byte[] selectiveStartKey = "CUST2000".getBytes();
        byte[] selectiveEndKey = "CUST3000".getBytes();
        
        long selectiveStartTime = System.currentTimeMillis();
        List<Record> selectiveResults = storage.scan(selectiveStartKey, selectiveEndKey, projectedColumns);
        long selectiveTime = System.currentTimeMillis() - selectiveStartTime;
        
        System.out.println("Selective range scan returned " + selectiveResults.size() + " records in " + selectiveTime + "ms");
        
        // Educational output about Parquet benefits
        System.out.println("\n📊 PARQUET PERFORMANCE BENEFITS:");
        System.out.println("• Columnar Storage: Only reads requested columns (" + projectedColumns + ")");
        System.out.println("• Row Group Filtering: Skips irrelevant row groups based on key range");
        System.out.println("• Compression: Reduces I/O through column-specific compression");
        
        if (projectionTime < fullScanTime) {
            double improvement = ((double)(fullScanTime - projectionTime) / fullScanTime) * 100;
            System.out.println("• Column projection was " + String.format("%.1f", improvement) + "% faster");
        }
    }
    
    /**
     * Creates a large dataset for performance testing.
     * Generates records with realistic data distribution.
     */
    private List<Record> createLargeTestDataset(int recordCount) {
        List<Record> records = new ArrayList<>();
        Random random = new Random(42); // Fixed seed for reproducible tests
        
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                          "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"};
        String[] domains = {"gmail.com", "yahoo.com", "outlook.com", "company.com", "startup.io"};
        
        for (int i = 1; i <= recordCount; i++) {
            String custId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "user" + i + "@" + domains[random.nextInt(domains.length)];
            Integer age = 18 + random.nextInt(65); // Age 18-82
            String city = cities[random.nextInt(cities.length)];
            
            records.add(new Record(custId.getBytes(), 
                createCustomerData(custId, name, email, age, city)));
        }
        
        return records;
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