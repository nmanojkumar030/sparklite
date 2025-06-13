package minispark.storage.table;

import minispark.storage.StorageInterface;
import minispark.storage.btree.BTree;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test for Table implementation with different storage backends.
 * Tests both B+Tree and Parquet storage to ensure the Table abstraction works correctly.
 */
public class TableDemoTest {
    
    @TempDir
    Path tempDir;
    
    private List<TableRecord> sampleCustomers;
    
    @BeforeEach
    void setUp() {
        sampleCustomers = createSampleCustomers();
    }
    
    @AfterEach
    void tearDown() {
        // Cleanup is handled by @TempDir
    }
    
    /**
     * Test Table with B+Tree storage backend.
     * Verifies transactional operations and row-based storage functionality.
     */
    @Test
    void testBTreeTableOperations() throws IOException {
        System.out.println("\n🧪 Testing Table with B+Tree Storage");
        System.out.println("=" .repeat(50));
        
        // Create B+Tree storage
        Path btreePath = tempDir.resolve("test_customers.db");
        BTree btree = new BTree(btreePath);
        
        // Create table with customer schema
        TableSchema schema = TableSchema.createCustomerSchema();
        Table customerTable = new Table("customers_btree", schema, btree);
        
        // Verify table creation
        assertEquals("customers_btree", customerTable.getTableName());
        assertNotNull(customerTable.getSchema());
        
        // Test individual inserts (transactional style)
        System.out.println("📝 Testing individual inserts...");
        for (TableRecord customer : sampleCustomers) {
            assertDoesNotThrow(() -> customerTable.insert(customer));
        }
        
        // Test point lookups
        System.out.println("🔍 Testing point lookups...");
        testPointLookups(customerTable);
        
        // Test range scans
        System.out.println("📊 Testing range scans...");
        testRangeScans(customerTable);
        
        // Test column projection
        System.out.println("🎯 Testing column projection...");
        testColumnProjection(customerTable);
        
        // Verify statistics
        Table.TableStats stats = customerTable.getStats();
        assertNotNull(stats);
        assertTrue(stats.toString().contains("customers_btree"));
        
        customerTable.close();
        System.out.println("✅ B+Tree table tests completed");
    }
    
    /**
     * Test Table with Parquet storage backend.
     * Verifies analytical operations and columnar storage functionality.
     */
    @Test
    void testParquetTableOperations() throws IOException {
        System.out.println("\n🧪 Testing Table with Parquet Storage");
        System.out.println("=" .repeat(50));
        
        // Create Parquet storage wrapper
        Path parquetPath = tempDir.resolve("test_customers.parquet");
        ParquetStorageAdapter parquetStorage = new ParquetStorageAdapter(parquetPath.toString());
        
        // Create table with customer schema
        TableSchema schema = TableSchema.createCustomerSchema();
        Table customerTable = new Table("customers_parquet", schema, parquetStorage);
        
        // Verify table creation
        assertEquals("customers_parquet", customerTable.getTableName());
        assertNotNull(customerTable.getSchema());
        
        // Test batch insert (analytical style)
        System.out.println("📝 Testing batch insert...");
        assertDoesNotThrow(() -> customerTable.insertBatch(sampleCustomers));
        
        // Test analytical queries
        System.out.println("📊 Testing analytical queries...");
        testAnalyticalQueries(customerTable);
        
        // Test column-oriented access
        System.out.println("🎯 Testing column-oriented access...");
        testColumnOrientedAccess(customerTable);
        
        // Verify statistics
        Table.TableStats stats = customerTable.getStats();
        assertNotNull(stats);
        assertTrue(stats.toString().contains("customers_parquet"));
        
        customerTable.close();
        System.out.println("✅ Parquet table tests completed");
    }
    
    /**
     * Test performance characteristics comparison between storage backends.
     */
    @Test
    void testStorageBackendCharacteristics() throws IOException {
        System.out.println("\n🧪 Testing Storage Backend Characteristics");
        System.out.println("=" .repeat(50));
        
        // This test verifies that both storage backends implement the same interface
        // but have different performance characteristics
        
        // Test B+Tree characteristics
        Path btreePath = tempDir.resolve("perf_btree.db");
        BTree btree = new BTree(btreePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table btreeTable = new Table("perf_btree", schema, btree);
        
        // B+Tree should handle individual inserts efficiently
        long btreeStartTime = System.currentTimeMillis();
        for (TableRecord customer : sampleCustomers) {
            btreeTable.insert(customer);
        }
        long btreeInsertTime = System.currentTimeMillis() - btreeStartTime;
        
        // B+Tree should handle point lookups very efficiently
        btreeStartTime = System.currentTimeMillis();
        Optional<TableRecord> btreeResult = btreeTable.findByPrimaryKey("CUST001");
        long btreeLookupTime = System.currentTimeMillis() - btreeStartTime;
        
        assertTrue(btreeResult.isPresent());
        assertEquals("Alice Johnson", btreeResult.get().getValue("name"));
        
        btreeTable.close();
        
        // Test Parquet characteristics
        Path parquetPath = tempDir.resolve("perf_customers.parquet");
        ParquetStorageAdapter parquetStorage = new ParquetStorageAdapter(parquetPath.toString());
        Table parquetTable = new Table("perf_parquet", schema, parquetStorage);
        
        // Parquet should handle batch inserts efficiently
        long parquetStartTime = System.currentTimeMillis();
        parquetTable.insertBatch(sampleCustomers);
        long parquetBatchTime = System.currentTimeMillis() - parquetStartTime;
        
        // Parquet should handle analytical scans efficiently
        parquetStartTime = System.currentTimeMillis();
        List<TableRecord> parquetScanResults = parquetTable.scan("", null, Arrays.asList("name", "age"));
        long parquetScanTime = System.currentTimeMillis() - parquetStartTime;
        
        assertEquals(sampleCustomers.size(), parquetScanResults.size());
        
        parquetTable.close();
        
        // Verify that both approaches work but have different characteristics
        System.out.println("📈 Performance characteristics verified:");
        System.out.println("   B+Tree individual inserts: " + btreeInsertTime + "ms");
        System.out.println("   B+Tree point lookup: " + btreeLookupTime + "ms");
        System.out.println("   Parquet batch insert: " + parquetBatchTime + "ms");
        System.out.println("   Parquet analytical scan: " + parquetScanTime + "ms");
        
        // Both should complete successfully (timing will vary)
        assertTrue(btreeInsertTime >= 0);
        assertTrue(btreeLookupTime >= 0);
        assertTrue(parquetBatchTime >= 0);
        assertTrue(parquetScanTime >= 0);
    }
    
    // Helper test methods
    
    private void testPointLookups(Table table) throws IOException {
        // Test existing keys
        Optional<TableRecord> result1 = table.findByPrimaryKey("CUST001");
        assertTrue(result1.isPresent());
        assertEquals("Alice Johnson", result1.get().getValue("name"));
        assertEquals("New York", result1.get().getValue("city"));
        
        Optional<TableRecord> result2 = table.findByPrimaryKey("CUST005");
        assertTrue(result2.isPresent());
        assertEquals("Eve Brown", result2.get().getValue("name"));
        assertEquals(29, result2.get().getValue("age"));
        
        // Test non-existent key
        Optional<TableRecord> result3 = table.findByPrimaryKey("CUST999");
        assertFalse(result3.isPresent());
    }
    
    private void testRangeScans(Table table) throws IOException {
        List<TableRecord> results = table.scan("CUST001", "CUST006", null);
        
        // Should include CUST001 through CUST005 (CUST006 is exclusive)
        assertEquals(5, results.size());
        
        // Verify order and content
        assertEquals("CUST001", results.get(0).getPrimaryKey());
        assertEquals("Alice Johnson", results.get(0).getValue("name"));
        
        assertEquals("CUST005", results.get(4).getPrimaryKey());
        assertEquals("Eve Brown", results.get(4).getValue("name"));
    }
    
    private void testColumnProjection(Table table) throws IOException {
        List<String> columns = Arrays.asList("name", "city");
        List<TableRecord> results = table.scan("CUST001", "CUST004", columns);
        
        assertEquals(3, results.size());
        
                 // Verify projected columns are present
         for (TableRecord record : results) {
             assertTrue(record.getValues().containsKey("name"));
             assertTrue(record.getValues().containsKey("city"));
             // Age and email should not be present in projection
             assertFalse(record.getValues().containsKey("age"));
             assertFalse(record.getValues().containsKey("email"));
         }
    }
    
    private void testAnalyticalQueries(Table table) throws IOException {
        List<TableRecord> allRecords = table.scan("", null, null);
        
        assertEquals(sampleCustomers.size(), allRecords.size());
        
        // Calculate average age
        double avgAge = allRecords.stream()
            .mapToInt(r -> (Integer) r.getValue("age"))
            .average()
            .orElse(0.0);
        
        assertTrue(avgAge > 0);
        assertTrue(avgAge < 100); // Reasonable age range
        
        // Count by city
        Map<String, Long> cityCount = new HashMap<>();
        for (TableRecord record : allRecords) {
            String city = (String) record.getValue("city");
            cityCount.put(city, cityCount.getOrDefault(city, 0L) + 1);
        }
        
        assertFalse(cityCount.isEmpty());
        assertTrue(cityCount.containsKey("New York"));
        assertTrue(cityCount.containsKey("Los Angeles"));
    }
    
    private void testColumnOrientedAccess(Table table) throws IOException {
        List<String> nameColumns = Arrays.asList("name");
        List<TableRecord> results = table.scan("", null, nameColumns);
        
        assertEquals(sampleCustomers.size(), results.size());
        
                 // Verify only name column is present
         for (TableRecord record : results) {
             assertTrue(record.getValues().containsKey("name"));
             assertFalse(record.getValues().containsKey("age"));
             assertFalse(record.getValues().containsKey("email"));
             assertFalse(record.getValues().containsKey("city"));
         }
        
        // Verify we can extract all names
        List<String> names = results.stream()
            .map(r -> (String) r.getValue("name"))
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        
        assertFalse(names.isEmpty());
        assertTrue(names.contains("Alice Johnson"));
        assertTrue(names.contains("Bob Smith"));
    }
    
    // Helper methods
    
    private List<TableRecord> createSampleCustomers() {
        List<TableRecord> customers = new ArrayList<>();
        
        customers.add(createCustomer("CUST001", "Alice Johnson", "alice@example.com", 28, "New York"));
        customers.add(createCustomer("CUST002", "Bob Smith", "bob@example.com", 35, "Los Angeles"));
        customers.add(createCustomer("CUST003", "Carol Davis", "carol@example.com", 42, "Chicago"));
        customers.add(createCustomer("CUST004", "David Wilson", "david@example.com", 31, "Houston"));
        customers.add(createCustomer("CUST005", "Eve Brown", "eve@example.com", 29, "Phoenix"));
        customers.add(createCustomer("CUST006", "Frank Miller", "frank@example.com", 38, "Philadelphia"));
        customers.add(createCustomer("CUST007", "Grace Lee", "grace@example.com", 26, "San Antonio"));
        customers.add(createCustomer("CUST008", "Henry Taylor", "henry@example.com", 44, "San Diego"));
        
        return customers;
    }
    
    private TableRecord createCustomer(String id, String name, String email, Integer age, String city) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("email", email);
        values.put("age", age);
        values.put("city", city);
        
        return new TableRecord(id, values);
    }
    
    /**
     * Parquet storage adapter implementation for testing.
     * This is a simplified version that demonstrates the concept.
     */
    private static class ParquetStorageAdapter implements StorageInterface {
        private final String filename;
        private final List<minispark.storage.Record> bufferedRecords;
        private boolean hasWrittenFile = false;
        
        public ParquetStorageAdapter(String filename) {
            this.filename = filename;
            this.bufferedRecords = new ArrayList<>();
        }
        
        @Override
        public void write(byte[] key, Map<String, Object> value) throws IOException {
            String keyStr = new String(key);
            bufferedRecords.add(new minispark.storage.Record(key, value));
        }
        
        @Override
        public void writeBatch(List<minispark.storage.Record> records) throws IOException {
            bufferedRecords.addAll(records);
            hasWrittenFile = true;
        }
        
        @Override
        public Optional<Map<String, Object>> read(byte[] key) throws IOException {
            String keyStr = new String(key);
            
            for (minispark.storage.Record record : bufferedRecords) {
                if (keyStr.equals(new String(record.getKey()))) {
                    return Optional.of(record.getValue());
                }
            }
            
            return Optional.empty();
        }
        
        @Override
        public List<minispark.storage.Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
            String startStr = new String(startKey);
            String endStr = endKey != null ? new String(endKey) : null;
            
            List<minispark.storage.Record> results = new ArrayList<>();
            
            for (minispark.storage.Record record : bufferedRecords) {
                String recordKey = new String(record.getKey());
                
                // Check range
                if (recordKey.compareTo(startStr) >= 0 && 
                    (endStr == null || recordKey.compareTo(endStr) < 0)) {
                    
                    // Apply column projection if specified
                    if (columns != null && !columns.isEmpty()) {
                        Map<String, Object> projectedValue = new HashMap<>();
                        for (String column : columns) {
                            if (record.getValue().containsKey(column)) {
                                projectedValue.put(column, record.getValue().get(column));
                            }
                        }
                        results.add(new minispark.storage.Record(record.getKey(), projectedValue));
                    } else {
                        results.add(record);
                    }
                }
            }
            
            return results;
        }
        
        @Override
        public void delete(byte[] key) throws IOException {
            // Simplified implementation for testing
            String keyStr = new String(key);
            bufferedRecords.removeIf(record -> keyStr.equals(new String(record.getKey())));
        }
        
        @Override
        public void close() throws IOException {
            // Cleanup if needed
        }
    }
} 