package minispark.storage.btree;

import minispark.storage.table.Table;
import minispark.storage.table.TableRecord;
import minispark.storage.table.TableSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Educational Test Class: Advanced B+Tree Scenarios
 * 
 * This test class demonstrates advanced B+Tree capabilities including persistence,
 * large dataset handling, and real-world usage patterns.
 * 
 * Learning Objectives:
 * 1. Understand B+Tree persistence across sessions
 * 2. See how B+Tree scales to large datasets
 * 3. Learn about range scanning capabilities
 * 4. Observe real-world performance characteristics
 */
public class AdvancedScenariosTest {

    @Test
    public void testStory1_PersistenceAcrossSessions(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 1: B+Tree Persistence Across Sessions ===");
        System.out.println("Let's see how B+Tree data survives application restarts.");
        
        Path tablePath = tempDir.resolve("persistent.btree");
        
        // Session 1: Create and populate B+Tree
        System.out.println("\nSession 1: Creating and populating B+Tree...");
        {
            BTree btree = new BTree(tablePath);
            TableSchema schema = TableSchema.createCustomerSchema();
            Table table = new Table("customers", schema, btree);
            
            // Insert initial customers
            for (int i = 1; i <= 25; i++) {
                String customerId = String.format("CUST%03d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@example.com", 25 + i, "City" + i);
                table.insert(customer);
            }
            
            System.out.println("   Inserted 25 customers in Session 1");
            
            // Verify data is accessible
            Optional<TableRecord> testResult = table.findByPrimaryKey("CUST010");
            assertTrue(testResult.isPresent(), "Should find CUST010 in Session 1");
            assertEquals("Customer 10", testResult.get().getValue("name"));
            
            table.close();
            System.out.println("   Session 1 closed - data written to disk");
        }
        
        // Session 2: Reopen and verify data persisted
        System.out.println("\nSession 2: Reopening B+Tree file...");
        {
            BTree btree = new BTree(tablePath);
            TableSchema schema = TableSchema.createCustomerSchema();
            Table table = new Table("customers", schema, btree);
            
            System.out.println("   B+Tree file reopened successfully");
            
            // Verify all original data is still there
            for (int i = 1; i <= 25; i++) {
                String customerId = String.format("CUST%03d", i);
                Optional<TableRecord> result = table.findByPrimaryKey(customerId);
                assertTrue(result.isPresent(), "Should find " + customerId + " after reopen");
                assertEquals("Customer " + i, result.get().getValue("name"));
            }
            
            System.out.println("   All 25 customers successfully recovered from disk");
            
            // Add more data to existing B+Tree
            for (int i = 26; i <= 50; i++) {
                String customerId = String.format("CUST%03d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@example.com", 25 + i, "City" + i);
                table.insert(customer);
            }
            
            System.out.println("   Added 25 more customers to existing B+Tree");
            
            table.close();
        }
        
        // Session 3: Final verification
        System.out.println("\nSession 3: Final verification...");
        {
            BTree btree = new BTree(tablePath);
            TableSchema schema = TableSchema.createCustomerSchema();
            Table table = new Table("customers", schema, btree);
            
            // Verify all 50 customers are accessible
            for (int i = 1; i <= 50; i++) {
                String customerId = String.format("CUST%03d", i);
                Optional<TableRecord> result = table.findByPrimaryKey(customerId);
                assertTrue(result.isPresent(), "Should find " + customerId + " in final session");
            }
            
            System.out.println("   All 50 customers accessible across multiple sessions");
            
            table.close();
        }
        
        System.out.println("\nSUCCESS: Persistence across sessions demonstrated!");
        System.out.println("Key Learning: B+Tree provides durable storage that survives application restarts.");
        System.out.println("This is essential for database systems and persistent applications.");
    }

    @Test
    public void testStory2_LargeDatasetScaling(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 2: Large Dataset Scaling ===");
        System.out.println("Let's see how B+Tree performs with larger datasets.");
        
        Path tablePath = tempDir.resolve("large_dataset.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Inserting 200 customers...");
        
        // Insert a larger dataset
        for (int i = 1; i <= 200; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 20 + (i % 50), "City" + (i % 20));
            table.insert(customer);
            
            if (i % 50 == 0) {
                System.out.println("   Inserted " + i + " customers...");
            }
        }
        
        System.out.println("\nStep 2: Testing lookup efficiency with large dataset...");
        
        // Test lookups across the range
        String[] testKeys = {"CUST0001", "CUST0050", "CUST0100", "CUST0150", "CUST0200"};
        
        for (String key : testKeys) {
            btree.resetPageAccessCounters();
            Optional<TableRecord> result = table.findByPrimaryKey(key);
            long reads = btree.getPageReadsCount();
            
            assertTrue(result.isPresent(), "Should find " + key + " in large dataset");
            System.out.println("   " + key + ": found with " + reads + " page reads");
            
            // Even with 200 customers, should maintain logarithmic efficiency
            assertTrue(reads <= 5, "Should maintain efficiency with large dataset");
        }
        
        System.out.println("\nStep 3: Testing range scan performance...");
        
        btree.resetPageAccessCounters();
        List<TableRecord> rangeResults = table.scan("CUST0050", "CUST0100", null);
        long scanReads = btree.getPageReadsCount();
        
        assertFalse(rangeResults.isEmpty(), "Range scan should return results");
        System.out.println("   Range scan (CUST0050-0100): " + rangeResults.size() + " customers");
        System.out.println("   Range scan efficiency: " + scanReads + " page reads");
        
        System.out.println("\nSUCCESS: Large dataset scaling demonstrated!");
        System.out.println("Key Learning: B+Tree maintains O(log n) performance even with large datasets.");
        System.out.println("This scalability is why B+Trees are used in production databases.");
        
        table.close();
    }

    @Test
    public void testStory3_RangeScanCapabilities(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 3: Range Scan Capabilities ===");
        System.out.println("Let's explore B+Tree's powerful range scanning features.");
        
        Path tablePath = tempDir.resolve("range_scan.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Setting up test data with predictable patterns...");
        
        // Insert customers with predictable IDs for range testing
        for (int i = 10; i <= 90; i += 10) {
            String customerId = String.format("CUST%03d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "Region" + (i / 10));
            table.insert(customer);
        }
        
        System.out.println("   Inserted customers: CUST010, CUST020, ..., CUST090");
        
        System.out.println("\nStep 2: Testing various range scan patterns...");
        
        // Test different range patterns
        String[][] rangeTests = {
            {"CUST010", "CUST040", "Early range"},
            {"CUST030", "CUST070", "Middle range"},
            {"CUST060", "CUST090", "Late range"},
            {"CUST020", "CUST080", "Wide range"},
            {"CUST045", "CUST055", "Narrow range"}
        };
        
        for (String[] test : rangeTests) {
            String startKey = test[0];
            String endKey = test[1];
            String description = test[2];
            
            btree.resetPageAccessCounters();
            List<TableRecord> results = table.scan(startKey, endKey, null);
            long scanReads = btree.getPageReadsCount();
            
            System.out.println("   " + description + " (" + startKey + " to " + endKey + "):");
            System.out.println("     Found " + results.size() + " customers");
            System.out.println("     Required " + scanReads + " page reads");
            
            // Verify results are in sorted order
            for (int i = 0; i < results.size() - 1; i++) {
                String currentKey = results.get(i).getPrimaryKey();
                String nextKey = results.get(i + 1).getPrimaryKey();
                assertTrue(currentKey.compareTo(nextKey) < 0, 
                    "Results should be in sorted order");
            }
        }
        
        System.out.println("\nStep 3: Testing open-ended range scans...");
        
        // Test open-ended scans
        List<TableRecord> fromMiddle = table.scan("CUST050", null, null);
        System.out.println("   Open-ended scan from CUST050: " + fromMiddle.size() + " customers");
        assertTrue(fromMiddle.size() >= 4, "Should find customers from CUST050 onwards");
        
        System.out.println("\nSUCCESS: Range scan capabilities demonstrated!");
        System.out.println("Key Learning: B+Tree excels at range queries, not just point lookups.");
        System.out.println("This makes it perfect for analytical queries and reporting.");
        
        table.close();
    }

    @Test
    public void testStory4_RealWorldUsagePattern(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 4: Real-World Usage Pattern ===");
        System.out.println("Let's simulate a realistic application usage pattern.");
        
        Path tablePath = tempDir.resolve("real_world.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Initial data load (simulating application startup)...");
        
        // Initial data load
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 20 + (i % 60), "City" + (i % 15));
            table.insert(customer);
        }
        
        System.out.println("   Loaded 100 initial customers");
        
        System.out.println("\nStep 2: Mixed workload simulation...");
        
        // Simulate mixed read/write workload
        int lookupCount = 0;
        int insertCount = 0;
        int scanCount = 0;
        
        for (int operation = 1; operation <= 50; operation++) {
            if (operation % 3 == 0) {
                // Point lookup (most common operation)
                String lookupKey = String.format("CUST%04d", 1 + (operation % 100));
                Optional<TableRecord> result = table.findByPrimaryKey(lookupKey);
                if (result.isPresent()) {
                    lookupCount++;
                }
            } else if (operation % 7 == 0) {
                // Range scan (analytical query)
                int start = 1 + (operation % 80);
                int end = start + 20;
                String startKey = String.format("CUST%04d", start);
                String endKey = String.format("CUST%04d", end);
                List<TableRecord> scanResults = table.scan(startKey, endKey, null);
                if (!scanResults.isEmpty()) {
                    scanCount++;
                }
            } else {
                // Insert new customer (ongoing data growth)
                String newId = String.format("NEW%04d", operation);
                TableRecord newCustomer = createCustomer(newId, "New Customer " + operation, 
                    "new" + operation + "@example.com", 25 + operation, "NewCity" + operation);
                table.insert(newCustomer);
                insertCount++;
            }
        }
        
        System.out.println("   Completed mixed workload:");
        System.out.println("     Point lookups: " + lookupCount);
        System.out.println("     Range scans: " + scanCount);
        System.out.println("     New inserts: " + insertCount);
        
        System.out.println("\nStep 3: Performance verification after mixed workload...");
        
        // Verify performance is still good after mixed workload
        btree.resetPageAccessCounters();
        Optional<TableRecord> perfTest = table.findByPrimaryKey("CUST0050");
        long finalLookupReads = btree.getPageReadsCount();
        
        assertTrue(perfTest.isPresent(), "Should still find original customers");
        System.out.println("   Final lookup efficiency: " + finalLookupReads + " page reads");
        assertTrue(finalLookupReads <= 5, "Should maintain efficiency after mixed workload");
        
        // Test that new data is also accessible
        Optional<TableRecord> newDataTest = table.findByPrimaryKey("NEW0010");
        assertTrue(newDataTest.isPresent(), "Should find newly inserted data");
        
        System.out.println("\nSUCCESS: Real-world usage pattern demonstrated!");
        System.out.println("Key Learning: B+Tree handles mixed workloads efficiently.");
        System.out.println("It's designed for the read-heavy, occasionally-write patterns of real applications.");
        
        table.close();
    }

    @Test
    public void testStory5_PerformanceCharacteristics(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 5: Performance Characteristics ===");
        System.out.println("Let's measure and understand B+Tree performance characteristics.");
        
        Path tablePath = tempDir.resolve("performance.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Measuring insertion performance...");
        
        long startTime = System.currentTimeMillis();
        
        // Insert customers and measure time
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 20 + i, "City" + i);
            table.insert(customer);
        }
        
        long insertTime = System.currentTimeMillis() - startTime;
        System.out.println("   Inserted 100 customers in " + insertTime + "ms");
        System.out.println("   Average: " + (insertTime / 100.0) + "ms per insert");
        
        System.out.println("\nStep 2: Measuring lookup performance...");
        
        // Measure lookup performance
        long totalLookupTime = 0;
        long totalPageReads = 0;
        int lookupTests = 20;
        
        for (int i = 1; i <= lookupTests; i++) {
            String testKey = String.format("CUST%04d", i * 5);
            
            btree.resetPageAccessCounters();
            startTime = System.currentTimeMillis();
            
            Optional<TableRecord> result = table.findByPrimaryKey(testKey);
            
            long lookupTime = System.currentTimeMillis() - startTime;
            long pageReads = btree.getPageReadsCount();
            
            assertTrue(result.isPresent(), "Should find " + testKey);
            totalLookupTime += lookupTime;
            totalPageReads += pageReads;
        }
        
        double avgLookupTime = totalLookupTime / (double) lookupTests;
        double avgPageReads = totalPageReads / (double) lookupTests;
        
        System.out.println("   Average lookup time: " + String.format("%.2f", avgLookupTime) + "ms");
        System.out.println("   Average page reads: " + String.format("%.1f", avgPageReads));
        
        System.out.println("\nStep 3: Measuring range scan performance...");
        
        btree.resetPageAccessCounters();
        startTime = System.currentTimeMillis();
        
        List<TableRecord> rangeResults = table.scan("CUST0020", "CUST0080", null);
        
        long scanTime = System.currentTimeMillis() - startTime;
        long scanPageReads = btree.getPageReadsCount();
        
        System.out.println("   Range scan (60 customers): " + scanTime + "ms");
        System.out.println("   Range scan page reads: " + scanPageReads);
        System.out.println("   Results returned: " + rangeResults.size());
        
        assertTrue(rangeResults.size() >= 50, "Should return substantial range results");
        
        System.out.println("\nSUCCESS: Performance characteristics demonstrated!");
        System.out.println("Key Learning: B+Tree provides predictable, efficient performance.");
        System.out.println("Point lookups are very fast, and range scans are efficient for their scope.");
        
        table.close();
    }
    
    // Helper method to create customer records
    private TableRecord createCustomer(String id, String name, String email, Integer age, String city) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("email", email);
        values.put("age", age);
        values.put("city", city);
        return new TableRecord(id, values);
    }
} 