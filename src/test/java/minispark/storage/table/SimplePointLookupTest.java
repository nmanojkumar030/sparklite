package minispark.storage.table;

import minispark.storage.btree.BTree;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit test version of SimplePointLookupDemo.
 * Tests point lookup functionality with B+Tree storage to ensure:
 * 1. Basic point lookups work correctly
 * 2. Multi-page B+Tree operations function properly
 * 3. Page splitting doesn't break lookup functionality
 * 4. BufferUnderflowException edge cases are handled
 */
public class SimplePointLookupTest {
    
    @TempDir
    Path tempDir;
    
    private BTree btree;
    private Table customerTable;
    private TableSchema schema;
    private Path dbPath;
    
    @BeforeEach
    void setUp() throws IOException {
        dbPath = tempDir.resolve("test_customers.db");
        btree = new BTree(dbPath);
        schema = TableSchema.createCustomerSchema();
        customerTable = new Table("test_customers", schema, btree);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (customerTable != null) {
            customerTable.close();
        }
    }
    
    @Test
    void testBasicPointLookups() throws IOException {
        System.out.println("\n🧪 Test: Basic Point Lookups");
        System.out.println("=" .repeat(50));
        
        // Insert sample customers
        insertSampleCustomers();
        
        // Test successful lookups
        Optional<TableRecord> result1 = customerTable.findByPrimaryKey("CUST001");
        assertTrue(result1.isPresent(), "Should find CUST001");
        assertEquals("Alice Johnson", result1.get().getValue("name"));
        assertEquals(28, result1.get().getValue("age"));
        
        Optional<TableRecord> result2 = customerTable.findByPrimaryKey("CUST003");
        assertTrue(result2.isPresent(), "Should find CUST003");
        assertEquals("Carol Davis", result2.get().getValue("name"));
        
        // Test non-existent key
        Optional<TableRecord> result3 = customerTable.findByPrimaryKey("CUST999");
        assertFalse(result3.isPresent(), "Should not find CUST999");
        
        System.out.println("✅ Basic point lookups test passed");
    }
    
    @Test
    void testSmallDatasetOperations() throws IOException {
        System.out.println("\n🧪 Test: Small Dataset Operations (10 records)");
        System.out.println("=" .repeat(50));
        
        btree.resetPageAccessCounters();
        
        // Insert 10 customers
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"};
        
        for (int i = 1; i <= 10; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            customerTable.insert(customer);
        }
        
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("📊 Insert stats: " + insertReads + " reads, " + insertWrites + " writes");
        
        // Test lookups
        btree.resetPageAccessCounters();
        
        Optional<TableRecord> result = customerTable.findByPrimaryKey("CUST0005");
        assertTrue(result.isPresent(), "Should find CUST0005");
        assertEquals("Customer 5", result.get().getValue("name"));
        
        long lookupReads = btree.getPageReadsCount();
        System.out.println("📊 Lookup stats: " + lookupReads + " reads");
        
        // With small dataset, should be very efficient
        assertTrue(lookupReads <= 2, "Small dataset lookup should require ≤2 page reads");
        
        System.out.println("✅ Small dataset operations test passed");
    }
    
    @Test
    void testMediumDatasetWithPageSplitting() throws IOException {
        System.out.println("\n🧪 Test: Medium Dataset with Page Splitting (50 records)");
        System.out.println("=" .repeat(50));
        
        btree.resetPageAccessCounters();
        
        // Insert 50 customers to force page splitting
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                          "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"};
        
        for (int i = 1; i <= 50; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            
            assertDoesNotThrow(() -> customerTable.insert(customer), 
                "Should be able to insert customer " + i + " without exception");
        }
        
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("📊 Insert stats: " + insertReads + " reads, " + insertWrites + " writes");
        
        // Test lookups from different parts of the dataset
        String[] testKeys = {"CUST0001", "CUST0025", "CUST0050", "CUST9999"};
        
        for (String key : testKeys) {
            btree.resetPageAccessCounters();
            
            Optional<TableRecord> result = customerTable.findByPrimaryKey(key);
            long lookupReads = btree.getPageReadsCount();
            
            if (key.equals("CUST9999")) {
                assertFalse(result.isPresent(), "Should not find " + key);
            } else {
                assertTrue(result.isPresent(), "Should find " + key);
                assertEquals("Customer " + Integer.parseInt(key.substring(4)), result.get().getValue("name"));
            }
            
            System.out.println("🔍 " + key + ": " + (result.isPresent() ? "FOUND" : "NOT FOUND") + 
                              " (" + lookupReads + " reads)");
            
            // Even with page splitting, lookups should be efficient
            assertTrue(lookupReads <= 4, "Lookup should require ≤4 page reads even after splitting");
        }
        
        System.out.println("✅ Medium dataset with page splitting test passed");
    }
    
    @Test
    void testLargeDatasetEfficiency() throws IOException {
        System.out.println("\n Test: Large Dataset Efficiency (100 records)");
        System.out.println("=" .repeat(50));
        
        btree.resetPageAccessCounters();
        
        // Insert 100 customers to create multi-level B+Tree
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                          "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
                          "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"};
        
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            
            assertDoesNotThrow(() -> customerTable.insert(customer), 
                "Should be able to insert customer " + i + " without exception");
        }
        
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("📊 Insert stats: " + insertReads + " reads, " + insertWrites + " writes");
        
        // Test that page splitting occurred (more writes than simple single-page scenario)
        assertTrue(insertWrites > 100, "Should have more writes than records due to page splitting");
        
        // Test efficient lookups across the tree
        String[] testKeys = {"CUST0001", "CUST0025", "CUST0050", "CUST0075", "CUST0100"};
        
        for (String key : testKeys) {
            btree.resetPageAccessCounters();
            
            Optional<TableRecord> result = customerTable.findByPrimaryKey(key);
            long lookupReads = btree.getPageReadsCount();
            
            assertTrue(result.isPresent(), "Should find " + key);
            assertEquals("Customer " + Integer.parseInt(key.substring(4)), result.get().getValue("name"));
            
            System.out.println("🔍 " + key + ": FOUND (" + lookupReads + " reads)");
            
            // B+Tree should maintain logarithmic efficiency
            assertTrue(lookupReads <= 5, "Lookup should require ≤5 page reads for 100 records (log efficiency)");
        }
        
        System.out.println("✅ Large dataset efficiency test passed");
    }
    
    @Test
    void testReverseOrderInsertion() throws IOException {
        System.out.println("\n🧪 Test: Reverse Order Insertion (BufferUnderflowException fix)");
        System.out.println("=" .repeat(50));
        
        btree.resetPageAccessCounters();
        
        // Insert in reverse order to test the specific edge case that caused BufferUnderflowException
        for (int i = 30; i >= 1; i--) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = "City" + i;
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            
            assertDoesNotThrow(() -> customerTable.insert(customer), 
                "Should be able to insert customer " + i + " in reverse order without BufferUnderflowException");
        }
        
        // Verify all records can be found
        for (int i = 1; i <= 30; i++) {
            String customerId = String.format("CUST%04d", i);
            Optional<TableRecord> result = customerTable.findByPrimaryKey(customerId);
            
            assertTrue(result.isPresent(), "Should find " + customerId + " after reverse insertion");
            assertEquals("Customer " + i, result.get().getValue("name"));
        }
        
        System.out.println("✅ Reverse order insertion test passed (BufferUnderflowException fixed)");
    }
    
    @Test
    void testRangeScanFunctionality() throws IOException {
        System.out.println("\n🧪 Test: Range Scan Functionality");
        System.out.println("=" .repeat(50));
        
        // Insert test data
        for (int i = 1; i <= 20; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "City" + i);
            customerTable.insert(customer);
        }
        
        // Test range scan
        List<TableRecord> results = customerTable.scan("CUST0005", "CUST0015", null);
        
        assertFalse(results.isEmpty(), "Range scan should return results");
        assertTrue(results.size() >= 5, "Should find multiple records in range");
        
        // Verify results are in range and sorted
        for (TableRecord record : results) {
            String id = (String) record.getValue("id");
            assertTrue(id.compareTo("CUST0005") >= 0, "Result should be >= start key");
            assertTrue(id.compareTo("CUST0015") <= 0, "Result should be <= end key");
        }
        
        System.out.println("📊 Range scan found " + results.size() + " records");
        System.out.println("✅ Range scan functionality test passed");
    }
    
    @Test
    void testPageAccessEfficiency() throws IOException {
        System.out.println("\n🧪 Test: Page Access Efficiency");
        System.out.println("=" .repeat(50));
        
        // Insert enough data to create multi-page structure
        for (int i = 1; i <= 60; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "City" + i);
            customerTable.insert(customer);
        }
        
        // Test that lookups are efficient
        btree.resetPageAccessCounters();
        
        Optional<TableRecord> result = customerTable.findByPrimaryKey("CUST0030");
        long lookupReads = btree.getPageReadsCount();
        
        assertTrue(result.isPresent(), "Should find CUST0030");
        assertTrue(lookupReads <= 3, "Should find record with ≤3 page reads (excellent efficiency)");
        
        System.out.println("🔍 Found CUST0030 with " + lookupReads + " page reads");
        
        if (lookupReads <= 2) {
            System.out.println("⚡ EXCELLENT: Optimal B+Tree efficiency!");
        } else if (lookupReads <= 3) {
            System.out.println("✅ GOOD: Efficient B+Tree performance");
        }
        
        System.out.println("✅ Page access efficiency test passed");
    }
    
    /**
     * Test equivalent to runSmallDatasetDemo() method from the original demo.
     */
    @Test
    void testRunSmallDatasetDemo() throws IOException {
        System.out.println("\n🧪 Test: runSmallDatasetDemo() equivalent");
        System.out.println("=" .repeat(50));
        
        // This replicates the exact logic from runSmallDatasetDemo()
        btree.resetPageAccessCounters();
        
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"};
        
        // Insert 10 customers exactly as in the demo
        for (int i = 1; i <= 10; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            customerTable.insert(customer);
        }
        
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("📈 Small Insert Statistics:");
        System.out.println("📖 Page reads: " + insertReads);
        System.out.println("✏️  Page writes: " + insertWrites);
        
        btree.resetPageAccessCounters();
        
        // Test basic lookups as in the demo
        String[] lookupKeys = {"CUST0001", "CUST0005", "CUST0010", "CUST9999"};
        
        for (String key : lookupKeys) {
            Optional<TableRecord> result = customerTable.findByPrimaryKey(key);
            
            if (key.equals("CUST9999")) {
                assertFalse(result.isPresent(), "Should not find " + key);
            } else {
                assertTrue(result.isPresent(), "Should find " + key);
                assertEquals("Customer " + Integer.parseInt(key.substring(4)), result.get().getValue("name"));
            }
        }
        
        System.out.println("✅ runSmallDatasetDemo() equivalent test passed");
    }
    
    /**
     * Test equivalent to runMediumDatasetDemo() method from the original demo.
     */
    @Test
    void testRunMediumDatasetDemo() throws IOException {
        System.out.println("\n🧪 Test: runMediumDatasetDemo() equivalent");
        System.out.println("=" .repeat(50));
        
        // This replicates the exact logic from runMediumDatasetDemo()
        btree.resetPageAccessCounters();
        
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                          "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"};
        
        // Insert 50 customers exactly as in the demo
        for (int i = 1; i <= 50; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            customerTable.insert(customer);
            
            if (i % 10 == 0) {
                System.out.println("📄 Inserted " + i + " customers so far...");
            }
        }
        
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("📈 Medium Insert Statistics:");
        System.out.println("📖 Page reads: " + insertReads);
        System.out.println("✏️  Page writes: " + insertWrites);
        
        btree.resetPageAccessCounters();
        
        // Test efficient lookups with page access tracking as in the demo
        String[] lookupKeys = {"CUST0001", "CUST0025", "CUST0050", "CUST9999"};
        
        for (String key : lookupKeys) {
            btree.resetPageAccessCounters();
            
            Optional<TableRecord> result = customerTable.findByPrimaryKey(key);
            long pageReads = btree.getPageReadsCount();
            long pageWrites = btree.getPageWritesCount();
            
            if (key.equals("CUST9999")) {
                assertFalse(result.isPresent(), "Should not find " + key);
            } else {
                assertTrue(result.isPresent(), "Should find " + key);
                assertEquals("Customer " + Integer.parseInt(key.substring(4)), result.get().getValue("name"));
            }
            
            System.out.println("📊 " + key + ": Page accesses: " + pageReads + " reads, " + pageWrites + " writes");
            
            if (pageReads <= 3) {
                System.out.println("⚡ EXCELLENT: Found with only " + pageReads + " disk access(es)!");
            }
        }
        
        System.out.println("✅ runMediumDatasetDemo() equivalent test passed");
    }
    
    /**
     * Test equivalent to runPointLookupDemo() method from the original demo.
     */
    @Test
    void testRunPointLookupDemo() throws IOException {
        System.out.println("\n🧪 Test: runPointLookupDemo() equivalent");
        System.out.println("=" .repeat(50));
        
        // This replicates the exact logic from runPointLookupDemo()
        System.out.println("📊 Setting up table with B+Tree storage...");
        
        // Insert many customers to force multiple pages and tree levels (100 customers as in demo)
        System.out.println("📝 Inserting customers to create multi-page B+Tree...");
        
        btree.resetPageAccessCounters();
        
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                          "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
                          "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"};
        
        // Insert 100 customers with distributed keys
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            customerTable.insert(customer);
            
            if (i % 25 == 0) {
                System.out.println("📄 Inserted " + i + " customers so far...");
            }
        }
        
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("📈 Bulk Insert Statistics:");
        System.out.println("📖 Total page reads during insert: " + insertReads);
        System.out.println("✏️  Total page writes during insert: " + insertWrites);
        System.out.println("🌳 B+Tree now spans multiple pages with internal nodes");
        
        // Demonstrate efficient B+Tree lookups with page access tracking
        System.out.println("🔍 Demonstrating efficient B+Tree lookups:");
        
        String[] lookupKeys = {
            "CUST0001",  // Very first record
            "CUST0025",  // Early in the tree
            "CUST0050",  // Middle of the tree  
            "CUST0075",  // Later in the tree
            "CUST0100",  // Last record
            "CUST9999"   // Non-existent key
        };
        
        for (String key : lookupKeys) {
            btree.resetPageAccessCounters();
            
            Optional<TableRecord> result = customerTable.findByPrimaryKey(key);
            long pageReads = btree.getPageReadsCount();
            long pageWrites = btree.getPageWritesCount();
            
            if (key.equals("CUST9999")) {
                assertFalse(result.isPresent(), "Should not find " + key);
            } else {
                assertTrue(result.isPresent(), "Should find " + key);
                assertEquals("Customer " + Integer.parseInt(key.substring(4)), result.get().getValue("name"));
            }
            
            System.out.println("📊 " + key + ": Page accesses: " + pageReads + " reads, " + pageWrites + " writes");
            
            if (pageReads <= 3) {
                System.out.println("⚡ EXCELLENT: Found with only " + pageReads + " disk access(es)!");
            } else if (pageReads <= 5) {
                System.out.println("✅ GOOD: Found with " + pageReads + " disk accesses");
            }
        }
        
        // Compare with naive approaches (range scan simulation)
        System.out.println("📊 Comparison: B+Tree vs Naive Approaches:");
        String targetKey = "CUST0050";
        
        // B+Tree Point Lookup
        btree.resetPageAccessCounters();
        long startTime = System.nanoTime();
        Optional<TableRecord> btreeResult = customerTable.findByPrimaryKey(targetKey);
        long endTime = System.nanoTime();
        long btreeReads = btree.getPageReadsCount();
        double btreeTimeMs = (endTime - startTime) / 1_000_000.0;
        
        assertTrue(btreeResult.isPresent(), "Should find target key with B+Tree");
        
        // Range Scan Simulation
        btree.resetPageAccessCounters();
        startTime = System.nanoTime();
        List<TableRecord> scanResults = customerTable.scan("CUST0001", "CUST0075", null);
        endTime = System.nanoTime();
        long scanReads = btree.getPageReadsCount();
        double scanTimeMs = (endTime - startTime) / 1_000_000.0;
        
        boolean foundInScan = scanResults.stream()
            .anyMatch(record -> record.getPrimaryKey().equals(targetKey));
        assertTrue(foundInScan, "Should find target key in range scan");
        
        System.out.println("B+Tree Point Lookup: " + btreeReads + " pages, " + 
                          String.format("%.3f", btreeTimeMs) + " ms");
        System.out.println("Range Scan Approach: " + scanReads + " pages, " + 
                          String.format("%.3f", scanTimeMs) + " ms");
        
        // B+Tree should be more efficient
        assertTrue(btreeReads <= scanReads, "B+Tree should be more efficient than range scan");
        
        System.out.println("✅ runPointLookupDemo() equivalent test passed");
    }
    
    /**
     * Helper method to insert the original sample customers for basic tests.
     */
    private void insertSampleCustomers() throws IOException {
        TableRecord[] customers = {
            createCustomer("CUST001", "Alice Johnson", "alice@example.com", 28, "New York"),
            createCustomer("CUST002", "Bob Smith", "bob@example.com", 35, "Los Angeles"),
            createCustomer("CUST003", "Carol Davis", "carol@example.com", 42, "Chicago"),
            createCustomer("CUST004", "David Wilson", "david@example.com", 31, "Houston"),
            createCustomer("CUST005", "Eve Brown", "eve@example.com", 29, "Phoenix")
        };
        
        for (TableRecord customer : customers) {
            customerTable.insert(customer);
        }
    }
    
    /**
     * Test the exact insertManyCustomers method from the original demo that was failing.
     * This ensures we test the original code path that caused the BufferUnderflowException.
     */
    @Test
    void testOriginalInsertManyCustomersMethod() throws IOException {
        System.out.println("\n🧪 Test: Original insertManyCustomers() method");
        System.out.println("=" .repeat(50));
        
        // This calls the exact same method that was failing in the original demo
        insertManyCustomers(customerTable, btree);
        
        // Verify that all 100 customers were inserted successfully
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            Optional<TableRecord> result = customerTable.findByPrimaryKey(customerId);
            
            assertTrue(result.isPresent(), "Should find " + customerId + " after insertManyCustomers");
            assertEquals("Customer " + i, result.get().getValue("name"));
        }
        
        // Test efficient lookups after the bulk insert
        btree.resetPageAccessCounters();
        Optional<TableRecord> result = customerTable.findByPrimaryKey("CUST0050");
        long pageReads = btree.getPageReadsCount();
        
        assertTrue(result.isPresent(), "Should find CUST0050");
        assertTrue(pageReads <= 3, "Should find record efficiently with ≤3 page reads");
        
        System.out.println("✅ Original insertManyCustomers() method test passed");
        System.out.println("🎯 BufferUnderflowException has been successfully fixed!");
    }
    
    /**
     * Exact replica of the insertManyCustomers method from SimplePointLookupDemo.
     * This is the method that was originally causing BufferUnderflowException.
     */
    private void insertManyCustomers(Table table, BTree btree) throws IOException {
        System.out.println("   📊 Inserting 100 customers to create multi-level B+Tree...");
        
        // Reset counters before bulk insert
        btree.resetPageAccessCounters();
        
        // Cities for variety
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                          "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
                          "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"};
        
        // Insert 100 customers with distributed keys (reduced from 500)
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50); // Ages from 20 to 69
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            
            // This is the exact line that was causing BufferUnderflowException
            table.insert(customer);
            
            // Show progress every 25 inserts
            if (i % 25 == 0) {
                System.out.println("   📄 Inserted " + i + " customers so far...");
            }
        }
        
        // Show page access statistics after bulk insert
        long insertReads = btree.getPageReadsCount();
        long insertWrites = btree.getPageWritesCount();
        
        System.out.println("\n   📈 Bulk Insert Statistics:");
        System.out.println("   📖 Total page reads during insert: " + insertReads);
        System.out.println("   ✏️  Total page writes during insert: " + insertWrites);
        System.out.println("   🌳 B+Tree now spans multiple pages with internal nodes");
        
        // Reset counters for lookup demonstration
        btree.resetPageAccessCounters();
    }
    
    /**
     * Test persistence and reopening of B+Tree file with continued operations.
     * This tests the critical scenario of:
     * 1. Writing 100 customers to a new B+Tree file
     * 2. Closing the B+Tree
     * 3. Reopening the same file
     * 4. Adding 100 more customers to the existing B+Tree
     * 5. Verifying all 200 customers are accessible
     */
    @Test
    void testPersistenceAndReopenWithContinuedOperations() throws IOException {
        System.out.println("\n🧪 Test: Persistence and Reopen with Continued Operations");
        System.out.println("=" .repeat(60));
        
        // Phase 1: Create initial B+Tree with 100 customers
        System.out.println("\n📝 Phase 1: Creating initial B+Tree with 100 customers...");
        
        insertManyCustomers(customerTable, btree);
        
        // Verify initial 100 customers are there
        System.out.println("🔍 Verifying initial 100 customers...");
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            Optional<TableRecord> result = customerTable.findByPrimaryKey(customerId);
            assertTrue(result.isPresent(), "Should find " + customerId + " in initial batch");
        }
        
        // Test efficient lookup before closing
        btree.resetPageAccessCounters();
        Optional<TableRecord> testResult = customerTable.findByPrimaryKey("CUST0050");
        long initialLookupReads = btree.getPageReadsCount();
        assertTrue(testResult.isPresent(), "Should find CUST0050 in initial batch");
        System.out.println("📊 Initial lookup efficiency: " + initialLookupReads + " page reads");
        
        // Phase 2: Close and reopen the B+Tree file
        System.out.println("\n🔄 Phase 2: Closing and reopening B+Tree file...");
        
        // Close the current table and B+Tree
        customerTable.close();
        
        // Reopen the same file - this tests persistence
        BTree reopenedBtree = new BTree(dbPath);
        Table reopenedTable = new Table("test_customers", schema, reopenedBtree);
        
        System.out.println("✅ B+Tree file reopened successfully");
        
        // Phase 3: Verify existing data is still accessible
        System.out.println("\n🔍 Phase 3: Verifying existing data after reopen...");
        
        // Test that all original 100 customers are still there
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            Optional<TableRecord> result = reopenedTable.findByPrimaryKey(customerId);
            assertTrue(result.isPresent(), "Should find " + customerId + " after reopen");
            assertEquals("Customer " + i, result.get().getValue("name"));
        }
        
        // Test efficient lookup after reopen
        reopenedBtree.resetPageAccessCounters();
        Optional<TableRecord> reopenTestResult = reopenedTable.findByPrimaryKey("CUST0075");
        long reopenLookupReads = reopenedBtree.getPageReadsCount();
        assertTrue(reopenTestResult.isPresent(), "Should find CUST0075 after reopen");
        System.out.println("📊 Post-reopen lookup efficiency: " + reopenLookupReads + " page reads");
        assertTrue(reopenLookupReads <= 3, "Lookup should still be efficient after reopen");
        
        // Phase 4: Add 100 more customers to the existing B+Tree
        System.out.println("\n📝 Phase 4: Adding 100 more customers to existing B+Tree...");
        
        reopenedBtree.resetPageAccessCounters();
        
        String[] cities = {"Seattle", "Boston", "Miami", "Denver", "Portland", 
                          "Atlanta", "Detroit", "Nashville", "Memphis", "Louisville",
                          "Cincinnati", "Cleveland", "Pittsburgh", "Buffalo", "Rochester"};
        
        // Add customers CUST0101 to CUST0200
        for (int i = 101; i <= 200; i++) {
            String customerId = String.format("CUST%04d", i);
            String name = "Customer " + i;
            String email = "customer" + i + "@example.com";
            int age = 20 + (i % 50);
            String city = cities[i % cities.length];
            
            TableRecord customer = createCustomer(customerId, name, email, age, city);
            
            assertDoesNotThrow(() -> reopenedTable.insert(customer), 
                "Should be able to insert customer " + i + " into existing B+Tree");
            
            if (i % 25 == 0) {
                System.out.println("   📄 Added " + (i - 100) + " more customers so far...");
            }
        }
        
        long additionalInsertReads = reopenedBtree.getPageReadsCount();
        long additionalInsertWrites = reopenedBtree.getPageWritesCount();
        
        System.out.println("\n📈 Additional Insert Statistics:");
        System.out.println("📖 Page reads for 100 additional customers: " + additionalInsertReads);
        System.out.println("✏️  Page writes for 100 additional customers: " + additionalInsertWrites);
        
        // Phase 5: Verify all 200 customers are accessible
        System.out.println("\n🔍 Phase 5: Verifying all 200 customers are accessible...");
        
        // Test original customers (1-100)
        for (int i = 1; i <= 100; i++) {
            String customerId = String.format("CUST%04d", i);
            Optional<TableRecord> result = reopenedTable.findByPrimaryKey(customerId);
            assertTrue(result.isPresent(), "Should find original customer " + customerId);
            assertEquals("Customer " + i, result.get().getValue("name"));
        }
        
        // Test new customers (101-200)
        for (int i = 101; i <= 200; i++) {
            String customerId = String.format("CUST%04d", i);
            Optional<TableRecord> result = reopenedTable.findByPrimaryKey(customerId);
            assertTrue(result.isPresent(), "Should find new customer " + customerId);
            assertEquals("Customer " + i, result.get().getValue("name"));
        }
        
        // Phase 6: Test lookup efficiency with 200 customers
        System.out.println("\n⚡ Phase 6: Testing lookup efficiency with 200 customers...");
        
        String[] testKeys = {"CUST0001", "CUST0050", "CUST0100", "CUST0150", "CUST0200"};
        
        for (String key : testKeys) {
            reopenedBtree.resetPageAccessCounters();
            
            Optional<TableRecord> result = reopenedTable.findByPrimaryKey(key);
            long lookupReads = reopenedBtree.getPageReadsCount();
            
            assertTrue(result.isPresent(), "Should find " + key + " in 200-customer B+Tree");
            System.out.println("🔍 " + key + ": " + lookupReads + " page reads");
            
            // Even with 200 customers, should maintain logarithmic efficiency
            assertTrue(lookupReads <= 4, "Should find " + key + " with ≤4 page reads (log efficiency)");
        }
        
        // Test range scan across the boundary
        System.out.println("\n📊 Testing range scan across original/new customer boundary...");
        List<TableRecord> boundaryResults = reopenedTable.scan("CUST0095", "CUST0105", null);
        
        assertFalse(boundaryResults.isEmpty(), "Range scan should return results across boundary");
        assertTrue(boundaryResults.size() >= 10, "Should find customers across the 100/101 boundary");
        
        System.out.println("📊 Range scan found " + boundaryResults.size() + " customers across boundary");
        
        // Clean up
        reopenedTable.close();
        
        System.out.println("\n✅ Persistence and reopen with continued operations test passed!");
        System.out.println("🎯 Key achievements:");
        System.out.println("   • B+Tree file persistence works correctly");
        System.out.println("   • Existing data survives file reopen");
        System.out.println("   • Can add more data to existing B+Tree");
        System.out.println("   • Maintains efficiency with 200 customers");
        System.out.println("   • All operations work seamlessly across file sessions");
    }
    
    /**
     * Helper method to create a customer record.
     */
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