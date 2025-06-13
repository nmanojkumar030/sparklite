package minispark.storage.table;

import minispark.storage.btree.BTree;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive JUnit tests for B+Tree page splitting corner cases.
 * Tests each scenario systematically to identify where BufferUnderflowException occurs.
 */
public class TablePageSplittingCornerCasesTest {
    
    @TempDir
    Path tempDir;
    
    private BTree btree;
    private Table table;
    private TableSchema schema;
    private Path dbPath;
    
    @BeforeEach
    void setUp() throws IOException {
        dbPath = tempDir.resolve("page_splitting_test.db");
        btree = new BTree(dbPath);
        schema = TableSchema.createCustomerSchema();
        table = new Table("test_customers", schema, btree);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (table != null) {
            table.close();
        }
    }
    
    @Test
    void testCase1_SinglePageOperations() throws IOException {
        System.out.println("\n🧪 Test Case 1: Single Page Operations (No Splitting)");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        // Insert 5 records that should fit in single page
        for (int i = 1; i <= 5; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@test.com", 25 + i, "City" + i);
            
            assertDoesNotThrow(() -> table.insert(customer), 
                "Should be able to insert record " + i + " without exception");
            
            System.out.println("   ✅ Inserted: " + customerId);
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 Single Page Statistics:");
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        // Test lookups
        btree.resetPageAccessCounters();
        Optional<TableRecord> result = table.findByPrimaryKey("CUST0003");
        long lookupReads = btree.getPageReadsCount();
        
        assertTrue(result.isPresent(), "Should find CUST0003");
        assertEquals(1, lookupReads, "Should only need 1 page read for single page lookup");
        
        System.out.println("   🔍 Lookup test: FOUND with " + lookupReads + " page read(s)");
        System.out.println("   ✅ Test Case 1 PASSED");
    }
    
    @Test
    void testCase2_FillSinglePageToCapacity() throws IOException {
        System.out.println("\n🧪 Test Case 2: Fill Single Page to Capacity");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        int recordCount = 0;
        boolean insertionSucceeded = true;
        
        // Insert records until we hit capacity or an error
        for (int i = 1; i <= 30 && insertionSucceeded; i++) {
            try {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                recordCount++;
                
                if (i % 5 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            } catch (Exception e) {
                System.out.println("   ⚠️  Insertion failed at record " + i + ": " + e.getMessage());
                insertionSucceeded = false;
            }
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 Capacity Test Results:");
        System.out.println("   📄 Records inserted: " + recordCount);
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        assertTrue(recordCount > 0, "Should be able to insert at least some records");
        
        System.out.println("   ✅ Test Case 2 COMPLETED");
    }
    
    @Test
    void testCase3_FirstLeafPageSplit() throws IOException {
        System.out.println("\n🧪 Test Case 3: First Leaf Page Split");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        int recordCount = 0;
        Exception caughtException = null;
        
        // Insert enough records to force first page split
        for (int i = 1; i <= 40; i++) {
            try {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                recordCount++;
                
                if (i % 10 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            } catch (Exception e) {
                caughtException = e;
                System.out.println("   ❌ Insertion failed at record " + i + ": " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 First Split Results:");
        System.out.println("   📄 Records inserted: " + recordCount);
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        if (caughtException != null) {
            System.out.println("   ❌ Exception occurred: " + caughtException.getClass().getSimpleName());
            System.out.println("       " + caughtException.getMessage());
            
            // Check if this is the BufferUnderflowException we're investigating
            if (caughtException instanceof java.nio.BufferUnderflowException) {
                System.out.println("   🎯 FOUND THE ISSUE: BufferUnderflowException during page splitting!");
                fail("BufferUnderflowException occurred at record " + (recordCount + 1) + 
                     ". This indicates a bug in page splitting logic.");
            }
        } else {
            System.out.println("   ✅ All insertions succeeded - page splitting may be working!");
            
            // Test lookups after potential splitting
            System.out.println("\n   🔍 Testing lookups after split:");
            String[] testKeys = {"CUST0005", "CUST0015", "CUST0025", "CUST0035"};
            
            for (String key : testKeys) {
                btree.resetPageAccessCounters();
                Optional<TableRecord> result = table.findByPrimaryKey(key);
                long lookupReads = btree.getPageReadsCount();
                
                assertTrue(result.isPresent(), "Should find " + key);
                System.out.println("   " + key + ": FOUND (reads: " + lookupReads + ")");
            }
            
            System.out.println("   ✅ Test Case 3 PASSED");
        }
    }
    
    @Test
    void testCase4_MultipleLeafPageSplits() throws IOException {
        System.out.println("\n🧪 Test Case 4: Multiple Leaf Page Splits");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        int recordCount = 0;
        Exception caughtException = null;
        
        // Insert enough records to force multiple page splits
        for (int i = 1; i <= 80; i++) {
            try {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                recordCount++;
                
                if (i % 20 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            } catch (Exception e) {
                caughtException = e;
                System.out.println("   ❌ Insertion failed at record " + i + ": " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 Multiple Splits Results:");
        System.out.println("   📄 Records inserted: " + recordCount);
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        if (caughtException != null) {
            System.out.println("   ❌ Exception occurred: " + caughtException.getClass().getSimpleName());
            if (caughtException instanceof java.nio.BufferUnderflowException) {
                System.out.println("   🎯 BufferUnderflowException in multiple splits test!");
                fail("BufferUnderflowException at record " + (recordCount + 1));
            }
        } else {
            System.out.println("   ✅ Multiple splits test completed successfully!");
        }
    }
    
    @Test 
    void testCase5_RootPageSplit() throws IOException {
        System.out.println("\n🧪 Test Case 5: Root Page Split (Tree Height Increase)");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        int recordCount = 0;
        Exception caughtException = null;
        
        // Insert enough records to force root page split
        for (int i = 1; i <= 120; i++) {
            try {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                recordCount++;
                
                if (i % 30 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            } catch (Exception e) {
                caughtException = e;
                System.out.println("   ❌ Insertion failed at record " + i + ": " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 Root Split Results:");
        System.out.println("   📄 Records inserted: " + recordCount);
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        if (caughtException != null) {
            System.out.println("   ❌ Exception occurred: " + caughtException.getClass().getSimpleName());
            if (caughtException instanceof java.nio.BufferUnderflowException) {
                System.out.println("   🎯 BufferUnderflowException in root split test!");
                fail("BufferUnderflowException at record " + (recordCount + 1));
            }
        } else {
            System.out.println("   ✅ Root split test completed successfully!");
            
            // Test lookups with increased tree height
            System.out.println("\n   🔍 Testing lookups with increased tree height:");
            String[] testKeys = {"CUST0001", "CUST0030", "CUST0060", "CUST0090", "CUST0120"};
            
            for (String key : testKeys) {
                btree.resetPageAccessCounters();
                Optional<TableRecord> result = table.findByPrimaryKey(key);
                long lookupReads = btree.getPageReadsCount();
                
                if (result.isPresent()) {
                    System.out.println("   " + key + ": FOUND (reads: " + lookupReads + ")");
                    assertTrue(lookupReads <= 3, "Lookup should require at most 3 page reads for this tree size");
                } else {
                    System.out.println("   " + key + ": NOT FOUND");
                }
            }
        }
    }
    
    @Test
    void testCase6_LargeValueInsertions() throws IOException {
        System.out.println("\n🧪 Test Case 6: Large Value Insertions");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        int recordCount = 0;
        Exception caughtException = null;
        
        // Insert records with large values to test edge cases
        for (int i = 1; i <= 15; i++) {
            try {
                String customerId = String.format("CUST%04d", i);
                String largeName = "Customer " + i + " with very long name ".repeat(20);
                String largeEmail = "very.long.email.address.for.customer." + i + "@example.com";
                
                TableRecord customer = createCustomer(customerId, largeName, largeEmail, 25 + i, "City" + i);
                
                table.insert(customer);
                recordCount++;
                
                System.out.println("   ✅ Inserted large record " + i);
            } catch (Exception e) {
                caughtException = e;
                System.out.println("   ❌ Large value insertion failed at record " + i + ": " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
        
        System.out.println("\n   📊 Large Value Results:");
        System.out.println("   📄 Records inserted: " + recordCount);
        
        if (caughtException != null && caughtException instanceof java.nio.BufferUnderflowException) {
            System.out.println("   🎯 BufferUnderflowException with large values!");
            fail("BufferUnderflowException with large values at record " + (recordCount + 1));
        }
    }
    
    @Test
    void testCase7_ReverseOrderInsertion() throws IOException {
        System.out.println("\n🧪 Test Case 7: Reverse Order Insertion");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        int recordCount = 0;
        Exception caughtException = null;
        
        // Insert in reverse order to test different split patterns
        for (int i = 50; i >= 1; i--) {
            try {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                recordCount++;
                
                if ((51 - i) % 10 == 0) {
                    System.out.println("   📄 Inserted " + (51 - i) + " records so far...");
                }
            } catch (Exception e) {
                caughtException = e;
                System.out.println("   ❌ Reverse insertion failed at record " + i + ": " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
        
        System.out.println("\n   📊 Reverse Insertion Results:");
        System.out.println("   📄 Records inserted: " + recordCount);
        
        if (caughtException != null && caughtException instanceof java.nio.BufferUnderflowException) {
            System.out.println("   🎯 BufferUnderflowException with reverse insertion!");
            fail("BufferUnderflowException with reverse insertion");
        } else {
            System.out.println("   ✅ Reverse insertion completed successfully!");
        }
    }
    
    @Test
    void testCase8_IdentifyBufferUnderflowConditions() throws IOException {
        System.out.println("\n🧪 Test Case 8: Identify BufferUnderflowException Conditions");
        System.out.println("=" .repeat(60));
        
        btree.resetPageAccessCounters();
        
        // Start with very conservative insertions and gradually increase
        int[] testSizes = {10, 20, 30, 35, 40, 45, 50};
        
        for (int testSize : testSizes) {
            System.out.println("\n   🔬 Testing with " + testSize + " records...");
            
            // Create fresh B+Tree for each test
            Path testPath = tempDir.resolve("buffer_test_" + testSize + ".db");
            BTree testBtree = new BTree(testPath);
            Table testTable = new Table("test_" + testSize, schema, testBtree);
            
            testBtree.resetPageAccessCounters();
            
            int successCount = 0;
            Exception caughtException = null;
            
            for (int i = 1; i <= testSize; i++) {
                try {
                    String customerId = String.format("CUST%04d", i);
                    TableRecord customer = createCustomer(customerId, "Customer " + i, 
                        "customer" + i + "@test.com", 25 + i, "City" + i);
                    
                    testTable.insert(customer);
                    successCount++;
                } catch (Exception e) {
                    caughtException = e;
                    break;
                }
            }
            
            long reads = testBtree.getPageReadsCount();
            long writes = testBtree.getPageWritesCount();
            
            System.out.println("     📊 Results: " + successCount + "/" + testSize + 
                              " records, " + reads + " reads, " + writes + " writes");
            
            if (caughtException != null) {
                System.out.println("     ❌ Exception: " + caughtException.getClass().getSimpleName());
                if (caughtException instanceof java.nio.BufferUnderflowException) {
                    System.out.println("     🎯 FOUND IT! BufferUnderflowException occurs at " + testSize + " records!");
                    System.out.println("        Successful inserts before failure: " + successCount);
                    caughtException.printStackTrace();
                    
                    testTable.close();
                    fail("BufferUnderflowException identified at " + testSize + " records with " + 
                         successCount + " successful inserts");
                }
            } else {
                System.out.println("     ✅ All " + testSize + " records inserted successfully");
            }
            
            testTable.close();
        }
        
        System.out.println("\n   ✅ All test sizes completed without BufferUnderflowException");
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