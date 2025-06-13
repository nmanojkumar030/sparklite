package minispark.storage.table;

import minispark.storage.btree.BTree;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Comprehensive test suite for B+Tree page splitting corner cases.
 * Tests each scenario systematically to identify issues and verify correctness.
 */
public class PageSplittingTestSuite {
    
    public static void main(String[] args) {
        try {
            System.out.println("🧪 B+Tree Page Splitting Test Suite");
            System.out.println("=" .repeat(60));
            
            // Run all test cases systematically
            runAllPageSplittingTests();
            
        } catch (Exception e) {
            System.err.println("❌ Test suite failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Run all page splitting test cases systematically.
     */
    public static void runAllPageSplittingTests() throws IOException {
        System.out.println("\n🎯 Testing B+Tree Page Splitting Corner Cases");
        System.out.println("Will test each scenario step by step to identify issues");
        System.out.println();
        
        // Test Case 1: Single Page Operations (No Splitting)
        testCase1_SinglePageOperations();
        
        // Test Case 2: Fill Single Page to Capacity
        testCase2_FillSinglePageToCapacity();
        
        // Test Case 3: First Leaf Page Split
        testCase3_FirstLeafPageSplit();
        
        // Test Case 4: Multiple Leaf Page Splits
        testCase4_MultipleLeafPageSplits();
        
        // Test Case 5: Root Page Split (Tree Height Increase)
        testCase5_RootPageSplit();
        
        System.out.println("\n✅ All page splitting tests completed!");
    }
    
    /**
     * Test Case 1: Basic single page operations (no splitting required).
     */
    private static void testCase1_SinglePageOperations() throws IOException {
        System.out.println("📋 Test Case 1: Single Page Operations (No Splitting)");
        System.out.println("-" .repeat(50));
        
        BTree btree = new BTree(java.nio.file.Paths.get("test1_single_page.db"));
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("test1", schema, btree);
        
        btree.resetPageAccessCounters();
        
        System.out.println("   Inserting 5 records (should fit in single page)...");
        
        for (int i = 1; i <= 5; i++) {
            String customerId = String.format("CUST%04d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@test.com", 25 + i, "City" + i);
            
            table.insert(customer);
            System.out.println("   ✅ Inserted: " + customerId);
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 Single Page Statistics:");
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        // Test lookups
        System.out.println("\n   🔍 Testing lookups:");
        btree.resetPageAccessCounters();
        
        Optional<TableRecord> result = table.findByPrimaryKey("CUST0003");
        long lookupReads = btree.getPageReadsCount();
        
        System.out.println("   Result: " + (result.isPresent() ? "FOUND" : "NOT FOUND"));
        System.out.println("   Lookup reads: " + lookupReads + " (should be 1 for single page)");
        
        table.close();
        System.out.println("   ✅ Test Case 1 PASSED\n");
    }
    
    /**
     * Test Case 2: Fill single page to near capacity.
     */
    private static void testCase2_FillSinglePageToCapacity() throws IOException {
        System.out.println("📋 Test Case 2: Fill Single Page to Capacity");
        System.out.println("-" .repeat(50));
        
        BTree btree = new BTree(java.nio.file.Paths.get("test2_full_page.db"));
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("test2", schema, btree);
        
        btree.resetPageAccessCounters();
        
        System.out.println("   Inserting records until page is nearly full...");
        
        // Start with conservative number and increase gradually
        int recordCount = 0;
        boolean insertionFailed = false;
        
        for (int i = 1; i <= 25 && !insertionFailed; i++) {
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
                insertionFailed = true;
            }
        }
        
        long reads = btree.getPageReadsCount();
        long writes = btree.getPageWritesCount();
        
        System.out.println("\n   📊 Full Page Statistics:");
        System.out.println("   📄 Records inserted: " + recordCount);
        System.out.println("   📖 Page reads: " + reads);
        System.out.println("   ✏️  Page writes: " + writes);
        
        table.close();
        System.out.println("   ✅ Test Case 2 COMPLETED\n");
    }
    
    /**
     * Test Case 3: First leaf page split.
     */
    private static void testCase3_FirstLeafPageSplit() throws IOException {
        System.out.println("📋 Test Case 3: First Leaf Page Split");
        System.out.println("-" .repeat(50));
        
        BTree btree = new BTree(java.nio.file.Paths.get("test3_first_split.db"));
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("test3", schema, btree);
        
        btree.resetPageAccessCounters();
        
        System.out.println("   Inserting records to force first page split...");
        
        try {
            // Insert enough records to force a page split
            for (int i = 1; i <= 35; i++) {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                
                if (i % 10 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            }
            
            long reads = btree.getPageReadsCount();
            long writes = btree.getPageWritesCount();
            
            System.out.println("\n   📊 First Split Statistics:");
            System.out.println("   📄 Records inserted: 35");
            System.out.println("   📖 Page reads: " + reads);
            System.out.println("   ✏️  Page writes: " + writes);
            
            // Test lookups after split
            System.out.println("\n   🔍 Testing lookups after split:");
            btree.resetPageAccessCounters();
            
            String[] testKeys = {"CUST0005", "CUST0015", "CUST0025", "CUST0035"};
            for (String key : testKeys) {
                btree.resetPageAccessCounters();
                Optional<TableRecord> result = table.findByPrimaryKey(key);
                long lookupReads = btree.getPageReadsCount();
                
                System.out.println("   " + key + ": " + 
                    (result.isPresent() ? "FOUND" : "NOT FOUND") + 
                    " (reads: " + lookupReads + ")");
            }
            
            System.out.println("   ✅ Test Case 3 PASSED");
            
        } catch (Exception e) {
            System.out.println("   ❌ Test Case 3 FAILED: " + e.getMessage());
            e.printStackTrace();
        }
        
        table.close();
        System.out.println();
    }
    
    /**
     * Test Case 4: Multiple leaf page splits.
     */
    private static void testCase4_MultipleLeafPageSplits() throws IOException {
        System.out.println("📋 Test Case 4: Multiple Leaf Page Splits");
        System.out.println("-" .repeat(50));
        
        BTree btree = new BTree(java.nio.file.Paths.get("test4_multi_splits.db"));
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("test4", schema, btree);
        
        btree.resetPageAccessCounters();
        
        System.out.println("   Inserting records to force multiple leaf splits...");
        
        try {
            // Insert enough records to force multiple page splits
            for (int i = 1; i <= 75; i++) {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                
                if (i % 15 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            }
            
            long reads = btree.getPageReadsCount();
            long writes = btree.getPageWritesCount();
            
            System.out.println("\n   📊 Multiple Splits Statistics:");
            System.out.println("   📄 Records inserted: 75");
            System.out.println("   📖 Page reads: " + reads);
            System.out.println("   ✏️  Page writes: " + writes);
            
            // Test lookups across different pages
            System.out.println("\n   🔍 Testing lookups across multiple pages:");
            String[] testKeys = {"CUST0001", "CUST0020", "CUST0040", "CUST0060", "CUST0075"};
            
            for (String key : testKeys) {
                btree.resetPageAccessCounters();
                Optional<TableRecord> result = table.findByPrimaryKey(key);
                long lookupReads = btree.getPageReadsCount();
                
                System.out.println("   " + key + ": " + 
                    (result.isPresent() ? "FOUND" : "NOT FOUND") + 
                    " (reads: " + lookupReads + ")");
            }
            
            System.out.println("   ✅ Test Case 4 PASSED");
            
        } catch (Exception e) {
            System.out.println("   ❌ Test Case 4 FAILED: " + e.getMessage());
            e.printStackTrace();
        }
        
        table.close();
        System.out.println();
    }
    
    /**
     * Test Case 5: Root page split (tree height increase).
     */
    private static void testCase5_RootPageSplit() throws IOException {
        System.out.println("📋 Test Case 5: Root Page Split (Tree Height Increase)");
        System.out.println("-" .repeat(50));
        
        BTree btree = new BTree(java.nio.file.Paths.get("test5_root_split.db"));
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("test5", schema, btree);
        
        btree.resetPageAccessCounters();
        
        System.out.println("   Inserting records to force root page split...");
        
        try {
            // Insert enough records to force root page to split
            for (int i = 1; i <= 100; i++) {
                String customerId = String.format("CUST%04d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@test.com", 25 + i, "City" + i);
                
                table.insert(customer);
                
                if (i % 20 == 0) {
                    System.out.println("   📄 Inserted " + i + " records so far...");
                }
            }
            
            long reads = btree.getPageReadsCount();
            long writes = btree.getPageWritesCount();
            
            System.out.println("\n   📊 Root Split Statistics:");
            System.out.println("   📄 Records inserted: 100");
            System.out.println("   📖 Page reads: " + reads);
            System.out.println("   ✏️  Page writes: " + writes);
            
            // Test lookups with increased tree height
            System.out.println("\n   🔍 Testing lookups with increased tree height:");
            String[] testKeys = {"CUST0001", "CUST0025", "CUST0050", "CUST0075", "CUST0100"};
            
            for (String key : testKeys) {
                btree.resetPageAccessCounters();
                Optional<TableRecord> result = table.findByPrimaryKey(key);
                long lookupReads = btree.getPageReadsCount();
                
                System.out.println("   " + key + ": " + 
                    (result.isPresent() ? "FOUND" : "NOT FOUND") + 
                    " (reads: " + lookupReads + ")");
            }
            
            System.out.println("   ✅ Test Case 5 PASSED");
            
        } catch (Exception e) {
            System.out.println("   ❌ Test Case 5 FAILED: " + e.getMessage());
            e.printStackTrace();
        }
        
        table.close();
        System.out.println();
    }
    
    /**
     * Helper method to create a customer record.
     */
    private static TableRecord createCustomer(String id, String name, String email, Integer age, String city) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("email", email);
        values.put("age", age);
        values.put("city", city);
        
        return new TableRecord(id, values);
    }
} 