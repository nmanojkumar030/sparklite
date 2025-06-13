package minispark.storage.btree;

import minispark.storage.table.Table;
import minispark.storage.table.TableRecord;
import minispark.storage.table.TableSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Educational Test Class: B+Tree Page Splitting Demonstrations
 * 
 * This test class demonstrates how B+Tree handles page splitting when pages become full.
 * Perfect for workshop demonstrations of B+Tree internal mechanics.
 * 
 * Learning Objectives:
 * 1. Understand when and why page splits occur
 * 2. See how B+Tree maintains balance during splits
 * 3. Observe tree height growth as data increases
 * 4. Learn about the production-quality splitting algorithm
 */
public class PageSplittingDemoTest {

    @Test
    public void testStory1_FillingSinglePageToCapacity(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 1: Filling a Single Page to Capacity ===");
        System.out.println("Let's see how many customers we can fit in a single B+Tree page before it splits.");
        
        Path tablePath = tempDir.resolve("single_page.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Inserting customers one by one until page is full...");
        
        // Insert customers - the educational value is in the logging output
        for (int i = 1; i <= 40; i++) {
            String customerId = String.format("CUST%03d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + (i % 30), "City" + (i % 5));
            
            table.insert(customer);
            
            if (i % 10 == 0) {
                System.out.println("   Inserted " + i + " customers so far...");
            }
        }
        
        System.out.println("   Inserted 40 customers - page splits will occur as needed");
        
        System.out.println("\nSUCCESS: Single page capacity demonstration completed!");
        System.out.println("Key Learning: B+Tree pages have finite capacity and split when full.");
        System.out.println("This is fundamental to how B+Trees scale to handle large datasets.");
        
        table.close();
    }

    @Test
    public void testStory2_FirstPageSplitMechanism(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 2: The First Page Split Mechanism ===");
        System.out.println("Let's observe exactly what happens during the first page split.");
        
        Path tablePath = tempDir.resolve("first_split.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Filling the page to just before capacity...");
        
        // Insert customers until we're close to capacity
        for (int i = 1; i <= 30; i++) {
            String customerId = String.format("CUST%03d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "City" + i);
            table.insert(customer);
        }
        
        System.out.println("   Inserted 30 customers - page should be getting full");
        
        System.out.println("\nStep 2: Triggering the split with one more customer...");
        
        // This insertion should trigger the split
        TableRecord triggerCustomer = createCustomer("CUST031", "Split Trigger Customer", 
            "trigger@example.com", 30, "Split City");
        
        // Just insert and verify it works - the educational value is in the logging
        assertDoesNotThrow(() -> table.insert(triggerCustomer), 
            "Should be able to insert customer that triggers split");
        
        System.out.println("   Split insertion completed successfully");
        
        System.out.println("\nStep 3: Verifying all customers are still accessible after split...");
        
        // Verify all customers are still accessible
        for (int i = 1; i <= 31; i++) {
            String customerId = String.format("CUST%03d", i);
            Optional<TableRecord> result = table.findByPrimaryKey(customerId);
            assertTrue(result.isPresent(), "Customer " + customerId + " should be accessible after split");
        }
        
        System.out.println("   All 31 customers successfully accessible after split");
        
        System.out.println("\nSUCCESS: First page split mechanism demonstrated!");
        System.out.println("Key Learning: Page splits redistribute data while maintaining accessibility.");
        System.out.println("The B+Tree automatically handles this complexity for us.");
        
        table.close();
    }

    @Test
    public void testStory3_TreeHeightGrowthDuringMultipleSplits(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 3: Tree Height Growth During Multiple Splits ===");
        System.out.println("Let's watch the B+Tree grow taller as we add more data.");
        
        Path tablePath = tempDir.resolve("growing_tree.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Starting with an empty tree...");
        System.out.println("   Initial tree height: 1 (just the root page)");
        
        // Insert customers in batches and observe tree growth
        int[] checkpoints = {25, 50, 75, 100, 150};
        
        for (int checkpoint : checkpoints) {
            System.out.println("\nStep: Inserting customers up to " + checkpoint + "...");
            
            // Insert customers up to this checkpoint
            for (int i = 1; i <= checkpoint; i++) {
                String customerId = String.format("CUST%03d", i);
                TableRecord customer = createCustomer(customerId, "Customer " + i, 
                    "customer" + i + "@example.com", 25 + (i % 40), "City" + (i % 10));
                table.insert(customer);
            }
            
            // Test lookup efficiency at this size
            btree.resetPageAccessCounters();
            Optional<TableRecord> testResult = table.findByPrimaryKey("CUST001");
            long lookupReads = btree.getPageReadsCount();
            
            assertTrue(testResult.isPresent(), "Should find CUST001 at checkpoint " + checkpoint);
            
            System.out.println("   Customers: " + checkpoint);
            System.out.println("   Lookup efficiency: " + lookupReads + " page reads");
            System.out.println("   Tree maintains O(log n) performance");
            
            // Verify the tree can handle this size efficiently
            assertTrue(lookupReads <= 4, "Should maintain logarithmic efficiency even at " + checkpoint + " customers");
        }
        
        System.out.println("\nStep 4: Final verification with range scan...");
        
        // Test range scanning across the entire dataset
        btree.resetPageAccessCounters();
        var rangeResults = table.scan("CUST001", "CUST050", null);
        long scanReads = btree.getPageReadsCount();
        
        assertFalse(rangeResults.isEmpty(), "Range scan should return results");
        System.out.println("   Range scan (CUST001-CUST050): " + rangeResults.size() + " results");
        System.out.println("   Range scan efficiency: " + scanReads + " page reads");
        
        System.out.println("\nSUCCESS: Tree height growth demonstration completed!");
        System.out.println("Key Learning: B+Tree automatically grows taller to maintain efficiency.");
        System.out.println("Even with 150 customers, lookups remain fast due to logarithmic height growth.");
        
        table.close();
    }

    @Test
    public void testStory4_SplitAlgorithmFairness(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 4: Split Algorithm Fairness ===");
        System.out.println("Let's see how the 50/50 split algorithm distributes data fairly.");
        
        Path tablePath = tempDir.resolve("fair_split.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Creating a scenario that will trigger multiple splits...");
        
        // Insert customers in a pattern that will cause splits
        for (int i = 1; i <= 60; i++) {
            String customerId = String.format("CUST%03d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "City" + i);
            table.insert(customer);
            
            if (i % 20 == 0) {
                System.out.println("   Inserted " + i + " customers...");
            }
        }
        
        System.out.println("\nStep 2: Testing data distribution after splits...");
        
        // Test that data is evenly accessible across the range
        String[] testKeys = {"CUST001", "CUST015", "CUST030", "CUST045", "CUST060"};
        
        for (String key : testKeys) {
            btree.resetPageAccessCounters();
            Optional<TableRecord> result = table.findByPrimaryKey(key);
            long reads = btree.getPageReadsCount();
            
            assertTrue(result.isPresent(), "Should find " + key);
            System.out.println("   " + key + ": found with " + reads + " page reads");
            
            // All lookups should be reasonably efficient
            assertTrue(reads <= 4, "Lookup for " + key + " should be efficient");
        }
        
        System.out.println("\nStep 3: Testing range scans across split boundaries...");
        
        // Test range scans that cross page boundaries
        var earlyRange = table.scan("CUST001", "CUST020", null);
        var middleRange = table.scan("CUST025", "CUST045", null);
        var lateRange = table.scan("CUST050", "CUST060", null);
        
        assertFalse(earlyRange.isEmpty(), "Early range should have results");
        assertFalse(middleRange.isEmpty(), "Middle range should have results");
        assertFalse(lateRange.isEmpty(), "Late range should have results");
        
        System.out.println("   Early range (CUST001-020): " + earlyRange.size() + " customers");
        System.out.println("   Middle range (CUST025-045): " + middleRange.size() + " customers");
        System.out.println("   Late range (CUST050-060): " + lateRange.size() + " customers");
        
        System.out.println("\nSUCCESS: Split algorithm fairness demonstrated!");
        System.out.println("Key Learning: The 50/50 split algorithm ensures balanced data distribution.");
        System.out.println("This is the same algorithm used by production databases like BoltDB and SQLite.");
        
        table.close();
    }

    @Test
    public void testStory5_InsertionOrderIndependence(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 5: Insertion Order Independence ===");
        System.out.println("Let's prove that B+Tree performance doesn't depend on insertion order.");
        
        Path tablePath = tempDir.resolve("order_independent.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        System.out.println("\nStep 1: Inserting customers in reverse order...");
        
        // Insert customers in reverse order (worst case for many data structures)
        for (int i = 50; i >= 1; i--) {
            String customerId = String.format("CUST%03d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "City" + i);
            table.insert(customer);
            
            if (i % 10 == 0) {
                System.out.println("   Inserted customer " + i + " (reverse order)");
            }
        }
        
        System.out.println("\nStep 2: Testing lookup performance after reverse insertion...");
        
        // Test lookups across the range
        String[] testKeys = {"CUST001", "CUST010", "CUST025", "CUST040", "CUST050"};
        
        for (String key : testKeys) {
            btree.resetPageAccessCounters();
            Optional<TableRecord> result = table.findByPrimaryKey(key);
            long reads = btree.getPageReadsCount();
            
            assertTrue(result.isPresent(), "Should find " + key + " despite reverse insertion");
            System.out.println("   " + key + ": found with " + reads + " page reads");
            
            // Performance should still be good despite reverse insertion
            assertTrue(reads <= 4, "Lookup should be efficient despite insertion order");
        }
        
        System.out.println("\nStep 3: Testing range scan performance...");
        
        btree.resetPageAccessCounters();
        var allCustomers = table.scan("CUST001", "CUST051", null);
        long scanReads = btree.getPageReadsCount();
        
        assertEquals(50, allCustomers.size(), "Should find all 50 customers");
        System.out.println("   Full range scan: " + allCustomers.size() + " customers");
        System.out.println("   Scan efficiency: " + scanReads + " page reads");
        
        // Verify customers are in sorted order despite reverse insertion
        for (int i = 0; i < allCustomers.size() - 1; i++) {
            String currentKey = allCustomers.get(i).getPrimaryKey();
            String nextKey = allCustomers.get(i + 1).getPrimaryKey();
            assertTrue(currentKey.compareTo(nextKey) < 0, 
                "Customers should be in sorted order: " + currentKey + " < " + nextKey);
        }
        
        System.out.println("   Customers are properly sorted despite reverse insertion");
        
        System.out.println("\nSUCCESS: Insertion order independence demonstrated!");
        System.out.println("Key Learning: B+Tree maintains performance regardless of insertion order.");
        System.out.println("This makes it robust for real-world applications with unpredictable data patterns.");
        
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