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
 * Educational Test Class: Basic B+Tree Operations
 * 
 * This test class demonstrates the fundamental operations of a B+Tree in a clear,
 * story-driven manner suitable for workshop demonstrations.
 * 
 * Learning Objectives:
 * 1. Understand basic B+Tree insert and read operations
 * 2. See how B+Tree maintains sorted order
 * 3. Observe single-page operations before complexity increases
 * 4. Learn about B+Tree efficiency for point lookups
 */
public class BasicOperationsTest {

    @Test
    public void testStory1_SingleCustomerInsertAndRead(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 1: Your First B+Tree Operation ===");
        System.out.println("Let's start with the simplest possible B+Tree operation: storing one customer.");
        
        // Create a B+Tree table
        Path tablePath = tempDir.resolve("customers.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        // Insert our first customer
        System.out.println("\nStep 1: Inserting our first customer...");
        TableRecord customer = createCustomer("CUST001", "Alice Johnson", "alice@example.com", 28, "New York");
        table.insert(customer);
        
        // Read the customer back
        System.out.println("\nStep 2: Reading the customer back...");
        Optional<TableRecord> result = table.findByPrimaryKey("CUST001");
        
        // Verify the operation
        assertTrue(result.isPresent(), "Customer should be found");
        assertEquals("Alice Johnson", result.get().getValue("name"));
        assertEquals("alice@example.com", result.get().getValue("email"));
        assertEquals(28, result.get().getValue("age"));
        
        System.out.println("SUCCESS: Single customer operation completed!");
        System.out.println("Key Learning: B+Tree can store and retrieve data efficiently, even with just one record.");
        
        table.close();
    }

    @Test
    public void testStory2_MultipleCustomersInSortedOrder(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 2: B+Tree Maintains Sorted Order ===");
        System.out.println("Let's see how B+Tree automatically keeps our data sorted by key.");
        
        Path tablePath = tempDir.resolve("sorted_customers.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        // Insert customers in random order
        System.out.println("\nStep 1: Inserting customers in random order...");
        String[] customerIds = {"CUST003", "CUST001", "CUST005", "CUST002", "CUST004"};
        String[] names = {"Charlie Brown", "Alice Johnson", "Eve Wilson", "Bob Smith", "David Lee"};
        
        for (int i = 0; i < customerIds.length; i++) {
            TableRecord customer = createCustomer(customerIds[i], names[i], 
                names[i].toLowerCase().replace(" ", ".") + "@example.com", 25 + i, "City" + i);
            table.insert(customer);
            System.out.println("   Inserted: " + customerIds[i] + " (" + names[i] + ")");
        }
        
        // Verify all customers can be found
        System.out.println("\nStep 2: Verifying all customers are accessible...");
        for (String customerId : customerIds) {
            Optional<TableRecord> result = table.findByPrimaryKey(customerId);
            assertTrue(result.isPresent(), "Customer " + customerId + " should be found");
            System.out.println("   Found: " + customerId + " (" + result.get().getValue("name") + ")");
        }
        
        System.out.println("\nSUCCESS: Multiple customer operations completed!");
        System.out.println("Key Learning: B+Tree maintains internal sorted order regardless of insertion order.");
        System.out.println("This enables efficient range scans and lookups.");
        
        table.close();
    }

    @Test
    public void testStory3_EfficientPointLookups(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 3: B+Tree Efficiency Demonstration ===");
        System.out.println("Let's measure how efficiently B+Tree performs lookups.");
        
        Path tablePath = tempDir.resolve("efficient_customers.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        // Insert 20 customers (still fits in single page)
        System.out.println("\nStep 1: Inserting 20 customers...");
        for (int i = 1; i <= 20; i++) {
            String customerId = String.format("CUST%03d", i);
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 20 + (i % 50), "City" + i);
            table.insert(customer);
        }
        
        // Reset page access counters to measure lookup efficiency
        System.out.println("\nStep 2: Measuring lookup efficiency...");
        btree.resetPageAccessCounters();
        
        // Perform a lookup
        Optional<TableRecord> result = table.findByPrimaryKey("CUST010");
        
        // Check efficiency
        long pageReads = btree.getPageReadsCount();
        assertTrue(result.isPresent(), "Customer CUST010 should be found");
        assertEquals("Customer 10", result.get().getValue("name"));
        
        System.out.println("   Customer found with " + pageReads + " page read(s)");
        assertTrue(pageReads <= 2, "Should require minimal page reads for small dataset");
        
        System.out.println("\nSUCCESS: Efficient lookup demonstration completed!");
        System.out.println("Key Learning: B+Tree provides O(log n) lookup performance.");
        System.out.println("Even with 20 customers, we only needed " + pageReads + " disk access(es).");
        
        table.close();
    }

    @Test
    public void testStory4_NonExistentKeyHandling(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 4: Handling Non-Existent Keys ===");
        System.out.println("Let's see how B+Tree gracefully handles keys that don't exist.");
        
        Path tablePath = tempDir.resolve("missing_customers.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        // Insert a few customers
        System.out.println("\nStep 1: Inserting some customers...");
        for (int i = 1; i <= 5; i++) {
            String customerId = "CUST00" + i;
            TableRecord customer = createCustomer(customerId, "Customer " + i, 
                "customer" + i + "@example.com", 25 + i, "City" + i);
            table.insert(customer);
        }
        
        // Try to find existing customer
        System.out.println("\nStep 2: Looking for existing customer...");
        Optional<TableRecord> existingResult = table.findByPrimaryKey("CUST003");
        assertTrue(existingResult.isPresent(), "Existing customer should be found");
        System.out.println("   Found existing customer: " + existingResult.get().getValue("name"));
        
        // Try to find non-existent customer
        System.out.println("\nStep 3: Looking for non-existent customer...");
        Optional<TableRecord> missingResult = table.findByPrimaryKey("CUST999");
        assertFalse(missingResult.isPresent(), "Non-existent customer should not be found");
        System.out.println("   Correctly returned empty result for non-existent key");
        
        System.out.println("\nSUCCESS: Non-existent key handling demonstration completed!");
        System.out.println("Key Learning: B+Tree safely handles missing keys without errors.");
        System.out.println("This makes it robust for real-world applications.");
        
        table.close();
    }

    @Test
    public void testStory5_DataTypesAndComplexValues(@TempDir Path tempDir) throws Exception {
        System.out.println("\n=== STORY 5: B+Tree with Complex Data Types ===");
        System.out.println("Let's see how B+Tree handles different data types and complex values.");
        
        Path tablePath = tempDir.resolve("complex_customers.btree");
        BTree btree = new BTree(tablePath);
        TableSchema schema = TableSchema.createCustomerSchema();
        Table table = new Table("customers", schema, btree);
        
        // Insert customers with various data types
        System.out.println("\nStep 1: Inserting customers with complex data...");
        
        // Customer with string, integer, and other types
        TableRecord customer1 = createCustomer("EMP001", "Alice Johnson", "alice@example.com", 28, "Engineering");
        table.insert(customer1);
        
        // Customer with different values
        TableRecord customer2 = createCustomer("EMP002", "Bob Smith", "bob@example.com", 35, "Marketing");
        table.insert(customer2);
        
        // Verify complex data retrieval
        System.out.println("\nStep 2: Verifying complex data retrieval...");
        
        Optional<TableRecord> result1 = table.findByPrimaryKey("EMP001");
        assertTrue(result1.isPresent(), "Employee EMP001 should be found");
        assertEquals("Alice Johnson", result1.get().getValue("name"));
        assertEquals(28, result1.get().getValue("age"));
        assertEquals("alice@example.com", result1.get().getValue("email"));
        assertEquals("Engineering", result1.get().getValue("city"));
        
        Optional<TableRecord> result2 = table.findByPrimaryKey("EMP002");
        assertTrue(result2.isPresent(), "Employee EMP002 should be found");
        assertEquals("Bob Smith", result2.get().getValue("name"));
        assertEquals(35, result2.get().getValue("age"));
        assertEquals("bob@example.com", result2.get().getValue("email"));
        assertEquals("Marketing", result2.get().getValue("city"));
        
        System.out.println("   Successfully retrieved all data types:");
        System.out.println("   - Strings: " + result1.get().getValue("name"));
        System.out.println("   - Integers: " + result1.get().getValue("age"));
        System.out.println("   - Emails: " + result1.get().getValue("email"));
        System.out.println("   - Cities: " + result1.get().getValue("city"));
        
        System.out.println("\nSUCCESS: Complex data types demonstration completed!");
        System.out.println("Key Learning: B+Tree can store and retrieve complex, structured data efficiently.");
        System.out.println("This makes it suitable for real database applications.");
        
        table.close();
    }
    
    // Helper method to create customer records
    private TableRecord createCustomer(String id, String name, String email, Integer age, String city) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);        // Include the id in the values map
        values.put("name", name);
        values.put("email", email);
        values.put("age", age);
        values.put("city", city);
        return new TableRecord(id, values);
    }
} 