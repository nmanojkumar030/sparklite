package minispark.storage.btree;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Test to see what happens when we try to add more data than fits in a single page.
 * This will reveal whether the B+Tree actually implements page splitting.
 */
public class BTreePageSplittingTest {
    @TempDir
    Path tempDir;
    
    private BTree btree;
    private Path dbPath;
    
    @BeforeEach
    void setUp() throws IOException {
        dbPath = tempDir.resolve("splitting_test.btree");
        btree = new BTree(dbPath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (btree != null) {
            btree.close();
        }
    }
    
    @Test
    void testPageSplittingBehavior() throws IOException {
        System.out.println("🧪 TESTING B+TREE PAGE SPLITTING BEHAVIOR");
        System.out.println("==========================================");
        System.out.println();
        
        btree.resetPageAccessCounters();
        
        // Try to insert many records to force page splitting
        System.out.println("📝 Attempting to insert 100 records...");
        
        int successfulInserts = 0;
        int failedInserts = 0;
        
        for (int i = 1; i <= 100; i++) {
            try {
                Map<String, Object> value = new HashMap<>();
                value.put("id", i);
                value.put("name", "User" + String.format("%03d", i));
                value.put("email", "user" + i + "@example.com");
                value.put("description", "This is a longer description for user " + i + " to make the record larger");
                value.put("score", i * 10);
                value.put("active", true);
                
                String key = "key" + String.format("%03d", i);
                btree.write(key.getBytes(), value);
                successfulInserts++;
                
                if (i % 10 == 0) {
                    System.out.println("   ✅ Successfully inserted " + i + " records so far");
                }
                
            } catch (Exception e) {
                failedInserts++;
                System.out.println("   ❌ Failed to insert record " + i + ": " + e.getMessage());
                break; // Stop on first failure
            }
        }
        
        System.out.println();
        System.out.println("📊 INSERTION RESULTS:");
        System.out.println("   Successful inserts: " + successfulInserts);
        System.out.println("   Failed inserts: " + failedInserts);
        System.out.println();
        
        btree.printPageAccessStatistics();
        
        // Test reading some of the inserted records
        System.out.println("📖 Testing reads of inserted records...");
        btree.resetPageAccessCounters();
        
        for (int i = 1; i <= Math.min(10, successfulInserts); i++) {
            String key = "key" + String.format("%03d", i);
            var result = btree.read(key.getBytes());
            if (result.isPresent()) {
                System.out.println("   ✅ Successfully read key: " + key);
            } else {
                System.out.println("   ❌ Failed to read key: " + key);
            }
        }
        
        System.out.println();
        btree.printPageAccessStatistics();
        
        System.out.println("🔍 ANALYSIS:");
        System.out.println("=============");
        if (btree.getPageWritesCount() > successfulInserts) {
            System.out.println("✅ Multiple pages were created! (More writes than inserts)");
        } else {
            System.out.println("❌ Only single page used - no page splitting implemented");
        }
        
        if (failedInserts > 0) {
            System.out.println("⚠️  Some inserts failed - likely due to page being full");
            System.out.println("   This indicates page splitting is NOT implemented");
        } else {
            System.out.println("✅ All inserts succeeded - page splitting might be working");
        }
        
        System.out.println();
        System.out.println("💡 EXPECTED B+TREE BEHAVIOR:");
        System.out.println("   - When a leaf page gets full, it should split into two pages");
        System.out.println("   - A new internal/branch page should be created to point to both leaf pages");
        System.out.println("   - This should result in multiple page reads/writes");
        System.out.println("   - All inserts should succeed regardless of data volume");
        System.out.println();
    }
    
    @Test
    void testSinglePageCapacity() throws IOException {
        System.out.println("🧪 TESTING SINGLE PAGE CAPACITY");
        System.out.println("===============================");
        System.out.println();
        
        btree.resetPageAccessCounters();
        
        // Insert records one by one until we hit the limit
        int recordCount = 0;
        boolean canInsertMore = true;
        
        while (canInsertMore && recordCount < 1000) { // Safety limit
            try {
                recordCount++;
                Map<String, Object> value = new HashMap<>();
                value.put("id", recordCount);
                value.put("name", "User" + recordCount);
                value.put("data", "Some data for record " + recordCount);
                
                String key = "key" + String.format("%04d", recordCount);
                btree.write(key.getBytes(), value);
                
                if (recordCount % 5 == 0) {
                    System.out.println("   📝 Inserted " + recordCount + " records");
                }
                
            } catch (Exception e) {
                canInsertMore = false;
                recordCount--; // Last insert failed
                System.out.println("   ❌ Failed at record " + (recordCount + 1) + ": " + e.getMessage());
            }
        }
        
        System.out.println();
        System.out.println("📊 SINGLE PAGE CAPACITY RESULTS:");
        System.out.println("   Maximum records in single page: " + recordCount);
        System.out.println("   Page size: 4096 bytes");
        System.out.println("   Average bytes per record: " + (recordCount > 0 ? (4096 / recordCount) : "N/A"));
        System.out.println();
        
        btree.printPageAccessStatistics();
        
        // Verify we can read all inserted records
        System.out.println("📖 Verifying all records can be read...");
        btree.resetPageAccessCounters();
        
        int readSuccesses = 0;
        for (int i = 1; i <= recordCount; i++) {
            String key = "key" + String.format("%04d", i);
            var result = btree.read(key.getBytes());
            if (result.isPresent()) {
                readSuccesses++;
            }
        }
        
        System.out.println("   ✅ Successfully read " + readSuccesses + " out of " + recordCount + " records");
        btree.printPageAccessStatistics();
    }
    
    @Test
    void testPageSplittingWithLargeRecords() throws IOException {
        System.out.println("🧪 TESTING B+TREE PAGE SPLITTING WITH LARGE RECORDS");
        System.out.println("===================================================");
        System.out.println();
        
        btree.resetPageAccessCounters();
        
        // Create large records that will fill up pages quickly
        System.out.println("📝 Inserting large records to force page splitting...");
        
        int successfulInserts = 0;
        
        for (int i = 1; i <= 20; i++) {
            try {
                Map<String, Object> value = new HashMap<>();
                value.put("id", i);
                value.put("name", "User" + String.format("%03d", i));
                value.put("email", "user" + i + "@example.com");
                
                // Create a large description to fill up the page faster
                StringBuilder largeDesc = new StringBuilder();
                for (int j = 0; j < 50; j++) {
                    largeDesc.append("This is a very long description for user ").append(i)
                             .append(" with lots of text to make the record large. ");
                }
                value.put("description", largeDesc.toString());
                value.put("score", i * 10);
                value.put("active", true);
                
                String key = "key" + String.format("%03d", i);
                btree.write(key.getBytes(), value);
                successfulInserts++;
                
                System.out.println("   ✅ Successfully inserted record " + i);
                
            } catch (Exception e) {
                System.out.println("   ❌ Failed to insert record " + i + ": " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }
        
        System.out.println();
        System.out.println("📊 LARGE RECORD INSERTION RESULTS:");
        System.out.println("   Successful inserts: " + successfulInserts);
        System.out.println();
        
        btree.printPageAccessStatistics();
        
        // Test reading some records
        System.out.println("📖 Testing reads after page splitting...");
        btree.resetPageAccessCounters();
        
        for (int i = 1; i <= Math.min(5, successfulInserts); i++) {
            String key = "key" + String.format("%03d", i);
            var result = btree.read(key.getBytes());
            if (result.isPresent()) {
                System.out.println("   ✅ Successfully read key: " + key);
            } else {
                System.out.println("   ❌ Failed to read key: " + key);
            }
        }
        
        System.out.println();
        btree.printPageAccessStatistics();
        
        System.out.println("🔍 ANALYSIS:");
        System.out.println("=============");
        long totalWrites = btree.getPageWritesCount();
        long totalReads = btree.getPageReadsCount();
        
        if (totalWrites > successfulInserts * 2) {
            System.out.println("✅ Multiple pages were created! Page splitting is working!");
            System.out.println("   Expected writes for " + successfulInserts + " inserts without splitting: " + (successfulInserts * 2));
            System.out.println("   Actual writes: " + totalWrites);
            System.out.println("   Extra writes indicate page splitting occurred");
        } else {
            System.out.println("❌ No evidence of page splitting");
            System.out.println("   Writes: " + totalWrites + ", Expected for single page: " + (successfulInserts * 2));
        }
        
        System.out.println();
    }
} 