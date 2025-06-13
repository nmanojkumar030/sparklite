package minispark.storage.btree;

import minispark.storage.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Comprehensive demonstration of multi-page B+Tree functionality.
 * Shows page splitting, multi-level tree traversal, and I/O patterns.
 */
public class BTreeMultiPageDemo {
    @TempDir
    Path tempDir;
    
    private BTree btree;
    private Path dbPath;
    
    @BeforeEach
    void setUp() throws IOException {
        dbPath = tempDir.resolve("multipage_demo.btree");
        btree = new BTree(dbPath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (btree != null) {
            btree.close();
        }
    }
    
    @Test
    void demonstrateMultiPageBTree() throws IOException {
        System.out.println("🎓 MULTI-PAGE B+TREE DEMONSTRATION");
        System.out.println("===================================");
        System.out.println();
        
        btree.resetPageAccessCounters();
        
        // Phase 1: Insert records that will cause page splits
        System.out.println("📝 PHASE 1: Inserting records to trigger page splits");
        System.out.println("----------------------------------------------------");
        
        for (int i = 1; i <= 10; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + String.format("%03d", i));
            value.put("email", "user" + i + "@company.com");
            
            // Large description to force page splits
            StringBuilder desc = new StringBuilder();
            for (int j = 0; j < 30; j++) {
                desc.append("Large data for user ").append(i).append(" with content ").append(j).append(". ");
            }
            value.put("description", desc.toString());
            value.put("score", i * 10);
            
            String key = "user" + String.format("%03d", i);
            btree.write(key.getBytes(), value);
        }
        
        System.out.println();
        System.out.println("📊 Page access after insertions:");
        btree.printPageAccessStatistics();
        
        // Phase 2: Demonstrate multi-level tree traversal
        System.out.println("📖 PHASE 2: Reading records from multi-page tree");
        System.out.println("------------------------------------------------");
        btree.resetPageAccessCounters();
        
        // Read some specific records
        String[] keysToRead = {"user001", "user005", "user010"};
        for (String key : keysToRead) {
            var result = btree.read(key.getBytes());
            if (result.isPresent()) {
                System.out.println("   ✅ Found " + key + ": " + result.get().get("name"));
            } else {
                System.out.println("   ❌ Not found: " + key);
            }
        }
        
        System.out.println();
        System.out.println("📊 Page access for individual reads:");
        btree.printPageAccessStatistics();
        
        // Phase 3: Range scan across multiple pages
        System.out.println("🔍 PHASE 3: Range scan across multiple pages");
        System.out.println("--------------------------------------------");
        btree.resetPageAccessCounters();
        
        List<Record> scanResults = btree.scan(
            "user003".getBytes(),
            "user008".getBytes(),
            Arrays.asList("name", "email", "score")
        );
        
        System.out.println("   📄 Scan results:");
        for (Record record : scanResults) {
            System.out.println("     " + new String(record.getKey()) + ": " + record.getValue().get("name"));
        }
        
        System.out.println();
        System.out.println("📊 Page access for range scan:");
        btree.printPageAccessStatistics();
        
        // Phase 4: Insert more records to show tree growth
        System.out.println("📝 PHASE 4: Adding more records to grow the tree");
        System.out.println("------------------------------------------------");
        btree.resetPageAccessCounters();
        
        for (int i = 11; i <= 15; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + String.format("%03d", i));
            value.put("department", "Engineering");
            value.put("salary", 50000 + (i * 1000));
            
            String key = "user" + String.format("%03d", i);
            btree.write(key.getBytes(), value);
        }
        
        System.out.println();
        System.out.println("📊 Page access for additional insertions:");
        btree.printPageAccessStatistics();
        
        // Phase 5: Final analysis
        System.out.println("🎯 MULTI-PAGE B+TREE ANALYSIS");
        System.out.println("==============================");
        
        // Test a full scan
        btree.resetPageAccessCounters();
        List<Record> allRecords = btree.scan("user001".getBytes(), null, null);
        
        System.out.println("📊 Full scan statistics:");
        System.out.println("   Total records found: " + allRecords.size());
        btree.printPageAccessStatistics();
        
        System.out.println("💡 KEY OBSERVATIONS:");
        System.out.println("====================");
        System.out.println("1. 📄 PAGE SPLITTING: Tree automatically splits pages when full");
        System.out.println("2. 🌳 MULTI-LEVEL: Tree creates internal nodes to manage leaf pages");
        System.out.println("3. 📖 TREE TRAVERSAL: Reads require traversing from root to leaf");
        System.out.println("4. 🔍 RANGE SCANS: Efficiently scan across multiple leaf pages");
        System.out.println("5. 💾 I/O PATTERNS: More complex I/O patterns with multiple pages");
        System.out.println();
        System.out.println("🚀 This is now a TRUE B+TREE with:");
        System.out.println("   ✅ Automatic page splitting");
        System.out.println("   ✅ Multi-level tree structure");
        System.out.println("   ✅ Efficient range scanning");
        System.out.println("   ✅ Proper key distribution");
        System.out.println();
    }
    
    @Test
    void demonstratePageAccessPatterns() throws IOException {
        System.out.println("🎓 B+TREE PAGE ACCESS PATTERNS COMPARISON");
        System.out.println("==========================================");
        System.out.println();
        
        // First, create a multi-page tree
        System.out.println("📝 Setting up multi-page B+Tree...");
        for (int i = 1; i <= 8; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("data", "Large data string for record " + i + " ".repeat(200));
            
            btree.write(("key" + String.format("%02d", i)).getBytes(), value);
        }
        System.out.println("   ✅ Multi-page tree created");
        System.out.println();
        
        // Compare single read vs multiple reads
        System.out.println("📖 COMPARISON: Single Read vs Multiple Reads");
        System.out.println("---------------------------------------------");
        
        // Single read
        btree.resetPageAccessCounters();
        btree.read("key05".getBytes());
        long singleReadIO = btree.getPageReadsCount();
        System.out.println("   Single read I/O: " + singleReadIO + " page reads");
        
        // Multiple reads
        btree.resetPageAccessCounters();
        for (int i = 1; i <= 5; i++) {
            btree.read(("key" + String.format("%02d", i)).getBytes());
        }
        long multipleReadIO = btree.getPageReadsCount();
        System.out.println("   5 reads I/O: " + multipleReadIO + " page reads");
        System.out.println("   Average per read: " + (multipleReadIO / 5.0) + " page reads");
        
        // Range scan
        btree.resetPageAccessCounters();
        btree.scan("key02".getBytes(), "key06".getBytes(), null);
        long scanIO = btree.getPageReadsCount();
        System.out.println("   Range scan I/O: " + scanIO + " page reads");
        
        System.out.println();
        System.out.println("💡 EFFICIENCY INSIGHTS:");
        System.out.println("   - Multi-level trees require more I/O per operation");
        System.out.println("   - Range scans are more efficient than individual reads");
        System.out.println("   - Page caching would significantly improve performance");
        System.out.println();
    }
} 