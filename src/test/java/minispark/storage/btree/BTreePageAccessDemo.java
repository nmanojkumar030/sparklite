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
 * Demonstration test to show page access patterns in B+Tree operations.
 * This is useful for educational purposes to understand disk I/O behavior.
 */
public class BTreePageAccessDemo {
    @TempDir
    Path tempDir;
    
    private BTree btree;
    private Path dbPath;
    
    @BeforeEach
    void setUp() throws IOException {
        dbPath = tempDir.resolve("demo.btree");
        btree = new BTree(dbPath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (btree != null) {
            btree.close();
        }
    }
    
    @Test
    void demonstratePageAccessPatterns() throws IOException {
        System.out.println("🎓 B+TREE PAGE ACCESS DEMONSTRATION");
        System.out.println("=====================================");
        System.out.println();
        
        // Reset counters to start fresh
        btree.resetPageAccessCounters();
        
        // DEMO 1: Single Write Operation
        System.out.println("📝 DEMO 1: Single Write Operation");
        System.out.println("----------------------------------");
        Map<String, Object> value1 = new HashMap<>();
        value1.put("name", "Alice");
        value1.put("age", 25);
        btree.write("user001".getBytes(), value1);
        btree.printPageAccessStatistics();
        
        // DEMO 2: Single Read Operation
        System.out.println("📖 DEMO 2: Single Read Operation");
        System.out.println("---------------------------------");
        btree.resetPageAccessCounters();
        Optional<Map<String, Object>> result = btree.read("user001".getBytes());
        btree.printPageAccessStatistics();
        
        // DEMO 3: Multiple Write Operations
        System.out.println("📝 DEMO 3: Multiple Write Operations (Batch)");
        System.out.println("---------------------------------------------");
        btree.resetPageAccessCounters();
        for (int i = 2; i <= 10; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("name", "User" + i);
            value.put("age", 20 + i);
            btree.write(("user" + String.format("%03d", i)).getBytes(), value);
        }
        btree.printPageAccessStatistics();
        
        // DEMO 4: Multiple Read Operations
        System.out.println("📖 DEMO 4: Multiple Read Operations");
        System.out.println("-----------------------------------");
        btree.resetPageAccessCounters();
        for (int i = 1; i <= 5; i++) {
            btree.read(("user" + String.format("%03d", i)).getBytes());
        }
        btree.printPageAccessStatistics();
        
        // DEMO 5: Range Scan Operation
        System.out.println("🔍 DEMO 5: Range Scan Operation");
        System.out.println("-------------------------------");
        btree.resetPageAccessCounters();
        List<Record> scanResults = btree.scan(
            "user003".getBytes(),
            "user007".getBytes(),
            Arrays.asList("name", "age")
        );
        btree.printPageAccessStatistics();
        
        // DEMO 6: Non-existent Key Read
        System.out.println("❌ DEMO 6: Non-existent Key Read");
        System.out.println("--------------------------------");
        btree.resetPageAccessCounters();
        btree.read("nonexistent".getBytes());
        btree.printPageAccessStatistics();
        
        // DEMO 7: Key Update Operation
        System.out.println("🔄 DEMO 7: Key Update Operation");
        System.out.println("-------------------------------");
        btree.resetPageAccessCounters();
        Map<String, Object> updatedValue = new HashMap<>();
        updatedValue.put("name", "Alice Updated");
        updatedValue.put("age", 26);
        updatedValue.put("city", "New York");
        btree.write("user001".getBytes(), updatedValue);
        btree.printPageAccessStatistics();
        
        System.out.println("🎯 KEY OBSERVATIONS:");
        System.out.println("====================");
        System.out.println("1. Each write operation requires reading the target page first, then writing it back");
        System.out.println("2. Read operations only require reading pages (no writes)");
        System.out.println("3. Range scans may read multiple pages if data spans across pages");
        System.out.println("4. Non-existent key reads still require page access to verify absence");
        System.out.println("5. Updates require the same I/O as inserts (read + write)");
        System.out.println();
        System.out.println("💡 In a real B+Tree with multiple levels:");
        System.out.println("   - Each operation would traverse from root to leaf");
        System.out.println("   - More levels = more page reads per operation");
        System.out.println("   - Page caching would reduce actual disk I/O");
        System.out.println();
    }
    
    @Test
    void demonstratePageAccessWithMoreData() throws IOException {
        System.out.println("🎓 B+TREE PAGE ACCESS WITH MODERATE DATASET");
        System.out.println("===========================================");
        System.out.println();
        
        // Reset counters
        btree.resetPageAccessCounters();
        
        // Insert 10 records (smaller dataset to fit in one page)
        System.out.println("📝 Inserting 10 records...");
        for (int i = 1; i <= 10; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + i);
            value.put("score", i * 10);
            btree.write(("key" + String.format("%02d", i)).getBytes(), value);
        }
        
        System.out.println("📊 Page access after inserting 10 records:");
        btree.printPageAccessStatistics();
        
        // Reset and test random reads
        btree.resetPageAccessCounters();
        System.out.println("📖 Reading 5 random records...");
        Random random = new Random(42); // Fixed seed for reproducible results
        for (int i = 0; i < 5; i++) {
            int randomId = random.nextInt(10) + 1;
            btree.read(("key" + String.format("%02d", randomId)).getBytes());
        }
        
        System.out.println("📊 Page access after 5 random reads:");
        btree.printPageAccessStatistics();
        
        // Reset and test range scan
        btree.resetPageAccessCounters();
        System.out.println("🔍 Range scan from key03 to key07...");
        btree.scan("key03".getBytes(), "key07".getBytes(), null);
        
        System.out.println("📊 Page access after range scan:");
        btree.printPageAccessStatistics();
        
        System.out.println("🎯 OBSERVATIONS FOR TRAINING:");
        System.out.println("=============================");
        System.out.println("1. 📝 WRITES: Each write = 1 read + 1 write (2 I/O operations)");
        System.out.println("2. 📖 READS: Each read = 1 read (1 I/O operation)");
        System.out.println("3. 🔍 SCANS: Range scan = 1 read (since all data fits in one page)");
        System.out.println("4. 💾 TOTAL I/O: Write-heavy workloads have 2x more I/O than read-heavy");
        System.out.println("5. 🚀 OPTIMIZATION: Page caching would eliminate repeated reads of same page");
        System.out.println();
    }
} 