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
 * Educational demonstration of B+Tree height correlation with I/O operations.
 * 
 * This demo directly addresses Assignment 2 requirements:
 * - Compare single-page vs multi-page performance
 * - Measure tree height impact on I/O operations
 * - Demonstrate efficiency patterns across different query types
 * - Show clear correlation between tree height and disk I/O count
 */
public class BTreeHeightAnalysisDemo {
    @TempDir
    Path tempDir;
    
    private BTree singlePageTree;
    private BTree multiPageTree;
    private BTree deepTree;
    private Path singlePagePath;
    private Path multiPagePath;
    private Path deepTreePath;
    
    @BeforeEach
    void setUp() throws IOException {
        singlePagePath = tempDir.resolve("single_page.btree");
        multiPagePath = tempDir.resolve("multi_page.btree");
        deepTreePath = tempDir.resolve("deep_tree.btree");
        
        singlePageTree = new BTree(singlePagePath);
        multiPageTree = new BTree(multiPagePath);
        deepTree = new BTree(deepTreePath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (singlePageTree != null) singlePageTree.close();
        if (multiPageTree != null) multiPageTree.close();
        if (deepTree != null) deepTree.close();
    }
    
    @Test
    void demonstrateTreeHeightIOCorrelation() throws IOException {
        System.out.println("🎓 B+TREE HEIGHT vs I/O OPERATIONS ANALYSIS");
        System.out.println("============================================");
        System.out.println();
        
        // Phase 1: Setup trees with different heights
        System.out.println("📊 PHASE 1: Creating Trees of Different Heights");
        System.out.println("===============================================");
        
        setupSinglePageTree();
        setupMultiPageTree();
        setupDeepTree();
        
        // Phase 2: Measure I/O patterns for identical operations
        System.out.println("📊 PHASE 2: I/O Measurement Comparison");
        System.out.println("======================================");
        
        measureAndComparePointLookups();
        measureAndCompareRangeScans();
        measureAndCompareInsertions();
        
        // Phase 3: Summary and insights
        System.out.println("📊 PHASE 3: Analysis Summary");
        System.out.println("============================");
        
        printCorrelationAnalysis();
    }
    
    private void setupSinglePageTree() throws IOException {
        System.out.println("🌱 Creating Single-Page Tree (Height = 1)");
        System.out.println("------------------------------------------");
        
        // Insert small records that fit in one page
        for (int i = 1; i <= 5; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + i);
            value.put("score", i * 10);
            
            singlePageTree.write(("key" + String.format("%02d", i)).getBytes(), value);
        }
        
        System.out.println("   ✅ Single-page tree created with 5 records");
        System.out.println("   📏 Expected Height: 1 (all data in leaf pages)");
        System.out.println();
    }
    
    private void setupMultiPageTree() throws IOException {
        System.out.println("🌳 Creating Multi-Page Tree (Height = 2)");
        System.out.println("-----------------------------------------");
        
        // Insert larger records to force page splits and create height 2
        for (int i = 1; i <= 12; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + String.format("%03d", i));
            value.put("email", "user" + i + "@company.com");
            
            // Large description to force page splits
            StringBuilder desc = new StringBuilder();
            for (int j = 0; j < 25; j++) {
                desc.append("Large data content for user ").append(i).append(" section ").append(j).append(". ");
            }
            value.put("description", desc.toString());
            value.put("score", i * 10);
            
            multiPageTree.write(("key" + String.format("%02d", i)).getBytes(), value);
        }
        
        System.out.println("   ✅ Multi-page tree created with 12 large records");
        System.out.println("   📏 Expected Height: 2 (internal nodes + leaf pages)");
        System.out.println();
    }
    
    private void setupDeepTree() throws IOException {
        System.out.println("🏗️ Creating Deep Tree (Height = 3+)");
        System.out.println("-----------------------------------");
        
        // Insert even more records with very large payloads to force deeper tree
        for (int i = 1; i <= 25; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + String.format("%03d", i));
            value.put("email", "user" + i + "@company.com");
            value.put("department", "Engineering");
            value.put("location", "Building-" + (i % 5 + 1));
            
            // Very large description to force multiple page splits
            StringBuilder desc = new StringBuilder();
            for (int j = 0; j < 40; j++) {
                desc.append("Very large data content for user ").append(i)
                    .append(" in section ").append(j).append(" with lots of details. ");
            }
            value.put("description", desc.toString());
            value.put("notes", "Additional notes for user " + i + " ".repeat(50));
            value.put("score", i * 10);
            
            deepTree.write(("key" + String.format("%02d", i)).getBytes(), value);
        }
        
        System.out.println("   ✅ Deep tree created with 25 very large records");
        System.out.println("   📏 Expected Height: 3+ (multiple levels of internal nodes)");
        System.out.println();
    }
    
    private void measureAndComparePointLookups() throws IOException {
        System.out.println("🔍 POINT LOOKUP COMPARISON");
        System.out.println("==========================");
        
        String[] testKeys = {"key01", "key05", "key10"};
        
        for (String key : testKeys) {
            System.out.println("📍 Testing key: " + key);
            
            // Test single-page tree
            singlePageTree.resetPageAccessCounters();
            singlePageTree.read(key.getBytes());
            long singlePageIO = singlePageTree.getPageReadsCount();
            
            // Test multi-page tree
            multiPageTree.resetPageAccessCounters();
            multiPageTree.read(key.getBytes());
            long multiPageIO = multiPageTree.getPageReadsCount();
            
            // Test deep tree
            deepTree.resetPageAccessCounters();
            deepTree.read(key.getBytes());
            long deepTreeIO = deepTree.getPageReadsCount();
            
            System.out.println("   Height 1 (Single-page): " + singlePageIO + " page reads");
            System.out.println("   Height 2 (Multi-page):  " + multiPageIO + " page reads");
            System.out.println("   Height 3+ (Deep tree):  " + deepTreeIO + " page reads");
            System.out.println("   📈 I/O Scaling: " + singlePageIO + " → " + multiPageIO + " → " + deepTreeIO);
            System.out.println();
        }
    }
    
    private void measureAndCompareRangeScans() throws IOException {
        System.out.println("📊 RANGE SCAN COMPARISON");
        System.out.println("========================");
        
        String[][] scanRanges = {
            {"key01", "key03"},
            {"key05", "key08"},
            {"key01", "key10"}
        };
        
        for (String[] range : scanRanges) {
            System.out.println("📍 Testing range: " + range[0] + " to " + range[1]);
            
            // Test single-page tree
            singlePageTree.resetPageAccessCounters();
            List<Record> results1 = singlePageTree.scan(range[0].getBytes(), range[1].getBytes(), null);
            long singlePageIO = singlePageTree.getPageReadsCount();
            
            // Test multi-page tree
            multiPageTree.resetPageAccessCounters();
            List<Record> results2 = multiPageTree.scan(range[0].getBytes(), range[1].getBytes(), null);
            long multiPageIO = multiPageTree.getPageReadsCount();
            
            // Test deep tree
            deepTree.resetPageAccessCounters();
            List<Record> results3 = deepTree.scan(range[0].getBytes(), range[1].getBytes(), null);
            long deepTreeIO = deepTree.getPageReadsCount();
            
            System.out.println("   Height 1: " + singlePageIO + " page reads (" + results1.size() + " records)");
            System.out.println("   Height 2: " + multiPageIO + " page reads (" + results2.size() + " records)");
            System.out.println("   Height 3+: " + deepTreeIO + " page reads (" + results3.size() + " records)");
            System.out.println("   📈 I/O Scaling: " + singlePageIO + " → " + multiPageIO + " → " + deepTreeIO);
            System.out.println();
        }
    }
    
    private void measureAndCompareInsertions() throws IOException {
        System.out.println("✍️ INSERTION COMPARISON");
        System.out.println("=======================");
        
        String[] newKeys = {"key99", "key98", "key97"};
        
        for (String key : newKeys) {
            System.out.println("📍 Testing insertion: " + key);
            
            Map<String, Object> value = new HashMap<>();
            value.put("id", 99);
            value.put("name", "TestUser");
            value.put("data", "test data");
            
            // Test single-page tree
            singlePageTree.resetPageAccessCounters();
            singlePageTree.write(key.getBytes(), new HashMap<>(value));
            long singlePageIO = singlePageTree.getPageReadsCount() + singlePageTree.getPageWritesCount();
            
            // Test multi-page tree
            multiPageTree.resetPageAccessCounters();
            multiPageTree.write(key.getBytes(), new HashMap<>(value));
            long multiPageIO = multiPageTree.getPageReadsCount() + multiPageTree.getPageWritesCount();
            
            // Test deep tree
            deepTree.resetPageAccessCounters();
            deepTree.write(key.getBytes(), new HashMap<>(value));
            long deepTreeIO = deepTree.getPageReadsCount() + deepTree.getPageWritesCount();
            
            System.out.println("   Height 1: " + singlePageIO + " total I/O operations");
            System.out.println("   Height 2: " + multiPageIO + " total I/O operations");
            System.out.println("   Height 3+: " + deepTreeIO + " total I/O operations");
            System.out.println("   📈 I/O Scaling: " + singlePageIO + " → " + multiPageIO + " → " + deepTreeIO);
            System.out.println();
        }
    }
    
    private void printCorrelationAnalysis() {
        System.out.println("🎯 KEY INSIGHTS: TREE HEIGHT vs I/O CORRELATION");
        System.out.println("================================================");
        System.out.println();
        
        System.out.println("📊 FUNDAMENTAL PRINCIPLE:");
        System.out.println("   Tree Height = Number of Levels to Traverse");
        System.out.println("   I/O Operations = Tree Height (for point lookups)");
        System.out.println();
        
        System.out.println("🔍 POINT LOOKUP PATTERNS:");
        System.out.println("   Height 1 (Single-page): 1 page read per lookup");
        System.out.println("   Height 2 (Multi-page):  2 page reads per lookup (root → leaf)");
        System.out.println("   Height 3+ (Deep tree):  3+ page reads per lookup (root → internal → leaf)");
        System.out.println();
        
        System.out.println("📈 RANGE SCAN PATTERNS:");
        System.out.println("   Height 1: 1 page read (all data in one page)");
        System.out.println("   Height 2: 1 + N page reads (root traversal + N leaf pages)");
        System.out.println("   Height 3+: Multiple internal page reads + N leaf pages");
        System.out.println();
        
        System.out.println("✍️ INSERTION PATTERNS:");
        System.out.println("   Height 1: 1 read + 1 write = 2 I/O operations");
        System.out.println("   Height 2: 2 reads + 1 write = 3 I/O operations (traverse + modify)");
        System.out.println("   Height 3+: 3+ reads + 1+ writes = 4+ I/O operations");
        System.out.println();
        
        System.out.println("🚀 PERFORMANCE IMPLICATIONS:");
        System.out.println("   ✅ Deeper trees can store more data");
        System.out.println("   ❌ Deeper trees require more I/O per operation");
        System.out.println("   ⚖️ Trade-off: Storage capacity vs Query performance");
        System.out.println();
        
        System.out.println("💡 OPTIMIZATION STRATEGIES:");
        System.out.println("   1. 📄 Larger page sizes → Shorter, wider trees");
        System.out.println("   2. 🗄️ Page caching → Reduce actual disk I/O");
        System.out.println("   3. 🎯 Clustered indexes → Better locality");
        System.out.println("   4. 📊 Bulk operations → Amortize I/O costs");
        System.out.println();
        
        System.out.println("🎓 EDUCATIONAL TAKEAWAYS:");
        System.out.println("   • Tree height directly correlates with I/O operations per query");
        System.out.println("   • Each additional level adds one more disk read to each operation");
        System.out.println("   • Database design must balance tree height with data capacity");
        System.out.println("   • Understanding I/O patterns is crucial for query optimization");
        System.out.println();
    }
    
    @Test
    void demonstratePageCacheEffects() throws IOException {
        System.out.println("🎓 PAGE CACHE EFFECTS DEMONSTRATION");
        System.out.println("===================================");
        System.out.println();
        
        // Setup a tree with some data
        BTree cacheTree = new BTree(tempDir.resolve("cache_demo.btree"));
        
        // Insert test data
        for (int i = 1; i <= 10; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i);
            value.put("name", "User" + i);
            value.put("data", "Some data for user " + i);
            
            cacheTree.write(("key" + String.format("%02d", i)).getBytes(), value);
        }
        
        System.out.println("📊 FIRST ACCESS (Cold Cache):");
        System.out.println("-----------------------------");
        
        // First access - cold cache
        cacheTree.resetPageAccessCounters();
        cacheTree.read("key05".getBytes());
        long coldCacheIO = cacheTree.getPageReadsCount();
        System.out.println("   Cold cache I/O: " + coldCacheIO + " page reads");
        
        System.out.println("📊 SECOND ACCESS (Warm Cache):");
        System.out.println("------------------------------");
        
        // Second access - warm cache (same page likely in memory)
        cacheTree.resetPageAccessCounters();
        cacheTree.read("key05".getBytes());
        long warmCacheIO = cacheTree.getPageReadsCount();
        System.out.println("   Warm cache I/O: " + warmCacheIO + " page reads");
        
        System.out.println("💡 CACHE INSIGHT:");
        System.out.println("   Real-world databases use page caching to reduce I/O");
        System.out.println("   First access: " + coldCacheIO + " reads, Second access: " + warmCacheIO + " reads");
        System.out.println("   Cache hit ratio is crucial for performance!");
        System.out.println();
        
        cacheTree.close();
    }
} 