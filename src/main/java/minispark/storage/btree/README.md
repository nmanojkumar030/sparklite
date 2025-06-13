# B+Tree Storage Engine with Page Access Logging

This B+Tree implementation includes comprehensive page access logging for educational purposes, making it perfect for demonstrating database internals and I/O patterns. **Now with full page splitting and multi-level tree support!**

## 🎓 Educational Features

### Page Access Logging
Every disk I/O operation is logged with detailed information:
- **📖 Page Reads**: When pages are read from disk
- **💾 Page Writes**: When pages are written to disk  
- **📊 Statistics**: Total read/write counts and I/O operations
- **🔄 Reset Counters**: Ability to reset counters between demonstrations

### B+Tree Operations Logging
- **📄 Page Splitting**: When leaf pages split due to overflow
- **🌳 Root Creation**: When new root pages are created
- **🆕 Page Allocation**: When new pages are allocated
- **✅ Operation Success**: Confirmation of successful operations

### Visual Output Example
```
🔍 BTree.write() - Writing key: user004
📖 PAGE READ #1 - Reading page 0 from disk
   ✅ Successfully read page 0 (4096 bytes)
📄 Leaf page 0 is full, splitting...
💾 PAGE WRITE #1 - Writing page 1 to disk
   ✅ Successfully wrote page 1 (4096 bytes)
🆕 Allocated new page: 1
✅ Split leaf page 0 -> 0 + 1 (separator: user003)
🌳 Creating new root page due to root split
✅ New root page created: 2
✅ Successfully wrote key: user004

📊 PAGE ACCESS STATISTICS:
   Total page reads:  5
   Total page writes: 4
   Total page I/O:    9
```

## 🚀 Running the Demonstrations

### Basic Page Access Patterns
```bash
./gradlew test --tests "minispark.storage.btree.BTreePageAccessDemo.demonstratePageAccessPatterns"
```

This demo shows:
1. **Single Write Operation** - Shows read-then-write pattern
2. **Single Read Operation** - Shows read-only access
3. **Multiple Write Operations** - Demonstrates batch I/O patterns
4. **Multiple Read Operations** - Shows repeated page access
5. **Range Scan Operation** - Demonstrates scan efficiency
6. **Non-existent Key Read** - Shows I/O cost of failed lookups
7. **Key Update Operation** - Shows update = read + write

### Multi-Page B+Tree Demonstration
```bash
./gradlew test --tests "minispark.storage.btree.BTreeMultiPageDemo.demonstrateMultiPageBTree"
```

This comprehensive demo shows:
- **Page Splitting**: Automatic page splits when pages become full
- **Multi-Level Tree**: Creation of internal nodes and tree growth
- **Tree Traversal**: Navigation from root to leaf pages
- **Range Scanning**: Efficient scanning across multiple pages
- **Complex I/O Patterns**: Real B+Tree disk access patterns

### Page Splitting Behavior
```bash
./gradlew test --tests "minispark.storage.btree.BTreePageSplittingTest.testPageSplittingWithLargeRecords"
```

This demo forces page splitting with large records to show:
- When and how pages split
- Creation of new pages
- Root page splitting and new root creation
- Multi-page tree structure

## 📊 Key Learning Points

### I/O Operation Patterns

#### Single-Page Tree (Small Dataset)
- **Write Operations**: 1 read + 1 write = 2 I/O operations
- **Read Operations**: 1 read = 1 I/O operation  
- **Range Scans**: 1 read for multiple records (very efficient)

#### Multi-Page Tree (Large Dataset)
- **Write Operations**: 2-3 reads + 2-3 writes = 4-6 I/O operations (tree traversal)
- **Read Operations**: 2-3 reads = 2-3 I/O operations (root → internal → leaf)
- **Range Scans**: 2-4 reads for multiple pages (still efficient)
- **Page Splits**: 4-6 additional I/O operations (read, split, write, update parent)

### Performance Implications
- **Tree Height**: Each level adds one more read per operation
- **Page Splitting**: Temporary I/O spike during splits, but improves long-term performance
- **Range Scans**: Become more efficient relative to individual reads as tree grows
- **Page Caching**: Would dramatically reduce I/O in real systems

## 🔧 Using Page Access Logging in Your Code

### Basic Usage
```java
// Create B+Tree with logging enabled
BTree btree = new BTree(Paths.get("mydb.btree"));

// Reset counters before measurement
btree.resetPageAccessCounters();

// Perform operations
btree.write("key1".getBytes(), value);
btree.read("key1".getBytes());

// Print statistics
btree.printPageAccessStatistics();

// Get specific counts
long reads = btree.getPageReadsCount();
long writes = btree.getPageWritesCount();
```

### Educational Demonstrations
The logging is designed to be:
- **Visual**: Uses emojis and clear formatting
- **Detailed**: Shows every I/O operation with byte counts
- **Measurable**: Provides exact statistics
- **Resettable**: Can reset counters between demonstrations
- **Comprehensive**: Shows page splits, tree growth, and multi-level operations

## 🎯 Training Scenarios

### Scenario 1: Compare Single-Page vs Multi-Page Performance
```java
// Small dataset (single page)
btree.resetPageAccessCounters();
for (int i = 0; i < 5; i++) {
    btree.write(("key" + i).getBytes(), smallValue);
}
long singlePageIO = btree.getPageReadsCount() + btree.getPageWritesCount();

// Large dataset (multi-page)
btree.resetPageAccessCounters();
for (int i = 0; i < 50; i++) {
    btree.write(("key" + i).getBytes(), largeValue);
}
long multiPageIO = btree.getPageReadsCount() + btree.getPageWritesCount();

System.out.println("Single-page I/O: " + singlePageIO);
System.out.println("Multi-page I/O: " + multiPageIO);
// Shows how tree structure affects performance
```

### Scenario 2: Demonstrate Page Splitting Impact
```java
btree.resetPageAccessCounters();
// Insert until page splits
btree.write("key1".getBytes(), largeValue);
btree.write("key2".getBytes(), largeValue); // This might trigger split
long splitIO = btree.getPageReadsCount() + btree.getPageWritesCount();

// Shows the I/O cost of page splitting
```

### Scenario 3: Tree Traversal Depth Analysis
```java
// Measure reads for different tree sizes
btree.resetPageAccessCounters();
btree.read("somekey".getBytes());
long readsForTreeHeight = btree.getPageReadsCount();
// Shows how tree height affects read performance
```

## 🏗️ Current Implementation Status

### ✅ Fully Implemented Features
- **Page Splitting**: Automatic leaf and branch page splitting
- **Multi-Level Trees**: Root splitting and tree height growth
- **Tree Traversal**: Proper navigation from root to leaf
- **Range Scanning**: Efficient scanning across multiple pages
- **Page Access Logging**: Comprehensive I/O tracking
- **Key Distribution**: Proper key separation and ordering
- **Page Linking**: Leaf pages linked for efficient scanning

### 🚧 Future Enhancements for Education
- **Page Merging**: Handling page underflow (deletion scenarios)
- **Page Cache Simulation**: Show impact of caching on I/O patterns
- **Concurrent Access**: Multi-threaded access patterns
- **Different Page Sizes**: Performance comparison with various page sizes
- **Index vs Full Scan**: Comparison of indexed vs sequential access
- **B+Tree vs B-Tree**: Comparison of different tree structures

## 🎯 Perfect for Teaching

This implementation is ideal for:
- **Database Internals Courses**: Shows real storage engine behavior
- **Data Structures Classes**: Demonstrates advanced tree operations
- **Performance Analysis**: Quantifies I/O costs of different operations
- **System Design**: Illustrates trade-offs in storage systems

### Key Educational Value
1. **Visual Learning**: Clear, emoji-enhanced output
2. **Quantitative Analysis**: Exact I/O measurements
3. **Real-World Relevance**: Actual database storage patterns
4. **Progressive Complexity**: From single-page to multi-level trees
5. **Performance Insights**: Understanding of storage system trade-offs

This implementation provides a complete foundation for understanding how modern database storage engines work at the page level! 