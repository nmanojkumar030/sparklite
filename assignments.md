# SparkLite Workshop Assignments

Welcome to the SparkLite educational project! This document contains hands-on assignments designed to teach you about database storage engines, query optimization, and distributed systems concepts.


## **Assignment 1: Build City Index (Working Around Insert-Only B+Tree)**

### **Overview**
Build a secondary index for city lookups while working around the limitation that SparkLite's B+Tree only supports insert operations, not updates.

### **Real-World Context**
In production databases, secondary indexes update incrementally as data is inserted. However, our educational B+Tree implementation only supports insert operations.

### **The Challenge**
- **Focus**: Secondary indexes and B+Tree limitations
- **Our Limitation**: SparkLite B+Tree only supports insert, no updates
- **Workaround**: Collect city mappings in-memory during insertion phase, then create B+Tree city index from complete in-memory collection

### **Implementation Strategy**
1. **Phase 1**: During customer data insertion, collect `(city, customer_id)` mappings in memory
2. **Phase 2**: After all insertions complete, build a dedicated B+Tree index for city lookups
3. **Phase 3**: Use the city index to efficiently find all customers in a specific city

### **Key Insights**
- Secondary indexes require careful coordination with primary data
- Insert-only systems often use batch rebuilding strategies
- The learning focus is on secondary index benefits, not the implementation workaround

---

## 📊 **Assignment 2: Analyze B+Tree Disk Access Patterns**

### **Overview**
Observe and analyze real disk I/O patterns in B+Tree operations to understand the relationship between tree structure and performance.

### **What You'll Do**
1. **Run Existing Tests**: Execute `BTreePageAccessDemo` and `BTreeMultiPageDemo`
2. **Observe I/O Logging**: See real page reads/writes in the output
3. **Compare Scenarios**: Single-page vs multi-page tree performance
4. **Measure Efficiency**: Count disk operations for different query patterns

### **Test Commands**
```bash
# Run the page access demonstration
./gradlew test --tests "*BTreePageAccessDemo*"

# Run the multi-page demonstration  
./gradlew test --tests "*BTreeMultiPageDemo*"
```

### **What to Look For**
- **Page Read Operations**: How many disk reads per search?
- **Page Write Operations**: When do pages get written to disk?
- **Tree Height Impact**: How does tree height affect I/O count?
- **Cache Behavior**: Which pages stay in memory vs get evicted?

### **Key Insights**
- **Tree height directly correlates with I/O operations per query**
- Deeper trees = more disk reads for each search
- Page size affects both tree height and I/O efficiency
- Buffer management is crucial for performance?

---

## 🗂️ **Assignment 3: Implement Parquet File Reader**

### **Overview**
Implement efficient Parquet reading using metadata-driven optimization techniques used by modern query engines like Spark and Presto.

### **Files to Implement**
- `src/main/java/minispark/storage/parquet/assignment/SimpleParquetReader.java`

### **Test File**
- `src/test/java/minispark/storage/parquet/ParquetAssignmentTest.java`

### **Implementation Parts**

#### **Part 1: Read Footer Metadata**
- Extract row group statistics from Parquet file footer
- Understand how column min/max values enable optimization
- **Method**: `readFooter(String filePath)`

#### **Part 2: Implement Predicate Pushdown**
- Use min/max statistics to skip entire row groups
- Skip row groups where `max_value < filter_value`
- **Method**: `selectRowGroups(ParquetMetadata metadata, String column, int minValue)`

#### **Part 3: Implement Column Pruning**
- Read only required columns, not all columns
- Reduce I/O by skipping unnecessary data
- **Method**: `readRowGroups(String filePath, List<Integer> rowGroupIndices, List<String> columns)`

#### **Part 4: Measure I/O Efficiency**
- Compare optimized vs naive full-file reading
- Demonstrate measurable performance improvements
- **Method**: `readWithFilter(String filePath, String filterColumn, int minValue, List<String> columns)`

### **Testing Your Implementation**

```bash
./gradlew test --tests "*ParquetAssignmentTest*"
```

### **Reference Implementation**
If you get stuck, you can reference `SimpleParquetReaderSolution.java` to see a complete implementation.