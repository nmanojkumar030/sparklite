# Parquet Assignment: Understanding Columnar Storage

## Overview

This assignment helps students learn **Parquet file structure** and **efficient reading patterns** through hands-on implementation. Students will understand how columnar storage enables massive performance optimizations in analytical systems like Apache Spark.

## 🎯 Learning Objectives

By completing this assignment, students will understand:

1. **Parquet Footer Structure**: How metadata is stored and accessed
2. **Predicate Pushdown**: Using statistics to skip entire row groups  
3. **Column Pruning**: Reading only required columns
4. **Performance Optimization**: Comparing naive vs optimized approaches
5. **Real-world Applications**: How systems like Spark leverage these patterns

## 📁 Assignment Structure

### Complete Implementations (Provided)

- **`TestCustomerParquetFileWriter.java`** - Creates sample customer data with educational row group structure
- **`ParquetMetadata.java`** - Wrapper for accessing Parquet metadata with helper methods
- **Test Classes** - Comprehensive tests demonstrating performance comparisons

### Student Implementation Required

- **`SimpleParquetReader.java`** - Contains skeleton methods for students to implement

## 🚀 Getting Started

### 1. Understand the Data Structure

The assignment creates a `customers.parquet` file with **3 row groups**:

- **Row Group 1**: Young customers (ages 20-35) - 100 records
- **Row Group 2**: Middle-aged customers (ages 40-65) - 100 records  
- **Row Group 3**: Mixed ages (ages 25-45) - 100 records

This age distribution enables meaningful **predicate pushdown** demonstrations.

### 2. Run the Assignment Demo

```bash
./gradlew test --tests ParquetAssignmentTest
```

Initially, this will show:
- ✅ Naive approach working (reads entire file)
- ⚠️ Student methods not implemented yet

### 3. Implement Student Methods

Students must implement these methods in `SimpleParquetReader.java`:

#### Method 1: `readFooter(String filePath)`

**Learning**: Parquet footer contains ALL metadata needed for optimization

```java
public ParquetMetadata readFooter(String filePath) throws IOException {
    // TODO: Students implement
    // Steps:
    // 1. Open Parquet file using Hadoop Path and Configuration
    // 2. Create ParquetFileReader using HadoopInputFile  
    // 3. Get footer metadata using fileReader.getFooter()
    // 4. Wrap in educational ParquetMetadata class
    // 5. Log educational information about what was found
}
```

**Hints**:
- Use `ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))`
- Get metadata with `fileReader.getFooter()`
- Wrap with `new ParquetMetadata(nativeMetadata)`

#### Method 2: `selectRowGroups(ParquetMetadata metadata, String column, int minValue)`

**Learning**: Predicate pushdown - skip row groups that can't contain matching data

```java
public List<Integer> selectRowGroups(ParquetMetadata metadata, String column, int minValue) {
    // TODO: Students implement  
    // Steps:
    // 1. Iterate through all row groups in metadata
    // 2. For each row group, get statistics for the specified column
    // 3. Check if max_value >= minValue (if not, skip this row group)
    // 4. Log which row groups are skipped and why
    // 5. Return list of row group indices that might contain data
}
```

**Hints**:
- Use `metadata.getAllRowGroupStats()` to iterate
- Check `stats.mightContainAge(minValue)` for each row group
- Log educational information about skipped vs. selected row groups

#### Method 3: `readRowGroups(String filePath, List<Integer> rowGroupIndices, List<String> columns)`

**Learning**: Selective I/O - read only selected row groups and columns

```java
public List<Record> readRowGroups(String filePath, List<Integer> rowGroupIndices, 
                                List<String> columns) throws IOException {
    // TODO: Students implement
    // Steps:
    // 1. Open ParquetFileReader
    // 2. For each row group index in rowGroupIndices:
    //    - Read only that row group  
    //    - Extract only the specified columns
    //    - Convert Parquet Groups to Record objects
    // 3. Log I/O statistics (bytes read, records processed, time taken)
    // 4. Return combined results
}
```

**Hints**:
- Use existing `ParquetOperations` class for low-level operations
- Process only the row groups in `rowGroupIndices`
- Project only the specified columns
- Measure and log performance metrics

## 📊 Expected Performance Results

Once implemented correctly, students should see results like:

```
🔍 QUERY: Find customers with age > 35

📈 APPROACH 1: NAIVE (Read Everything)
📦 Total records read: 300
✅ Matching records: 167  
⏱️  Time taken: 45 ms

🚀 APPROACH 2: OPTIMIZED (Metadata-Driven)
📊 Row groups to read: [1, 2]  
✅ Matching records: 167
⏱️  Time taken: 18 ms
🚀 I/O reduction: 33.3%
🏃 Performance improvement: 2.5x faster

=== ROW GROUP ANALYSIS ===
Row Group 0: age range [20-35], records=100 ⏭️  SKIPPED
Row Group 1: age range [40-65], records=100 ✅ READ  
Row Group 2: age range [25-45], records=100 ✅ READ
```

## 🎓 Educational Insights

### Query Optimization Scenarios

1. **Highly Selective Queries** (`age > 50`)
   - Can skip most row groups
   - Massive I/O reduction (66%+)
   - Demonstrates power of predicate pushdown

2. **Moderate Selectivity** (`25 ≤ age ≤ 40`)  
   - Needs multiple row groups
   - Some optimization possible
   - Shows selectivity impact

3. **Low Selectivity** (`age > 15`)
   - Must read all row groups  
   - No optimization possible
   - Demonstrates limits of approach

### Real-World Applications

- **Apache Spark**: Uses identical patterns for massive datasets
- **Presto/Trino**: Leverages statistics for query planning
- **Data Warehouses**: Enable sub-second queries on petabyte datasets
- **BigQuery/Snowflake**: Built on columnar storage principles

## 🔧 Technical Architecture

### Reuses Existing Infrastructure

This assignment **reuses existing codebase** components:

- **`minispark.storage.Record`** - Data representation
- **`ParquetStorage`** - File writing operations  
- **`ParquetOperations`** - Low-level Parquet operations
- **`ParquetEducationalLogger`** - Educational logging
- **`TableSchema`** - Schema definitions

### Benefits of Reuse

- **No Duplication**: Leverages existing, tested components
- **Real Implementation**: Uses actual Parquet libraries  
- **Educational Focus**: Students focus on learning concepts, not infrastructure
- **Scalable**: Can easily extend to more complex scenarios

## 🏃 Quick Start Commands

```bash
# Run the educational test
./gradlew test --tests ParquetEducationalAssignmentTest

# Run all Parquet tests  
./gradlew test --tests "*Parquet*"

# Create sample file manually
./gradlew compileJava
# Then run TestCustomerParquetFileWriter.createCustomersFile("sample.parquet")
```

## 📚 Additional Resources

- [Apache Parquet Documentation](https://parquet.apache.org/docs/)
- [Dremel Paper](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf) - Original columnar storage paper
- [Spark SQL Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html) - Real-world applications

## 🎯 Success Criteria

Students successfully complete the assignment when:

1. ✅ All three methods are implemented correctly
2. ✅ Tests pass with proper performance optimizations
3. ✅ Row group selection demonstrates predicate pushdown
4. ✅ Performance analysis shows I/O reduction
5. ✅ Students can explain the optimization principles

## 💡 Extension Ideas

Advanced students can explore:

- **Multiple Predicates**: Combining age and city filters
- **Complex Queries**: Range queries, IN clauses
- **Column Statistics**: Min/max for other data types
- **Bloom Filters**: Additional pruning techniques
- **Partition Pruning**: Directory-based filtering 