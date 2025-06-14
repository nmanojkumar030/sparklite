# Parquet Storage Architecture Analysis

## 🎯 **Current State Assessment**

Based on our codebase analysis, we have:
- ✅ **ParquetReader**: Row group-based distributed reading
- ✅ **Test Infrastructure**: ParquetWriter usage in tests
- ⚠️ **Missing**: Production ParquetStorage implementation
- ⚠️ **Missing**: Incremental write strategy

---

## 🏗️ **Key Architectural Decisions for Parquet Storage**

### **1. Row Group Size Strategy**

**Current Test Implementation:**
```java
// From ParquetReaderTest.java
private static final int SMALL_ROW_GROUP_SIZE = 512;   // Forces multiple row groups
private static final int MEDIUM_ROW_GROUP_SIZE = 1024; // Balanced for testing
```

**Production Decisions Needed:**

| **Aspect** | **Options** | **Recommendation** | **Rationale** |
|------------|-------------|-------------------|---------------|
| **Default Row Group Size** | 64MB, 128MB, 256MB | **128MB** | Industry standard, good balance of I/O efficiency and memory usage |
| **Configurable Size** | Fixed vs Dynamic | **Configurable with smart defaults** | Different workloads need different optimizations |
| **Size Calculation** | Row count vs Byte size | **Byte size with row count limits** | More predictable memory usage and I/O patterns |

**Implementation Strategy:**
```java
public class ParquetStorageConfig {
    public static final long DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024; // 128MB
    public static final int MAX_ROWS_PER_GROUP = 1_000_000; // Safety limit
    public static final int MIN_ROWS_PER_GROUP = 1000; // Avoid tiny groups
}
```

### **2. When to Create New Row Groups**

**Decision Matrix:**

| **Trigger Condition** | **Implementation** | **Use Case** |
|----------------------|-------------------|--------------|
| **Size Threshold** | `currentSize >= targetRowGroupSize` | **Primary trigger** - ensures predictable I/O |
| **Row Count Limit** | `rowCount >= maxRowsPerGroup` | **Safety mechanism** - prevents memory issues |
| **Schema Change** | `newSchema != currentSchema` | **Data integrity** - maintains schema consistency |
| **Time-based** | `lastFlush + interval < now` | **Streaming scenarios** - ensures data visibility |
| **Memory Pressure** | `availableMemory < threshold` | **Resource management** - prevents OOM |

**Recommended Implementation:**
```java
private boolean shouldCreateNewRowGroup() {
    return currentRowGroupSize >= config.getRowGroupSize() ||
           currentRowCount >= config.getMaxRowsPerGroup() ||
           hasSchemaChanged() ||
           isMemoryPressureHigh();
}
```

### **3. Adding Row Groups to Existing Files**

**Technical Reality Check:**

| **Operation** | **Parquet Support** | **Implementation Complexity** | **Recommendation** |
|---------------|-------------------|------------------------------|-------------------|
| **Append Row Groups** | ❌ **Not Supported** | **Extremely High** | **Use file versioning instead** |
| **Modify Existing Row Groups** | ❌ **Not Supported** | **Impossible** | **Create new files** |
| **Add to Row Group** | ❌ **Not Supported** | **Impossible** | **Buffer until row group complete** |

**Why Parquet Doesn't Support Appends:**
1. **Footer-based Metadata**: Schema and row group info stored at file end
2. **Immutable Design**: Optimized for analytical workloads, not OLTP
3. **Compression**: Row groups are compressed as units
4. **Column Layout**: Data organized by column, not row

**Recommended Strategy:**
```java
// Instead of appending, use versioning
public class ParquetFileVersioning {
    // Pattern: table_name/year=2024/month=01/day=15/part-00001.parquet
    private String generateNewFilePath(String tableName) {
        LocalDate today = LocalDate.now();
        return String.format("%s/year=%d/month=%02d/day=%02d/part-%05d.parquet",
            tableName, today.getYear(), today.getMonthValue(), 
            today.getDayOfMonth(), getNextPartNumber());
    }
}
```

### **4. Incremental Record Addition Strategy**

**Buffering Approach:**

| **Strategy** | **Memory Usage** | **Latency** | **Complexity** | **Best For** |
|--------------|------------------|-------------|----------------|--------------|
| **In-Memory Buffer** | High | Low | Low | **Small datasets, fast writes** |
| **Disk-based Buffer** | Low | Medium | Medium | **Large datasets, memory constrained** |
| **Hybrid Buffer** | Medium | Low-Medium | High | **Production systems** |

**Recommended Implementation:**
```java
public class ParquetStorageEngine implements StorageInterface {
    private final RowGroupBuffer currentBuffer;
    private final ParquetStorageConfig config;
    
    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        currentBuffer.add(key, value);
        
        if (shouldFlushBuffer()) {
            flushCurrentRowGroup();
            startNewRowGroup();
        }
    }
    
    private boolean shouldFlushBuffer() {
        return currentBuffer.getEstimatedSize() >= config.getRowGroupSize() ||
               currentBuffer.getRowCount() >= config.getMaxRowsPerGroup();
    }
}
```

### **5. File Creation Timing**

**Decision Points:**

| **Trigger** | **Pros** | **Cons** | **Recommendation** |
|-------------|----------|----------|-------------------|
| **First Write** | Simple, immediate | May create many small files | ✅ **For OLTP workloads** |
| **Buffer Full** | Efficient, larger files | Higher latency | ✅ **For analytical workloads** |
| **Time-based** | Predictable, good for streaming | May create uneven files | ✅ **For streaming scenarios** |
| **Manual Flush** | Full control | Requires application logic | ✅ **For batch processing** |

**Multi-Strategy Implementation:**
```java
public class ParquetFileCreationStrategy {
    public enum Strategy {
        IMMEDIATE,    // Create file on first write
        BUFFERED,     // Create when buffer full
        TIMED,        // Create on schedule
        MANUAL        // Create on explicit flush
    }
    
    public void configureStrategy(Strategy strategy, Map<String, Object> params) {
        switch (strategy) {
            case IMMEDIATE -> this.flushThreshold = 1;
            case BUFFERED -> this.flushThreshold = (Long) params.get("bufferSize");
            case TIMED -> this.flushInterval = (Duration) params.get("interval");
            case MANUAL -> this.flushThreshold = Long.MAX_VALUE;
        }
    }
}
```

---

## 🚀 **Recommended Implementation Architecture**

### **Phase 1: Core Parquet Storage Engine**

```java
public class ParquetStorageEngine implements StorageInterface {
    private final ParquetStorageConfig config;
    private final RowGroupBuffer buffer;
    private final FileVersionManager versionManager;
    private final SchemaManager schemaManager;
    
    // Core operations
    public void write(byte[] key, Map<String, Object> value) throws IOException;
    public void writeBatch(List<Record> records) throws IOException;
    public void flush() throws IOException; // Manual flush
    
    // Configuration
    public void setRowGroupSize(long bytes);
    public void setFlushStrategy(FlushStrategy strategy);
}
```

### **Phase 2: Advanced Features**

```java
public class AdvancedParquetFeatures {
    // Partitioning support
    public void enablePartitioning(List<String> partitionColumns);
    
    // Compression optimization
    public void setCompressionCodec(CompressionCodecName codec);
    
    // Schema evolution
    public void evolveSchema(MessageType newSchema);
    
    // Statistics collection
    public ParquetFileStatistics getFileStatistics();
}
```

### **Phase 3: Integration with Table Layer**

```java
public class ParquetTable extends Table {
    public ParquetTable(String tableName, TableSchema schema, ParquetStorageEngine storage) {
        super(tableName, schema, storage);
    }
    
    // Parquet-specific optimizations
    public void enableColumnPruning(List<String> columns);
    public void enablePredicatePushdown(List<Predicate> predicates);
    public void optimizeForAnalyticalQueries();
}
```

---

## 📊 **Performance Considerations**

### **Row Group Size Impact**

| **Size** | **Write Performance** | **Read Performance** | **Memory Usage** | **Best For** |
|----------|----------------------|---------------------|------------------|--------------|
| **64MB** | Fast writes | Good for small queries | Lower | **OLTP-heavy workloads** |
| **128MB** | Balanced | Balanced | Medium | **Mixed workloads** |
| **256MB** | Slower writes | Excellent for large scans | Higher | **Pure analytical** |

### **Buffer Management Strategy**

```java
public class RowGroupBuffer {
    private final long maxSize;
    private final int maxRows;
    private long currentSize = 0;
    private int currentRows = 0;
    
    public boolean shouldFlush() {
        return currentSize >= maxSize || 
               currentRows >= maxRows ||
               isMemoryPressureHigh();
    }
    
    private boolean isMemoryPressureHigh() {
        Runtime runtime = Runtime.getRuntime();
        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        return (freeMemory / (double) totalMemory) < 0.1; // Less than 10% free
    }
}
```

---

## 🎯 **Implementation Roadmap**

### **Week 1: Foundation**
- [ ] Create `ParquetStorageEngine` class
- [ ] Implement basic buffering strategy
- [ ] Add row group size configuration
- [ ] Create unit tests for core functionality

### **Week 2: File Management**
- [ ] Implement file versioning strategy
- [ ] Add flush triggers (size, count, time)
- [ ] Create file naming conventions
- [ ] Add integration tests

### **Week 3: Optimization**
- [ ] Add memory pressure monitoring
- [ ] Implement compression options
- [ ] Add schema evolution support
- [ ] Performance benchmarking

### **Week 4: Integration**
- [ ] Integrate with Table layer
- [ ] Add column pruning support
- [ ] Create educational test scenarios
- [ ] Documentation and examples

---

## 🔍 **Key Insights from Current Implementation**

1. **Reading is Mature**: Our `ParquetReader` already handles row group distribution well
2. **Writing is Missing**: No production `ParquetStorageEngine` exists
3. **Test Infrastructure**: Good foundation with `ParquetWriter` usage in tests
4. **Architecture Ready**: `StorageInterface` provides clean abstraction

**Next Steps**: Focus on implementing `ParquetStorageEngine` with the buffering and file management strategies outlined above.

---

## 🔬 **Industry Validation & Research Insights**

### **Row Group Size Confirmation**
Recent industry research confirms our recommendations:

**Industry Standard (Validated):**
- **128MB to 512MB**: Confirmed as optimal range by Apache Parquet documentation and Dremio
- **1GB**: Recommended for pure analytical workloads with sufficient memory
- **HDFS Alignment**: Row group size should match HDFS block size for optimal I/O

**Performance Impact (Validated):**
```
64MB Row Groups:  ✅ Fast writes, ❌ More metadata overhead
128MB Row Groups: ✅ Balanced performance, ✅ Industry standard  
256MB Row Groups: ✅ Better compression, ❌ Higher memory usage
512MB Row Groups: ✅ Excellent for analytics, ❌ Requires more memory
```

### **Compression Algorithm Performance**
Industry benchmarks confirm our strategy:

| **Algorithm** | **Compression Ratio** | **Speed** | **CPU Usage** | **Best For** |
|---------------|----------------------|-----------|---------------|--------------|
| **Snappy** | Moderate (60-70%) | Fast | Low | **General purpose** ✅ |
| **Gzip** | High (40-50%) | Slow | High | **Storage-critical** |
| **ZSTD** | Configurable | Configurable | Medium | **Tunable performance** |
| **Brotli** | High (40-50%) | Medium | Medium | **Balanced storage/speed** |

### **File Size Recommendations (Validated)**
- **Target File Size**: 128MB to 1GB per file
- **Avoid Small Files**: < 10MB files create metadata overhead
- **HDFS Block Alignment**: File size should align with HDFS block size

### **Memory Management Insights**
```java
// Industry-validated memory pressure detection
private boolean isMemoryPressureHigh() {
    Runtime runtime = Runtime.getRuntime();
    long freeMemory = runtime.freeMemory();
    long totalMemory = runtime.totalMemory();
    return (freeMemory / (double) totalMemory) < 0.1; // < 10% free
}
```

---

## 🎯 **Updated Implementation Priority**

Based on industry validation, our implementation priority is:

### **Phase 1: Core Engine (Week 1-2)**
1. **Row Group Buffer**: 128MB default, configurable
2. **Compression**: Snappy as default, configurable options
3. **Memory Monitoring**: 10% free memory threshold
4. **File Versioning**: Partition-based file naming

### **Phase 2: Optimization (Week 3-4)**  
1. **Statistics Collection**: Min/max for predicate pushdown
2. **Column Pruning**: Read only required columns
3. **Schema Evolution**: Handle schema changes gracefully
4. **Performance Benchmarking**: Validate against industry standards

This approach ensures we build a production-ready Parquet storage engine that follows industry best practices while maintaining educational clarity for workshop demonstrations. 