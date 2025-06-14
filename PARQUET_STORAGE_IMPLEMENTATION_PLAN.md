# ParquetStorage Implementation Plan

## 🎯 **Objective**
Create a production-ready `ParquetStorage` class that implements `StorageInterface`, enabling `Table` to work with Parquet files just like it works with B+Tree.

---

## 🏗️ **Architecture Overview**

```
Table (High-level API)
    ↓
StorageInterface (Contract)
    ↓
ParquetStorage (Implementation)
    ↓
Apache Parquet Libraries
```

---

## 📋 **Implementation Strategy**

### **Phase 1: Core ParquetStorage Class**

**File**: `src/main/java/minispark/storage/parquet/ParquetStorage.java`

```java
public class ParquetStorage implements StorageInterface {
    private final String basePath;
    private final TableSchema schema;
    private final ParquetBufferManager bufferManager;
    private final ParquetFileManager fileManager;
    
    // Core implementation
}
```

### **Phase 2: Supporting Components**

1. **ParquetBufferManager**: Handles in-memory buffering until row group size threshold
2. **ParquetFileManager**: Manages file versioning and partitioning
3. **ParquetSchemaConverter**: Converts TableSchema to Parquet schema
4. **ParquetRecordConverter**: Converts between Table records and Parquet Groups

---

## 🔧 **Technical Implementation Details**

### **1. StorageInterface Method Mapping**

| **Method** | **Parquet Implementation Strategy** | **Complexity** |
|------------|-----------------------------------|----------------|
| `write()` | Buffer in memory until row group threshold | Medium |
| `writeBatch()` | Direct write to Parquet file | Easy |
| `read()` | **Challenge**: Point lookups in columnar format | Hard |
| `scan()` | Natural fit for Parquet's columnar design | Easy |
| `delete()` | **Challenge**: Immutable files, need versioning | Hard |
| `close()` | Flush buffers, close file handles | Easy |

### **2. Key Design Decisions**

#### **A. Buffering Strategy**
```java
public class ParquetBufferManager {
    private final List<Record> buffer = new ArrayList<>();
    private final int rowGroupSizeThreshold = 128_MB;
    private final int rowCountThreshold = 1_000_000;
    
    public boolean shouldFlush() {
        return getCurrentSize() >= rowGroupSizeThreshold || 
               buffer.size() >= rowCountThreshold;
    }
}
```

#### **B. File Versioning for Updates/Deletes**
```java
public class ParquetFileManager {
    // Pattern: table_name/part-{version}-{timestamp}.parquet
    private final String baseDir;
    private int currentVersion = 0;
    
    public String getNextFileName() {
        return String.format("%s/part-%05d-%d.parquet", 
                           baseDir, ++currentVersion, System.currentTimeMillis());
    }
}
```

#### **C. Point Lookup Strategy**
```java
// Challenge: Parquet is optimized for scans, not point lookups
// Solution: Maintain lightweight index or use bloom filters
public Optional<Map<String, Object>> read(byte[] key) {
    // Option 1: Scan all files (slow but simple)
    // Option 2: Maintain separate key-to-file index
    // Option 3: Use Parquet bloom filters (if available)
}
```

---

## 📁 **File Structure**

```
src/main/java/minispark/storage/parquet/
├── ParquetStorage.java              # Main StorageInterface implementation
├── ParquetBufferManager.java        # Memory buffering logic
├── ParquetFileManager.java          # File versioning and management
├── ParquetSchemaConverter.java      # TableSchema ↔ Parquet schema
├── ParquetRecordConverter.java      # Record format conversion
└── ParquetIndexManager.java         # Optional: Point lookup optimization
```

---

## 🧪 **Testing Strategy**

### **Test Classes to Create**

1. **ParquetStorageTest.java**: Core functionality tests
2. **ParquetTableIntegrationTest.java**: Table + ParquetStorage integration
3. **ParquetPerformanceTest.java**: Performance characteristics
4. **ParquetCompatibilityTest.java**: Ensure files work with other tools

### **Test Scenarios**

```java
@Test
void testParquetTableOperations() {
    // Create ParquetStorage
    ParquetStorage storage = new ParquetStorage("/tmp/test_table");
    
    // Create Table with ParquetStorage (same as BTree!)
    Table table = new Table("customers", schema, storage);
    
    // Test all operations
    table.insertBatch(customers);
    List<TableRecord> results = table.scan("CUST001", "CUST999", null);
    Optional<TableRecord> customer = table.findByPrimaryKey("CUST001");
}
```

---

## ⚠️ **Implementation Challenges & Solutions**

### **Challenge 1: Point Lookups in Columnar Format**

**Problem**: Parquet is optimized for analytical scans, not transactional point lookups.

**Solutions**:
- **Simple**: Linear scan through all files (acceptable for small datasets)
- **Advanced**: Maintain separate key-to-file mapping index
- **Future**: Use Parquet bloom filters or min/max statistics

### **Challenge 2: Updates/Deletes in Immutable Format**

**Problem**: Parquet files cannot be modified after creation.

**Solutions**:
- **File Versioning**: Create new files for updates, mark old records as deleted
- **Compaction**: Periodic merge of files to remove deleted records
- **Delta Architecture**: Separate delta files for changes

### **Challenge 3: Memory Management**

**Problem**: Buffering large datasets before writing to Parquet.

**Solutions**:
- **Configurable Thresholds**: Row group size and count limits
- **Memory Monitoring**: Flush when memory pressure detected
- **Streaming**: Write multiple row groups to same file

---

## 🚀 **Implementation Phases**

### **Phase 1: Basic Implementation (Week 1)**
- [ ] Create `ParquetStorage` skeleton
- [ ] Implement `writeBatch()` and `scan()` methods
- [ ] Basic schema conversion
- [ ] Simple integration test

### **Phase 2: Buffering & Memory Management (Week 2)**
- [ ] Implement `ParquetBufferManager`
- [ ] Add `write()` method with buffering
- [ ] Memory pressure handling
- [ ] Row group size optimization

### **Phase 3: Point Lookups (Week 3)**
- [ ] Implement `read()` method
- [ ] Add basic indexing for point lookups
- [ ] Performance optimization
- [ ] Comprehensive testing

### **Phase 4: Updates/Deletes (Week 4)**
- [ ] Implement `delete()` method with versioning
- [ ] File management and compaction
- [ ] Advanced testing scenarios
- [ ] Performance benchmarking

---

## 📊 **Expected Performance Characteristics**

| **Operation** | **B+Tree** | **ParquetStorage** | **Use Case** |
|---------------|------------|-------------------|--------------|
| **Point Lookup** | O(log n) - Excellent | O(files) - Acceptable | OLTP vs OLAP |
| **Range Scan** | O(log n + k) - Good | O(k) - Excellent | Analytics |
| **Batch Insert** | O(n log n) - Good | O(n) - Excellent | ETL |
| **Updates** | O(log n) - Excellent | O(files) - Acceptable | OLTP vs OLAP |

---

## 🎓 **Educational Value**

This implementation will demonstrate:

1. **Storage Engine Patterns**: How different storage engines optimize for different workloads
2. **Columnar vs Row Storage**: Practical trade-offs in real implementations
3. **Immutable Data Structures**: How to handle updates in append-only systems
4. **Memory Management**: Buffering strategies for batch-oriented systems
5. **Schema Evolution**: How columnar formats handle schema changes

---

## 🔄 **Integration with Existing Code**

The beauty of this approach is **zero changes** to existing code:

```java
// B+Tree Table (existing)
BTree btree = new BTree("customers.db");
Table btreeTable = new Table("customers", schema, btree);

// Parquet Table (new - same interface!)
ParquetStorage parquet = new ParquetStorage("customers_parquet");
Table parquetTable = new Table("customers", schema, parquet);

// Same operations work on both!
btreeTable.insert(customer);
parquetTable.insert(customer);
```

This demonstrates the power of **interface-based design** and **storage engine abstraction**. 