# ParquetStorage Implementation Plan

## 🎯 **Objective**
Implement a production-ready `ParquetStorage` class that implements `StorageInterface` with **clean code practices** and **low toxicity** for workshop demonstrations.

---

## 📊 **Code Quality Standards**

Based on [Erik Dörnenburg's Toxicity Metrics](https://erik.doernenburg.com/2008/11/how-toxic-is-your-code/):

| **Metric** | **Threshold** | **Our Target** |
|------------|---------------|----------------|
| File Length | 500 lines | < 300 lines |
| Method Length | 30 lines | < 20 lines |
| Parameter Number | 6 parameters | < 4 parameters |
| Cyclomatic Complexity | 10 | < 7 |
| Class Fan-Out | 30 | < 15 |
| Nested If Depth | 3 | < 2 |

**Workshop Quality Goals:**
- ✅ **Single Responsibility**: Each class has one clear purpose
- ✅ **Readable Methods**: Short, focused methods with descriptive names
- ✅ **Minimal Dependencies**: Low coupling between components
- ✅ **Clear Abstractions**: Easy to understand and explain

---

## 🏗️ **Implementation Steps**

### **Step 1: Create Directory Structure**
```
src/main/java/minispark/storage/parquet/
├── ParquetStorage.java              # Main implementation (< 200 lines)
├── ParquetBufferManager.java        # Memory management (< 150 lines)
├── ParquetFileManager.java          # File operations (< 150 lines)
├── ParquetSchemaConverter.java      # Schema conversion (< 100 lines)
└── ParquetRecordConverter.java      # Record conversion (< 100 lines)
```

### **Step 2: Core ParquetStorage Implementation**
- Implement `StorageInterface` methods
- Keep each method under 20 lines
- Use composition over inheritance
- Clear error handling and logging

### **Step 3: Supporting Components**
- `ParquetBufferManager`: Handle memory buffering
- `ParquetFileManager`: Manage file operations
- `ParquetSchemaConverter`: Convert schemas
- `ParquetRecordConverter`: Convert records

### **Step 4: Integration Tests**
- Create comprehensive test suite
- Test with existing `Table` class
- Verify compatibility with B+Tree tests

### **Step 5: Performance & Cleanup**
- Optimize for workshop demonstrations
- Add educational logging
- Final code review for toxicity

---

## 🧪 **Testing Strategy**

After each step, run:
```bash
./gradlew clean test
```

**Test Coverage:**
- Unit tests for each component
- Integration tests with `Table` class
- Performance comparison with B+Tree
- Error handling scenarios

---

## 📋 **Implementation Checklist**

### **Step 1: Setup** ✅
- [x] Create package structure
- [x] Run tests to ensure baseline

### **Step 2: Core Implementation** ✅
- [x] Create `ParquetStorage` skeleton
- [x] Implement `writeBatch()` method
- [x] Implement `scan()` method
- [x] Run tests

### **Step 3: Buffer Management** ✅
- [x] Create `ParquetBufferManager`
- [x] Implement `write()` with buffering
- [x] Add memory management
- [x] Run tests

### **Step 4: Schema & Record Conversion** ✅
- [x] Create `ParquetSchemaConverter` (basic version)
- [x] Create `ParquetRecordConverter`
- [x] Integrate with main storage
- [x] Run tests

### **Step 5: File Management** ✅
- [x] Create `ParquetFileManager`
- [x] Implement file versioning
- [x] Add cleanup methods
- [x] Run tests

### **Step 6: Point Lookups**
- [ ] Implement `read()` method
- [ ] Add basic indexing
- [ ] Optimize performance
- [ ] Run tests

### **Step 7: Updates & Deletes**
- [ ] Implement `delete()` method
- [ ] Add versioning support
- [ ] Handle edge cases
- [ ] Run tests

### **Step 8: Integration & Polish** ✅
- [x] Create integration tests
- [x] Add educational logging
- [x] Code review for toxicity
- [x] Final test run

---

## 🎓 **Educational Focus**

Each component will demonstrate:

1. **Clean Architecture**: Separation of concerns
2. **Interface Design**: How abstractions enable flexibility
3. **Memory Management**: Buffering strategies
4. **File Format Handling**: Working with columnar data
5. **Performance Trade-offs**: OLTP vs OLAP optimizations

---

## 🚀 **Success Criteria** ✅

- [x] All existing tests pass
- [x] New ParquetStorage tests pass
- [x] Code toxicity below thresholds
- [x] Table works with both B+Tree and Parquet
- [x] Clear, workshop-ready code
- [x] Comprehensive documentation

---

## 📝 **Notes**

- Keep methods focused and small
- Use descriptive variable names
- Add educational comments
- Minimize nested conditions
- Handle errors gracefully
- Test after every change 