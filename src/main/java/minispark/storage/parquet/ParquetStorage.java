package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.StorageInterface;
import minispark.storage.table.TableSchema;

import java.io.IOException;
import java.util.*;

/**
 * EDUCATIONAL: Parquet-based storage engine implementation.
 * 
 * Demonstrates columnar storage optimized for analytical workloads:
 * - Batch-oriented writes with row group buffering
 * - Efficient column-oriented scans
 * - Immutable file format with versioning for updates
 * 
 * Design Principles:
 * - Single Responsibility: Focus on Parquet storage operations
 * - Composition: Delegate to specialized managers
 * - Clean Interface: Implement StorageInterface contract
 */
public class ParquetStorage implements StorageInterface {
    
    private final String basePath;
    private final TableSchema schema;
    private final ParquetBufferManager bufferManager;
    private final ParquetFileManager fileManager;
    private final ParquetRecordConverter recordConverter;
    
    /**
     * Creates a new Parquet storage engine.
     * 
     * @param basePath Base directory for Parquet files
     * @param schema Table schema for data validation
     */
    public ParquetStorage(String basePath, TableSchema schema) {
        this.basePath = basePath;
        this.schema = schema;
        this.bufferManager = new ParquetBufferManager();
        this.fileManager = new ParquetFileManager(basePath);
        this.recordConverter = new ParquetRecordConverter(schema);
        
        logStorageCreation();
    }
    
    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        logSingleWrite(key);
        
        Record record = new Record(key, value);
        bufferManager.addRecord(record);
        
        if (bufferManager.shouldFlush()) {
            flushBuffer();
        }
    }
    
    @Override
    public void writeBatch(List<Record> records) throws IOException {
        validateRecords(records);
        logBatchWrite(records.size());
        
        String filename = fileManager.getNextFileName();
        writeRecordsToParquet(records, filename);
    }
    
    @Override
    public Optional<Map<String, Object>> read(byte[] key) throws IOException {
        logPointLookup(key);
        
        // EDUCATIONAL: Point lookups in columnar format
        // This demonstrates the trade-off: excellent for scans, slower for point lookups
        List<String> parquetFiles = fileManager.getAllParquetFiles();
        
        for (String filename : parquetFiles) {
            Optional<Map<String, Object>> result = searchInFile(filename, key);
            if (result.isPresent()) {
                return result;
            }
        }
        
        return Optional.empty();
    }
    
    @Override
    public List<Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        logRangeScan(startKey, endKey, columns);
        
        List<Record> results = new ArrayList<>();
        List<String> parquetFiles = fileManager.getAllParquetFiles();
        
        for (String filename : parquetFiles) {
            List<Record> fileResults = scanFile(filename, startKey, endKey, columns);
            results.addAll(fileResults);
        }
        
        return sortResults(results);
    }
    
    @Override
    public void delete(byte[] key) throws IOException {
        logDelete(key);
        
        // EDUCATIONAL: Immutable files require versioning for deletes
        // This demonstrates how columnar formats handle updates
        fileManager.markRecordAsDeleted(key);
    }
    
    @Override
    public void close() throws IOException {
        logStorageClose();
        
        if (bufferManager.hasBufferedRecords()) {
            flushBuffer();
        }
        
        fileManager.close();
    }
    
    // Private helper methods (keeping methods small and focused)
    
    private void flushBuffer() throws IOException {
        List<Record> bufferedRecords = bufferManager.getBufferedRecords();
        String filename = fileManager.getNextFileName();
        writeRecordsToParquet(bufferedRecords, filename);
        bufferManager.clearBuffer();
    }
    
    private void writeRecordsToParquet(List<Record> records, String filename) throws IOException {
        // TODO: Implement actual Parquet writing
        // For now, this is a placeholder that will be implemented in next steps
        System.out.println("   📝 Writing " + records.size() + " records to: " + filename);
    }
    
    private Optional<Map<String, Object>> searchInFile(String filename, byte[] key) throws IOException {
        // TODO: Implement Parquet file search
        // This will use ParquetReader to scan for specific key
        return Optional.empty();
    }
    
    private List<Record> scanFile(String filename, byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        // TODO: Implement Parquet file scanning
        // This will use ParquetReader with column projection
        return new ArrayList<>();
    }
    
    private List<Record> sortResults(List<Record> results) {
        results.sort((r1, r2) -> Arrays.compare(r1.getKey(), r2.getKey()));
        return results;
    }
    
    private void validateRecords(List<Record> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records cannot be null or empty");
        }
    }
    
    // Educational logging methods (clear, descriptive output for workshops)
    
    private void logStorageCreation() {
        System.out.println("🏗️ EDUCATIONAL: Creating ParquetStorage");
        System.out.println("   📁 Base path: " + basePath);
        System.out.println("   📋 Schema primary key: " + schema.getPrimaryKeyColumn());
        System.out.println("   🎯 Optimized for: Analytical workloads (OLAP)");
    }
    
    private void logSingleWrite(byte[] key) {
        System.out.println("📝 EDUCATIONAL: ParquetStorage.write()");
        System.out.println("   🔑 Key: " + new String(key));
        System.out.println("   💾 Strategy: Buffer until row group threshold");
    }
    
    private void logBatchWrite(int recordCount) {
        System.out.println("📝 EDUCATIONAL: ParquetStorage.writeBatch()");
        System.out.println("   📊 Records: " + recordCount);
        System.out.println("   🚀 Strategy: Direct write to Parquet file");
    }
    
    private void logPointLookup(byte[] key) {
        System.out.println("🔍 EDUCATIONAL: ParquetStorage.read()");
        System.out.println("   🔑 Key: " + new String(key));
        System.out.println("   ⚠️ Trade-off: Slower than B+Tree for point lookups");
    }
    
    private void logRangeScan(byte[] startKey, byte[] endKey, List<String> columns) {
        System.out.println("🔍 EDUCATIONAL: ParquetStorage.scan()");
        System.out.println("   📊 Range: [" + new String(startKey) + ", " + 
                          (endKey != null ? new String(endKey) : "END") + "]");
        System.out.println("   🎯 Columns: " + (columns != null ? columns : "ALL"));
        System.out.println("   ✅ Strength: Excellent for analytical scans");
    }
    
    private void logDelete(byte[] key) {
        System.out.println("🗑️ EDUCATIONAL: ParquetStorage.delete()");
        System.out.println("   🔑 Key: " + new String(key));
        System.out.println("   📝 Strategy: Mark as deleted (immutable files)");
    }
    
    private void logStorageClose() {
        System.out.println("🔒 EDUCATIONAL: Closing ParquetStorage");
        System.out.println("   💾 Flushing any buffered records");
        System.out.println("   🧹 Cleaning up resources");
    }
} 