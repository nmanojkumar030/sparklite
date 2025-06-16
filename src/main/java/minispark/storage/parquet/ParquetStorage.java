package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.StorageInterface;
import minispark.storage.table.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

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
 * 
 * Refactored to reduce toxicity:
 * - Extracted educational logging to ParquetEducationalLogger
 * - Delegated complex operations to ParquetOperations
 * - Reduced method lengths and parameter counts
 * - Decreased class coupling
 */
public class ParquetStorage implements StorageInterface {
    
    private final String basePath;
    private final TableSchema schema;
    private final ParquetFileManager fileManager;
    private final ParquetOperations operations;
    
    // Parquet's native row group size configuration (128MB industry standard)
    private static final long DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024; // 128MB
    
    /**
     * Creates a new Parquet storage engine.
     * 
     * @param basePath Base directory for Parquet files
     * @param schema Table schema for data validation
     */
    public ParquetStorage(String basePath, TableSchema schema) {
        this.basePath = basePath;
        this.schema = schema;
        this.fileManager = new ParquetFileManager(basePath);
        this.operations = new ParquetOperations(schema);
        
        ParquetLogHelper.logStorageCreation();
    }
    
    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        ParquetLogHelper.logSingleWrite(key);
        
        // For single writes, we create a batch of one record
        // This leverages Parquet's native buffering rather than our own
        Record record = new Record(key, value);
        writeBatch(Arrays.asList(record));
    }
    
    @Override
    public void writeBatch(List<Record> records) throws IOException {
        validateRecords(records);
        ParquetLogHelper.logBatchWrite(records.size());
        
        String filename = fileManager.getNextFileName();
        writeRecordsToParquet(records, filename);
    }
    
    @Override
    public Optional<Map<String, Object>> read(byte[] key) throws IOException {
        ParquetLogHelper.logPointLookup(key);
        
        // EDUCATIONAL: Point lookups in columnar format
        // This demonstrates the trade-off: excellent for scans, slower for point lookups
        List<String> parquetFiles = fileManager.getAllParquetFiles();
        
        for (String filename : parquetFiles) {
            Optional<Map<String, Object>> result = operations.searchInFile(filename, key);
            if (result.isPresent()) {
                return result;
            }
        }
        
        return Optional.empty();
    }
    
    @Override
    public List<Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        ParquetLogHelper.logRangeScan(startKey, endKey, columns);
        
        List<Record> results = new ArrayList<>();
        List<String> parquetFiles = fileManager.getAllParquetFiles();
        
        for (String filename : parquetFiles) {
            List<Record> fileResults = operations.scanFile(filename, startKey, endKey, columns);
            results.addAll(fileResults);
        }
        
        return sortResults(results);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        throw new UnsupportedOperationException("Delete operation is not supported in Parquet storage.");
    }
    
    @Override
    public void close() throws IOException {
        ParquetLogHelper.logStorageClose();
        
        fileManager.close();
    }
    
    // Private helper methods (keeping methods small and focused)
    
    private void writeRecordsToParquet(List<Record> records, String filename) throws IOException {
        ParquetLogHelper.logParquetWriteStart(records.size(), filename);
        
        try {
            performParquetWrite(records, filename);
            ParquetLogHelper.logParquetWriteComplete(records.size(), filename);
        } catch (IOException e) {
            ParquetLogHelper.logParquetWriteError(filename, e);
            throw new IOException("Failed to write Parquet file: " + filename, e);
        }
    }
    
    /**
     * Writes records using Parquet's native buffering and row group management.
     * 
     * Educational Note: ParquetWriter handles all buffering internally:
     * - Records are buffered in memory until row group size threshold is reached
     * - When threshold is hit, the entire row group is compressed and written to disk
     * - Memory management is handled by Parquet's MemoryManager
     * - Row group boundaries are determined by actual data size, not estimated size
     * 
     * This is more efficient than custom buffering because:
     * 1. Parquet knows the exact compressed size of data
     * 2. Compression happens at row group boundaries for optimal efficiency
     * 3. Memory pressure is managed by the Parquet MemoryManager
     * 4. No double-buffering overhead
     */
    private void performParquetWrite(List<Record> records, String filename) throws IOException {
        // Create Parquet schema from TableSchema
        MessageType parquetSchema = ParquetSchemaConverter.convertToParquetSchema(schema);
        
        // Setup Hadoop configuration and path
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(parquetSchema, conf);
        Path parquetPath = new Path(filename);
        
        // Create ParquetWriter with native buffering (row group size = 128MB)
        // The writer will automatically create row groups when size threshold is reached
        try (ParquetWriter<Group> writer = operations.createParquetWriter(
                parquetPath, conf, parquetSchema, DEFAULT_ROW_GROUP_SIZE)) {
            
            // Write records directly to ParquetWriter
            // Parquet handles all buffering, compression, and row group creation
            for (Record record : records) {
                Group group = operations.convertRecordToGroup(record, parquetSchema);
                writer.write(group); // Parquet buffers this internally
            }
            
            // When writer.close() is called (via try-with-resources),
            // any remaining buffered data is flushed as the final row group
        }
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
} 