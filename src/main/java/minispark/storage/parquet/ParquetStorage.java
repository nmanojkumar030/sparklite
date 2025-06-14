package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.StorageInterface;
import minispark.storage.table.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;

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
    private final ParquetBufferManager bufferManager;
    private final ParquetFileManager fileManager;
    private final ParquetOperations operations;
    
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
        this.operations = new ParquetOperations(schema);
        
        ParquetEducationalLogger.logStorageCreation();
    }
    
    @Override
    public void write(byte[] key, Map<String, Object> value) throws IOException {
        ParquetEducationalLogger.logSingleWrite(key);
        
        Record record = new Record(key, value);
        bufferManager.addRecord(record);
        
        if (bufferManager.shouldFlush()) {
            flushBuffer();
        }
    }
    
    @Override
    public void writeBatch(List<Record> records) throws IOException {
        validateRecords(records);
        ParquetEducationalLogger.logBatchWrite(records.size());
        
        String filename = fileManager.getNextFileName();
        writeRecordsToParquet(records, filename);
    }
    
    @Override
    public Optional<Map<String, Object>> read(byte[] key) throws IOException {
        ParquetEducationalLogger.logPointLookup(key);
        
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
        ParquetEducationalLogger.logRangeScan(startKey, endKey, columns);
        
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
        ParquetEducationalLogger.logDelete(key);
        
        // EDUCATIONAL: Immutable files require versioning for deletes
        // This demonstrates how columnar formats handle updates
        fileManager.markRecordAsDeleted(key);
    }
    
    @Override
    public void close() throws IOException {
        ParquetEducationalLogger.logStorageClose();
        
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
        ParquetEducationalLogger.logParquetWriteStart(records.size(), filename);
        
        try {
            performParquetWrite(records, filename);
            ParquetEducationalLogger.logParquetWriteComplete(records.size(), filename);
        } catch (IOException e) {
            ParquetEducationalLogger.logParquetWriteError(filename, e);
            throw new IOException("Failed to write Parquet file: " + filename, e);
        }
    }
    
    private void performParquetWrite(List<Record> records, String filename) throws IOException {
        // Create Parquet schema from TableSchema
        MessageType parquetSchema = ParquetSchemaConverter.convertToParquetSchema(schema);
        
        // Setup Hadoop configuration and path
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(parquetSchema, conf);
        Path parquetPath = new Path(filename);
        
        // Create ParquetWriter with industry-standard settings
        try (ParquetWriter<Group> writer = operations.createParquetWriter(
                parquetPath, conf, parquetSchema, bufferManager.getStats().getMaxSizeBytes())) {
            
            // Convert and write each record
            for (Record record : records) {
                Group group = operations.convertRecordToGroup(record, parquetSchema);
                writer.write(group);
            }
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