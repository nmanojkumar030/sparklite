package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.table.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ParquetProperties;

import java.io.IOException;
import java.util.*;

/**
 * Helper class for Parquet file operations.
 * 
 * Extracted from ParquetStorage to reduce method length toxicity
 * and improve separation of concerns.
 */
public class ParquetOperations {
    
    private final TableSchema schema;
    
    public ParquetOperations(TableSchema schema) {
        this.schema = schema;
    }
    
    /**
     * Searches for a specific key in a Parquet file.
     * Uses row group filtering for optimization.
     */
    public Optional<Map<String, Object>> searchInFile(String filename, byte[] key) throws IOException {
        ParquetLogHelper.logParquetSearchStart(filename, key);
        
        try {
            Path parquetPath = new Path(filename);
            Configuration conf = new Configuration();
            
            try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, conf))) {
                return performKeySearch(fileReader, key);
            }
            
        } catch (IOException e) {
            ParquetLogHelper.logParquetSearchError(filename, key, e);
            throw new IOException("Failed to search Parquet file: " + filename, e);
        }
    }
    
    /**
     * Scans a Parquet file for records within a key range.
     * Uses row group filtering and column projection.
     */
    public List<Record> scanFile(String filename, byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        ParquetLogHelper.logParquetScanStart(filename, startKey, endKey, columns);
        
        List<Record> results = new ArrayList<>();
        
        try {
            Path parquetPath = new Path(filename);
            Configuration conf = new Configuration();
            
            try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, conf))) {
                results = performRangeScan(fileReader, startKey, endKey, columns);
            }
            
            ParquetLogHelper.logParquetScanComplete(filename, results.size());
            return results;
            
        } catch (IOException e) {
            ParquetLogHelper.logParquetScanError(filename, e);
            throw new IOException("Failed to scan Parquet file: " + filename, e);
        }
    }
    
    /**
     * Creates a ParquetWriter with optimized settings.
     */
    public ParquetWriter<Group> createParquetWriter(Path path, Configuration conf, MessageType schema, long bufferSize) throws IOException {
        return new ParquetWriter<Group>(
            path,
            new GroupWriteSupport(),
            CompressionCodecName.SNAPPY,
            (int) bufferSize,  // Row group size
            1024,  // Page size
            512,   // Dictionary page size
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf
        );
    }
    
    /**
     * Converts a Record to a Parquet Group.
     */
    public Group convertRecordToGroup(Record record, MessageType schema) {
        SimpleGroup group = new SimpleGroup(schema);
        Map<String, Object> values = record.getValue();
        
        for (String fieldName : values.keySet()) {
            Object value = values.get(fieldName);
            addValueToGroup(group, fieldName, value);
        }
        
        return group;
    }
    
    /**
     * Converts a Parquet Group to a Map.
     */
    public Map<String, Object> convertGroupToMap(Group group, MessageType schema) {
        Map<String, Object> result = new HashMap<>();
        
        for (org.apache.parquet.schema.Type field : schema.getFields()) {
            String fieldName = field.getName();
            Object value = extractValueFromGroup(group, fieldName, schema);
            if (value != null) {
                result.put(fieldName, value);
            }
        }
        
        return result;
    }
    
    /**
     * Projects columns from record data.
     * Always includes the primary key column.
     */
    public Map<String, Object> projectColumns(Map<String, Object> recordData, List<String> columns) {
        Map<String, Object> projected = new HashMap<>();
        
        // Always include the primary key column
        String primaryKeyColumn = schema.getPrimaryKeyColumn();
        if (recordData.containsKey(primaryKeyColumn)) {
            projected.put(primaryKeyColumn, recordData.get(primaryKeyColumn));
        }
        
        // Include requested columns
        for (String column : columns) {
            if (recordData.containsKey(column)) {
                projected.put(column, recordData.get(column));
            }
        }
        
        return projected;
    }
    
    // Private helper methods
    
    private Optional<Map<String, Object>> performKeySearch(ParquetFileReader fileReader, byte[] key) throws IOException {
        MessageType parquetSchema = fileReader.getFooter().getFileMetaData().getSchema();
        String primaryKeyField = schema.getPrimaryKeyColumn();
        
        // Get all row groups and filter using metadata statistics
        List<org.apache.parquet.hadoop.metadata.BlockMetaData> rowGroups = fileReader.getFooter().getBlocks();
        String keyString = new String(key);
        
        // OPTIMIZATION: Filter row groups using min/max statistics
        List<Integer> candidateRowGroups = ParquetRowGroupFilter.filterRowGroupsForPointLookup(
            rowGroups, primaryKeyField, keyString);
        
        // Only search in candidate row groups
        for (Integer rowGroupIndex : candidateRowGroups) {
            Optional<Map<String, Object>> result = searchInRowGroup(fileReader, parquetSchema, rowGroupIndex, primaryKeyField, key);
            if (result.isPresent()) {
                return result;
            }
        }
        
        return Optional.empty();
    }
    
    private List<Record> performRangeScan(ParquetFileReader fileReader, byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
        List<Record> results = new ArrayList<>();
        MessageType parquetSchema = fileReader.getFooter().getFileMetaData().getSchema();
        String primaryKeyField = schema.getPrimaryKeyColumn();
        
        // Get all row groups and filter using metadata statistics
        List<org.apache.parquet.hadoop.metadata.BlockMetaData> rowGroups = fileReader.getFooter().getBlocks();
        String startKeyString = startKey != null ? new String(startKey) : null;
        String endKeyString = endKey != null ? new String(endKey) : null;
        
        // OPTIMIZATION: Filter row groups using min/max statistics for range scan
        List<Integer> candidateRowGroups = ParquetRowGroupFilter.filterRowGroupsForRangeScan(
            rowGroups, primaryKeyField, startKeyString, endKeyString);
        
        // Only scan candidate row groups
        for (Integer rowGroupIndex : candidateRowGroups) {
            ParquetScanConfig config = ParquetScanConfig.builder()
                .fileReader(fileReader)
                .schema(parquetSchema)
                .rowGroupIndex(rowGroupIndex)
                .primaryKeyField(primaryKeyField)
                .keyRange(startKey, endKey)
                .columns(columns)
                .build();
            List<Record> rowGroupResults = scanRowGroup(config);
            results.addAll(rowGroupResults);
        }
        
        return results;
    }
    
    private Optional<Map<String, Object>> searchInRowGroup(ParquetFileReader fileReader, MessageType schema, 
                                                         int rowGroupIndex, String primaryKeyField, byte[] key) throws IOException {
        String keyString = new String(key);
        
        // Read the row group
        org.apache.parquet.column.page.PageReadStore pageStore = fileReader.readRowGroup(rowGroupIndex);
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
        RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(schema));
        
        // Read all records in this row group
        org.apache.parquet.hadoop.metadata.BlockMetaData blockMetaData = fileReader.getFooter().getBlocks().get(rowGroupIndex);
        long rowCount = blockMetaData.getRowCount();
        
        for (long i = 0; i < rowCount; i++) {
            Group group = recordReader.read();
            if (group != null) {
                // Check if this record matches the key
                String recordKey = group.getString(primaryKeyField, 0);
                if (keyString.equals(recordKey)) {
                    // Found the record, convert to Map
                    return Optional.of(convertGroupToMap(group, schema));
                }
            }
        }
        
        return Optional.empty();
    }
    
    private List<Record> scanRowGroup(ParquetScanConfig config) throws IOException {
        List<Record> results = new ArrayList<>();
        
        // Setup row group reader
        RecordReader<Group> recordReader = createRowGroupReader(config);
        long rowCount = getRowCount(config);
        
        // Process all records in this row group
        for (long i = 0; i < rowCount; i++) {
            Group group = recordReader.read();
            if (group != null) {
                processRowGroupRecord(group, config, results);
            }
        }
        
        return results;
    }
    
    private RecordReader<Group> createRowGroupReader(ParquetScanConfig config) throws IOException {
        org.apache.parquet.column.page.PageReadStore pageStore = 
            config.getFileReader().readRowGroup(config.getRowGroupIndex());
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(config.getSchema());
        return columnIO.getRecordReader(pageStore, new GroupRecordConverter(config.getSchema()));
    }
    
    private long getRowCount(ParquetScanConfig config) {
        org.apache.parquet.hadoop.metadata.BlockMetaData blockMetaData = 
            config.getFileReader().getFooter().getBlocks().get(config.getRowGroupIndex());
        return blockMetaData.getRowCount();
    }
    
    private void processRowGroupRecord(Group group, ParquetScanConfig config, List<Record> results) {
        String recordKey = group.getString(config.getPrimaryKeyField(), 0);
        
        if (isKeyInRange(recordKey, config.getStartKeyString(), config.getEndKeyString())) {
            Map<String, Object> recordData = convertGroupToMap(group, config.getSchema());
            
            // Apply column projection if specified
            if (config.getColumns() != null && !config.getColumns().isEmpty()) {
                recordData = projectColumns(recordData, config.getColumns());
            }
            
            Record record = new Record(recordKey.getBytes(), recordData);
            results.add(record);
        }
    }
    
    private boolean isKeyInRange(String key, String startKey, String endKey) {
        if (startKey != null && key.compareTo(startKey) < 0) {
            return false;
        }
        if (endKey != null && key.compareTo(endKey) > 0) {
            return false;
        }
        return true;
    }
    
    private void addValueToGroup(SimpleGroup group, String fieldName, Object value) {
        if (value instanceof String) {
            group.add(fieldName, (String) value);
        } else if (value instanceof Integer) {
            group.add(fieldName, (Integer) value);
        } else if (value instanceof Long) {
            group.add(fieldName, (Long) value);
        } else if (value instanceof Double) {
            group.add(fieldName, (Double) value);
        } else if (value instanceof Boolean) {
            group.add(fieldName, (Boolean) value);
        } else {
            // Convert to string as fallback
            group.add(fieldName, value.toString());
        }
    }
    
    private Object extractValueFromGroup(Group group, String fieldName, MessageType schema) {
        try {
            org.apache.parquet.schema.Type field = schema.getType(fieldName);
            if (field.isPrimitive()) {
                org.apache.parquet.schema.PrimitiveType primitiveType = field.asPrimitiveType();
                switch (primitiveType.getPrimitiveTypeName()) {
                    case BINARY:
                        return group.getString(fieldName, 0);
                    case INT32:
                        return group.getInteger(fieldName, 0);
                    case INT64:
                        return group.getLong(fieldName, 0);
                    case DOUBLE:
                        return group.getDouble(fieldName, 0);
                    case BOOLEAN:
                        return group.getBoolean(fieldName, 0);
                    default:
                        return group.getString(fieldName, 0);
                }
            }
        } catch (Exception e) {
            // Skip fields that can't be read
        }
        return null;
    }
} 