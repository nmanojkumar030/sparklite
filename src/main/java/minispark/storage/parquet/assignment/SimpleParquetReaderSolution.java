package minispark.storage.parquet.assignment;

import minispark.storage.Record;
import minispark.storage.parquet.ParquetOperations;
import minispark.storage.table.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.statistics.Statistics;

import java.io.IOException;
import java.util.*;

/**
 * SOLUTION: Complete implementation of SimpleParquetReader assignment.
 * 
 * Demonstrates production-grade Parquet reading patterns:
 * - Footer metadata analysis for query optimization
 * - Predicate pushdown using row group statistics
 * - Selective I/O with row group and column pruning
 * - Performance comparison between optimized and naive approaches
 * 
 * Educational Value:
 * - Shows how modern query engines (Spark, Presto) optimize Parquet reads
 * - Demonstrates the power of columnar storage metadata
 * - Illustrates real-world performance optimization techniques
 */
public class SimpleParquetReaderSolution {
    
    private final TableSchema schema;
    private final ParquetOperations operations;
    
    /**
     * Constructor that reuses existing infrastructure
     */
    public SimpleParquetReaderSolution(TableSchema schema) {
        this.schema = schema;
        this.operations = new ParquetOperations(schema);
    }
    
    /**
     * SOLUTION: Read Parquet footer and extract metadata
     * 
     * LEARNING: Footer contains ALL row group statistics that enable query optimization
     * 
     * Real-world context: Spark, Presto, and other engines read footers first
     * to plan which row groups to read, enabling massive performance gains.
     * 
     * @param filePath Path to the Parquet file
     * @return ParquetMetadata wrapper with educational helpers
     * @throws IOException if file reading fails
     */
    public minispark.storage.parquet.assignment.ParquetMetadata readFooter(String filePath) throws IOException {
        logFooterReadStart(filePath);
        
        // Step 1: Setup Hadoop configuration and path
        Configuration conf = new Configuration();
        Path parquetPath = new Path(filePath);
        
        // Step 2: Create HadoopInputFile for Parquet reading
        HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetPath, conf);
        
        // Step 3: Open ParquetFileReader and get footer metadata
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata parquetMetadata = fileReader.getFooter();
            
            // Step 4: Wrap in our educational metadata class
            minispark.storage.parquet.assignment.ParquetMetadata wrappedMetadata = 
                new minispark.storage.parquet.assignment.ParquetMetadata(parquetMetadata);
            
            logFooterReadComplete(wrappedMetadata);
            return wrappedMetadata;
        }
    }
    
    /**
     * SOLUTION: Use statistics to determine which row groups to read
     * 
     * LEARNING: Predicate pushdown optimization - skip entire row groups
     * 
     * Real-world context: This is how Spark achieves 10x-100x speedups on large datasets.
     * Instead of reading terabytes, you might only read gigabytes.
     * 
     * @param metadata Parquet metadata containing row group statistics
     * @param column Column name to filter on (e.g., "age")
     * @param minValue Minimum value filter (e.g., age > 35)
     * @return List of row group indices that might contain matching data
     */
    public List<Integer> selectRowGroups(minispark.storage.parquet.assignment.ParquetMetadata metadata, 
                                       String column, int minValue) {
        logRowGroupSelectionStart(column, minValue);
        
        List<Integer> selectedRowGroups = new ArrayList<>();
                 List<BlockMetaData> rowGroups = metadata.getMetadata().getBlocks();
        
        // Step 1: Iterate through all row groups
        for (int i = 0; i < rowGroups.size(); i++) {
            BlockMetaData rowGroup = rowGroups.get(i);
            
            // Step 2: Find the column chunk for our target column
            ColumnChunkMetaData columnChunk = findColumnChunk(rowGroup, column);
            
            if (columnChunk != null) {
                // Step 3: Get statistics for this column in this row group
                Statistics<?> stats = columnChunk.getStatistics();
                
                if (stats != null && !stats.isEmpty()) {
                    // Step 4: Check if this row group might contain matching data
                    // If max_value >= minValue, this row group might have matching records
                    if (couldContainMatchingRecords(stats, minValue)) {
                        selectedRowGroups.add(i);
                        logRowGroupSelected(i, stats);
                    } else {
                        logRowGroupSkipped(i, stats);
                    }
                } else {
                    // No statistics available - must include this row group to be safe
                    selectedRowGroups.add(i);
                    logRowGroupIncludedNoStats(i);
                }
            } else {
                // Column not found - include row group to be safe
                selectedRowGroups.add(i);
                logRowGroupIncludedColumnNotFound(i, column);
            }
        }
        
        logRowGroupSelectionComplete(selectedRowGroups.size(), rowGroups.size());
        return selectedRowGroups;
    }
    
    /**
     * SOLUTION: Read only specified row groups and columns
     * 
     * LEARNING: Selective I/O based on metadata analysis
     * 
     * Real-world context: Combined with predicate pushdown, this enables
     * reading only the data you need, both row-wise and column-wise.
     * 
     * @param filePath Path to the Parquet file
     * @param rowGroupIndices Which row groups to read (from selectRowGroups)
     * @param columns Which columns to read (column pruning)
     * @return List of matching records
     * @throws IOException if file reading fails
     */
    public List<Record> readRowGroups(String filePath, List<Integer> rowGroupIndices, 
                                    List<String> columns) throws IOException {
        return readRowGroups(filePath, rowGroupIndices, columns, null, -1);
    }
    
    /**
     * SOLUTION: Read only specified row groups and columns with optional predicate filtering
     * 
     * @param filePath Path to the Parquet file
     * @param rowGroupIndices Which row groups to read (from selectRowGroups)
     * @param columns Which columns to read (column pruning)
     * @param filterColumn Column to apply predicate filter on (can be null)
     * @param minValue Minimum value for predicate filter (ignored if filterColumn is null)
     * @return List of matching records
     * @throws IOException if file reading fails
     */
    public List<Record> readRowGroups(String filePath, List<Integer> rowGroupIndices, 
                                    List<String> columns, String filterColumn, int minValue) throws IOException {
        logSelectiveReadStart(filePath, rowGroupIndices, columns);
        
        List<Record> results = new ArrayList<>();
        Configuration conf = new Configuration();
        Path parquetPath = new Path(filePath);
        HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetPath, conf);
        
        // Step 1: Open ParquetFileReader
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
            
            // Step 2: For each selected row group, read only that row group
            for (Integer rowGroupIndex : rowGroupIndices) {
                logReadingRowGroup(rowGroupIndex);
                
                // Read specific row group
                org.apache.parquet.hadoop.metadata.BlockMetaData rowGroupMetadata = 
                    fileReader.getFooter().getBlocks().get(rowGroupIndex);
                
                // Create record reader for this row group
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(
                    fileReader.readRowGroup(rowGroupIndex), 
                    new GroupRecordConverter(schema)
                );
                
                // Step 3: Read all records from this row group
                long recordCount = rowGroupMetadata.getRowCount();
                for (long i = 0; i < recordCount; i++) {
                                         Group group = recordReader.read();
                     if (group != null) {
                         // Convert Group to Record, applying column pruning
                         Record record = convertGroupToRecord(group, columns);
                         if (record != null) {
                             // Apply predicate filter if specified
                             if (filterColumn == null || passesFilter(record, filterColumn, minValue)) {
                                 results.add(record);
                             }
                         }
                     }
                }
            }
        }
        
        logSelectiveReadComplete(results.size());
        return results;
    }
    
    /**
     * SOLUTION: Read selected row groups with predicate filtering
     * 
     * This method combines row group selection with predicate filtering for optimal performance.
     * 
     * @param filePath Path to the Parquet file
     * @param filterColumn Column to filter on (e.g., "age")
     * @param minValue Minimum value for the filter (e.g., age > 35)
     * @param columns Which columns to read (column pruning)
     * @return List of matching records
     * @throws IOException if file reading fails
     */
    public List<Record> readWithFilter(String filePath, String filterColumn, int minValue, 
                                     List<String> columns) throws IOException {
        // Step 1: Read footer metadata
        minispark.storage.parquet.assignment.ParquetMetadata metadata = readFooter(filePath);
        
        // Step 2: Select relevant row groups using predicate pushdown
        List<Integer> relevantRowGroups = selectRowGroups(metadata, filterColumn, minValue);
        
        // Step 3: Read selected row groups with filtering
        return readRowGroups(filePath, relevantRowGroups, columns, filterColumn, minValue);
    }
    
    /**
     * HELPER METHOD: Naive approach - read entire file
     * 
     * This is provided for performance comparison.
     * Shows the difference between optimized and unoptimized approaches.
     */
    public List<Record> readEntireFile(String filePath) throws IOException {
        logNaiveReadStart(filePath);
        
        // Use existing ParquetOperations to scan entire file
        List<Record> results = operations.scanFile(filePath, null, null, null);
        
        logNaiveReadComplete(results.size());
        return results;
    }
    
    // Private helper methods
    
    /**
     * Finds the column chunk metadata for a specific column in a row group
     */
    private ColumnChunkMetaData findColumnChunk(BlockMetaData rowGroup, String columnName) {
        for (ColumnChunkMetaData columnChunk : rowGroup.getColumns()) {
            // Get the column path - for simple schemas, it's just the column name
            String[] pathParts = columnChunk.getPath().toArray();
            if (pathParts.length > 0 && pathParts[0].equals(columnName)) {
                return columnChunk;
            }
        }
        return null;
    }
    
    /**
     * Checks if a row group's statistics indicate it could contain matching records
     */
    private boolean couldContainMatchingRecords(Statistics<?> stats, int minValue) {
        if (!stats.hasNonNullValue()) {
            return false; // All nulls, can't match
        }
        
        // For integer statistics, check if max >= minValue
        if (stats.genericGetMax() instanceof Integer) {
            Integer maxValue = (Integer) stats.genericGetMax();
            return maxValue >= minValue;
        }
        
        // For other types, be conservative and include the row group
        return true;
    }
    
    /**
     * Converts a Parquet Group to a Record, applying column pruning
     */
    private Record convertGroupToRecord(Group group, List<String> columns) {
        try {
            // Extract key (assuming first field is the key)
            String keyString = group.getString(0, 0);
            byte[] key = keyString.getBytes();
            
            // Extract values for specified columns (or all if columns is null)
            Map<String, Object> values = new HashMap<>();
            
            for (int i = 0; i < group.getType().getFieldCount(); i++) {
                String fieldName = group.getType().getFieldName(i);
                
                // Apply column pruning - only include requested columns
                if (columns == null || columns.contains(fieldName)) {
                    if (group.getFieldRepetitionCount(i) > 0) {
                        Object value = extractFieldValue(group, i);
                        if (value != null) {
                            values.put(fieldName, value);
                        }
                    }
                }
            }
            
            return new Record(key, values);
        } catch (Exception e) {
            // Log error and skip this record
            System.err.println("Error converting group to record: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Checks if a record passes the predicate filter
     */
    private boolean passesFilter(Record record, String filterColumn, int minValue) {
        Object value = record.getValue().get(filterColumn);
        if (value instanceof Integer) {
            return (Integer) value > minValue;
        }
        return false; // Conservative approach - exclude if not an integer
    }
    
    /**
     * Extracts a field value from a Group based on its type
     */
    private Object extractFieldValue(Group group, int fieldIndex) {
        try {
            org.apache.parquet.schema.Type fieldType = group.getType().getType(fieldIndex);
            
            if (fieldType.isPrimitive()) {
                org.apache.parquet.schema.PrimitiveType primitiveType = fieldType.asPrimitiveType();
                
                switch (primitiveType.getPrimitiveTypeName()) {
                    case INT32:
                        return group.getInteger(fieldIndex, 0);
                    case INT64:
                        return group.getLong(fieldIndex, 0);
                    case DOUBLE:
                        return group.getDouble(fieldIndex, 0);
                    case FLOAT:
                        return group.getFloat(fieldIndex, 0);
                    case BOOLEAN:
                        return group.getBoolean(fieldIndex, 0);
                    case BINARY:
                        return group.getString(fieldIndex, 0);
                    default:
                        return group.getValueToString(fieldIndex, 0);
                }
            }
            
            return group.getValueToString(fieldIndex, 0);
        } catch (Exception e) {
            return null;
        }
    }
    
    // Educational logging methods
    
    private void logFooterReadStart(String filePath) {
        System.out.println("SOLUTION: Reading Parquet footer metadata");
        System.out.println("   File: " + filePath);
        System.out.println("   Goal: Extract row group statistics for query optimization");
    }
    
        private void logFooterReadComplete(minispark.storage.parquet.assignment.ParquetMetadata metadata) {
        System.out.println("   Footer read successfully");
        System.out.println("   Row groups found: " + metadata.getTotalRowGroups());
        System.out.println("   Schema fields: " + metadata.getColumnCount());
        System.out.println();
    }
    
    private void logRowGroupSelectionStart(String column, int minValue) {
        System.out.println("SOLUTION: Selecting row groups using predicate pushdown");
        System.out.println("   Filter: " + column + " >= " + minValue);
        System.out.println("   Strategy: Skip row groups where max(" + column + ") < " + minValue);
    }
    
    private void logRowGroupSelected(int index, Statistics<?> stats) {
        System.out.println("   Row group " + index + " selected (max=" + stats.genericGetMax() + ")");
    }
    
    private void logRowGroupSkipped(int index, Statistics<?> stats) {
        System.out.println("   Row group " + index + " skipped (max=" + stats.genericGetMax() + ")");
    }
    
    private void logRowGroupIncludedNoStats(int index) {
        System.out.println("   Row group " + index + " included (no statistics available)");
    }
    
    private void logRowGroupIncludedColumnNotFound(int index, String column) {
        System.out.println("   Row group " + index + " included (column '" + column + "' not found)");
    }
    
    private void logRowGroupSelectionComplete(int selected, int total) {
        System.out.println("   Selection complete: " + selected + "/" + total + " row groups selected");
        double skipPercentage = ((double)(total - selected) / total) * 100;
        System.out.println("   I/O reduction: " + String.format("%.1f", skipPercentage) + "% of data skipped");
        System.out.println();
    }
    
    private void logSelectiveReadStart(String filePath, List<Integer> rowGroupIndices, List<String> columns) {
        System.out.println("SOLUTION: Reading selected row groups with column pruning");
        System.out.println("   File: " + filePath);
        System.out.println("   Row groups to read: " + rowGroupIndices);
        System.out.println("   Columns to read: " + (columns != null ? columns : "ALL"));
    }
    
    private void logReadingRowGroup(int index) {
        System.out.println("   Reading row group " + index + "...");
    }
    
    private void logSelectiveReadComplete(int recordCount) {
        System.out.println("   Selective read complete: " + recordCount + " records read");
        System.out.println();
    }
    
    private void logNaiveReadStart(String filePath) {
        System.out.println("BASELINE: Reading entire file (naive approach)");
        System.out.println("   File: " + filePath);
        System.out.println("   No optimization - reading all data");
    }
    
    private void logNaiveReadComplete(int recordCount) {
        System.out.println("   Naive read complete: " + recordCount + " records read");
        System.out.println();
    }
} 