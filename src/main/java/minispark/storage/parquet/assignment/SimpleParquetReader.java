package minispark.storage.parquet.assignment;

import minispark.storage.Record;
import minispark.storage.parquet.ParquetOperations;
import minispark.storage.table.TableSchema;

import java.io.IOException;
import java.util.List;

/**
 * SimpleParquetReader skeleton  to implement.
 * 
 * Demonstrates efficient Parquet reading patterns:
 * - Footer metadata usage for query optimization
 * - Predicate pushdown using row group statistics
 * - Selective I/O optimization techniques
 */
public class SimpleParquetReader {
    
    private final TableSchema schema;
    private final ParquetOperations operations;
    
    /**
     * Constructor that reuses existing infrastructure
     */
    public SimpleParquetReader(TableSchema schema) {
        this.schema = schema;
        this.operations = new ParquetOperations(schema);
    }
    
    /**
     * TODO : Read Parquet footer and extract metadata
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
        // TODO: implement this method
        // Steps:
        // 1. Open Parquet file using Hadoop Path and Configuration
        // 2. Create ParquetFileReader using HadoopInputFile
        // 3. Get footer metadata using fileReader.getFooter()
        // 4. Wrap in ParquetMetadata class
        // 5. Return the wrapped metadata
        
        throw new UnsupportedOperationException(
            "must implement readFooter() - " +
            "Use ParquetFileReader.open() and getFooter()"
        );
    }
    
    /**
     * TODO : Use statistics to determine which row groups to read
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
    public List<Integer> selectRowGroups(minispark.storage.parquet.assignment.ParquetMetadata metadata, String column, int minValue) {
        // TODO: implement this method
        // Steps:
        // 1. Iterate through all row groups in metadata
        // 2. For each row group, get statistics for the specified column
        // 3. Check if max_value >= minValue (if not, skip this row group)
        // 4. Return list of row group indices that might contain data
        
        throw new UnsupportedOperationException(
            "must implement selectRowGroups() - " +
            "Check if maxValue >= minValue for each row group"
        );
    }
    
    /**
     * TODO: Read only specified row groups and columns
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
        // TODO: implement this method
        // Steps:
        // 1. Open ParquetFileReader
        // 2. For each row group index in rowGroupIndices:
        //    - Read only that row group
        //    - Extract only the specified columns
        //    - Convert Parquet Groups to Record objects
        // 3. Return combined results
        
        throw new UnsupportedOperationException(
            "must implement readRowGroups() - " +
            "Use ParquetFileReader to read specific row groups"
        );
    }
    
    /**
     * TODO: Read selected row groups with predicate filtering
     * 
     * This method should combine row group selection with predicate filtering for optimal performance.
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
        // TODO: implement this method by combining the other methods
        // Steps:
        // 1. Read footer metadata using readFooter()
        // 2. Select relevant row groups using selectRowGroups()
        // 3. Read selected row groups and apply filtering
        // 4. Return filtered results
        
        throw new UnsupportedOperationException(
            "must implement readWithFilter() - " +
            "Combine readFooter(), selectRowGroups(), and readRowGroups() with filtering"
        );
    }
    
    /**
     * HELPER METHOD: Naive approach - read entire file
     * 
     * This is provided for performance comparison.
     * Shows the difference between optimized and unoptimized approaches.
     */
    public List<Record> readEntireFile(String filePath) throws IOException {
        // Use existing ParquetOperations to scan entire file
        return operations.scanFile(filePath, null, null, null);
    }
} 