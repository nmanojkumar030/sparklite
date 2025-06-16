package minispark.storage.parquet.assignment;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.column.statistics.Statistics;

import java.util.List;
import java.util.ArrayList;

/**
 * Educational wrapper for Parquet metadata with helper methods for easy access.
 * 
 * REUSES EXISTING INFRASTRUCTURE:
 * - Wraps org.apache.parquet.hadoop.metadata.ParquetMetadata
 * - Provides educational methods for students to understand metadata structure
 * 
 * Learning objective: Understand how metadata-driven query optimization works
 * in columnar storage systems like Parquet.
 */
public class ParquetMetadata {
    private final org.apache.parquet.hadoop.metadata.ParquetMetadata metadata;
    private final List<RowGroupStats> rowGroupStats;
    
    /**
     * Constructor wrapping the native Parquet metadata
     * 
     * @param metadata Native Parquet metadata from the file footer
     */
    public ParquetMetadata(org.apache.parquet.hadoop.metadata.ParquetMetadata metadata) {
        this.metadata = metadata;
        this.rowGroupStats = extractRowGroupStats();
    }
    
    /**
     * Get the total number of row groups in the file
     * 
     * Learning note: Row groups are the fundamental unit of parallelization
     * in Parquet. Each row group contains statistics that enable predicate pushdown.
     */
    public int getTotalRowGroups() {
        return metadata.getBlocks().size();
    }
    
    /**
     * Get the total number of records across all row groups
     */
    public long getTotalRecords() {
        return metadata.getBlocks().stream()
                .mapToLong(BlockMetaData::getRowCount)
                .sum();
    }
    
    /**
     * Get statistics for a specific row group
     * 
     * @param rowGroupIndex Zero-based row group index
     * @return Statistics for the specified row group
     */
    public RowGroupStats getRowGroupStats(int rowGroupIndex) {
        if (rowGroupIndex < 0 || rowGroupIndex >= rowGroupStats.size()) {
            throw new IndexOutOfBoundsException("Row group index out of bounds: " + rowGroupIndex);
        }
        return rowGroupStats.get(rowGroupIndex);
    }
    
    /**
     * Get all row group statistics
     */
    public List<RowGroupStats> getAllRowGroupStats() {
        return new ArrayList<>(rowGroupStats);
    }
    
    /**
     * Get the underlying Parquet metadata for advanced operations
     */
    public org.apache.parquet.hadoop.metadata.ParquetMetadata getMetadata() {
        return metadata;
    }
    
    /**
     * Get the number of columns in the schema
     */
    public int getColumnCount() {
        return metadata.getFileMetaData().getSchema().getFieldCount();
    }
    
    /**
     * Extract row group statistics from the metadata
     * 
     * Learning note: This shows how statistics are embedded in the Parquet footer
     * and how they can be accessed without reading the actual data.
     */
    private List<RowGroupStats> extractRowGroupStats() {
        List<RowGroupStats> stats = new ArrayList<>();
        
        for (int i = 0; i < metadata.getBlocks().size(); i++) {
            BlockMetaData block = metadata.getBlocks().get(i);
            
            Integer minAge = null, maxAge = null;
            long recordCount = block.getRowCount();
            
            // Extract age column statistics
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.getPath().toDotString().equals("age")) {
                    Statistics<?> columnStats = column.getStatistics();
                    if (columnStats != null && !columnStats.isEmpty()) {
                        minAge = (Integer) columnStats.genericGetMin();
                        maxAge = (Integer) columnStats.genericGetMax();
                    }
                    break;
                }
            }
            
            stats.add(new RowGroupStats(i, recordCount, minAge, maxAge));
        }
        
        return stats;
    }
    
    /**
     * Educational toString method showing metadata structure
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== PARQUET FILE METADATA ===\n");
        sb.append(String.format("Total Row Groups: %d\n", getTotalRowGroups()));
        sb.append(String.format("Total Records: %d\n", getTotalRecords()));
        sb.append("\n=== ROW GROUP STATISTICS ===\n");
        
        for (RowGroupStats stats : rowGroupStats) {
            sb.append(String.format("Row Group %d: records=%d, age_range=[%s-%s]\n",
                    stats.getRowGroupIndex(),
                    stats.getRecordCount(),
                    stats.getMinAge() != null ? stats.getMinAge() : "null",
                    stats.getMaxAge() != null ? stats.getMaxAge() : "null"));
        }
        
        return sb.toString();
    }
    
    /**
     * Statistics for a single row group
     * 
     * Learning note: These statistics are crucial for query optimization.
     * They allow systems to skip entire row groups that cannot contain
     * matching data based on the query predicates.
     */
    public static class RowGroupStats {
        private final int rowGroupIndex;
        private final long recordCount;
        private final Integer minAge;
        private final Integer maxAge;
        
        public RowGroupStats(int rowGroupIndex, long recordCount, Integer minAge, Integer maxAge) {
            this.rowGroupIndex = rowGroupIndex;
            this.recordCount = recordCount;
            this.minAge = minAge;
            this.maxAge = maxAge;
        }
        
        public int getRowGroupIndex() { return rowGroupIndex; }
        public long getRecordCount() { return recordCount; }
        public Integer getMinAge() { return minAge; }
        public Integer getMaxAge() { return maxAge; }
        
        /**
         * Check if this row group might contain records matching the age predicate
         * 
         * @param minAgeFilter Minimum age filter value
         * @return true if row group might contain matching records, false if it can be skipped
         */
        public boolean mightContainAge(int minAgeFilter) {
            if (maxAge == null) {
                // No statistics available, must read row group
                return true;
            }
            // Skip if all ages in this row group are below the filter
            return maxAge >= minAgeFilter;
        }
    }
} 