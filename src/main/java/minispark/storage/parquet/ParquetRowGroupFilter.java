package minispark.storage.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.column.statistics.Statistics;

import java.util.List;
import java.util.ArrayList;

/**
 * EDUCATIONAL: Demonstrates Parquet row group filtering using metadata statistics.
 * 
 * This is one of Parquet's key performance optimizations:
 * - Each row group stores min/max statistics for every column
 * - Queries can skip entire row groups based on these statistics
 * - This is called "predicate pushdown" or "row group pruning"
 * 
 * Example: If looking for key "CUST500" and a row group has:
 * - min_key = "CUST001", max_key = "CUST100"
 * - We can skip this entire row group without reading any data!
 */
public class ParquetRowGroupFilter {
    
    /**
     * Filters row groups based on a point lookup key.
     * Uses min/max statistics to eliminate row groups that cannot contain the key.
     * 
     * @param rowGroups All row groups in the file
     * @param primaryKeyColumn Name of the primary key column
     * @param searchKey Key to search for
     * @return List of row group indices that might contain the key
     */
    public static List<Integer> filterRowGroupsForPointLookup(List<BlockMetaData> rowGroups, 
                                                            String primaryKeyColumn, 
                                                            String searchKey) {
        List<Integer> candidateRowGroups = new ArrayList<>();
        
        logFilteringStart(searchKey, rowGroups.size());
        
        for (int i = 0; i < rowGroups.size(); i++) {
            BlockMetaData rowGroup = rowGroups.get(i);
            
            if (rowGroupMightContainKey(rowGroup, primaryKeyColumn, searchKey)) {
                candidateRowGroups.add(i);
                logRowGroupIncluded(i, searchKey);
            } else {
                logRowGroupSkipped(i, searchKey, rowGroup, primaryKeyColumn);
            }
        }
        
        logFilteringComplete(candidateRowGroups.size(), rowGroups.size());
        return candidateRowGroups;
    }
    
    /**
     * Filters row groups based on a range scan.
     * Uses min/max statistics to eliminate row groups outside the range.
     * 
     * @param rowGroups All row groups in the file
     * @param primaryKeyColumn Name of the primary key column
     * @param startKey Start of range (null = no lower bound)
     * @param endKey End of range (null = no upper bound)
     * @return List of row group indices that overlap with the range
     */
    public static List<Integer> filterRowGroupsForRangeScan(List<BlockMetaData> rowGroups,
                                                          String primaryKeyColumn,
                                                          String startKey,
                                                          String endKey) {
        List<Integer> candidateRowGroups = new ArrayList<>();
        
        logRangeFilteringStart(startKey, endKey, rowGroups.size());
        
        for (int i = 0; i < rowGroups.size(); i++) {
            BlockMetaData rowGroup = rowGroups.get(i);
            
            if (rowGroupOverlapsRange(rowGroup, primaryKeyColumn, startKey, endKey)) {
                candidateRowGroups.add(i);
                logRowGroupIncludedForRange(i, startKey, endKey);
            } else {
                logRowGroupSkippedForRange(i, startKey, endKey, rowGroup, primaryKeyColumn);
            }
        }
        
        logRangeFilteringComplete(candidateRowGroups.size(), rowGroups.size());
        return candidateRowGroups;
    }
    
    /**
     * Checks if a row group might contain the search key.
     * Uses min/max statistics for efficient filtering.
     */
    private static boolean rowGroupMightContainKey(BlockMetaData rowGroup, 
                                                 String primaryKeyColumn, 
                                                 String searchKey) {
        // Find the column chunk for the primary key
        ColumnChunkMetaData keyColumn = findColumnChunk(rowGroup, primaryKeyColumn);
        if (keyColumn == null) {
            // If we can't find statistics, we must include this row group
            return true;
        }
        
        Statistics<?> stats = keyColumn.getStatistics();
        if (stats == null || !stats.hasNonNullValue()) {
            // No statistics available, must include this row group
            return true;
        }
        
        // Get min/max values as strings for comparison
        String minValue = convertStatisticToString(stats.minAsString());
        String maxValue = convertStatisticToString(stats.maxAsString());
        
        // EDUCATIONAL: Show the actual statistics being used
        logStatisticsComparison(searchKey, minValue, maxValue, stats.minAsString(), stats.maxAsString());
        
        // Check if search key falls within [min, max] range
        return searchKey.compareTo(minValue) >= 0 && searchKey.compareTo(maxValue) <= 0;
    }
    
    /**
     * Checks if a row group overlaps with the given range.
     * Uses min/max statistics for efficient range filtering.
     */
    private static boolean rowGroupOverlapsRange(BlockMetaData rowGroup,
                                               String primaryKeyColumn,
                                               String startKey,
                                               String endKey) {
        // Find the column chunk for the primary key
        ColumnChunkMetaData keyColumn = findColumnChunk(rowGroup, primaryKeyColumn);
        if (keyColumn == null) {
            return true; // Include if no statistics
        }
        
        Statistics<?> stats = keyColumn.getStatistics();
        if (stats == null || !stats.hasNonNullValue()) {
            return true; // Include if no statistics
        }
        
        String minValue = convertStatisticToString(stats.minAsString());
        String maxValue = convertStatisticToString(stats.maxAsString());
        
        // Check for range overlap: [startKey, endKey] overlaps [minValue, maxValue]
        // No overlap if: endKey < minValue OR startKey > maxValue
        
        if (endKey != null && endKey.compareTo(minValue) < 0) {
            return false; // Range ends before row group starts
        }
        
        if (startKey != null && startKey.compareTo(maxValue) > 0) {
            return false; // Range starts after row group ends
        }
        
        return true; // Ranges overlap
    }
    
    /**
     * Finds the column chunk metadata for a specific column.
     */
    private static ColumnChunkMetaData findColumnChunk(BlockMetaData rowGroup, String columnName) {
        for (ColumnChunkMetaData column : rowGroup.getColumns()) {
            // Column path is stored as array, primary key is typically at root level
            if (column.getPath().toDotString().equals(columnName)) {
                return column;
            }
        }
        return null;
    }
    
    // Educational logging methods
    
    private static void logFilteringStart(String searchKey, int totalRowGroups) {
        System.out.println("   🔍 ROW GROUP FILTERING: Point lookup optimization");
        System.out.println("      Search key: " + searchKey);
        System.out.println("      Total row groups: " + totalRowGroups);
        System.out.println("      Strategy: Use min/max statistics to skip row groups");
    }
    
    private static void logRowGroupIncluded(int rowGroupIndex, String searchKey) {
        System.out.println("      ✅ Row group " + rowGroupIndex + ": Might contain '" + searchKey + "'");
    }
    
    private static void logRowGroupSkipped(int rowGroupIndex, String searchKey, 
                                         BlockMetaData rowGroup, String primaryKeyColumn) {
        ColumnChunkMetaData keyColumn = findColumnChunk(rowGroup, primaryKeyColumn);
        if (keyColumn != null && keyColumn.getStatistics() != null) {
            Statistics<?> stats = keyColumn.getStatistics();
            System.out.println("      ⏭️  Row group " + rowGroupIndex + ": SKIPPED");
            System.out.println("         Range: [" + stats.minAsString() + ", " + stats.maxAsString() + "]");
            System.out.println("         Key '" + searchKey + "' is outside this range");
        }
    }
    
    private static void logFilteringComplete(int candidateCount, int totalCount) {
        System.out.println("      📊 Filtering result: " + candidateCount + "/" + totalCount + " row groups to scan");
        if (candidateCount < totalCount) {
            int skipped = totalCount - candidateCount;
            double skipPercentage = (skipped * 100.0) / totalCount;
            System.out.println("      🚀 Performance gain: Skipped " + skipped + " row groups (" + 
                              String.format("%.1f", skipPercentage) + "%)");
        }
    }
    
    private static void logRangeFilteringStart(String startKey, String endKey, int totalRowGroups) {
        System.out.println("   🔍 ROW GROUP FILTERING: Range scan optimization");
        System.out.println("      Range: [" + (startKey != null ? startKey : "BEGIN") + 
                          ", " + (endKey != null ? endKey : "END") + "]");
        System.out.println("      Total row groups: " + totalRowGroups);
        System.out.println("      Strategy: Use min/max statistics to skip non-overlapping row groups");
    }
    
    private static void logRowGroupIncludedForRange(int rowGroupIndex, String startKey, String endKey) {
        System.out.println("      ✅ Row group " + rowGroupIndex + ": Overlaps with range [" + 
                          (startKey != null ? startKey : "BEGIN") + ", " + 
                          (endKey != null ? endKey : "END") + "]");
    }
    
    private static void logRowGroupSkippedForRange(int rowGroupIndex, String startKey, String endKey,
                                                 BlockMetaData rowGroup, String primaryKeyColumn) {
        ColumnChunkMetaData keyColumn = findColumnChunk(rowGroup, primaryKeyColumn);
        if (keyColumn != null && keyColumn.getStatistics() != null) {
            Statistics<?> stats = keyColumn.getStatistics();
            System.out.println("      ⏭️  Row group " + rowGroupIndex + ": SKIPPED");
            System.out.println("         Row group range: [" + stats.minAsString() + ", " + stats.maxAsString() + "]");
            System.out.println("         Query range: [" + (startKey != null ? startKey : "BEGIN") + 
                              ", " + (endKey != null ? endKey : "END") + "]");
            System.out.println("         No overlap between ranges");
        }
    }
    
    private static void logRangeFilteringComplete(int candidateCount, int totalCount) {
        System.out.println("      📊 Range filtering result: " + candidateCount + "/" + totalCount + " row groups to scan");
        if (candidateCount < totalCount) {
            int skipped = totalCount - candidateCount;
            double skipPercentage = (skipped * 100.0) / totalCount;
            System.out.println("      🚀 Performance gain: Skipped " + skipped + " row groups (" + 
                              String.format("%.1f", skipPercentage) + "%)");
        }
    }
    
    /**
     * Converts Parquet statistics string to a readable string.
     * Parquet may return hex-encoded strings for binary data.
     */
    private static String convertStatisticToString(String statValue) {
        if (statValue == null) {
            return null;
        }
        
        // If it's a hex string (starts with 0x), convert it to ASCII
        if (statValue.startsWith("0x")) {
            try {
                // Remove "0x" prefix and convert hex to bytes, then to string
                String hexString = statValue.substring(2);
                StringBuilder result = new StringBuilder();
                for (int i = 0; i < hexString.length(); i += 2) {
                    String hexByte = hexString.substring(i, Math.min(i + 2, hexString.length()));
                    int byteValue = Integer.parseInt(hexByte, 16);
                    result.append((char) byteValue);
                }
                return result.toString();
            } catch (Exception e) {
                // If conversion fails, return original value
                return statValue;
            }
        }
        
        // Return as-is if not hex-encoded
        return statValue;
    }
    
    /**
     * EDUCATIONAL: Log the statistics comparison for debugging
     */
    private static void logStatisticsComparison(String searchKey, String minValue, String maxValue, 
                                              String rawMin, String rawMax) {
        System.out.printf("         📊 Statistics: min='%s' (raw: %s), max='%s' (raw: %s), searching for='%s'%n", 
                         minValue, rawMin, maxValue, rawMax, searchKey);
        System.out.printf("         🔍 Comparison: %s >= %s? %s, %s <= %s? %s%n",
                         searchKey, minValue, searchKey.compareTo(minValue) >= 0,
                         searchKey, maxValue, searchKey.compareTo(maxValue) <= 0);
    }
} 