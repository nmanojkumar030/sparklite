package minispark.fileformat;

import minispark.core.Partition;
import java.util.Iterator;
import java.util.List;

/**
 * Generic interface for reading different file formats.
 * Each file format (Parquet, CSV, JSON, etc.) implements this interface.
 *
 * @param <T> The type of records produced by this reader
 */
public interface FormatReader<T> {
    
    /**
     * Analyze the file and create partitions based on the format-specific strategy.
     * For Parquet: partitions based on row groups
     * For CSV: partitions based on line ranges or file size
     * For JSON: partitions based on size or record boundaries
     *
     * @param filePath Path to the file to analyze
     * @param targetPartitions Desired number of partitions
     * @return Array of partitions for this file
     */
    FilePartition[] createPartitions(String filePath, int targetPartitions);
    
    /**
     * Read data from a specific partition of the file.
     *
     * @param filePath Path to the file
     * @param partition The partition to read from
     * @return Iterator over the records in this partition
     */
    Iterator<T> readPartition(String filePath, FilePartition partition);
    
    /**
     * Get preferred locations for reading this partition (for data locality).
     *
     * @param filePath Path to the file
     * @param partition The partition
     * @return List of preferred hosts/locations
     */
    List<String> getPreferredLocations(String filePath, FilePartition partition);
    
    /**
     * Get the format name for this reader.
     *
     * @return Format name (e.g., "parquet", "csv", "json")
     */
    String getFormatName();
} 