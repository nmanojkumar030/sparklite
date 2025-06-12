package minispark.fileformat;

import minispark.core.Partition;
import java.util.Map;

/**
 * Represents a partition of a file with format-specific metadata.
 * This is a generic partition that can store different types of partition information
 * depending on the file format.
 */
public class FilePartition implements Partition {
    private final int partitionId;
    private final String filePath;
    private final long startOffset;
    private final long length;
    private final Map<String, Object> formatSpecificMetadata;
    
    /**
     * Create a new FilePartition.
     *
     * @param partitionId The partition index
     * @param filePath Path to the file this partition belongs to
     * @param startOffset Start offset in the file (for formats that support it)
     * @param length Length of data in this partition
     * @param formatSpecificMetadata Additional metadata specific to the file format
     */
    public FilePartition(int partitionId, String filePath, long startOffset, long length, 
                        Map<String, Object> formatSpecificMetadata) {
        this.partitionId = partitionId;
        this.filePath = filePath;
        this.startOffset = startOffset;
        this.length = length;
        this.formatSpecificMetadata = formatSpecificMetadata;
    }
    
    @Override
    public int index() {
        return partitionId;
    }
    
    public String getFilePath() {
        return filePath;
    }
    
    public long getStartOffset() {
        return startOffset;
    }
    
    public long getLength() {
        return length;
    }
    
    public Map<String, Object> getFormatSpecificMetadata() {
        return formatSpecificMetadata;
    }
    
    /**
     * Get a format-specific metadata value.
     *
     * @param key The metadata key
     * @return The metadata value, or null if not present
     */
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key) {
        return (T) formatSpecificMetadata.get(key);
    }
    
    @Override
    public String toString() {
        return String.format("FilePartition(id=%d, file=%s, offset=%d, length=%d, metadata=%s)", 
            partitionId, filePath, startOffset, length, formatSpecificMetadata);
    }
} 