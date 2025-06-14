package minispark.storage.parquet;

import minispark.storage.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * EDUCATIONAL: Manages in-memory buffering for Parquet writes.
 * 
 * Demonstrates memory management strategies for columnar storage:
 * - Buffer records until row group size threshold
 * - Monitor memory usage to prevent OOM
 * - Provide clear flush triggers
 * 
 * Design Principles:
 * - Single Responsibility: Only handles buffering logic
 * - Clear Thresholds: Configurable limits for workshop demos
 * - Memory Awareness: Prevents memory pressure
 */
public class ParquetBufferManager {
    
    // EDUCATIONAL: Industry-standard row group size (128MB)
    private static final int DEFAULT_ROW_GROUP_SIZE_MB = 128;
    private static final int DEFAULT_MAX_RECORDS = 1_000_000;
    
    private final List<Record> buffer;
    private final int maxRowGroupSizeBytes;
    private final int maxRecordCount;
    private long currentBufferSizeBytes;
    
    /**
     * Creates buffer manager with default thresholds.
     */
    public ParquetBufferManager() {
        this(DEFAULT_ROW_GROUP_SIZE_MB * 1024 * 1024, DEFAULT_MAX_RECORDS);
    }
    
    /**
     * Creates buffer manager with custom thresholds.
     * 
     * @param maxRowGroupSizeBytes Maximum buffer size in bytes
     * @param maxRecordCount Maximum number of records to buffer
     */
    public ParquetBufferManager(int maxRowGroupSizeBytes, int maxRecordCount) {
        this.buffer = new ArrayList<>();
        this.maxRowGroupSizeBytes = maxRowGroupSizeBytes;
        this.maxRecordCount = maxRecordCount;
        this.currentBufferSizeBytes = 0;
    }
    
    /**
     * Adds a record to the buffer.
     * 
     * @param record Record to buffer
     */
    public void addRecord(Record record) {
        buffer.add(record);
        currentBufferSizeBytes += estimateRecordSize(record);
        
        logRecordAdded(record);
    }
    
    /**
     * Checks if buffer should be flushed to disk.
     * 
     * @return true if buffer should be flushed
     */
    public boolean shouldFlush() {
        boolean sizeThreshold = currentBufferSizeBytes >= maxRowGroupSizeBytes;
        boolean countThreshold = buffer.size() >= maxRecordCount;
        
        if (sizeThreshold || countThreshold) {
            logFlushTrigger(sizeThreshold, countThreshold);
            return true;
        }
        
        return false;
    }
    
    /**
     * Gets all buffered records.
     * 
     * @return List of buffered records
     */
    public List<Record> getBufferedRecords() {
        return new ArrayList<>(buffer);
    }
    
    /**
     * Clears the buffer after successful flush.
     */
    public void clearBuffer() {
        int recordCount = buffer.size();
        long sizeBytes = currentBufferSizeBytes;
        
        buffer.clear();
        currentBufferSizeBytes = 0;
        
        logBufferCleared(recordCount, sizeBytes);
    }
    
    /**
     * Checks if buffer has records.
     * 
     * @return true if buffer contains records
     */
    public boolean hasBufferedRecords() {
        return !buffer.isEmpty();
    }
    
    /**
     * Gets current buffer statistics.
     * 
     * @return Buffer statistics for monitoring
     */
    public BufferStats getStats() {
        return new BufferStats(
            buffer.size(),
            currentBufferSizeBytes,
            maxRecordCount,
            maxRowGroupSizeBytes
        );
    }
    
    // Private helper methods
    
    private long estimateRecordSize(Record record) {
        // Simple estimation: key size + estimated value size
        long keySize = record.getKey().length;
        long valueSize = estimateValueSize(record.getValue());
        return keySize + valueSize;
    }
    
    private long estimateValueSize(java.util.Map<String, Object> value) {
        // Rough estimation for educational purposes
        return value.size() * 50; // Assume ~50 bytes per field
    }
    
    // Educational logging methods
    
    private void logRecordAdded(Record record) {
        System.out.println("   💾 Buffer: Added record, size=" + buffer.size() + 
                          ", bytes=" + currentBufferSizeBytes);
    }
    
    private void logFlushTrigger(boolean sizeThreshold, boolean countThreshold) {
        System.out.println("   🚨 FLUSH TRIGGER:");
        if (sizeThreshold) {
            System.out.println("      📏 Size threshold reached: " + 
                              currentBufferSizeBytes + " >= " + maxRowGroupSizeBytes);
        }
        if (countThreshold) {
            System.out.println("      🔢 Count threshold reached: " + 
                              buffer.size() + " >= " + maxRecordCount);
        }
    }
    
    private void logBufferCleared(int recordCount, long sizeBytes) {
        System.out.println("   🧹 Buffer cleared: " + recordCount + " records, " + 
                          sizeBytes + " bytes");
    }
    
    /**
     * Buffer statistics for monitoring and educational purposes.
     */
    public static class BufferStats {
        private final int recordCount;
        private final long sizeBytes;
        private final int maxRecords;
        private final long maxSizeBytes;
        
        public BufferStats(int recordCount, long sizeBytes, int maxRecords, long maxSizeBytes) {
            this.recordCount = recordCount;
            this.sizeBytes = sizeBytes;
            this.maxRecords = maxRecords;
            this.maxSizeBytes = maxSizeBytes;
        }
        
        public int getRecordCount() { return recordCount; }
        public long getSizeBytes() { return sizeBytes; }
        public int getMaxRecords() { return maxRecords; }
        public long getMaxSizeBytes() { return maxSizeBytes; }
        
        @Override
        public String toString() {
            return String.format("BufferStats{records=%d/%d, bytes=%d/%d}", 
                               recordCount, maxRecords, sizeBytes, maxSizeBytes);
        }
    }
} 