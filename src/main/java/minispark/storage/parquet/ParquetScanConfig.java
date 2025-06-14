package minispark.storage.parquet;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

import java.util.List;

/**
 * Configuration object for Parquet row group scanning operations.
 * 
 * Reduces parameter count toxicity by encapsulating scan parameters
 * into a single, cohesive configuration object with builder pattern.
 */
public class ParquetScanConfig {
    private final ParquetFileReader fileReader;
    private final MessageType schema;
    private final int rowGroupIndex;
    private final String primaryKeyField;
    private final byte[] startKey;
    private final byte[] endKey;
    private final List<String> columns;
    
    private ParquetScanConfig(Builder builder) {
        this.fileReader = builder.fileReader;
        this.schema = builder.schema;
        this.rowGroupIndex = builder.rowGroupIndex;
        this.primaryKeyField = builder.primaryKeyField;
        this.startKey = builder.startKey;
        this.endKey = builder.endKey;
        this.columns = builder.columns;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private ParquetFileReader fileReader;
        private MessageType schema;
        private int rowGroupIndex;
        private String primaryKeyField;
        private byte[] startKey;
        private byte[] endKey;
        private List<String> columns;
        
        public Builder fileReader(ParquetFileReader fileReader) {
            this.fileReader = fileReader;
            return this;
        }
        
        public Builder schema(MessageType schema) {
            this.schema = schema;
            return this;
        }
        
        public Builder rowGroupIndex(int rowGroupIndex) {
            this.rowGroupIndex = rowGroupIndex;
            return this;
        }
        
        public Builder primaryKeyField(String primaryKeyField) {
            this.primaryKeyField = primaryKeyField;
            return this;
        }
        
        public Builder keyRange(byte[] startKey, byte[] endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
            return this;
        }
        
        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }
        
        public ParquetScanConfig build() {
            return new ParquetScanConfig(this);
        }
    }
    
    public ParquetFileReader getFileReader() { return fileReader; }
    public MessageType getSchema() { return schema; }
    public int getRowGroupIndex() { return rowGroupIndex; }
    public String getPrimaryKeyField() { return primaryKeyField; }
    public byte[] getStartKey() { return startKey; }
    public byte[] getEndKey() { return endKey; }
    public List<String> getColumns() { return columns; }
    
    public String getStartKeyString() {
        return startKey != null ? new String(startKey) : null;
    }
    
    public String getEndKeyString() {
        return endKey != null ? new String(endKey) : null;
    }
} 