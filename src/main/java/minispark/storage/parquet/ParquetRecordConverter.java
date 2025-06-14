package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.table.TableSchema;

import java.util.Map;

/**
 * EDUCATIONAL: Converts between Table records and Parquet format.
 * 
 * Demonstrates data format conversion for columnar storage:
 * - Schema-aware record validation
 * - Type conversion and mapping
 * - Educational logging for workshops
 * 
 * Design Principles:
 * - Single Responsibility: Only handles record conversion
 * - Schema Validation: Ensure data integrity
 * - Clear Transformations: Easy to understand conversions
 */
public class ParquetRecordConverter {
    
    private final TableSchema schema;
    
    /**
     * Creates converter for the specified schema.
     * 
     * @param schema Table schema for validation and conversion
     */
    public ParquetRecordConverter(TableSchema schema) {
        this.schema = schema;
        logConverterCreation();
    }
    
    /**
     * Validates a record against the schema.
     * 
     * @param record Record to validate
     * @throws IllegalArgumentException If record is invalid
     */
    public void validateRecord(Record record) {
        if (record == null) {
            throw new IllegalArgumentException("Record cannot be null");
        }
        
        if (record.getKey() == null || record.getKey().length == 0) {
            throw new IllegalArgumentException("Record key cannot be null or empty");
        }
        
        if (record.getValue() == null || record.getValue().isEmpty()) {
            throw new IllegalArgumentException("Record value cannot be null or empty");
        }
        
        validateSchemaCompliance(record);
        logRecordValidated(record);
    }
    
    /**
     * Converts a Table record to Parquet-compatible format.
     * 
     * @param record Table record to convert
     * @return Converted record data
     */
    public Map<String, Object> convertToParquetFormat(Record record) {
        validateRecord(record);
        
        // For now, return the record value as-is
        // In a full implementation, this would handle:
        // - Type conversions (e.g., Java types to Parquet types)
        // - Schema mapping
        // - Data encoding
        
        logRecordConversion(record, "Table -> Parquet");
        return record.getValue();
    }
    
    /**
     * Converts Parquet data back to Table record format.
     * 
     * @param key Record key
     * @param parquetData Data from Parquet file
     * @return Table record
     */
    public Record convertFromParquetFormat(byte[] key, Map<String, Object> parquetData) {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        
        if (parquetData == null || parquetData.isEmpty()) {
            throw new IllegalArgumentException("Parquet data cannot be null or empty");
        }
        
        // For now, create record directly
        // In a full implementation, this would handle:
        // - Type conversions (e.g., Parquet types to Java types)
        // - Schema validation
        // - Data decoding
        
        Record record = new Record(key, parquetData);
        logRecordConversion(record, "Parquet -> Table");
        return record;
    }
    
    /**
     * Gets the associated schema.
     * 
     * @return Table schema
     */
    public TableSchema getSchema() {
        return schema;
    }
    
    // Private helper methods
    
    private void validateSchemaCompliance(Record record) {
        // Basic validation - in a full implementation, this would:
        // - Check all required fields are present
        // - Validate field types match schema
        // - Ensure no extra fields (if strict mode)
        
        Map<String, Object> values = record.getValue();
        
        // Check if primary key field exists in values
        String primaryKeyField = schema.getPrimaryKeyColumn();
        if (!values.containsKey(primaryKeyField)) {
            throw new IllegalArgumentException(
                "Record missing primary key field: " + primaryKeyField);
        }
        
        // Additional schema validation would go here
    }
    
    // Educational logging methods
    
    private void logConverterCreation() {
        System.out.println("   🔄 Created ParquetRecordConverter for schema with primary key: " + schema.getPrimaryKeyColumn());
    }
    
    private void logRecordValidated(Record record) {
        System.out.println("   ✅ Record validated: " + new String(record.getKey()));
    }
    
    private void logRecordConversion(Record record, String direction) {
        System.out.println("   🔄 Converting record (" + direction + "): " + new String(record.getKey()));
    }
} 