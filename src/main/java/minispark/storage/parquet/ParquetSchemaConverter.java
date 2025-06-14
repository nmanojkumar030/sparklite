package minispark.storage.parquet;

import minispark.storage.table.TableSchema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

/**
 * EDUCATIONAL: Converts TableSchema to Parquet MessageType schema.
 * 
 * Demonstrates schema mapping between different data formats:
 * - TableSchema (application level) to MessageType (Parquet level)
 * - Type mapping and validation
 * - Schema evolution considerations
 * 
 * Design Principles:
 * - Single Responsibility: Only handles schema conversion
 * - Clear Mapping: Explicit type conversions
 * - Educational Logging: Shows conversion process
 */
public class ParquetSchemaConverter {
    
    /**
     * Converts TableSchema to Parquet MessageType.
     * 
     * @param tableSchema Table schema to convert
     * @return Parquet MessageType schema
     */
    public static MessageType convertToParquetSchema(TableSchema tableSchema) {
        logSchemaConversion(tableSchema);
        
        Types.MessageTypeBuilder builder = Types.buildMessage();
        
        // Convert each column from TableSchema to Parquet field
        for (String columnName : tableSchema.getColumnNames()) {
            TableSchema.ColumnDefinition column = tableSchema.getColumn(columnName);
            addColumnToParquetSchema(builder, column);
        }
        
        // Use table name or default name for schema
        String schemaName = "record"; // Default name since TableSchema doesn't have table name
        MessageType parquetSchema = builder.named(schemaName);
        
        logSchemaCreated(parquetSchema);
        return parquetSchema;
    }
    
    /**
     * Adds a single column to the Parquet schema builder.
     * 
     * @param builder Parquet schema builder
     * @param column Table column definition
     */
    private static void addColumnToParquetSchema(Types.MessageTypeBuilder builder, 
                                               TableSchema.ColumnDefinition column) {
        String columnName = column.getName();
        TableSchema.ColumnType columnType = column.getType();
        boolean isRequired = column.isRequired();
        
        // Convert TableSchema types to Parquet primitive types
        PrimitiveType.PrimitiveTypeName parquetType = convertColumnType(columnType);
        
        // Add field to schema (required vs optional)
        if (isRequired) {
            builder.required(parquetType).named(columnName);
        } else {
            builder.optional(parquetType).named(columnName);
        }
        
        logColumnConversion(columnName, columnType, parquetType, isRequired);
    }
    
    /**
     * Converts TableSchema column type to Parquet primitive type.
     * 
     * @param columnType Table column type
     * @return Parquet primitive type
     */
    private static PrimitiveType.PrimitiveTypeName convertColumnType(TableSchema.ColumnType columnType) {
        switch (columnType) {
            case STRING:
                return PrimitiveType.PrimitiveTypeName.BINARY;
            case INTEGER:
                return PrimitiveType.PrimitiveTypeName.INT32;
            case LONG:
                return PrimitiveType.PrimitiveTypeName.INT64;
            case DOUBLE:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case BOOLEAN:
                return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
    }
    
    // Educational logging methods
    
    private static void logSchemaConversion(TableSchema tableSchema) {
        System.out.println("   🔄 Converting TableSchema to Parquet MessageType");
        System.out.println("      Columns: " + tableSchema.getColumnCount());
        System.out.println("      Primary key: " + tableSchema.getPrimaryKeyColumn());
    }
    
    private static void logColumnConversion(String columnName, TableSchema.ColumnType tableType, 
                                          PrimitiveType.PrimitiveTypeName parquetType, boolean isRequired) {
        System.out.println("      📋 " + columnName + ": " + tableType + " → " + parquetType + 
                          (isRequired ? " (required)" : " (optional)"));
    }
    
    private static void logSchemaCreated(MessageType parquetSchema) {
        System.out.println("   ✅ Parquet schema created: " + parquetSchema.getName());
        System.out.println("      Fields: " + parquetSchema.getFieldCount());
    }
} 