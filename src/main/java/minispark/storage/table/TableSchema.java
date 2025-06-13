package minispark.storage.table;

import java.util.*;

/**
 * Defines the schema for a table, including column definitions and validation rules.
 * Used to ensure data consistency and provide metadata about table structure.
 */
public class TableSchema {
    private final String primaryKeyColumn;
    private final Map<String, ColumnDefinition> columns;
    private final List<String> columnOrder;
    
    /**
     * Creates a new table schema.
     *
     * @param primaryKeyColumn Name of the primary key column
     * @param columns List of column definitions
     */
    public TableSchema(String primaryKeyColumn, List<ColumnDefinition> columns) {
        this.primaryKeyColumn = Objects.requireNonNull(primaryKeyColumn, "Primary key column cannot be null");
        this.columns = new LinkedHashMap<>();
        this.columnOrder = new ArrayList<>();
        
        // Build column map and order
        for (ColumnDefinition column : columns) {
            this.columns.put(column.getName(), column);
            this.columnOrder.add(column.getName());
        }
        
        // Validate primary key exists
        if (!this.columns.containsKey(primaryKeyColumn)) {
            throw new IllegalArgumentException("Primary key column '" + primaryKeyColumn + "' not found in schema");
        }
    }
    
    /**
     * Validates a table record against this schema.
     *
     * @param record The record to validate
     * @throws IllegalArgumentException If the record is invalid
     */
    public void validate(TableRecord record) {
        Objects.requireNonNull(record, "Record cannot be null");
        
        // Validate primary key
        String primaryKey = record.getPrimaryKey();
        if (primaryKey == null || primaryKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Primary key cannot be null or empty");
        }
        
        Map<String, Object> values = record.getValues();
        
        // Check for required columns
        for (ColumnDefinition column : columns.values()) {
            if (column.isRequired() && !values.containsKey(column.getName())) {
                throw new IllegalArgumentException("Required column '" + column.getName() + "' is missing");
            }
        }
        
        // Validate each value
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            ColumnDefinition column = columns.get(columnName);
            if (column == null) {
                throw new IllegalArgumentException("Unknown column '" + columnName + "'");
            }
            
            column.validate(value);
        }
    }
    
    /**
     * Checks if a column exists in the schema.
     *
     * @param columnName The column name
     * @return true if the column exists
     */
    public boolean hasColumn(String columnName) {
        return columns.containsKey(columnName);
    }
    
    /**
     * Gets the definition for a specific column.
     *
     * @param columnName The column name
     * @return The column definition, or null if not found
     */
    public ColumnDefinition getColumn(String columnName) {
        return columns.get(columnName);
    }
    
    /**
     * Gets all column names in order.
     *
     * @return List of column names
     */
    public List<String> getColumnNames() {
        return new ArrayList<>(columnOrder);
    }
    
    /**
     * Gets the number of columns.
     *
     * @return The column count
     */
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Gets the primary key column name.
     *
     * @return The primary key column name
     */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    
    /**
     * Creates a customer schema for demonstration purposes.
     * Matches the schema used in Spark and Parquet tests.
     *
     * @return Customer table schema
     */
    public static TableSchema createCustomerSchema() {
        List<ColumnDefinition> columns = Arrays.asList(
            new ColumnDefinition("id", ColumnType.STRING, true),
            new ColumnDefinition("name", ColumnType.STRING, true),
            new ColumnDefinition("email", ColumnType.STRING, true),
            new ColumnDefinition("age", ColumnType.INTEGER, false),
            new ColumnDefinition("city", ColumnType.STRING, false)
        );
        
        return new TableSchema("id", columns);
    }
    
    @Override
    public String toString() {
        return String.format("TableSchema{primaryKey='%s', columns=%s}", 
            primaryKeyColumn, columnOrder);
    }
    
    /**
     * Defines a column in the table schema.
     */
    public static class ColumnDefinition {
        private final String name;
        private final ColumnType type;
        private final boolean required;
        
        public ColumnDefinition(String name, ColumnType type, boolean required) {
            this.name = Objects.requireNonNull(name, "Column name cannot be null");
            this.type = Objects.requireNonNull(type, "Column type cannot be null");
            this.required = required;
        }
        
        public String getName() { return name; }
        public ColumnType getType() { return type; }
        public boolean isRequired() { return required; }
        
        /**
         * Validates a value against this column definition.
         *
         * @param value The value to validate
         * @throws IllegalArgumentException If the value is invalid
         */
        public void validate(Object value) {
            if (value == null) {
                if (required) {
                    throw new IllegalArgumentException("Column '" + name + "' is required but got null");
                }
                return;
            }
            
            if (!type.isValidValue(value)) {
                throw new IllegalArgumentException("Column '" + name + "' expects " + type + 
                    " but got " + value.getClass().getSimpleName() + ": " + value);
            }
        }
        
        @Override
        public String toString() {
            return String.format("%s:%s%s", name, type, required ? "*" : "");
        }
    }
    
    /**
     * Supported column types.
     */
    public enum ColumnType {
        STRING(String.class),
        INTEGER(Integer.class),
        LONG(Long.class),
        DOUBLE(Double.class),
        BOOLEAN(Boolean.class);
        
        private final Class<?> javaType;
        
        ColumnType(Class<?> javaType) {
            this.javaType = javaType;
        }
        
        public Class<?> getJavaType() {
            return javaType;
        }
        
        public boolean isValidValue(Object value) {
            return javaType.isInstance(value);
        }
    }
} 