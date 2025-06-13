package minispark.storage.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a record in a table with a primary key and column values.
 * This is a higher-level abstraction over the storage layer's Record class.
 */
public class TableRecord {
    private final String primaryKey;
    private final Map<String, Object> values;
    
    /**
     * Creates a new table record.
     *
     * @param primaryKey The primary key value
     * @param values Map of column names to values
     */
    public TableRecord(String primaryKey, Map<String, Object> values) {
        this.primaryKey = Objects.requireNonNull(primaryKey, "Primary key cannot be null");
        this.values = new HashMap<>(Objects.requireNonNull(values, "Values cannot be null"));
    }
    
    /**
     * Gets the primary key.
     *
     * @return The primary key value
     */
    public String getPrimaryKey() {
        return primaryKey;
    }
    
    /**
     * Gets all column values.
     *
     * @return Map of column names to values
     */
    public Map<String, Object> getValues() {
        return new HashMap<>(values);
    }
    
    /**
     * Gets the value of a specific column.
     *
     * @param columnName The column name
     * @return The column value, or null if not present
     */
    public Object getValue(String columnName) {
        return values.get(columnName);
    }
    
    /**
     * Gets the value of a specific column with type casting.
     *
     * @param columnName The column name
     * @param type The expected type
     * @param <T> The type parameter
     * @return The column value cast to the specified type
     * @throws ClassCastException If the value cannot be cast to the specified type
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(String columnName, Class<T> type) {
        Object value = values.get(columnName);
        if (value == null) {
            return null;
        }
        return (T) value;
    }
    
    /**
     * Checks if the record has a value for the specified column.
     *
     * @param columnName The column name
     * @return true if the column has a value
     */
    public boolean hasValue(String columnName) {
        return values.containsKey(columnName);
    }
    
    /**
     * Gets the number of columns in this record.
     *
     * @return The number of columns
     */
    public int getColumnCount() {
        return values.size();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TableRecord that = (TableRecord) obj;
        return primaryKey.equals(that.primaryKey) && values.equals(that.values);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, values);
    }
    
    @Override
    public String toString() {
        return String.format("TableRecord{primaryKey='%s', values=%s}", primaryKey, values);
    }
} 