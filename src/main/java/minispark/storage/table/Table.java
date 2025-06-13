package minispark.storage.table;

import minispark.storage.Record;
import minispark.storage.StorageInterface;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Table implementation that provides a higher-level interface over storage engines.
 * Demonstrates how database tables work with different storage backends:
 * - B+Tree for transactional workloads and row-based storage
 * - Parquet for analytical workloads and columnar storage
 * - Any other StorageInterface implementation
 */
public class Table {
    private final String tableName;
    private final TableSchema schema;
    private final StorageInterface storage;
    
    /**
     * Creates a new table with the specified schema and storage backend.
     *
     * @param tableName Name of the table
     * @param schema Table schema defining columns and types
     * @param storage Storage backend implementation
     */
    public Table(String tableName, TableSchema schema, StorageInterface storage) {
        this.tableName = tableName;
        this.schema = schema;
        this.storage = storage;
    }
    
    /**
     * Inserts a new record into the table.
     *
     * @param record The record to insert
     * @throws IOException If an I/O error occurs
     * @throws IllegalArgumentException If the record doesn't match the schema
     */
    public void insert(TableRecord record) throws IOException {
        System.out.println("📝 Table.insert() - Inserting record with key: " + record.getPrimaryKey());
        
        // Validate record against schema
        validateRecord(record);
        
        // Convert to storage format
        byte[] key = record.getPrimaryKey().getBytes();
        Map<String, Object> value = record.getValues();
        
        // Insert into primary storage
        storage.write(key, value);
        
        System.out.println("   ✅ Record inserted successfully");
    }
    
    /**
     * Inserts multiple records in batch.
     *
     * @param records List of records to insert
     * @throws IOException If an I/O error occurs
     */
    public void insertBatch(List<TableRecord> records) throws IOException {
        System.out.println("📝 Table.insertBatch() - Inserting " + records.size() + " records");
        
        // Validate all records first
        for (TableRecord record : records) {
            validateRecord(record);
        }
        
        // Convert to storage format
        List<Record> storageRecords = records.stream()
            .map(r -> new Record(r.getPrimaryKey().getBytes(), r.getValues()))
            .collect(Collectors.toList());
        
        // Batch insert into primary storage
        storage.writeBatch(storageRecords);
        
        System.out.println("   ✅ Batch insert completed successfully");
    }
    
    /**
     * Finds a record by its primary key.
     *
     * @param primaryKey The primary key value
     * @return Optional containing the record if found
     * @throws IOException If an I/O error occurs
     */
    public Optional<TableRecord> findByPrimaryKey(String primaryKey) throws IOException {
        System.out.println("🔍 Table.findByPrimaryKey() - Looking for key: " + primaryKey);
        
        Optional<Map<String, Object>> result = storage.read(primaryKey.getBytes());
        
        if (result.isPresent()) {
            TableRecord record = new TableRecord(primaryKey, result.get());
            System.out.println("   ✅ Record found");
            return Optional.of(record);
        } else {
            System.out.println("   ❌ Record not found");
            return Optional.empty();
        }
    }
    
    /**
     * Scans records within a primary key range.
     *
     * @param startKey Start of the key range (inclusive)
     * @param endKey End of the key range (exclusive), or null for open-ended
     * @param columns Specific columns to retrieve, or null for all columns
     * @return List of matching records
     * @throws IOException If an I/O error occurs
     */
    public List<TableRecord> scan(String startKey, String endKey, List<String> columns) throws IOException {
        System.out.println("🔍 Table.scan() - Range: [" + startKey + ", " + 
                          (endKey != null ? endKey : "END") + "]");
        
        // Validate columns exist in schema
        if (columns != null) {
            validateColumns(columns);
        }
        
        byte[] startKeyBytes = startKey.getBytes();
        byte[] endKeyBytes = endKey != null ? endKey.getBytes() : null;
        
        List<Record> storageRecords = storage.scan(startKeyBytes, endKeyBytes, columns);
        
        List<TableRecord> tableRecords = storageRecords.stream()
            .map(r -> new TableRecord(new String(r.getKey()), r.getValue()))
            .collect(Collectors.toList());
        
        System.out.println("   ✅ Scan completed. Found " + tableRecords.size() + " records");
        return tableRecords;
    }
    
    /**
     * Deletes a record by its primary key.
     *
     * @param primaryKey The primary key value
     * @throws IOException If an I/O error occurs
     */
    public void delete(String primaryKey) throws IOException {
        System.out.println("🗑️ Table.delete() - Deleting record with key: " + primaryKey);
        
        // Check if record exists first
        Optional<TableRecord> record = findByPrimaryKey(primaryKey);
        
        if (record.isPresent()) {
            // Delete from primary storage
            storage.delete(primaryKey.getBytes());
            System.out.println("   ✅ Record deleted successfully");
        } else {
            System.out.println("   ⚠️ Record not found, nothing to delete");
        }
    }
    
    /**
     * Gets table statistics for analysis.
     *
     * @return Table statistics
     */
    public TableStats getStats() {
        return new TableStats(tableName, schema);
    }
    
    /**
     * Gets the table name.
     *
     * @return The table name
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Gets the table schema.
     *
     * @return The table schema
     */
    public TableSchema getSchema() {
        return schema;
    }
    
    /**
     * Closes the table and releases resources.
     *
     * @throws IOException If an I/O error occurs
     */
    public void close() throws IOException {
        System.out.println("🔒 Closing table: " + tableName);
        storage.close();
        System.out.println("   ✅ Table closed successfully");
    }
    
    // Helper methods
    
    private void validateRecord(TableRecord record) {
        schema.validate(record);
    }
    
    private void validateColumns(List<String> columns) {
        for (String column : columns) {
            if (!schema.hasColumn(column)) {
                throw new IllegalArgumentException("Column '" + column + "' does not exist in schema");
            }
        }
    }
    
    /**
     * Table statistics for analysis and monitoring.
     */
    public static class TableStats {
        private final String tableName;
        private final TableSchema schema;
        
        public TableStats(String tableName, TableSchema schema) {
            this.tableName = tableName;
            this.schema = schema;
        }
        
        public String getTableName() { return tableName; }
        public TableSchema getSchema() { return schema; }
        
        @Override
        public String toString() {
            return String.format("TableStats{name='%s', columns=%d}", 
                tableName, schema.getColumnCount());
        }
    }
} 