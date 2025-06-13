package minispark.storage.table;

import minispark.storage.StorageInterface;
import minispark.storage.btree.BTree;
import minispark.storage.btree.page.PageManager;
import minispark.fileformat.ParquetReader;
import minispark.fileformat.FilePartition;

import java.io.IOException;
import java.util.*;

/**
 * Comprehensive demonstration of Table implementation with different storage backends.
 * Shows how the same Table interface works with:
 * 1. B+Tree storage for transactional workloads
 * 2. Parquet storage for analytical workloads
 */
public class TableDemo {
    
    public static void main(String[] args) {
        try {
            System.out.println("🚀 Table Demo - Storage Backend Comparison");
            System.out.println("=" .repeat(60));
            
            // Demo 1: Table with B+Tree Storage
            demoBTreeTable();
            
            System.out.println("\n" + "=" .repeat(60));
            
            // Demo 2: Table with Parquet Storage
            demoParquetTable();
            
            System.out.println("\n" + "=" .repeat(60));
            
            // Demo 3: Performance Comparison
            performanceComparison();
            
        } catch (Exception e) {
            System.err.println("❌ Demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Demonstrates Table with B+Tree storage backend.
     * Shows transactional operations and row-based storage.
     */
    private static void demoBTreeTable() throws IOException {
        System.out.println("\n📊 Demo 1: Table with B+Tree Storage");
        System.out.println("-" .repeat(40));
        
        // Create B+Tree storage
        BTree btree = new BTree(java.nio.file.Paths.get("btree_customers.db"));
        
        // Create table with customer schema
        TableSchema schema = TableSchema.createCustomerSchema();
        Table customerTable = new Table("customers_btree", schema, btree);
        
        System.out.println("🏗️ Created table: " + customerTable.getTableName());
        System.out.println("📋 Schema: " + schema);
        
        // Insert sample customers
        List<TableRecord> customers = createSampleCustomers();
        
        System.out.println("\n📝 Inserting customers one by one (transactional style):");
        for (TableRecord customer : customers) {
            customerTable.insert(customer);
        }
        
        // Demonstrate point lookups
        System.out.println("\n🔍 Point lookups (primary key access):");
        demonstratePointLookups(customerTable);
        
        // Demonstrate range scans
        System.out.println("\n📊 Range scans:");
        demonstrateRangeScans(customerTable);
        
        // Demonstrate column projection
        System.out.println("\n🎯 Column projection:");
        demonstrateColumnProjection(customerTable);
        
        // Show statistics
        System.out.println("\n📈 Table statistics:");
        System.out.println("   " + customerTable.getStats());
        
        customerTable.close();
        System.out.println("✅ B+Tree table demo completed");
    }
    
    /**
     * Demonstrates Table with Parquet storage backend.
     * Shows analytical operations and columnar storage.
     */
    private static void demoParquetTable() throws IOException {
        System.out.println("\n📊 Demo 2: Table with Parquet Storage");
        System.out.println("-" .repeat(40));
        
        // Create Parquet storage wrapper
        ParquetStorageAdapter parquetStorage = new ParquetStorageAdapter("customers.parquet");
        
        // Create table with customer schema
        TableSchema schema = TableSchema.createCustomerSchema();
        Table customerTable = new Table("customers_parquet", schema, parquetStorage);
        
        System.out.println("🏗️ Created table: " + customerTable.getTableName());
        System.out.println("📋 Schema: " + schema);
        
        // Insert sample customers in batch (analytical style)
        List<TableRecord> customers = createSampleCustomers();
        
        System.out.println("\n📝 Batch inserting customers (analytical style):");
        customerTable.insertBatch(customers);
        
        // Demonstrate analytical queries
        System.out.println("\n📊 Analytical queries:");
        demonstrateAnalyticalQueries(customerTable);
        
        // Demonstrate column-oriented access
        System.out.println("\n🎯 Column-oriented access:");
        demonstrateColumnOrientedAccess(customerTable);
        
        // Show statistics
        System.out.println("\n📈 Table statistics:");
        System.out.println("   " + customerTable.getStats());
        
        customerTable.close();
        System.out.println("✅ Parquet table demo completed");
    }
    
    /**
     * Compares performance characteristics of different storage backends.
     */
    private static void performanceComparison() throws IOException {
        System.out.println("\n📊 Demo 3: Performance Comparison");
        System.out.println("-" .repeat(40));
        
        System.out.println("🏁 B+Tree vs Parquet Performance Characteristics:");
        System.out.println();
        
        System.out.println("📈 B+Tree Storage (Row-based):");
        System.out.println("   ✅ Excellent for: Point lookups, range scans, transactions");
        System.out.println("   ✅ OLTP workloads, frequent updates/deletes");
        System.out.println("   ✅ Low latency individual record access");
        System.out.println("   ⚠️  Higher storage overhead for wide tables");
        System.out.println("   ⚠️  Less efficient for analytical aggregations");
        
        System.out.println();
        System.out.println("📊 Parquet Storage (Column-based):");
        System.out.println("   ✅ Excellent for: Analytical queries, aggregations");
        System.out.println("   ✅ OLAP workloads, data warehousing");
        System.out.println("   ✅ High compression ratios, efficient column access");
        System.out.println("   ⚠️  Higher latency for individual record access");
        System.out.println("   ⚠️  Not suitable for frequent updates");
        
        System.out.println();
        System.out.println("🎯 Use Case Recommendations:");
        System.out.println("   • B+Tree: User profiles, order processing, real-time apps");
        System.out.println("   • Parquet: Analytics, reporting, data science, ETL");
    }
    
    // Helper methods for demonstrations
    
    private static List<TableRecord> createSampleCustomers() {
        List<TableRecord> customers = new ArrayList<>();
        
        customers.add(createCustomer("CUST001", "Alice Johnson", "alice@example.com", 28, "New York"));
        customers.add(createCustomer("CUST002", "Bob Smith", "bob@example.com", 35, "Los Angeles"));
        customers.add(createCustomer("CUST003", "Carol Davis", "carol@example.com", 42, "Chicago"));
        customers.add(createCustomer("CUST004", "David Wilson", "david@example.com", 31, "Houston"));
        customers.add(createCustomer("CUST005", "Eve Brown", "eve@example.com", 29, "Phoenix"));
        customers.add(createCustomer("CUST006", "Frank Miller", "frank@example.com", 38, "Philadelphia"));
        customers.add(createCustomer("CUST007", "Grace Lee", "grace@example.com", 26, "San Antonio"));
        customers.add(createCustomer("CUST008", "Henry Taylor", "henry@example.com", 44, "San Diego"));
        
        return customers;
    }
    
    private static TableRecord createCustomer(String id, String name, String email, Integer age, String city) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("email", email);
        values.put("age", age);
        values.put("city", city);
        
        return new TableRecord(id, values);
    }
    
    private static void demonstratePointLookups(Table table) throws IOException {
        String[] lookupKeys = {"CUST001", "CUST005", "CUST999"};
        
        for (String key : lookupKeys) {
            Optional<TableRecord> result = table.findByPrimaryKey(key);
            if (result.isPresent()) {
                TableRecord record = result.get();
                System.out.println("   Found: " + record.getValue("name") + " (" + record.getValue("city") + ")");
            } else {
                System.out.println("   Not found: " + key);
            }
        }
    }
    
    private static void demonstrateRangeScans(Table table) throws IOException {
        System.out.println("   Range scan CUST001 to CUST005:");
        List<TableRecord> results = table.scan("CUST001", "CUST006", null);
        for (TableRecord record : results) {
            System.out.println("     " + record.getPrimaryKey() + ": " + record.getValue("name"));
        }
    }
    
    private static void demonstrateColumnProjection(Table table) throws IOException {
        System.out.println("   Selecting only name and city columns:");
        List<String> columns = Arrays.asList("name", "city");
        List<TableRecord> results = table.scan("CUST001", "CUST004", columns);
        for (TableRecord record : results) {
            System.out.println("     " + record.getValue("name") + " -> " + record.getValue("city"));
        }
    }
    
    private static void demonstrateAnalyticalQueries(Table table) throws IOException {
        System.out.println("   Full table scan for analytics:");
        List<TableRecord> allRecords = table.scan("", null, null);
        
        // Calculate average age
        double avgAge = allRecords.stream()
            .mapToInt(r -> (Integer) r.getValue("age"))
            .average()
            .orElse(0.0);
        
        System.out.println("     Total customers: " + allRecords.size());
        System.out.println("     Average age: " + String.format("%.1f", avgAge));
        
        // Count by city
        Map<String, Long> cityCount = new HashMap<>();
        for (TableRecord record : allRecords) {
            String city = (String) record.getValue("city");
            cityCount.put(city, cityCount.getOrDefault(city, 0L) + 1);
        }
        System.out.println("     Customers by city: " + cityCount);
    }
    
    private static void demonstrateColumnOrientedAccess(Table table) throws IOException {
        System.out.println("   Column-oriented access (names only):");
        List<String> nameColumns = Arrays.asList("name");
        List<TableRecord> results = table.scan("", null, nameColumns);
        
        List<String> names = results.stream()
            .map(r -> (String) r.getValue("name"))
            .sorted()
            .collect(java.util.stream.Collectors.toList());
        
        System.out.println("     All names (sorted): " + names);
    }
    
    /**
     * Real Parquet storage adapter that uses actual ParquetWriter and ParquetReader.
     * Demonstrates true columnar storage with compression and efficient column access.
     */
    private static class ParquetStorageAdapter implements StorageInterface {
        private final String filename;
        private final List<minispark.storage.Record> bufferedRecords;
        private final ParquetReader parquetReader;
        private final org.apache.parquet.schema.MessageType customerSchema;
        private boolean hasWrittenFile = false;
        
        public ParquetStorageAdapter(String filename) {
            this.filename = filename;
            this.bufferedRecords = new ArrayList<>();
            this.parquetReader = new ParquetReader();
            this.customerSchema = createCustomerParquetSchema();
        }
        
        @Override
        public void write(byte[] key, Map<String, Object> value) throws IOException {
            String keyStr = new String(key);
            bufferedRecords.add(new minispark.storage.Record(key, value));
            System.out.println("   📄 Parquet: Buffered record " + keyStr + " for batch write");
        }
        
        @Override
        public void writeBatch(List<minispark.storage.Record> records) throws IOException {
            System.out.println("   📄 Parquet: Writing " + records.size() + " records to " + filename);
            
            // Add to buffer
            bufferedRecords.addAll(records);
            
            // Write to actual Parquet file
            writeToParquetFile();
            
            System.out.println("   📄 Parquet: ✅ Successfully wrote " + bufferedRecords.size() + 
                             " records in columnar format with SNAPPY compression");
        }
        
        @Override
        public Optional<Map<String, Object>> read(byte[] key) throws IOException {
            String keyStr = new String(key);
            System.out.println("   📄 Parquet: Reading record " + keyStr + " (requires full file scan)");
            
            if (!hasWrittenFile) {
                return Optional.empty();
            }
            
            // Read from Parquet file
            List<minispark.storage.Record> allRecords = readFromParquetFile(null, null, null);
            
            for (minispark.storage.Record record : allRecords) {
                if (keyStr.equals(new String(record.getKey()))) {
                    System.out.println("   📄 Parquet: ✅ Found record " + keyStr + " (column reconstruction)");
                    return Optional.of(record.getValue());
                }
            }
            
            System.out.println("   📄 Parquet: ❌ Record " + keyStr + " not found");
            return Optional.empty();
        }
        
        @Override
        public List<minispark.storage.Record> scan(byte[] startKey, byte[] endKey, List<String> columns) throws IOException {
            String startStr = new String(startKey);
            String endStr = endKey != null ? new String(endKey) : null;
            
            System.out.println("   📄 Parquet: Scanning range [" + startStr + ", " + 
                             (endStr != null ? endStr : "END") + "]" +
                             (columns != null ? " columns=" + columns : " (all columns)"));
            
            if (!hasWrittenFile) {
                return new ArrayList<>();
            }
            
            List<minispark.storage.Record> allRecords = readFromParquetFile(startStr, endStr, columns);
            
            System.out.println("   📄 Parquet: ✅ Scan completed, " + allRecords.size() + 
                             " records (columnar efficiency" + 
                             (columns != null ? ", projected " + columns.size() + " columns" : "") + ")");
            return allRecords;
        }
        
        @Override
        public void delete(byte[] key) throws IOException {
            String keyStr = new String(key);
            System.out.println("   📄 Parquet: ⚠️ Delete operation on " + keyStr + 
                             " (Parquet is immutable, requires full rewrite)");
            // In a real implementation, this would require rewriting the entire file
        }
        
        @Override
        public void close() throws IOException {
            if (!bufferedRecords.isEmpty() && !hasWrittenFile) {
                System.out.println("   📄 Parquet: Writing buffered records on close");
                writeToParquetFile();
            }
            System.out.println("   📄 Parquet: ✅ Closed storage, file: " + filename);
        }
        
        private void writeToParquetFile() throws IOException {
            if (bufferedRecords.isEmpty()) {
                return;
            }
            
            java.io.File file = new java.io.File(filename);
            org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
            
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            org.apache.parquet.hadoop.example.GroupWriteSupport.setSchema(customerSchema, conf);
            
            try (org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer = 
                 new org.apache.parquet.hadoop.ParquetWriter<>(
                     parquetPath,
                     new org.apache.parquet.hadoop.example.GroupWriteSupport(),
                     org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY,
                     1024,  // Block size (row group size)
                     512,   // Page size
                     256,   // Dictionary page size
                     org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                     org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                     org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION,
                     conf
                 )) {
                
                for (minispark.storage.Record record : bufferedRecords) {
                    org.apache.parquet.example.data.simple.SimpleGroup group = 
                        new org.apache.parquet.example.data.simple.SimpleGroup(customerSchema);
                    
                    Map<String, Object> values = record.getValue();
                    group.add("id", (String) values.get("id"));
                    group.add("name", (String) values.get("name"));
                    group.add("email", (String) values.get("email"));
                    group.add("age", (Integer) values.get("age"));
                    group.add("city", (String) values.get("city"));
                    
                    writer.write(group);
                }
            }
            
            hasWrittenFile = true;
        }
        
        private List<minispark.storage.Record> readFromParquetFile(String startKey, String endKey, List<String> columns) throws IOException {
            java.io.File file = new java.io.File(filename);
            if (!file.exists()) {
                return new ArrayList<>();
            }
            
            // Create partitions and read data
            FilePartition[] partitions = parquetReader.createPartitions(file.getAbsolutePath(), 1);
            List<minispark.storage.Record> results = new ArrayList<>();
            
            for (FilePartition partition : partitions) {
                Iterator<org.apache.parquet.example.data.Group> iterator = 
                    parquetReader.readPartition(file.getAbsolutePath(), partition);
                
                while (iterator.hasNext()) {
                    org.apache.parquet.example.data.Group group = iterator.next();
                    
                    String id = group.getString("id", 0);
                    
                    // Range filtering
                    if (startKey != null && id.compareTo(startKey) < 0) continue;
                    if (endKey != null && id.compareTo(endKey) >= 0) continue;
                    
                    // Convert Group to Record
                    Map<String, Object> values = new HashMap<>();
                    
                    if (columns == null || columns.contains("id")) {
                        values.put("id", id);
                    }
                    if (columns == null || columns.contains("name")) {
                        values.put("name", group.getString("name", 0));
                    }
                    if (columns == null || columns.contains("email")) {
                        values.put("email", group.getString("email", 0));
                    }
                    if (columns == null || columns.contains("age")) {
                        values.put("age", group.getInteger("age", 0));
                    }
                    if (columns == null || columns.contains("city")) {
                        values.put("city", group.getString("city", 0));
                    }
                    
                    results.add(new minispark.storage.Record(id.getBytes(), values));
                }
            }
            
            return results;
        }
        
        private org.apache.parquet.schema.MessageType createCustomerParquetSchema() {
            return org.apache.parquet.schema.Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8).named("id")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8).named("name")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8).named("email")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("age")
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(org.apache.parquet.schema.OriginalType.UTF8).named("city")
                .named("customer");
        }
    }
} 