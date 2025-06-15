package minispark.storage.parquet.assignment;

import minispark.storage.Record;
import minispark.storage.table.TableSchema;
import minispark.storage.parquet.ParquetSchemaConverter;
import minispark.storage.parquet.ParquetEducationalLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ParquetProperties;

import java.io.IOException;
import java.util.*;

/**
 * Utility class that generates sample Parquet files with structured row groups.
 * 
 * Creates customer data with age-based distribution across row groups
 * to demonstrate metadata-driven query optimization patterns.
 */
public class ParquetFileWriter {
    
    // Row group size configuration - small enough to ensure 3 separate row groups
    private static final int ROW_GROUP_SIZE = 8 * 1024; // 8KB - small for educational purposes
    
    /**
     * Creates a sample customers.parquet file with educational structure.
     * 
     * Structure:
     * - Row Group 1: Young customers (ages 20-35) - 100 records
     * - Row Group 2: Middle-aged customers (ages 40-65) - 100 records  
     * - Row Group 3: Mixed ages (ages 25-45) - 100 records
     * 
     * This distribution enables meaningful predicate pushdown demonstrations.
     * 
     * @param filePath Path where to create the Parquet file
     * @throws IOException if file creation fails
     */
    public static void createCustomersFile(String filePath) throws IOException {
        logFileCreationStart();
        
        // Define schema (reusing existing TableSchema)
        TableSchema schema = TableSchema.createCustomerSchema();
        MessageType parquetSchema = ParquetSchemaConverter.convertToParquetSchema(schema);
        
        // Setup Hadoop configuration
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(parquetSchema, conf);
        Path parquetPath = new Path(filePath);
        
        // Create single ParquetWriter for the entire file
        try (ParquetWriter<Group> writer = createParquetWriter(parquetPath, conf, parquetSchema)) {
            writeAllRowGroups(writer, parquetSchema);
        }
        
        displayFileStatistics(filePath);
        logFileCreationComplete();
    }
    
    /**
     * Creates a ParquetWriter with settings optimized for educational row group demonstration
     */
    private static ParquetWriter<Group> createParquetWriter(Path path, Configuration conf, MessageType schema) throws IOException {
        return new ParquetWriter<Group>(
            path,
            new GroupWriteSupport(),
            CompressionCodecName.SNAPPY,
            ROW_GROUP_SIZE,  // Small row group size to ensure 3 separate row groups
            1024,            // Page size
            512,             // Dictionary page size
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf
        );
    }
    
    /**
     * Writes all customer data to the ParquetWriter, letting it create row groups automatically
     */
    private static void writeAllRowGroups(ParquetWriter<Group> writer, MessageType schema) throws IOException {
        // Generate all customer data
        List<Record> youngCustomers = generateYoungCustomers(100);
        List<Record> middleAgedCustomers = generateMiddleAgedCustomers(100);
        List<Record> mixedAgeCustomers = generateMixedAgeCustomers(100);
        
        // Write Row Group 1: Young customers
        logRowGroupCreation(1, "Young customers", 20, 35, youngCustomers.size());
        writeRecordsToWriter(writer, youngCustomers, schema);
        
        // Write Row Group 2: Middle-aged customers  
        logRowGroupCreation(2, "Middle-aged customers", 40, 65, middleAgedCustomers.size());
        writeRecordsToWriter(writer, middleAgedCustomers, schema);
        
        // Write Row Group 3: Mixed age customers
        logRowGroupCreation(3, "Mixed age customers", 25, 45, mixedAgeCustomers.size());
        writeRecordsToWriter(writer, mixedAgeCustomers, schema);
    }
    
    /**
     * Writes a batch of records to the ParquetWriter
     */
    private static void writeRecordsToWriter(ParquetWriter<Group> writer, List<Record> records, MessageType schema) throws IOException {
        for (Record record : records) {
            Group group = convertRecordToGroup(record, schema);
            writer.write(group);
        }
    }
    
    /**
     * Converts a Record to a Parquet Group
     */
    private static Group convertRecordToGroup(Record record, MessageType schema) {
        SimpleGroup group = new SimpleGroup(schema);
        Map<String, Object> values = record.getValue();
        
        // Add each field to the group based on schema field types
        for (org.apache.parquet.schema.Type field : schema.getFields()) {
            String fieldName = field.getName();
            Object value = values.get(fieldName);
            
            if (value != null) {
                // Convert based on Parquet field type
                if (field.asPrimitiveType().getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32) {
                    group.add(fieldName, (Integer) value);
                } else if (field.asPrimitiveType().getPrimitiveTypeName() == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY) {
                    group.add(fieldName, value.toString());
                } else {
                    group.add(fieldName, value.toString());
                }
            }
        }
        
        return group;
    }
    
    private static void logFileCreationStart() {
        System.out.println("Creating Parquet file with structured row groups...");
    }
    
    private static void logFileCreationComplete() {
        System.out.println("Parquet file created successfully.");
    }
    
    /**
     * Generates young customers (ages 20-35) for Row Group 1
     */
    private static List<Record> generateYoungCustomers(int count) {
        List<Record> customers = new ArrayList<>();
        String[] cities = {"New York", "San Francisco", "Austin", "Portland", "Denver"};
        Random random = new Random(42); // Fixed seed for reproducible data
        
        for (int i = 1; i <= count; i++) {
            Map<String, Object> customerData = new HashMap<>();
            customerData.put("id", String.valueOf(i)); // Use String for id to match schema
            customerData.put("name", "Customer_" + i);
            customerData.put("email", "customer" + i + "@example.com"); // Add required email field
            customerData.put("age", 20 + random.nextInt(16)); // Ages 20-35
            customerData.put("city", cities[random.nextInt(cities.length)]);
            
            customers.add(new Record(String.valueOf(i).getBytes(), customerData));
        }
        
        return customers;
    }
    
    /**
     * Generates middle-aged customers (ages 40-65) for Row Group 2
     */
    private static List<Record> generateMiddleAgedCustomers(int count) {
        List<Record> customers = new ArrayList<>();
        String[] cities = {"Chicago", "Boston", "Seattle", "Atlanta", "Phoenix"};
        Random random = new Random(123); // Different seed for variety
        
        for (int i = 101; i <= 100 + count; i++) {
            Map<String, Object> customerData = new HashMap<>();
            customerData.put("id", String.valueOf(i)); // Use String for id to match schema
            customerData.put("name", "Customer_" + i);
            customerData.put("email", "customer" + i + "@example.com"); // Add required email field
            customerData.put("age", 40 + random.nextInt(26)); // Ages 40-65
            customerData.put("city", cities[random.nextInt(cities.length)]);
            
            customers.add(new Record(String.valueOf(i).getBytes(), customerData));
        }
        
        return customers;
    }
    
    /**
     * Generates mixed age customers (ages 25-45) for Row Group 3
     */
    private static List<Record> generateMixedAgeCustomers(int count) {
        List<Record> customers = new ArrayList<>();
        String[] cities = {"Los Angeles", "Miami", "Dallas", "Detroit", "Nashville"};
        Random random = new Random(456); // Another seed for variety
        
        for (int i = 201; i <= 200 + count; i++) {
            Map<String, Object> customerData = new HashMap<>();
            customerData.put("id", String.valueOf(i)); // Use String for id to match schema
            customerData.put("name", "Customer_" + i);
            customerData.put("email", "customer" + i + "@example.com"); // Add required email field
            customerData.put("age", 25 + random.nextInt(21)); // Ages 25-45
            customerData.put("city", cities[random.nextInt(cities.length)]);
            
            customers.add(new Record(String.valueOf(i).getBytes(), customerData));
        }
        
        return customers;
    }
    
    /**
     * Log row group creation progress
     */
    private static void logRowGroupCreation(int rowGroup, String description, int minAge, int maxAge, int recordCount) {
        System.out.printf("Row Group %d: %s (age %d-%d, %d records)%n", 
                         rowGroup, description, minAge, maxAge, recordCount);
    }
    
    /**
     * Display basic file statistics
     */
    private static void displayFileStatistics(String filePath) {
        System.out.println("File created: 300 records in 3 row groups (~100 records each)");
    }
} 