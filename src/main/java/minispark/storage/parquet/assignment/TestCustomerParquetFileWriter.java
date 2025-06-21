package minispark.storage.parquet.assignment;

import minispark.storage.Record;
import minispark.storage.table.TableSchema;
import minispark.storage.parquet.ParquetOperations;
import minispark.storage.parquet.ParquetSchemaConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.*;

/**
 * Utility that generates sample Parquet files with structured row groups.
 * <p>
 * Creates customer data with age-based distribution across row groups
 * to demonstrate metadata-driven query optimization patterns.
 * <p>
 * This class reuses the production ParquetOperations for writing logic,
 * but configures small row group sizes for educational purposes.
 */
public class TestCustomerParquetFileWriter {

    // Very small row group size for educational purposes (forces multiple row groups)
    private static final int ROW_GROUP_SIZE = 2 * 1024; // 2KB

    /**
     * Creates a sample customers.parquet file with educational structure.
     * <p>
     * Structure:
     * - Row Group 1: Young customers (ages 20-30) - 100 records
     * - Row Group 2: Middle-aged customers (ages 40-50) - 100 records
     * - Row Group 3: Senior customers (ages 60-70) - 100 records
     * <p>
     * This distribution enables meaningful predicate pushdown demonstrations.
     * For example, filter "age > 35" should skip Row Group 1 entirely.
     *
     * @param filePath Path where to create the Parquet file
     * @throws IOException if file creation fails
     */
    public static void createCustomersFile(String filePath) throws IOException {
        logFileCreationStart();

        // Reuse existing schema and operations
        TableSchema schema = TableSchema.createCustomerSchema();
        ParquetOperations operations = new ParquetOperations(schema);
        MessageType parquetSchema = ParquetSchemaConverter.convertToParquetSchema(schema);

        // Setup Hadoop configuration
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(parquetSchema, conf);
        Path parquetPath = new Path(filePath);

        // Use ParquetOperations to create writer with educational row group size
        // The writer will automatically create row groups when size threshold is reached
        // (in this case, 8KB), resulting in multiple row groups.
        //The close method will automatically flush and close the writer.
        //When the writer is closed, the Parquet file will be created.
        try (ParquetWriter<Group> writer = operations.createParquetWriter(
                parquetPath, conf, parquetSchema, ROW_GROUP_SIZE)) {
            writeAllRowGroups(writer, operations, parquetSchema);
        }

        displayFileStatistics(filePath);
        logFileCreationComplete();
    }

    /**
     * Writes all customer data to the ParquetWriter, letting it create row groups automatically
     */
    private static void writeAllRowGroups(ParquetWriter<Group> writer, ParquetOperations operations, MessageType schema) throws IOException {
        // Generate all customer data with distinct age ranges
        List<Record> youngCustomers = generateYoungCustomers(100);
        List<Record> middleAgedCustomers = generateMiddleAgedCustomers(100);
        List<Record> seniorCustomers = generateSeniorCustomers(100);

        // Write Row Group 1: Young customers (ages 20-30)
        logRowGroupCreation(1, "Young customers", 20, 30, youngCustomers.size());
        writeRecordsToWriter(writer, operations, youngCustomers, schema);

        // Write Row Group 2: Middle-aged customers (ages 40-50)
        logRowGroupCreation(2, "Middle-aged customers", 40, 50, middleAgedCustomers.size());
        writeRecordsToWriter(writer, operations, middleAgedCustomers, schema);

        // Write Row Group 3: Senior customers (ages 60-70)
        logRowGroupCreation(3, "Senior customers", 60, 70, seniorCustomers.size());
        writeRecordsToWriter(writer, operations, seniorCustomers, schema);
    }

    /**
     * Writes a batch of records to the ParquetWriter using shared operations
     */
    private static void writeRecordsToWriter(ParquetWriter<Group> writer, ParquetOperations operations,
                                             List<Record> records, MessageType schema) throws IOException {
        for (Record record : records) {
            // Reuse the production record-to-group conversion logic
            Group group = operations.convertRecordToGroup(record, schema);
            writer.write(group);
        }
    }

    private static void logFileCreationStart() {
        System.out.println("Creating Parquet file with structured row groups...");
    }

    private static void logFileCreationComplete() {
        System.out.println("Parquet file created successfully.");
    }

    /**
     * Generates young customers (ages 20-30) for Row Group 1
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
            customerData.put("age", 20 + random.nextInt(11)); // Ages 20-30
            customerData.put("city", cities[random.nextInt(cities.length)]);

            customers.add(new Record(String.valueOf(i).getBytes(), customerData));
        }

        return customers;
    }

    /**
     * Generates middle-aged customers (ages 40-50) for Row Group 2
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
            customerData.put("age", 40 + random.nextInt(11)); // Ages 40-50
            customerData.put("city", cities[random.nextInt(cities.length)]);

            customers.add(new Record(String.valueOf(i).getBytes(), customerData));
        }

        return customers;
    }

    /**
     * Generates senior customers (ages 60-70) for Row Group 3
     */
    private static List<Record> generateSeniorCustomers(int count) {
        List<Record> customers = new ArrayList<>();
        String[] cities = {"Los Angeles", "Miami", "Dallas", "Detroit", "Nashville"};
        Random random = new Random(456); // Another seed for variety

        for (int i = 201; i <= 200 + count; i++) {
            Map<String, Object> customerData = new HashMap<>();
            customerData.put("id", String.valueOf(i)); // Use String for id to match schema
            customerData.put("name", "Customer_" + i);
            customerData.put("email", "customer" + i + "@example.com"); // Add required email field
            customerData.put("age", 60 + random.nextInt(11)); // Ages 60-70
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
        System.out.println("\n===== Parquet File Statistics =====");
        System.out.println("File path: " + filePath);

        try {
            // Use Hadoop configuration and Path
            Configuration conf = new Configuration();
            Path path = new Path(filePath);

            // Read the footer to get file metadata
            org.apache.parquet.hadoop.ParquetFileReader reader =
                    org.apache.parquet.hadoop.ParquetFileReader.open(conf, path);
            org.apache.parquet.hadoop.metadata.ParquetMetadata metadata = reader.getFooter();

            // Get file schema
            MessageType schema = metadata.getFileMetaData().getSchema();
            System.out.println("Schema: " + schema.toString());

            // Get row groups information
            List<org.apache.parquet.hadoop.metadata.BlockMetaData> blocks = metadata.getBlocks();
            System.out.println("Number of row groups: " + blocks.size());

            // Calculate total rows
            long totalRows = 0;
            for (org.apache.parquet.hadoop.metadata.BlockMetaData block : blocks) {
                totalRows += block.getRowCount();
            }
            System.out.println("Total rows: " + totalRows);

            // Print row group details
            System.out.println("\n----- Row Group Details -----");
            for (int i = 0; i < blocks.size(); i++) {
                org.apache.parquet.hadoop.metadata.BlockMetaData block = blocks.get(i);
                System.out.printf("Row Group %d:%n", i + 1);
                System.out.printf("  Rows: %d%n", block.getRowCount());
                System.out.printf("  Size: %d bytes%n", block.getTotalByteSize());

                // Print column chunk details
                System.out.println("  Column chunks:");
                List<org.apache.parquet.hadoop.metadata.ColumnChunkMetaData> columns = block.getColumns();
                for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column : columns) {
                    System.out.printf("    %s: %d bytes, %d values%n",
                            column.getPath().toDotString(),
                            column.getTotalSize(),
                            column.getValueCount());

                    // Print min/max statistics if available
                    if (column.getStatistics() != null) {
                        System.out.printf("      Min: %s, Max: %s, Null count: %d%n",
                                column.getStatistics().minAsString(),
                                column.getStatistics().maxAsString(),
                                column.getStatistics().getNumNulls());
                    }
                }
                System.out.println();
            }

            // Print file size
            java.io.File file = new java.io.File(filePath);
            System.out.printf("Total file size: %d bytes%n", file.length());

            // Close the reader
            reader.close();

        } catch (IOException e) {
            System.err.println("Error reading Parquet file statistics: " + e.getMessage());
        }

        System.out.println("===================================\n");
    }
} 