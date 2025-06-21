package minispark.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Workshop-ready unit tests for ParquetReader demonstrating distributed file processing concepts.
 * 
 * Learning Objectives:
 * 1. Understand how Parquet files are partitioned for distributed processing
 * 2. Learn row group-based partitioning strategies
 * 3. Validate data integrity across partitions
 * 4. Explore metadata extraction from Parquet files
 * 
 * Key Concepts Demonstrated:
 * - Partition creation based on row groups
 * - Data distribution without duplication or loss
 * - Metadata-driven optimization
 */
@DisplayName("ParquetReader Workshop Tests")
class ParquetReaderTest {
    
    // Test Configuration Constants
    private static final int SMALL_ROW_GROUP_SIZE = 512;  // Forces multiple row groups
    private static final int MEDIUM_ROW_GROUP_SIZE = 1024; // Balanced for testing
    private static final int DEFAULT_TARGET_PARTITIONS = 3;
    
    private ParquetReader parquetReader;
    private MessageType customerSchema;
    private TestCustomer[] testCustomers;
    
    @BeforeEach
    void setupTestEnvironment() {
        parquetReader = new ParquetReader();
        customerSchema = createCustomerSchema();
        testCustomers = createTestCustomerData();
    }
    
    /**
     * Workshop Test 1: Partition Creation
     * 
     * This test demonstrates how a Parquet file gets divided into partitions
     * for distributed processing. Each partition contains one or more row groups.
     * 
     * Key Learning Points:
     * - Row groups are the fundamental unit of Parquet partitioning
     * - Partitions balance data distribution across workers
     * - Metadata contains essential information for processing
     */
    @Test
    @DisplayName("Should create balanced partitions from Parquet row groups")
    void shouldCreateBalancedPartitionsFromRowGroups(@TempDir java.nio.file.Path tempDir) throws IOException {
        // Arrange: Create a Parquet file with multiple row groups
        String parquetFile = createTestParquetFile(tempDir, "workshop_customers.parquet", 
            testCustomers, SMALL_ROW_GROUP_SIZE);
        
        // Act: Create partitions for distributed processing
        FilePartition[] partitions = parquetReader.createPartitions(parquetFile, DEFAULT_TARGET_PARTITIONS).join();
        
        // Assert: Verify partition structure and metadata
        assertPartitionBasicProperties(partitions, parquetFile);
        assertPartitionMetadataIntegrity(partitions);
        assertDataDistributionBalance(partitions, testCustomers.length);
        
        logPartitionSummary(partitions);
    }
    
    /**
     * Workshop Test 2: Partition Reading
     * 
     * This test shows how individual partitions are read and processed.
     * It validates that data integrity is maintained across the distributed system.
     * 
     * Key Learning Points:
     * - Only assigned row groups are read (optimization)
     * - No data duplication between partitions
     * - All original data is preserved
     */
    @Test
    @DisplayName("Should read partition data without duplication or loss")
    void shouldReadPartitionDataWithoutDuplicationOrLoss(@TempDir java.nio.file.Path tempDir) throws IOException {
        // Arrange: Create test file and partitions
        String parquetFile = createTestParquetFile(tempDir, "read_test_customers.parquet", 
            testCustomers, MEDIUM_ROW_GROUP_SIZE);
        FilePartition[] partitions = parquetReader.createPartitions(parquetFile, 2).join();
        
        // Act: Read data from all partitions
        List<TestCustomer> allReadCustomers = readAllPartitions(parquetFile, partitions);
        
        // Assert: Verify data integrity
        assertDataCompleteness(allReadCustomers, testCustomers);
        assertNoDuplicates(allReadCustomers);
        assertDataAccuracy(allReadCustomers, testCustomers);
        
        logDataReadingSummary(partitions, allReadCustomers);
    }
    
    // ========================================
    // Helper Methods for Clean Test Structure
    // ========================================
    
    private MessageType createCustomerSchema() {
        return Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("name")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("email")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("city")
            .named("customer");
    }
    
    private TestCustomer[] createTestCustomerData() {
        return new TestCustomer[] {
            new TestCustomer(1, "Alice Johnson", "alice.johnson@email.com", 28, "New York"),
            new TestCustomer(2, "Bob Smith", "bob.smith@email.com", 35, "San Francisco"),
            new TestCustomer(3, "Carol Davis", "carol.davis@email.com", 42, "Chicago"),
            new TestCustomer(4, "David Wilson", "david.wilson@email.com", 31, "Boston"),
            new TestCustomer(5, "Eva Brown", "eva.brown@email.com", 27, "Seattle"),
            new TestCustomer(6, "Frank Miller", "frank.miller@email.com", 39, "Austin"),
            new TestCustomer(7, "Grace Lee", "grace.lee@email.com", 33, "Denver"),
            new TestCustomer(8, "Henry Taylor", "henry.taylor@email.com", 45, "Portland"),
            new TestCustomer(9, "Iris Chen", "iris.chen@email.com", 29, "Miami"),
            new TestCustomer(10, "Jack Anderson", "jack.anderson@email.com", 36, "Phoenix")
        };
    }
    
    private String createTestParquetFile(java.nio.file.Path tempDir, String filename, 
                                       TestCustomer[] customers, int rowGroupSize) throws IOException {
        File tempFile = tempDir.resolve(filename).toFile();
        Path parquetPath = new Path(tempFile.getAbsolutePath());
        
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(customerSchema, conf);
        
        try (ParquetWriter<Group> writer = createParquetWriter(parquetPath, conf, rowGroupSize)) {
            writeCustomersToParquet(writer, customers);
        }
        
        System.out.printf("📁 Created test file: %s (%d customers)%n", filename, customers.length);
        return tempFile.getAbsolutePath();
    }
    
    private ParquetWriter<Group> createParquetWriter(Path path, Configuration conf, int rowGroupSize) throws IOException {
        return new ParquetWriter<>(
            path,
            new GroupWriteSupport(),
            CompressionCodecName.SNAPPY,
            rowGroupSize,  // Block size controls row group size
            1024,          // Page size
            512,           // Dictionary page size
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf
        );
    }
    
    private void writeCustomersToParquet(ParquetWriter<Group> writer, TestCustomer[] customers) throws IOException {
        for (TestCustomer customer : customers) {
            SimpleGroup group = new SimpleGroup(customerSchema);
            group.add("id", customer.id);
            group.add("name", customer.name);
            group.add("email", customer.email);
            group.add("age", customer.age);
            group.add("city", customer.city);
            writer.write(group);
        }
    }
    
    // ========================================
    // Assertion Methods for Clear Validation
    // ========================================
    
    private void assertPartitionBasicProperties(FilePartition[] partitions, String expectedFilePath) {
        assertNotNull(partitions, "Partitions should not be null");
        assertTrue(partitions.length > 0, "Should create at least one partition");
        
        for (int i = 0; i < partitions.length; i++) {
            FilePartition partition = partitions[i];
            assertEquals(i, partition.index(), "Partition index should match array position");
            assertEquals(expectedFilePath, partition.getFilePath(), "All partitions should reference same file");
            assertTrue(partition.getLength() > 0, "Partition should have positive byte length");
        }
    }
    
    private void assertPartitionMetadataIntegrity(FilePartition[] partitions) {
        for (FilePartition partition : partitions) {
            @SuppressWarnings("unchecked")
            List<Integer> rowGroupIndices = partition.getMetadata("rowGroupIndices");
            Long totalRows = partition.getMetadata("totalRows");
            
            assertNotNull(rowGroupIndices, 
                "Partition should contain row group index information");
            assertNotNull(totalRows, 
                "Partition should contain total row count");
            assertTrue(rowGroupIndices.size() > 0, 
                "Partition should be assigned at least one row group");
            assertTrue(totalRows > 0, 
                "Partition should contain at least one row");
        }
    }
    
    private void assertDataDistributionBalance(FilePartition[] partitions, int expectedTotalRows) {
        int totalRowsAcrossPartitions = 0;
        for (FilePartition partition : partitions) {
            Long partitionRows = partition.getMetadata("totalRows");
            totalRowsAcrossPartitions += partitionRows.intValue();
        }
        
        assertEquals(expectedTotalRows, totalRowsAcrossPartitions,
            "Total rows across all partitions should equal original data");
    }
    
    private List<TestCustomer> readAllPartitions(String filePath, FilePartition[] partitions) {
        List<TestCustomer> allCustomers = new ArrayList<>();
        
        for (FilePartition partition : partitions) {
            Iterator<Group> iterator = parquetReader.readPartition(filePath, partition);
            List<TestCustomer> partitionCustomers = readCustomersFromIterator(iterator);
            allCustomers.addAll(partitionCustomers);
        }
        
        return allCustomers;
    }
    
    private List<TestCustomer> readCustomersFromIterator(Iterator<Group> iterator) {
        List<TestCustomer> customers = new ArrayList<>();
        while (iterator.hasNext()) {
            Group group = iterator.next();
            TestCustomer customer = TestCustomer.fromParquetGroup(group);
            customers.add(customer);
        }
        return customers;
    }
    
    private void assertDataCompleteness(List<TestCustomer> readCustomers, TestCustomer[] originalCustomers) {
        assertEquals(originalCustomers.length, readCustomers.size(),
            "Should read exactly the same number of customers as written");
    }
    
    private void assertNoDuplicates(List<TestCustomer> customers) {
        long uniqueIds = customers.stream().mapToInt(c -> c.id).distinct().count();
        assertEquals(customers.size(), uniqueIds,
            "No customer should appear multiple times across partitions");
    }
    
    private void assertDataAccuracy(List<TestCustomer> readCustomers, TestCustomer[] originalCustomers) {
        for (TestCustomer readCustomer : readCustomers) {
            boolean foundMatch = false;
            for (TestCustomer originalCustomer : originalCustomers) {
                if (originalCustomer.equals(readCustomer)) {
                    foundMatch = true;
                    break;
                }
            }
            assertTrue(foundMatch, 
                "Every read customer should match an original customer: " + readCustomer);
        }
    }
    
    // ========================================
    // Logging Methods for Workshop Visibility
    // ========================================
    
    private void logPartitionSummary(FilePartition[] partitions) {
        System.out.println("\n📊 Partition Creation Summary:");
        System.out.printf("   Total Partitions: %d%n", partitions.length);
        
        for (FilePartition partition : partitions) {
            @SuppressWarnings("unchecked")
            List<Integer> rowGroups = partition.getMetadata("rowGroupIndices");
            Long totalRows = partition.getMetadata("totalRows");
            
            System.out.printf("   Partition %d: %d row groups, %d rows%n", 
                partition.index(), rowGroups.size(), totalRows);
        }
        System.out.println("   ✅ All partitions created successfully");
    }
    
    private void logDataReadingSummary(FilePartition[] partitions, List<TestCustomer> allCustomers) {
        System.out.println("\n📖 Data Reading Summary:");
        System.out.printf("   Partitions Processed: %d%n", partitions.length);
        System.out.printf("   Total Customers Read: %d%n", allCustomers.size());
        System.out.printf("   Sample Customer: %s%n", 
            allCustomers.isEmpty() ? "None" : allCustomers.get(0));
        System.out.println("   ✅ Data integrity verified across all partitions");
    }
    
    // ========================================
    // Test Data Class - Clean and Simple
    // ========================================
    
    /**
     * Simple, immutable test data class representing a customer.
     * Demonstrates clean data modeling for workshop examples.
     */
    private static class TestCustomer {
        final int id;
        final String name;
        final String email;
        final int age;
        final String city;
        
        TestCustomer(int id, String name, String email, int age, String city) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.age = age;
            this.city = city;
        }
        
        static TestCustomer fromParquetGroup(Group group) {
            return new TestCustomer(
                group.getInteger("id", 0),
                group.getString("name", 0),
                group.getString("email", 0),
                group.getInteger("age", 0),
                group.getString("city", 0)
            );
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TestCustomer that = (TestCustomer) obj;
            return id == that.id &&
                   age == that.age &&
                   name.equals(that.name) &&
                   email.equals(that.email) &&
                   city.equals(that.city);
        }
        
        @Override
        public int hashCode() {
            return java.util.Objects.hash(id, name, email, age, city);
        }
        
        @Override
        public String toString() {
            return String.format("Customer{id=%d, name='%s', age=%d, city='%s'}", 
                id, name, age, city);
        }
    }
} 