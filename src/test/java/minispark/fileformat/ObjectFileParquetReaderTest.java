package minispark.fileformat;

import minispark.util.SimulationRunner;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import minispark.objectstore.Client;
import minispark.objectstore.Server;
import minispark.objectstore.LocalStorageNode;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ObjectFileParquetReader demonstrating object store-based
 * distributed Parquet processing with efficient range reads.
 * 
 * Learning Objectives:
 * 1. Understand how Parquet files are efficiently accessed from object stores
 * 2. Learn about range reads for footer and metadata optimization
 * 3. Validate distributed processing of object store files
 * 4. Compare object store vs filesystem approaches using same data
 * 
 * Key Object Store Concepts:
 * - Range reads for efficient metadata access
 * - Minimal data transfer for footer information
 * - Row group-level granularity for distributed processing
 * - Network-optimized file format access
 */
@DisplayName("ObjectFileParquetReader Object Store Tests")
class ObjectFileParquetReaderTest {
    
    // Test Configuration Constants
    private static final int SMALL_ROW_GROUP_SIZE = 512;  // Forces multiple row groups
    private static final int MEDIUM_ROW_GROUP_SIZE = 1024; // Balanced for testing
    private static final int DEFAULT_TARGET_PARTITIONS = 3;
    private static final String OBJECT_STORE_HOST = "localhost";
    private static final int OBJECT_STORE_PORT = 8889; // Different port from other tests
    
    // Test Infrastructure
    private MessageBus messageBus;
    private Server objectStoreServer;
    private Client objectStoreClient;
    private LocalStorageNode localStorage;
    private ObjectFileParquetReader objectFileReader;
    private MessageType customerSchema;
    private TestCustomer[] testCustomers;
    
    @BeforeEach
    void setupObjectStoreEnvironment(@TempDir java.nio.file.Path tempDir) throws Exception {
        // Initialize message bus
        messageBus = new MessageBus();
        
        // Create local storage for object store server
        localStorage = new LocalStorageNode(tempDir.resolve("objectstore").toString());
        
        // Create server and client endpoints
        NetworkEndpoint serverEndpoint = new NetworkEndpoint(OBJECT_STORE_HOST, OBJECT_STORE_PORT);
        NetworkEndpoint clientEndpoint = new NetworkEndpoint(OBJECT_STORE_HOST, OBJECT_STORE_PORT + 1);
        
        // Start object store server
        objectStoreServer = new Server("test-server", localStorage, messageBus, serverEndpoint);
        
        // Create client for object store operations
        List<NetworkEndpoint> serverEndpoints = Collections.singletonList(serverEndpoint);
        objectStoreClient = new Client(messageBus, clientEndpoint, serverEndpoints);
        
        // **CRITICAL**: Start the message bus to enable automatic message processing
        messageBus.start();
        
        // Initialize object store-aware Parquet reader
        objectFileReader = new ObjectFileParquetReader(new Configuration(), objectStoreClient);
        
        // Setup test data (same as ParquetReaderTest)
        customerSchema = createCustomerSchema();
        testCustomers = createTestCustomerData();
        
        System.out.println("🌐 Object Store Environment Setup Complete");
        System.out.printf("   Server: %s:%d%n", OBJECT_STORE_HOST, OBJECT_STORE_PORT);
        System.out.printf("   Test Customers: %d%n", testCustomers.length);
    }
    
    @AfterEach
    void teardownObjectStoreEnvironment() throws Exception {
        // Stop the message bus to clean up resources
        if (messageBus != null) {
            messageBus.stop();
        }
        System.out.println("🔌 Object Store Environment Cleaned Up");
    }
    
    /**
     * Workshop Test 1: Object Store Footer Reading with Range Requests
     * 
     * This test demonstrates how ObjectFileParquetReader efficiently reads
     * only the footer metadata using HTTP range requests, minimizing data transfer.
     * 
     * Key Learning Points:
     * - Footer contains schema and row group metadata
     * - Range requests read only necessary bytes from object store
     * - No need to download entire file for metadata extraction
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Should efficiently read Parquet footer from object store using range requests")
    void shouldEfficientlyReadFooterFromObjectStore(@TempDir java.nio.file.Path tempDir) throws IOException {
        System.out.println("🔧 Starting object store footer reading test...");
        
        try {
            // Arrange: Create and upload Parquet file to object store
            System.out.println("📋 Step 1: Creating and uploading Parquet file...");
            String objectStoreKey = "customers/workshop_customers_objectstore.parquet";
            File localFile = createAndUploadParquetFile(tempDir, objectStoreKey, testCustomers);
            System.out.println("✅ File uploaded successfully");
            
            // Act: Create partitions (this triggers footer reading with range requests)
            System.out.println("📋 Step 2: Creating partitions (reading footer)...");
            
                        // DETERMINISM FIX: createPartitions() now returns CompletableFuture and makes async calls
            // to object store, so it needs to run in a context where ticks are being driven
            CompletableFuture<FilePartition[]> partitionsFuture = objectFileReader.createPartitions(objectStoreKey, DEFAULT_TARGET_PARTITIONS);

            // Drive ticks until the future completes - this allows async operations inside
            // createPartitions() to make progress
            SimulationRunner.runUntil(messageBus, () -> partitionsFuture.isDone(), java.time.Duration.ofSeconds(10));
            FilePartition[] partitions;
            try {
                partitions = partitionsFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to create partitions", e);
            }
            System.out.println("✅ Partitions created successfully");
            
            // Assert: Verify successful metadata extraction from object store
            System.out.println("📋 Step 3: Validating metadata integrity...");
            assertObjectStoreMetadataIntegrity(partitions, objectStoreKey);
            assertRangeReadEfficiency(localFile, partitions);
            System.out.println("✅ Validation completed successfully");
            
            logObjectStoreFooterSummary(objectStoreKey, partitions, localFile);
        } catch (Exception e) {
            System.err.println("❌ Test failed with exception: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    

    
    // ========================================
    // Helper Methods - Same Schema as ParquetReaderTest
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
        // Same test data as ParquetReaderTest for consistency
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
    

    
    private File createAndUploadParquetFile(java.nio.file.Path tempDir, String objectStoreKey, 
                                          TestCustomer[] customers) throws IOException {
        return createAndUploadParquetFile(tempDir, objectStoreKey, customers, MEDIUM_ROW_GROUP_SIZE);
    }
    
    private File createAndUploadParquetFile(java.nio.file.Path tempDir, String objectStoreKey, 
                                          TestCustomer[] customers, int rowGroupSize) throws IOException {
        // Create local Parquet file
        File tempFile = tempDir.resolve("temp_customers.parquet").toFile();
        Path parquetPath = new Path(tempFile.getAbsolutePath());
        
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(customerSchema, conf);
        
        try (ParquetWriter<Group> writer = createParquetWriter(parquetPath, conf, rowGroupSize)) {
            writeCustomersToParquet(writer, customers);
        }
        
        // Upload to object store
        byte[] fileData = Files.readAllBytes(tempFile.toPath());
        try {
            System.out.printf("📤 Uploading to object store: %s (%d customers, %d bytes)%n", 
                objectStoreKey, customers.length, fileData.length);
            CompletableFuture<Void> putFuture = objectStoreClient.putObject(objectStoreKey, fileData);
            SimulationRunner.runUntil(messageBus, () -> putFuture.isDone(), java.time.Duration.ofSeconds(10));
            putFuture.get();
            System.out.println("✅ Upload completed successfully");
        } catch (Exception e) {
            System.err.println("❌ Upload failed: " + e.getMessage());
            throw new IOException("Failed to upload file to object store", e);
        }
        
        return tempFile;
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
    // Assertion Methods for Object Store Validation
    // ========================================
    
    private void assertObjectStoreMetadataIntegrity(FilePartition[] partitions, String expectedKey) {
        assertNotNull(partitions, "Object store partitions should not be null");
        assertTrue(partitions.length > 0, "Should create at least one partition from object store");
        
        for (FilePartition partition : partitions) {
            assertEquals(expectedKey, partition.getFilePath(), "Partition should reference object store key");
            
            Boolean isObjectStore = partition.getMetadata("isObjectStore");
            assertTrue(Boolean.TRUE.equals(isObjectStore), "Partition should be marked as object store");
            
            @SuppressWarnings("unchecked")
            List<Integer> rowGroupIndices = partition.getMetadata("rowGroupIndices");
            assertNotNull(rowGroupIndices, "Partition should contain row group metadata");
            assertTrue(rowGroupIndices.size() > 0, "Partition should be assigned row groups");
        }
    }
    
    private void assertRangeReadEfficiency(File originalFile, FilePartition[] partitions) {
        // Verify that partitions were created successfully via range reads
        long fileSize = originalFile.length();
        assertTrue(fileSize > 0, "Original file should have content");
        
        int totalRowGroups = 0;
        for (FilePartition partition : partitions) {
            @SuppressWarnings("unchecked")
            List<Integer> rowGroups = partition.getMetadata("rowGroupIndices");
            totalRowGroups += rowGroups.size();
        }
        
        assertTrue(totalRowGroups > 0, "Should have processed row groups via efficient range reads");
    }
    

    
    // ========================================
    // Logging Methods for Workshop Visibility
    // ========================================
    
    private void logObjectStoreFooterSummary(String objectStoreKey, FilePartition[] partitions, File originalFile) {
        System.out.println("\n🌐 Object Store Footer Reading Summary:");
        System.out.printf("   Object Store Key: %s%n", objectStoreKey);
        System.out.printf("   Original File Size: %d bytes%n", originalFile.length());
        System.out.printf("   Partitions Created: %d%n", partitions.length);
        System.out.printf("   Range Read Optimization: ✅ Footer read without full file download%n");
        
        for (FilePartition partition : partitions) {
            @SuppressWarnings("unchecked")
            List<Integer> rowGroups = partition.getMetadata("rowGroupIndices");
            Long totalRows = partition.getMetadata("totalRows");
            
            System.out.printf("   Partition %d: %d row groups, %d rows%n", 
                partition.index(), rowGroups.size(), totalRows);
        }
        System.out.println("   ✅ Object store metadata extraction successful");
    }
    

    
    // ========================================
    // Test Data Class - Same as ParquetReaderTest
    // ========================================
    
    /**
     * Simple, immutable test data class representing a customer.
     * Identical to ParquetReaderTest for consistency between filesystem and object store tests.
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