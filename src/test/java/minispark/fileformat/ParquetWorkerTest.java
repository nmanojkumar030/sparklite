package minispark.fileformat;

import minispark.core.MiniRDD;
import minispark.core.Partition;
import minispark.network.MessageBus;
import minispark.objectstore.SparkCluster;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end test for distributed Parquet processing using FileFormatRDD.
 * This test demonstrates how to:
 * 1. Create a Parquet file with multiple row groups
 * 2. Use FileFormatRDD with ParquetReader to create partitions based on row groups
 * 3. Process the data across multiple workers
 * 4. Collect and verify results
 */
class ParquetWorkerTest {
    private static final Logger logger = LoggerFactory.getLogger(ParquetWorkerTest.class);
    
    @TempDir
    java.nio.file.Path tempDir;
    
    private MessageBus messageBus;
    private SparkCluster sparkCluster;
    private String parquetFilePath;
    
    @BeforeEach
    void setUp() throws IOException {
        // Initialize message bus and Spark cluster
        messageBus = new MessageBus();
        sparkCluster = new SparkCluster(messageBus, 3); // 3 workers for processing
        
        // Start clusters
        sparkCluster.start();
        
        // Create test Parquet file
        parquetFilePath = createTestParquetFile();
    }
    
    @AfterEach
    void tearDown() {
        sparkCluster.stop();
    }
    
    @Test
    void testEndToEndParquetProcessingWithMultipleRowGroups() throws Exception {
        System.out.println("\n=== Parquet Distributed Processing Test ===");
        
        // Create FileFormatRDD with ParquetReader
        ParquetReader parquetReader = new ParquetReader();
        FileFormatRDD<Group> parquetRDD = new FileFormatRDD<>(
            sparkCluster.getSparkContext(),
            parquetFilePath,
            parquetReader,
            4 // Target 4 partitions
        );
        
        // Create a processing RDD that extracts customer data
        CustomerProcessingRDD customerRdd = new CustomerProcessingRDD(parquetRDD);
        
        System.out.println("Parquet file: " + parquetFilePath);
        System.out.println("Number of partitions: " + customerRdd.getPartitions().length);
        System.out.println("Number of workers: " + sparkCluster.getWorkers().size());
        
        // Use DAGScheduler to submit the job
        System.out.println("\nSubmitting job through DAGScheduler:");
        List<CompletableFuture<CustomerData>> futures = sparkCluster.getDagScheduler()
            .submitJob(customerRdd, customerRdd.getPartitions().length);
        
        // Wait for all tasks to complete
        List<CustomerData> processedCustomers = new ArrayList<>();
        for (CompletableFuture<CustomerData> future : futures) {
            try {
                SimulationRunner.runUntil(messageBus, () -> future.isDone(), java.time.Duration.ofSeconds(30));
                CustomerData result = future.get();
                if (result != null) {
                    processedCustomers.add(result);
                    System.out.println("Received customer: " + result.getName() + " (ID: " + result.getId() + ")");
                }
            } catch (Exception e) {
                System.err.println("Task failed: " + e.getMessage());
            }
        }
        
        // Verify results
        System.out.println("\nExecution Summary:");
        System.out.printf("Processed customers: %d%n", processedCustomers.size());
        System.out.printf("Expected customers: %d%n", 1000); // We created 1000 customers
        
        // Verify we got some results (since RDDTask only returns first element per partition)
        assertTrue(processedCustomers.size() > 0, "Should have received at least one customer");
        assertTrue(processedCustomers.size() <= customerRdd.getPartitions().length, 
            "Should not receive more customers than partitions");
        
        // Verify data integrity
        for (CustomerData customer : processedCustomers) {
            assertNotNull(customer.getId());
            assertNotNull(customer.getName());
            assertNotNull(customer.getEmail());
            assertTrue(customer.getAge() > 0);
            assertNotNull(customer.getCity());
        }
        
        System.out.println("✅ Test completed successfully!");
    }
    
    /**
     * Creates a test Parquet file with multiple row groups for distributed processing.
     */
    private String createTestParquetFile() throws IOException {
        System.out.println("Creating test Parquet file with multiple row groups...");
        
        // Define schema
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("name")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("email")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("city")
            .named("customer");
        
        // Create temporary file
        File tempFile = tempDir.resolve("customers.parquet").toFile();
        Path parquetPath = new Path(tempFile.getAbsolutePath());
        
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        
        // Create writer with small row group size to ensure multiple row groups
        ParquetWriter<Group> writer = new ParquetWriter<>(
            parquetPath,
            new GroupWriteSupport(),
            CompressionCodecName.SNAPPY,
            64 * 1024, // 64KB block size (small for testing)
            8 * 1024,  // 8KB page size
            4 * 1024,  // 4KB page size
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf
        );
        
        // Write 1000 customer records (should create multiple row groups)
        Random random = new Random(42); // Fixed seed for reproducible tests
        String[] cities = {"New York", "San Francisco", "Chicago", "Boston", "Seattle", 
                          "Los Angeles", "Miami", "Dallas", "Houston", "Phoenix"};
        
        for (int i = 1; i <= 1000; i++) {
            SimpleGroup group = new SimpleGroup(schema);
            group.add("id", i);
            group.add("name", "Customer " + i);
            group.add("email", "customer" + i + "@example.com");
            group.add("age", 20 + random.nextInt(50)); // Age between 20-70
            group.add("city", cities[random.nextInt(cities.length)]);
            
            writer.write(group);
        }
        
        writer.close();
        
        System.out.println("Created Parquet file: " + tempFile.getAbsolutePath());
        System.out.println("File size: " + tempFile.length() + " bytes");
        
        return tempFile.getAbsolutePath();
    }
    
    /**
     * Simple data class for customer information.
     */
    public static class CustomerData {
        private final int id;
        private final String name;
        private final String email;
        private final int age;
        private final String city;
        
        public CustomerData(int id, String name, String email, int age, String city) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.age = age;
            this.city = city;
        }
        
        public int getId() { return id; }
        public String getName() { return name; }
        public String getEmail() { return email; }
        public int getAge() { return age; }
        public String getCity() { return city; }
        
        @Override
        public String toString() {
            return String.format("Customer{id=%d, name='%s', email='%s', age=%d, city='%s'}", 
                id, name, email, age, city);
        }
    }
    
    /**
     * MiniRDD implementation that processes Parquet Group data into CustomerData objects.
     */
    private static class CustomerProcessingRDD implements MiniRDD<CustomerData> {
        private final FileFormatRDD<Group> parent;
        private static final Logger logger = LoggerFactory.getLogger(CustomerProcessingRDD.class);
        
        CustomerProcessingRDD(FileFormatRDD<Group> parent) {
            this.parent = parent;
        }
        
        @Override
        public Partition[] getPartitions() {
            return parent.getPartitions();
        }
        
        @Override
        public CompletableFuture<Iterator<CustomerData>> compute(Partition split) {
            logger.debug("Processing Parquet partition {}", split.index());
            
            return parent.compute(split).thenApply(iter -> {
                List<CustomerData> customers = new ArrayList<>();
                
                while (iter.hasNext()) {
                    Group group = iter.next();
                    try {
                        CustomerData customer = new CustomerData(
                            group.getInteger("id", 0),
                            group.getString("name", 0),
                            group.getString("email", 0),
                            group.getInteger("age", 0),
                            group.getString("city", 0)
                        );
                        customers.add(customer);
                        
                        logger.debug("Processed customer {} in partition {}", 
                            customer.getId(), split.index());
                    } catch (Exception e) {
                        logger.warn("Failed to process record in partition {}: {}", 
                            split.index(), e.getMessage());
                    }
                }
                
                logger.debug("Partition {} processed {} customers", 
                    split.index(), customers.size());
                
                return customers.iterator();
            });
        }
        
        @Override
        public List<MiniRDD<?>> getDependencies() {
            return Collections.singletonList(parent);
        }
        
        @Override
        public List<String> getPreferredLocations(Partition split) {
            return parent.getPreferredLocations(split);
        }
        
        @Override
        public <R> MiniRDD<R> map(Function<CustomerData, R> f) {
            throw new UnsupportedOperationException("map not implemented");
        }
        
        @Override
        public MiniRDD<CustomerData> filter(Predicate<CustomerData> f) {
            throw new UnsupportedOperationException("filter not implemented");
        }
        
        @Override
        public CompletableFuture<List<CustomerData>> collect() {
            List<CompletableFuture<Iterator<CustomerData>>> futures = new ArrayList<>();
            
            for (Partition partition : getPartitions()) {
                futures.add(compute(partition));
            }
            
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<CustomerData> result = new ArrayList<>();
                    for (CompletableFuture<Iterator<CustomerData>> future : futures) {
                        try {
                            Iterator<CustomerData> iter = future.get();
                            while (iter.hasNext()) {
                                result.add(iter.next());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to get partition result", e);
                        }
                    }
                    return result;
                });
        }
    }
} 