package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.apache.hadoop.conf.Configuration;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    private List<LocalStorageNode> storageNodes;
    private MessageBus messageBus;
    private List<Server> servers;
    private Client client;
    private List<NetworkEndpoint> serverEndpoints;
    private NetworkEndpoint clientEndpoint;
    
    @BeforeEach
    void setUp() throws IOException {
        setupStorageNodes();
        setupMessageBus();
        setupServers();
        setupClient();
        startMessageBus();
    }
    
    private void setupStorageNodes() throws IOException {
        storageNodes = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            java.nio.file.Path storagePath = tempDir.resolve("storage" + i);
            storageNodes.add(new LocalStorageNode(storagePath.toString()));
        }
    }
    
    private void setupMessageBus() {
        messageBus = new MessageBus();
        serverEndpoints = Arrays.asList(
            new NetworkEndpoint("localhost", 8081),
            new NetworkEndpoint("localhost", 8082),
            new NetworkEndpoint("localhost", 8083)
        );
        clientEndpoint = new NetworkEndpoint("localhost", 8080);
    }
    
    private void setupServers() {
        servers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Server server = new Server("server" + i, storageNodes.get(i), messageBus, serverEndpoints.get(i));
            servers.add(server);
            messageBus.registerHandler(serverEndpoints.get(i), server);
        }
    }
    
    private void setupClient() {
        client = new Client(messageBus, clientEndpoint, serverEndpoints);
        messageBus.registerHandler(clientEndpoint, client);
    }
    
    private void startMessageBus() {
        messageBus.start();
    }
    
    @Test
    void shouldDistributeCustomersAcrossServers() throws IOException {
        // Setup
        MessageType schema = createCustomerSchema();
        List<Map<String, Object>> customers = createSampleCustomers();
        
        // Execute
        for (Map<String, Object> customer : customers) {
            String filePath = createCustomerPath(customer);
            writeCustomerToParquet(schema, customer, filePath);
            
            // Verify the customer was stored on the correct server
            verifyCustomerStorage(customer, filePath);
        }
    }
    
    private MessageType createCustomerSchema() {
        return Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("name")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("email")
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("city")
            .named("customer");
    }
    
    private List<Map<String, Object>> createSampleCustomers() {
        List<Map<String, Object>> customers = new ArrayList<>();
        customers.add(createCustomer(1, "John Doe", "john@example.com", 30, "New York"));
        customers.add(createCustomer(2, "Jane Smith", "jane@example.com", 25, "San Francisco"));
        customers.add(createCustomer(3, "Bob Johnson", "bob@example.com", 35, "Chicago"));
        customers.add(createCustomer(4, "Alice Brown", "alice@example.com", 28, "Boston"));
        customers.add(createCustomer(5, "Charlie Wilson", "charlie@example.com", 32, "Seattle"));
        customers.add(createCustomer(6, "Diana Prince", "diana@example.com", 27, "Washington DC"));
        customers.add(createCustomer(7, "Edward Stark", "edward@example.com", 33, "Los Angeles"));
        customers.add(createCustomer(8, "Fiona Green", "fiona@example.com", 29, "Miami"));
        customers.add(createCustomer(9, "George White", "george@example.com", 31, "Houston"));
        customers.add(createCustomer(10, "Helen Black", "helen@example.com", 26, "Dallas"));
        return customers;
    }
    
    private Map<String, Object> createCustomer(int id, String name, String email, int age, String city) {
        return Map.of(
            "id", id,
            "name", name,
            "email", email,
            "age", age,
            "city", city
        );
    }
    
    private String createCustomerPath(Map<String, Object> customer) {
        try {
            String encodedCity = java.net.URLEncoder.encode((String) customer.get("city"), "UTF-8");
            return String.format("customers/%s/data_%d.parquet", encodedCity, (Integer) customer.get("id"));
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to encode city name", e);
        }
    }
    
    private void writeCustomerToParquet(MessageType schema, Map<String, Object> customer, String filePath) throws IOException {
        // Create a temporary file to write the Parquet data
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "parquet_temp");
        tempDir.mkdirs();
        File tempFile = new File(tempDir, String.format("temp_%d.parquet", System.currentTimeMillis()));
        tempFile.deleteOnExit();
        
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(tempFile.getAbsolutePath());
        
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = createParquetWriter(parquetPath, conf);
        
        SimpleGroup group = createCustomerGroup(schema, customer);
        writer.write(group);
        writer.close();
        
        // Read the Parquet file and store it using the client
        byte[] parquetData = java.nio.file.Files.readAllBytes(tempFile.toPath());
        
        // Create parent directories in the object store
        String parentPath = filePath.substring(0, filePath.lastIndexOf('/'));
        CompletableFuture<Void> parentFuture = client.putObject(parentPath + "/.keep", new byte[0]);
        
        // Use tick progression instead of blocking join()
        minispark.util.TestUtils.runUntil(messageBus,
            () -> parentFuture.isDone(),
            java.time.Duration.ofSeconds(5));
        
        // Store the actual file
        CompletableFuture<Void> fileFuture = client.putObject(filePath, parquetData);
        
        // Use tick progression instead of blocking join()
        minispark.util.TestUtils.runUntil(messageBus,
            () -> fileFuture.isDone(),
            java.time.Duration.ofSeconds(5));
        
        // Clean up
        tempFile.delete();
        tempDir.delete();
    }
    
    private ParquetWriter<Group> createParquetWriter(org.apache.hadoop.fs.Path parquetPath, Configuration conf) throws IOException {
        return new ParquetWriter<>(
            parquetPath,
            new GroupWriteSupport(),
            CompressionCodecName.SNAPPY,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf
        );
    }
    
    private SimpleGroup createCustomerGroup(MessageType schema, Map<String, Object> customer) {
        SimpleGroup group = new SimpleGroup(schema);
        group.add("id", (Integer) customer.get("id"));
        group.add("name", (String) customer.get("name"));
        group.add("email", (String) customer.get("email"));
        group.add("age", (Integer) customer.get("age"));
        group.add("city", (String) customer.get("city"));
        return group;
    }
    
    private void verifyCustomerStorage(Map<String, Object> customer, String filePath) {
        // Verify the customer data exists
        CompletableFuture<byte[]> futureData = client.getObject(filePath);
        
        // Use tick progression instead of blocking join()
        minispark.util.TestUtils.runUntil(messageBus,
            () -> futureData.isDone(),
            java.time.Duration.ofSeconds(5));
        
        byte[] data = futureData.join(); // Safe to join() after isDone() is true
        assertNotNull(data, "Customer data should exist");
        assertTrue(data.length > 0, "Customer data should not be empty");
        
        // Log the customer ID
        System.out.printf("Customer %d data verified%n", (Integer) customer.get("id"));
    }
} 