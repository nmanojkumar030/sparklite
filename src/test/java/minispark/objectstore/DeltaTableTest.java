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

import static org.junit.jupiter.api.Assertions.*;

class DeltaTableTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    private LocalStorageNode storageNode;
    private MessageBus messageBus;
    private Server server;
    private Client client;
    private NetworkEndpoint serverEndpoint;
    private NetworkEndpoint clientEndpoint;
    
    @BeforeEach
    void setUp() throws IOException {
        setupStorageNode();
        setupMessageBus();
        setupServer();
        setupClient();
        startMessageBus();
    }
    
    private void setupStorageNode() throws IOException {
        java.nio.file.Path storagePath = tempDir.resolve("storage");
        storageNode = new LocalStorageNode(storagePath.toString());
    }
    
    private void setupMessageBus() {
        messageBus = new MessageBus();
        serverEndpoint = new NetworkEndpoint("localhost", 8081);
        clientEndpoint = new NetworkEndpoint("localhost", 8080);
    }
    
    private void setupServer() {
        server = new Server(storageNode, messageBus, serverEndpoint);
    }
    
    private void setupClient() {
        client = new Client(messageBus, clientEndpoint, Arrays.asList(serverEndpoint));
        messageBus.registerHandler(clientEndpoint, client);
    }
    
    private void startMessageBus() {
        messageBus.start();
    }
    
    @Test
    void shouldCreateAndReadParquetFile() throws IOException {
        // Setup
        MessageType schema = createCustomerSchema();
        List<Map<String, Object>> customers = createSampleCustomers();
        String filePath = createTablePath();
        
        // Execute
        writeParquetFile(schema, customers, filePath);
        
        // Verify
        verifyParquetFile(filePath);
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
    
    private String createTablePath() throws IOException {
        String tablePath = "customers/year=2024/month=01";
        String fileName = "part-00000.parquet";
        String fullPath = tablePath + "/" + fileName;
        
        java.nio.file.Files.createDirectories(tempDir.resolve("storage").resolve(tablePath));
        return fullPath;
    }
    
    private void writeParquetFile(MessageType schema, List<Map<String, Object>> customers, String filePath) throws IOException {
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(
            tempDir.resolve("storage").resolve(filePath).toString()
        );
        
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = createParquetWriter(parquetPath, conf);
        
        writeCustomerRecords(writer, schema, customers);
        writer.close();
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
    
    private void writeCustomerRecords(ParquetWriter<Group> writer, MessageType schema, List<Map<String, Object>> customers) throws IOException {
        for (Map<String, Object> customer : customers) {
            SimpleGroup group = createCustomerGroup(schema, customer);
            writer.write(group);
        }
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
    
    private void verifyParquetFile(String filePath) {
        byte[] parquetData = storageNode.getObject(filePath);
        assertNotNull(parquetData);
        assertTrue(parquetData.length > 0);
    }
} 