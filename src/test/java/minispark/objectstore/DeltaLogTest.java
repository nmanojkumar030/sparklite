package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.junit.jupiter.api.Assertions.*;

class DeltaLogTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    private List<LocalStorageNode> storageNodes;
    private MessageBus messageBus;
    private List<Server> servers;
    private Client client;
    private List<NetworkEndpoint> serverEndpoints;
    private NetworkEndpoint clientEndpoint;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() throws IOException {
        setupStorageNodes();
        setupMessageBus();
        setupServers();
        setupClient();
        startMessageBus();
        objectMapper = new ObjectMapper();
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
    void shouldStoreAndRetrieveJsonLogsInDeltaTable() throws Exception {
        // Create multiple JSON log entries
        List<String> logFiles = new ArrayList<>();
        
        for (int i = 0; i < 5; i++) {
            // Create a transaction log entry
            ObjectNode transactionLog = objectMapper.createObjectNode();
            transactionLog.put("version", i);
            transactionLog.put("timestamp", System.currentTimeMillis());
            
            ObjectNode operation = objectMapper.createObjectNode();
            operation.put("op", "ADD");
            operation.put("path", "/data/file_" + i + ".parquet");
            operation.put("partitionValues", "{\"date\": \"2024-03-20\"}");
            
            transactionLog.set("operation", operation);
            
            // Create the log file path
            String logPath = String.format("_delta_log/%020d.json", i);
            logFiles.add(logPath);
            
            // Store the JSON log
            byte[] logData = objectMapper.writeValueAsBytes(transactionLog);
            client.putObject(logPath, logData);
            
            // Verify the log was stored
            CompletableFuture<byte[]> futureData = client.getObject(logPath);
            byte[] storedData = futureData.join();
            
            assertNotNull(storedData);
            String storedJson = new String(storedData, StandardCharsets.UTF_8);
            assertEquals(
                objectMapper.readTree(logData),
                objectMapper.readTree(storedJson),
                "Stored JSON should match the original"
            );
        }
        
        // Verify all logs can be retrieved in order
        for (String logPath : logFiles) {
            CompletableFuture<byte[]> futureData = client.getObject(logPath);
            byte[] data = futureData.join();
            assertNotNull(data, "Log file should exist: " + logPath);
            
            ObjectNode logEntry = (ObjectNode) objectMapper.readTree(data);
            assertTrue(logEntry.has("version"), "Log should have version");
            assertTrue(logEntry.has("timestamp"), "Log should have timestamp");
            assertTrue(logEntry.has("operation"), "Log should have operation");
        }
    }
} 