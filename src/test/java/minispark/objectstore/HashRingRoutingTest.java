package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HashRingRoutingTest {
    private static final Logger logger = LoggerFactory.getLogger(HashRingRoutingTest.class);

    @TempDir
    Path tempDir;

    private MessageBus messageBus;
    private List<NetworkEndpoint> serverEndpoints;
    private NetworkEndpoint clientEndpoint;
    private HashRing hashRing;
    private Client client;
    private List<Server> servers;

    @BeforeEach
    void setUp() throws IOException {
        messageBus = new MessageBus();
        serverEndpoints = Arrays.asList(
            new NetworkEndpoint("localhost", 8081),
            new NetworkEndpoint("localhost", 8082),
            new NetworkEndpoint("localhost", 8083)
        );
        clientEndpoint = new NetworkEndpoint("localhost", 8080);
        
        // Initialize hash ring with servers
        hashRing = new HashRing();
        serverEndpoints.forEach(hashRing::addServer);
        
        // Create servers with local storage
        servers = serverEndpoints.stream()
            .map(endpoint -> {
                try {
                    Path serverDir = tempDir.resolve("server-" + endpoint.getPort());
                    LocalStorageNode storage = new LocalStorageNode(serverDir.toString());
                    return new Server(storage, messageBus, endpoint);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create server storage", e);
                }
            })
            .toList();
            
        // Create client with list of server endpoints
        client = new Client(messageBus, clientEndpoint, serverEndpoints);
        
        // Start message bus
        messageBus.start();
    }

    @Test
    void shouldRouteRequestsToCorrectServer() throws Exception {
        // Test data
        String key1 = "test-key-1";
        String key2 = "test-key-2";
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

        // Get target servers for keys
        NetworkEndpoint server1 = hashRing.getServerForKey(key1);
        NetworkEndpoint server2 = hashRing.getServerForKey(key2);
        
        logger.info("Key '{}' mapped to server {}", key1, server1);
        logger.info("Key '{}' mapped to server {}", key2, server2);

        // Put objects
        CompletableFuture<Void> put1 = client.putObject(key1, value1);
        CompletableFuture<Void> put2 = client.putObject(key2, value2);
        
        // Wait for operations to complete
        put1.get(5, TimeUnit.SECONDS);
        put2.get(5, TimeUnit.SECONDS);

        // Get objects back
        CompletableFuture<byte[]> get1 = client.getObject(key1);
        CompletableFuture<byte[]> get2 = client.getObject(key2);
        
        // Verify objects are retrieved correctly
        assertArrayEquals(value1, get1.get(5, TimeUnit.SECONDS));
        assertArrayEquals(value2, get2.get(5, TimeUnit.SECONDS));
    }

    @Test
    void shouldHandleServerFailure() throws Exception {
        // Test data
        String key = "test-key";
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        
        // Get initial target server
        NetworkEndpoint initialServer = hashRing.getServerForKey(key);
        logger.info("Key '{}' initially mapped to server {}", key, initialServer);
        
        // Put object
        CompletableFuture<Void> put = client.putObject(key, value);
        put.get(5, TimeUnit.SECONDS);
        
        // Simulate server failure by removing it from hash ring
        hashRing.removeServer(initialServer);
        
        // Get new target server
        NetworkEndpoint newServer = hashRing.getServerForKey(key);
        logger.info("Key '{}' remapped to server {} after failure", key, newServer);
        
        // Try to get object from new server
        CompletableFuture<byte[]> get = client.getObject(key);
        
        // Verify object is accessible
        assertArrayEquals(value, get.get(5, TimeUnit.SECONDS));
    }

    @Test
    void shouldHandleServerAddition() throws Exception {
        // Test data
        String key = "test-key";
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        
        // Get initial target server
        NetworkEndpoint initialServer = hashRing.getServerForKey(key);
        logger.info("Key '{}' initially mapped to server {}", key, initialServer);
        
        // Put object
        CompletableFuture<Void> put = client.putObject(key, value);
        put.get(5, TimeUnit.SECONDS);
        
        // Add new server
        NetworkEndpoint newServerEndpoint = new NetworkEndpoint("localhost", 8084);
        Path newServerDir = tempDir.resolve("server-8084");
        LocalStorageNode newStorage = new LocalStorageNode(newServerDir.toString());
        Server newServer = new Server(newStorage, messageBus, newServerEndpoint);
        hashRing.addServer(newServerEndpoint);
        
        // Get new target server
        NetworkEndpoint newTargetServer = hashRing.getServerForKey(key);
        logger.info("Key '{}' mapped to server {} after addition", key, newTargetServer);
        
        // Try to get object
        CompletableFuture<byte[]> get = client.getObject(key);
        
        // Verify object is accessible
        assertArrayEquals(value, get.get(5, TimeUnit.SECONDS));
    }
} 