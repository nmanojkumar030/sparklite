package minispark.distributed.objectstore;

import minispark.distributed.network.MessageBus;
import minispark.distributed.network.NetworkEndpoint;
import minispark.util.EventLoopRunner;
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
                Path serverDir = tempDir.resolve("server-" + endpoint.getPort());
                LocalStorageNode storage = new LocalStorageNode(serverDir.toString());
                return new Server("server" + endpoint.getPort(), storage, messageBus, endpoint);
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

        // Put objects and wait for completion
        CompletableFuture<Void> put1 = client.putObject(key1, value1);
        CompletableFuture<Void> put2 = client.putObject(key2, value2);
        
        EventLoopRunner.runUntil(messageBus, () -> put1.isDone(), java.time.Duration.ofSeconds(5));
        put1.get();
        EventLoopRunner.runUntil(messageBus, () -> put2.isDone(), java.time.Duration.ofSeconds(5));
        put2.get();

        // Retrieve objects and verify they are stored correctly
        CompletableFuture<byte[]> get1 = client.getObject(key1);
        CompletableFuture<byte[]> get2 = client.getObject(key2);
        
        EventLoopRunner.runUntil(messageBus, () -> get1.isDone(), java.time.Duration.ofSeconds(5));
        EventLoopRunner.runUntil(messageBus, () -> get2.isDone(), java.time.Duration.ofSeconds(5));
        
        assertArrayEquals(value1, get1.get());
        assertArrayEquals(value2, get2.get());
    }
} 