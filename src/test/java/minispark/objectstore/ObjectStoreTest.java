package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import minispark.util.SimulationRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStoreTest {
    private MessageBus messageBus;
    private NetworkEndpoint clientEndpoint;
    private NetworkEndpoint serverEndpoint;
    private Client client;
    private Server server;
    private LocalStorageNode storageNode;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        // Initialize components
        messageBus = new MessageBus();
        clientEndpoint = new NetworkEndpoint("localhost", 8080);
        serverEndpoint = new NetworkEndpoint("localhost", 8081);
        storageNode = new LocalStorageNode(tempDir.toString());
        server = new Server("server1", storageNode, messageBus, serverEndpoint);
        client = new Client(messageBus, clientEndpoint, Collections.singletonList(serverEndpoint));

        // Start MessageBus
        messageBus.start();
    }

    @AfterEach
    void tearDown() {
        messageBus.stop();
    }

    @Test
    void shouldPutAndGetObject() throws Exception {
        // Test data
        String key = "test-key";
        String data = "Hello, World!";

        // Put object and wait for completion
        CompletableFuture<Void> putFuture = client.putObject(key, data.getBytes());
        SimulationRunner.runUntil(messageBus, () -> putFuture.isDone(), java.time.Duration.ofSeconds(5));
        putFuture.get();

        // Get object and verify
        CompletableFuture<byte[]> getFuture = client.getObject(key);
        SimulationRunner.runUntil(messageBus, () -> getFuture.isDone(), java.time.Duration.ofSeconds(5));
        byte[] retrievedData = getFuture.get();
        assertEquals(data, new String(retrievedData), "Retrieved data should match stored data");
    }

    @Test
    void shouldDeleteObject() throws Exception {
        String data = "test data";
        CompletableFuture<Void> putFuture = client.putObject("test-key", data.getBytes());
        SimulationRunner.runUntil(messageBus, () -> putFuture.isDone(), java.time.Duration.ofSeconds(5));
        putFuture.get();
        
        CompletableFuture<Void> deleteFuture = client.deleteObject("test-key");
        SimulationRunner.runUntil(messageBus, () -> deleteFuture.isDone(), java.time.Duration.ofSeconds(5));
        deleteFuture.get();

        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            CompletableFuture<byte[]> getFuture = client.getObject("test-key");
            SimulationRunner.runUntil(messageBus, () -> getFuture.isDone(), java.time.Duration.ofSeconds(5));
            getFuture.get();
        });
        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals("Failed to retrieve object: test-key", exception.getCause().getMessage());
    }

    @Test
    void shouldListObjects() throws Exception {
        // Put multiple objects
        CompletableFuture<Void> put1Future = client.putObject("key1", "data1".getBytes());
        SimulationRunner.runUntil(messageBus, () -> put1Future.isDone(), java.time.Duration.ofSeconds(5));
        put1Future.get();
        
        CompletableFuture<Void> put2Future = client.putObject("key2", "data2".getBytes());
        SimulationRunner.runUntil(messageBus, () -> put2Future.isDone(), java.time.Duration.ofSeconds(5));
        put2Future.get();

        // Get list of objects
        CompletableFuture<List<String>> listFuture = client.listObjects("");
        SimulationRunner.runUntil(messageBus, () -> listFuture.isDone(), java.time.Duration.ofSeconds(5));
        List<String> objects = listFuture.get();

        // Verify objects are listed
        assertEquals(2, objects.size(), "Should have 2 objects");
        assertTrue(objects.contains("key1"), "Should contain key1");
        assertTrue(objects.contains("key2"), "Should contain key2");
    }
} 