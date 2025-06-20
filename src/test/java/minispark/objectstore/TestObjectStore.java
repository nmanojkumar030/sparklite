package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import minispark.util.SimulationRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.concurrent.CompletableFuture;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TestObjectStore {
    @TempDir
    Path tempDir;
    private LocalStorageNode storageNode;
    private MessageBus messageBus;
    private Server server;
    private Client client;
    private NetworkEndpoint serverEndpoint;
    private NetworkEndpoint clientEndpoint;

    @BeforeEach
    void setUp() {
        storageNode = new LocalStorageNode(tempDir);
        messageBus = new MessageBus();
        serverEndpoint = new NetworkEndpoint("localhost", 8080);
        clientEndpoint = new NetworkEndpoint("localhost", 8081);
        server = new Server("server1", storageNode, messageBus, serverEndpoint);
        client = new Client(messageBus, clientEndpoint, Arrays.asList(serverEndpoint));
        messageBus.start();
    }

    @Test
    void testBasicObjectOperations() throws Exception {
        String key = "test-key";
        String data = "Hello World";
        
        CompletableFuture<Void> putFuture = client.putObject(key, data.getBytes());
        SimulationRunner.runUntil(messageBus, () -> putFuture.isDone(), java.time.Duration.ofSeconds(5));
        putFuture.get();
        
        CompletableFuture<byte[]> getFuture = client.getObject(key);
        SimulationRunner.runUntil(messageBus, () -> getFuture.isDone(), java.time.Duration.ofSeconds(5));
        byte[] retrieved = getFuture.get();
        assertEquals(data, new String(retrieved));
    }

    @Test
    void testMultipleObjects() throws Exception {
        String key1 = "key1", key2 = "key2";
        String data = "test data";
        
        CompletableFuture<Void> put1Future = client.putObject(key1, data.getBytes());
        SimulationRunner.runUntil(messageBus, () -> put1Future.isDone(), java.time.Duration.ofSeconds(5));
        put1Future.get();
        
        CompletableFuture<Void> put2Future = client.putObject(key2, data.getBytes());
        SimulationRunner.runUntil(messageBus, () -> put2Future.isDone(), java.time.Duration.ofSeconds(5));
        put2Future.get();
        
        CompletableFuture<List<String>> listFuture = client.listObjects("");
        SimulationRunner.runUntil(messageBus, () -> listFuture.isDone(), java.time.Duration.ofSeconds(5));
        List<String> objects = listFuture.get();
        assertEquals(2, objects.size());
    }
} 