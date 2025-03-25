package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

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
        server = new Server(storageNode, messageBus, serverEndpoint);
        client = new Client(messageBus, clientEndpoint, Arrays.asList(serverEndpoint));

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
        byte[] data = "Hello, World!".getBytes();

        // Put object and wait for completion
        client.putObject(key, data).get(5, TimeUnit.SECONDS);

        // Get object and verify
        byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
        assertArrayEquals(data, retrievedData, "Retrieved data should match stored data");
    }

    @Test
    void shouldDeleteObject() throws Exception {
        byte[] data = "test data".getBytes();
        client.putObject("test-key", data).get(5, TimeUnit.SECONDS);
        client.deleteObject("test-key").get(5, TimeUnit.SECONDS);

        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            client.getObject("test-key").get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals("Failed to retrieve object: test-key", exception.getCause().getMessage());
    }

    @Test
    void shouldListObjects() throws Exception {
        // Put multiple objects
        client.putObject("key1", "data1".getBytes()).get(5, TimeUnit.SECONDS);
        client.putObject("key2", "data2".getBytes()).get(5, TimeUnit.SECONDS);

        // Get list of objects
        List<String> objects = client.listObjects().get(5, TimeUnit.SECONDS);

        // Verify objects are listed
        assertEquals(2, objects.size(), "Should have 2 objects");
        assertTrue(objects.contains("key1"), "Should contain key1");
        assertTrue(objects.contains("key2"), "Should contain key2");
    }
} 