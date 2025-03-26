package minispark.objectstore;

import minispark.messages.*;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import minispark.objectstore.serialization.ObjectStoreSerializer;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    void testPutAndGet() throws Exception {
        String key = "test.txt";
        String data = "Hello, World!";
        client.putObject(key, data.getBytes()).get(5, TimeUnit.SECONDS);
        byte[] retrieved = client.getObject(key).get(5, TimeUnit.SECONDS);
        assertArrayEquals(data.getBytes(), retrieved);
    }

    @Test
    void testListObjects() throws Exception {
        String key1 = "test1.txt";
        String key2 = "test2.txt";
        String data = "Hello";
        client.putObject(key1, data.getBytes()).get(5, TimeUnit.SECONDS);
        client.putObject(key2, data.getBytes()).get(5, TimeUnit.SECONDS);
        List<String> objects = client.listObjects("").get(5, TimeUnit.SECONDS);
        assertTrue(objects.contains(key1));
        assertTrue(objects.contains(key2));
    }
} 