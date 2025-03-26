package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.core.MiniRDD;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStoreRDDTest {
    private MiniSparkContext sc;
    private MessageBus messageBus;
    private Client objectStoreClient;
    private LocalStorageNode localStorage;
    private Server objectStoreServer;
    private NetworkEndpoint serverEndpoint;
    private NetworkEndpoint clientEndpoint;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        // Create test directories and initialize components
        sc = new MiniSparkContext(2); // 2 partitions for testing
        messageBus = new MessageBus();
        
        // Setup endpoints
        serverEndpoint = new NetworkEndpoint("localhost", 8080);
        clientEndpoint = new NetworkEndpoint("localhost", 8081);
        
        // Setup LocalStorageNode
        localStorage = new LocalStorageNode(tempDir.toString());
        
        // Setup Server and Client
        objectStoreServer = new Server("test-server", localStorage, messageBus, serverEndpoint);
        objectStoreClient = new Client(messageBus, clientEndpoint, Arrays.asList(serverEndpoint));
        
        // Start the message bus
        messageBus.start();
    }

    @Test
    void testReadFromObjectStore() throws Exception {
        // Prepare test data
        String baseKey = "test-data";
        byte[] testData = "Hello, World!".getBytes();
        
        // Write test data to ObjectStore with partition-specific keys
        localStorage.putObject(baseKey + "-part-0", testData);
        localStorage.putObject(baseKey + "-part-1", testData);
        
        // Create ObjectStoreRDD
        ObjectStoreRDD<String> rdd = new ObjectStoreRDD<>(sc, objectStoreClient, baseKey, 2);
        
        // Verify partitions
        assertEquals(2, rdd.getPartitions().length);
        
        // Collect and verify data
        List<String> result = rdd.collect();
        assertEquals(2, result.size());
        assertEquals("Hello, World!", result.get(0));
        assertEquals("Hello, World!", result.get(1));
    }

    @Test
    void testReadWithMultiplePartitions() throws Exception {
        // Prepare test data
        String baseKey = "test-data";
        byte[] testData = "Hello, World!".getBytes();
        
        // Write test data to ObjectStore with partition-specific keys
        localStorage.putObject(baseKey + "-part-0", testData);
        localStorage.putObject(baseKey + "-part-1", testData);
        localStorage.putObject(baseKey + "-part-2", testData);
        localStorage.putObject(baseKey + "-part-3", testData);
        
        // Create ObjectStoreRDD with 4 partitions
        ObjectStoreRDD<String> rdd = new ObjectStoreRDD<>(sc, objectStoreClient, baseKey, 4);
        
        // Verify partitions
        assertEquals(4, rdd.getPartitions().length);
        
        // Collect and verify data
        List<String> result = rdd.collect();
        assertEquals(4, result.size());
        assertEquals("Hello, World!", result.get(0));
        assertEquals("Hello, World!", result.get(1));
        assertEquals("Hello, World!", result.get(2));
        assertEquals("Hello, World!", result.get(3));
    }

    @Test
    void testReadNonExistentData() {
        // Create ObjectStoreRDD for non-existent key
        ObjectStoreRDD<String> rdd = new ObjectStoreRDD<>(sc, objectStoreClient, "non-existent-key", 2);
        
        // Verify that reading non-existent data throws exception
        assertThrows(RuntimeException.class, () -> rdd.collect());
    }

    @Test
    void testReadWithEmptyData() throws Exception {
        // Prepare test data
        String baseKey = "empty-data";
        byte[] testData = "".getBytes();
        
        // Write empty data to ObjectStore with partition-specific keys
        localStorage.putObject(baseKey + "-part-0", testData);
        localStorage.putObject(baseKey + "-part-1", testData);
        
        // Create ObjectStoreRDD
        ObjectStoreRDD<String> rdd = new ObjectStoreRDD<>(sc, objectStoreClient, baseKey, 2);
        
        // Collect and verify empty result
        List<String> result = rdd.collect();
        assertTrue(result.isEmpty());
    }
} 