package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.core.MiniRDD;
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
import java.util.Collections;
import java.util.stream.Collectors;

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
    void setUp(@TempDir Path tempDir) throws Exception {
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
        objectStoreClient = new Client(messageBus, clientEndpoint, Collections.singletonList(serverEndpoint));
        
        // Start message bus
        messageBus.start();
    }

    @AfterEach
    void tearDown() {
        messageBus.stop();
    }

    @Test
    void testEmptyRDD() throws Exception {
        ObjectStoreRDD rdd = new ObjectStoreRDD(sc, objectStoreClient, "test-", 2);
        List<byte[]> result = rdd.collect();
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldMapAndFilterObjects() throws Exception {
        // Create test data
        String[] testData = {
            "customer-1:John Doe:30",
            "customer-2:Jane Smith:25",
            "customer-3:Bob Johnson:35",
            "customer-4:Alice Brown:28",
            "customer-5:Charlie Wilson:42"
        };

        // Write test data
        for (String data : testData) {
            String key = data.split(":")[0];
            objectStoreClient.putObject(key, data.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Create RDD
        ObjectStoreRDD rdd = new ObjectStoreRDD(sc, objectStoreClient, "customer-", 2);

        // Test map operation
        List<String> mappedNames = rdd.map(line -> new String(line).split(":")[1]).collect();
        assertEquals(5, mappedNames.size());
        assertTrue(mappedNames.contains("John Doe"));
        assertTrue(mappedNames.contains("Jane Smith"));

        // Test filter operation
        List<byte[]> filteredAges = rdd.filter(line -> Integer.parseInt(new String(line).split(":")[2]) > 30).collect();
        List<String> filetered = filteredAges.stream().map(b -> new String(b)).collect(Collectors.toList());
        assertEquals(2, filteredAges.size());
        assertTrue(filetered.contains("customer-3:Bob Johnson:35"));
        assertTrue(filetered.contains("customer-5:Charlie Wilson:42"));

        // Test chaining operations
        List<String> mappedAndFiltered = rdd
            .map(line -> new String(line).split(":")[2])
            .filter(age -> Integer.parseInt(age) > 30)
            .collect();
        assertEquals(2, mappedAndFiltered.size());
        assertTrue(mappedAndFiltered.contains("35"));
        assertTrue(mappedAndFiltered.contains("42"));
    }
} 