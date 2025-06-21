package minispark.distributed.objectstore;

import minispark.MiniSparkContext;
import minispark.distributed.network.MessageBus;
import minispark.distributed.network.NetworkEndpoint;
import minispark.util.EventLoopRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
        CompletableFuture<List<byte[]>> collectFuture = rdd.collect();
        
        // Use TestUtils to drive ticks until completion
        EventLoopRunner.runUntil(messageBus,
                () -> collectFuture.isDone(),
                java.time.Duration.ofSeconds(5));
        
        List<byte[]> result = collectFuture.get();
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
            CompletableFuture<Void> putFuture = objectStoreClient.putObject(key, data.getBytes());
            EventLoopRunner.runUntil(messageBus, () -> putFuture.isDone(), java.time.Duration.ofSeconds(5));
            putFuture.get();
        }

        // Create RDD
        ObjectStoreRDD rdd = new ObjectStoreRDD(sc, objectStoreClient, "customer-", 2);

        // Test map operation
        CompletableFuture<List<String>> mappedFuture = rdd.map(line -> new String(line).split(":")[1]).collect();
        EventLoopRunner.runUntil(messageBus, () -> mappedFuture.isDone(), java.time.Duration.ofSeconds(5));
        List<String> mappedNames = mappedFuture.get();
        assertEquals(5, mappedNames.size());
        assertTrue(mappedNames.contains("John Doe"));
        assertTrue(mappedNames.contains("Jane Smith"));

        // Test filter operation
        CompletableFuture<List<byte[]>> filteredFuture = rdd.filter(line -> Integer.parseInt(new String(line).split(":")[2]) > 30).collect();
        EventLoopRunner.runUntil(messageBus, () -> filteredFuture.isDone(), java.time.Duration.ofSeconds(5));
        List<byte[]> filteredAges = filteredFuture.get();
        List<String> filetered = filteredAges.stream().map(b -> new String(b)).collect(Collectors.toList());
        assertEquals(2, filteredAges.size());
        assertTrue(filetered.contains("customer-3:Bob Johnson:35"));
        assertTrue(filetered.contains("customer-5:Charlie Wilson:42"));

        // Test chaining operations
        CompletableFuture<List<String>> chainedFuture = rdd
            .map(line -> new String(line).split(":")[2])
            .filter(age -> Integer.parseInt(age) > 30)
            .collect();
        EventLoopRunner.runUntil(messageBus, () -> chainedFuture.isDone(), java.time.Duration.ofSeconds(5));
        List<String> mappedAndFiltered = chainedFuture.get();
        assertEquals(2, mappedAndFiltered.size());
        assertTrue(mappedAndFiltered.contains("35"));
        assertTrue(mappedAndFiltered.contains("42"));
    }
} 