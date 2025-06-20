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

import static org.junit.jupiter.api.Assertions.*;

class SyncClientTest {
    
    private MessageBus messageBus;
    private ObjectStoreCluster objectStoreCluster;
    private SyncClient syncClient;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        messageBus = new MessageBus();
        messageBus.start();
        objectStoreCluster = new ObjectStoreCluster(messageBus, tempDir, 3);
        objectStoreCluster.start();
        
        // Create SyncClient wrapper around the async client
        syncClient = new SyncClient(objectStoreCluster.getClient(), messageBus);
    }

    @AfterEach
    void tearDown() {
        objectStoreCluster.stop();
    }

    @Test
    void testSyncPutAndGet() {
        String key = "test-key";
        byte[] data = "test-data".getBytes();
        
        // Put object synchronously
        syncClient.putObject(key, data);
        
        // Get object synchronously
        byte[] retrievedData = syncClient.getObject(key);
        
        assertArrayEquals(data, retrievedData);
    }

    @Test
    void testSyncListObjects() {
        // Put multiple objects
        syncClient.putObject("prefix-1", "data1".getBytes());
        syncClient.putObject("prefix-2", "data2".getBytes());
        syncClient.putObject("other-1", "data3".getBytes());
        
        // List objects with prefix
        List<String> objects = syncClient.listObjects("prefix-");
        
        assertEquals(2, objects.size());
        assertTrue(objects.contains("prefix-1"));
        assertTrue(objects.contains("prefix-2"));
        assertFalse(objects.contains("other-1"));
    }

    @Test
    void testSyncObjectSize() {
        String key = "size-test";
        byte[] data = "test data for size".getBytes();
        
        syncClient.putObject(key, data);
        
        long size = syncClient.getObjectSize(key);
        
        assertEquals(data.length, size);
    }

    @Test
    void testSyncObjectRange() {
        String key = "range-test";
        byte[] data = "0123456789".getBytes();
        
        syncClient.putObject(key, data);
        
        // Get range [2, 5] (inclusive)
        byte[] range = syncClient.getObjectRange(key, 2, 5);
        
        assertArrayEquals("2345".getBytes(), range);
    }

    @Test
    void testSyncDeleteObject() {
        String key = "delete-test";
        byte[] data = "to be deleted".getBytes();
        
        // Put and verify exists
        syncClient.putObject(key, data);
        byte[] retrieved = syncClient.getObject(key);
        assertArrayEquals(data, retrieved);
        
        // Delete
        syncClient.deleteObject(key);
        
        // Verify deleted (should throw exception)
        assertThrows(RuntimeException.class, () -> {
            syncClient.getObject(key);
        });
    }

    @Test
    void testMultipleSequentialOperations() {
        // Test that multiple operations work correctly in sequence
        // This verifies that the tick progression works for complex scenarios
        
        String[] keys = {"seq-1", "seq-2", "seq-3"};
        byte[][] data = {
            "first data".getBytes(),
            "second data".getBytes(), 
            "third data".getBytes()
        };
        
        // Put all objects
        for (int i = 0; i < keys.length; i++) {
            syncClient.putObject(keys[i], data[i]);
        }
        
        // Get all objects and verify
        for (int i = 0; i < keys.length; i++) {
            byte[] retrieved = syncClient.getObject(keys[i]);
            assertArrayEquals(data[i], retrieved);
        }
        
        // List all objects
        List<String> allObjects = syncClient.listObjects("seq-");
        assertEquals(3, allObjects.size());
        
        // Delete all objects
        for (String key : keys) {
            syncClient.deleteObject(key);
        }
        
        // Verify all deleted
        List<String> remainingObjects = syncClient.listObjects("seq-");
        assertEquals(0, remainingObjects.size());
    }
} 