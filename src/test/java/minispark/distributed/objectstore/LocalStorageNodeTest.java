package minispark.distributed.objectstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class LocalStorageNodeTest {
    @TempDir
    Path tempDir;
    
    private LocalStorageNode storageNode;
    private static final String TEST_KEY = "test/key.txt";
    private static final byte[] TEST_DATA = "Hello, World!".getBytes();
    
    @BeforeEach
    void setUp() {
        storageNode = new LocalStorageNode(tempDir);
    }
    
    @Test
    void testPutAndGetObject() throws Exception {
        // Test putting an object
        storageNode.putObject(TEST_KEY, TEST_DATA);
        
        // Verify the file exists
        assertTrue(Files.exists(tempDir.resolve(TEST_KEY)));
        
        // Test getting the object
        byte[] retrievedData = storageNode.getObject(TEST_KEY);
        assertArrayEquals(TEST_DATA, retrievedData);
    }
    
    @Test
    void testPutObjectWithLock() throws Exception {
        // Test putting an object with overwrite=true
        storageNode.putObjectWithLock(TEST_KEY, TEST_DATA, true);
        assertArrayEquals(TEST_DATA, storageNode.getObject(TEST_KEY));
        
        // Test putting an object with overwrite=false
        byte[] newData = "New Data".getBytes();
        assertThrows(IOException.class, () -> 
            storageNode.putObjectWithLock(TEST_KEY, newData, false));
    }
    
    @Test
    void testDeleteObject() throws Exception {
        // Put an object first
        storageNode.putObject(TEST_KEY, TEST_DATA);
        assertTrue(Files.exists(tempDir.resolve(TEST_KEY)));
        
        // Delete the object
        storageNode.deleteObject(TEST_KEY);
        assertFalse(Files.exists(tempDir.resolve(TEST_KEY)));
        
        // Deleting non-existent object should not throw exception
        assertDoesNotThrow(() -> storageNode.deleteObject("non-existent.txt"));
    }
    
    @Test
    void testListObjects() throws Exception {
        // Create multiple objects with different prefixes
        storageNode.putObject("test1/file1.txt", "data1".getBytes());
        storageNode.putObject("test1/file2.txt", "data2".getBytes());
        storageNode.putObject("test2/file3.txt", "data3".getBytes());
        
        // Test listing all objects
        List<String> allObjects = storageNode.listObjects("");
        assertEquals(3, allObjects.size());
        assertTrue(allObjects.contains("test1/file1.txt"));
        assertTrue(allObjects.contains("test1/file2.txt"));
        assertTrue(allObjects.contains("test2/file3.txt"));
        
        // Test listing objects with prefix
        List<String> test1Objects = storageNode.listObjects("test1");
        assertEquals(2, test1Objects.size());
        assertTrue(test1Objects.contains("test1/file1.txt"));
        assertTrue(test1Objects.contains("test1/file2.txt"));
    }
    
    @Test
    void testListObjectsWithMetadata() throws Exception {
        // Create a test file
        storageNode.putObject("test/file.txt", TEST_DATA);
        
        // Get file metadata
        List<FileStatus> statuses = storageNode.listObjectsWithMetadata("test");
        assertEquals(1, statuses.size());
        
        FileStatus status = statuses.get(0);
        assertEquals(TEST_DATA.length, status.getLength());
        assertFalse(status.isDirectory());
        assertEquals(1, status.getReplication());
        assertEquals(4096, status.getBlockSize());
        assertTrue(status.getModificationTime() > 0);
        assertTrue(status.getUri().toString().endsWith("test/file.txt"));
    }
    
    @Test
    void testListFrom() throws Exception {
        // Create a nested directory structure
        storageNode.putObject("dir1/file1.txt", "data1".getBytes());
        storageNode.putObject("dir1/dir2/file2.txt", "data2".getBytes());
        
        // Test non-recursive listing
        List<FileStatus> nonRecursive = storageNode.listFrom("dir1", false);
        assertEquals(1, nonRecursive.size());
        assertTrue(nonRecursive.get(0).getUri().toString().endsWith("dir1/file1.txt"));
        
        // Test recursive listing
        List<FileStatus> recursive = storageNode.listFrom("dir1", true);
        assertEquals(2, recursive.size());
        assertTrue(recursive.stream().anyMatch(s -> s.getUri().toString().endsWith("dir1/file1.txt")));
        assertTrue(recursive.stream().anyMatch(s -> s.getUri().toString().endsWith("dir1/dir2/file2.txt")));
    }
    
    @Test
    void testConcurrentAccess() throws Exception {
        // Test concurrent put operations
        Thread t1 = new Thread(() -> {
            try {
                storageNode.putObject("concurrent/file1.txt", "data1".getBytes());
            } catch (Exception e) {
                fail("Thread 1 failed: " + e.getMessage());
            }
        });
        
        Thread t2 = new Thread(() -> {
            try {
                storageNode.putObject("concurrent/file2.txt", "data2".getBytes());
            } catch (Exception e) {
                fail("Thread 2 failed: " + e.getMessage());
            }
        });
        
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        
        // Verify both files were created successfully
        List<String> files = storageNode.listObjects("concurrent");
        assertEquals(2, files.size());
        assertTrue(files.contains("concurrent/file1.txt"));
        assertTrue(files.contains("concurrent/file2.txt"));
    }
} 