package minispark.deltalite;

import minispark.deltalite.actions.AddFile;
import minispark.deltalite.actions.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionTest {
    @TempDir
    Path tempDir;
    
    private DeltaLog deltaLog;
    
    @BeforeEach
    public void setUp() throws IOException {
        deltaLog = DeltaLog.forTable(tempDir.toString());
    }
    
    @Test
    public void testBasicTransaction() throws IOException {
        // Create a transaction
        OptimisticTransaction transaction = new OptimisticTransaction(tempDir.toString());
        
        // Add some actions
        transaction.addAction(new AddFile(
            "data/part-00000.parquet",
            1000L,
            System.currentTimeMillis(),
            true
        ));
        
        Map<String, String> partitionColumns = new HashMap<>();
        Map<String, String> configuration = new HashMap<>();
        transaction.addAction(new Metadata(
            "test-id",
            "test-table",
            "Test table",
            new Schema(),
            partitionColumns,
            configuration
        ));
        
        // Commit the transaction
        transaction.commit();
        
        // Verify the actions were added
        assertEquals(3, transaction.getActions().size()); // 2 actions + 1 commit info
        
        // Verify the log was written
        assertTrue(deltaLog.tableExists());
        
        // Get the latest snapshot
        Snapshot snapshot = deltaLog.snapshot();
        assertEquals(0, snapshot.getVersion());
        
        // Verify the actions in the snapshot
        assertEquals(1, snapshot.getActions(AddFile.class).size());
        assertEquals(1, snapshot.getActions(Metadata.class).size());
    }
    
    @Test
    public void testConcurrentModification() throws IOException {
        // Create two transactions
        OptimisticTransaction t1 = new OptimisticTransaction(tempDir.toString());
        OptimisticTransaction t2 = new OptimisticTransaction(tempDir.toString());
        
        // First transaction adds a file
        t1.addAction(new AddFile(
            "data/part-00000.parquet",
            1000L,
            System.currentTimeMillis(),
            true
        ));
        t1.commit();
        
        // Second transaction tries to modify the same file
        t2.addAction(new AddFile(
            "data/part-00000.parquet",
            2000L,
            System.currentTimeMillis(),
            true
        ));
        
        // Should throw ConcurrentModificationException
        assertThrows(ConcurrentModificationException.class, () -> t2.commit());
    }
} 