package minispark.deltalite;

import minispark.deltalite.actions.Action;
import minispark.deltalite.actions.AddFile;
import minispark.deltalite.actions.Metadata;
import minispark.deltalite.actions.RemoveFile;
import minispark.deltalite.actions.CommitInfo;
import minispark.deltalite.util.JsonUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ConcurrentModificationException;

public class OptimisticTransaction extends Transaction {
    private final String tablePath;
    private final DeltaLog deltaLog;
    private final ReentrantLock lock;
    private final long initialVersion;
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY = 100L; // 100ms

    public OptimisticTransaction(String tablePath) throws IOException {
        super(tablePath);
        this.tablePath = tablePath;
        this.deltaLog = DeltaLog.forTable(tablePath);
        this.initialVersion = deltaLog.getLatestVersion();
        this.lock = new ReentrantLock();
    }

    @Override
    public Transaction commit() throws IOException {
        executeWithRetry(() -> {
            try {
                commitTransaction();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        return this;
    }

    private void commitTransaction() throws IOException {
        // Get the current version
        long currentVersion = deltaLog.getLatestVersion();
        
        // Check for concurrent modifications
        if (currentVersion != initialVersion) {
            throw new minispark.deltalite.ConcurrentModificationException(
                "Table was modified concurrently. Expected version " + initialVersion + " but found version " + currentVersion
            );
        }
        
        // Create commit info
        Map<String, String> parameters = new HashMap<>();
        parameters.put("tablePath", tablePath);
        CommitInfo commitInfo = new CommitInfo(
            System.currentTimeMillis(),
            "commit",
            parameters,
            null // operationMetrics
        );
        
        // Add commit info to actions
        addAction(commitInfo);
        
        // Write the actions to the log
        deltaLog.write(currentVersion + 1, getActions());
        
        // Update the current snapshot
        deltaLog.update();
    }

    private <T> T executeWithRetry(TransactionOperation<T> operation) throws IOException {
        int retryCount = 0;
        long retryDelay = INITIAL_RETRY_DELAY;
        
        while (true) {
            try {
                lock.lock();
                return operation.execute();
            } catch (minispark.deltalite.ConcurrentModificationException e) {
                throw e; // Propagate ConcurrentModificationException
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                if (retryCount >= MAX_RETRIES) {
                    throw new IOException("Failed to commit transaction after " + MAX_RETRIES + " retries", e);
                }
                
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Transaction interrupted", ie);
                }
                
                retryCount++;
                retryDelay *= 2; // Exponential backoff
            } finally {
                lock.unlock();
            }
        }
    }

    @FunctionalInterface
    private interface TransactionOperation<T> {
        T execute() throws RuntimeException;
    }
} 