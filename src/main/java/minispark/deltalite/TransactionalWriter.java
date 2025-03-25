package minispark.deltalite;

import minispark.objectstore.Client;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.HashMap;

public class TransactionalWriter {
    private final Client client;
    private final String tablePath;
    private final Schema schema;
    private final List<String> partitionColumns;
    private final String transactionId;
    private final List<String> writtenFiles;

    public TransactionalWriter(Client client, String tablePath, Schema schema, List<String> partitionColumns) {
        this.client = client;
        this.tablePath = tablePath;
        this.schema = schema;
        this.partitionColumns = partitionColumns;
        this.transactionId = UUID.randomUUID().toString();
        this.writtenFiles = new ArrayList<>();
    }

    public CompletableFuture<Void> write(List<Row> rows) {
        // Create a temporary directory for this transaction
        String tempDir = tablePath + "/_temp/" + transactionId;
        
        // Partition the data if partition columns are specified
        Map<String, List<Row>> partitionedData = partitionData(rows);
        
        // Write each partition to a separate file
        CompletableFuture<Void>[] futures = new CompletableFuture[partitionedData.size()];
        int i = 0;
        
        for (Map.Entry<String, List<Row>> entry : partitionedData.entrySet()) {
            String partitionPath = entry.getKey();
            List<Row> partitionRows = entry.getValue();
            
            // Generate a unique file path for this partition
            String filePath = generateFilePath(tempDir, partitionPath);
            
            // Convert rows to Parquet format and write to object store
            byte[] data = convertToParquet(partitionRows);
            CompletableFuture<Void> future = client.putObject(filePath, data)
                .thenRun(() -> writtenFiles.add(filePath));
            futures[i++] = future;
        }
        
        // Wait for all writes to complete
        return CompletableFuture.allOf(futures)
            .thenRun(() -> commitTransaction(tempDir));
    }

    private void commitTransaction(String tempDir) {
        // Create a transaction log entry
        String logEntry = createTransactionLogEntry();
        String logPath = tablePath + "/_delta_log/" + transactionId + ".json";
        
        // Write the transaction log
        client.putObject(logPath, logEntry.getBytes()).join();
        
        // Move all files from temp directory to their final locations
        for (String filePath : writtenFiles) {
            String finalPath = filePath.replace(tempDir, tablePath);
            client.putObject(finalPath, client.getObject(filePath).join()).join();
            client.deleteObject(filePath).join();
        }
    }

    private String createTransactionLogEntry() {
        // Create a JSON representation of the transaction
        StringBuilder json = new StringBuilder("{");
        json.append("\"id\":\"").append(transactionId).append("\",");
        json.append("\"timestamp\":").append(System.currentTimeMillis()).append(",");
        json.append("\"operation\":\"WRITE\",");
        json.append("\"files\":[");
        
        for (int i = 0; i < writtenFiles.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append("\"").append(writtenFiles.get(i)).append("\"");
        }
        
        json.append("],");
        json.append("\"partitionColumns\":[");
        
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append("\"").append(partitionColumns.get(i)).append("\"");
        }
        
        json.append("]");
        json.append("}");
        return json.toString();
    }

    private Map<String, List<Row>> partitionData(List<Row> rows) {
        Map<String, List<Row>> partitionedData = new HashMap<>();
        
        for (Row row : rows) {
            String partitionValue = getPartitionValue(row);
            partitionedData.computeIfAbsent(partitionValue, k -> new ArrayList<>()).add(row);
        }
        
        return partitionedData;
    }

    private String getPartitionValue(Row row) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return "";
        }
        
        StringBuilder partitionValue = new StringBuilder();
        for (String column : partitionColumns) {
            if (partitionValue.length() > 0) {
                partitionValue.append("/");
            }
            partitionValue.append(column).append("=").append(row.get(column));
        }
        
        return partitionValue.toString();
    }

    private String generateFilePath(String basePath, String partitionPath) {
        String fileName = UUID.randomUUID().toString() + ".parquet";
        return partitionPath.isEmpty() ? 
            basePath + "/" + fileName :
            basePath + "/" + partitionPath + "/" + fileName;
    }

    private byte[] convertToParquet(List<Row> rows) {
        // TODO: Implement proper Parquet conversion
        // For now, just serialize to JSON
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append(rows.get(i).toString());
        }
        json.append("]");
        return json.toString().getBytes();
    }
} 