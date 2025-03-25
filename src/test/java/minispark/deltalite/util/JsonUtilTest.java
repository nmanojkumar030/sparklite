package minispark.deltalite.util;

import minispark.deltalite.Schema;
import minispark.deltalite.actions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class JsonUtilTest {
    @TempDir
    Path tempDir;
    
    @Test
    public void testAddFileSerializationDeserialization() throws IOException {
        AddFile original = new AddFile(
            "test/path.parquet",
            1000L,
            System.currentTimeMillis(),
            true
        );
        
        String json = JsonUtil.toJson(original);
        Files.writeString(tempDir.resolve("addfile.json"), json);
        System.out.println("AddFile JSON written to: " + tempDir.resolve("addfile.json"));
        System.out.println("AddFile JSON: " + json);
        
        Action deserialized = JsonUtil.fromJson(json);
        
        assertTrue(deserialized instanceof AddFile);
        AddFile deserializedAddFile = (AddFile) deserialized;
        assertEquals("add", deserializedAddFile.getType());
        assertEquals(original.getPath(), deserializedAddFile.getPath());
        assertEquals(original.getSize(), deserializedAddFile.getSize());
        assertEquals(original.getModificationTime(), deserializedAddFile.getModificationTime());
        assertEquals(original.isDataChange(), deserializedAddFile.isDataChange());
    }
    
    @Test
    public void testRemoveFileSerializationDeserialization() throws IOException {
        RemoveFile original = new RemoveFile(
            "test/path.parquet",
            System.currentTimeMillis()
        );
        
        String json = JsonUtil.toJson(original);
        Files.writeString(tempDir.resolve("removefile.json"), json);
        System.out.println("RemoveFile JSON written to: " + tempDir.resolve("removefile.json"));
        System.out.println("RemoveFile JSON: " + json);
        
        Action deserialized = JsonUtil.fromJson(json);
        
        assertTrue(deserialized instanceof RemoveFile);
        RemoveFile deserializedRemoveFile = (RemoveFile) deserialized;
        assertEquals("remove", deserializedRemoveFile.getType());
        assertEquals(original.getPath(), deserializedRemoveFile.getPath());
        assertEquals(original.getDeletionTimestamp(), deserializedRemoveFile.getDeletionTimestamp());
    }
    
    @Test
    public void testMetadataSerializationDeserialization() throws IOException {
        Map<String, String> partitionColumns = new HashMap<>();
        partitionColumns.put("date", "string");
        
        Map<String, String> configuration = new HashMap<>();
        configuration.put("key", "value");
        
        Metadata original = new Metadata(
            "test-id",
            "test-table",
            "Test Description",
            new Schema(),
            partitionColumns,
            configuration
        );
        
        String json = JsonUtil.toJson(original);
        Files.writeString(tempDir.resolve("metadata.json"), json);
        System.out.println("Metadata JSON written to: " + tempDir.resolve("metadata.json"));
        System.out.println("Metadata JSON: " + json);
        
        Action deserialized = JsonUtil.fromJson(json);
        
        assertTrue(deserialized instanceof Metadata);
        Metadata deserializedMetadata = (Metadata) deserialized;
        assertEquals("metadata", deserializedMetadata.getType());
        assertEquals(original.getId(), deserializedMetadata.getId());
        assertEquals(original.getName(), deserializedMetadata.getName());
        assertEquals(original.getDescription(), deserializedMetadata.getDescription());
        assertEquals(original.getPartitionColumns(), deserializedMetadata.getPartitionColumns());
        assertEquals(original.getConfiguration(), deserializedMetadata.getConfiguration());
    }
    
    @Test
    public void testCommitInfoSerializationDeserialization() throws IOException {
        Map<String, String> operationParameters = new HashMap<>();
        operationParameters.put("tablePath", "/test/path");
        
        CommitInfo original = new CommitInfo(
            System.currentTimeMillis(),
            "commit",
            operationParameters,
            null
        );
        
        String json = JsonUtil.toJson(original);
        Files.writeString(tempDir.resolve("commitinfo.json"), json);
        System.out.println("CommitInfo JSON written to: " + tempDir.resolve("commitinfo.json"));
        System.out.println("CommitInfo JSON: " + json);
        
        Action deserialized = JsonUtil.fromJson(json);
        
        assertTrue(deserialized instanceof CommitInfo);
        CommitInfo deserializedCommitInfo = (CommitInfo) deserialized;
        assertEquals("commitInfo", deserializedCommitInfo.getType());
        assertEquals(original.getTimestamp(), deserializedCommitInfo.getTimestamp());
        assertEquals(original.getOperation(), deserializedCommitInfo.getOperation());
        assertEquals(original.getOperationParameters(), deserializedCommitInfo.getOperationParameters());
        assertEquals(original.getOperationMetrics(), deserializedCommitInfo.getOperationMetrics());
    }
} 