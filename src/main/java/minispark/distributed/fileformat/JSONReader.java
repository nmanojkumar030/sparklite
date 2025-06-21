package minispark.distributed.fileformat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * JSON format reader that creates partitions based on record boundaries.
 * Supports both JSON Lines format (one JSON object per line) and JSON Arrays.
 * Each partition processes a specific range of JSON records.
 */
public class JSONReader implements FormatReader<JsonNode> {
    private static final Logger logger = LoggerFactory.getLogger(JSONReader.class);
    
    private final ObjectMapper objectMapper;
    private final JsonFormat format;
    private final String encoding;
    
    public enum JsonFormat {
        JSON_LINES,  // One JSON object per line (JSONL/NDJSON)
        JSON_ARRAY   // Array of JSON objects
    }
    
    /**
     * Create a JSON reader with default settings (JSON Lines format, UTF-8).
     */
    public JSONReader() {
        this(JsonFormat.JSON_LINES, "UTF-8");
    }
    
    /**
     * Create a JSON reader with custom settings.
     *
     * @param format The JSON format (JSON_LINES or JSON_ARRAY)
     * @param encoding File encoding (e.g., "UTF-8", "ISO-8859-1")
     */
    public JSONReader(JsonFormat format, String encoding) {
        this.objectMapper = new ObjectMapper();
        this.format = format;
        this.encoding = encoding;
    }
    
    @Override
    public CompletableFuture<FilePartition[]> createPartitions(String filePath, int targetPartitions) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("Analyzing JSON file: {} with format: {} and target partitions: {}", 
                    filePath, format, targetPartitions);
                
                if (format == JsonFormat.JSON_LINES) {
                    return createJsonLinesPartitions(filePath, targetPartitions);
                } else {
                    return createJsonArrayPartitions(filePath, targetPartitions);
                }
                
            } catch (IOException e) {
                throw new RuntimeException("Failed to analyze JSON file: " + filePath, e);
            }
        });
    }
    
    private FilePartition[] createJsonLinesPartitions(String filePath, int targetPartitions) throws IOException {
        // Count total lines (assuming each line is a JSON object)
        long totalLines;
        try (var lines = Files.lines(Paths.get(filePath))) {
            totalLines = lines.count();
        }
        
        if (totalLines == 0) {
            return new FilePartition[0];
        }
        
        // Calculate lines per partition
        long linesPerPartition = Math.max(1, totalLines / targetPartitions);
        int actualPartitions = (int) Math.ceil((double) totalLines / linesPerPartition);
        
        logger.info("JSON Lines analysis: {} total lines, {} lines per partition, {} actual partitions", 
            totalLines, linesPerPartition, actualPartitions);
        
        FilePartition[] partitions = new FilePartition[actualPartitions];
        
        for (int i = 0; i < actualPartitions; i++) {
            long startLine = i * linesPerPartition;
            long endLine = Math.min(startLine + linesPerPartition - 1, totalLines - 1);
            long partitionLines = endLine - startLine + 1;
            
            // JSON-specific metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("format", format);
            metadata.put("startLine", startLine);
            metadata.put("endLine", endLine);
            metadata.put("recordCount", partitionLines);
            metadata.put("encoding", encoding);
            
            partitions[i] = new FilePartition(
                i,
                filePath,
                startLine, // Use line number as offset
                partitionLines, // Use line count as length
                metadata
            );
            
            logger.debug("JSON Partition {}: lines {}-{} ({} records)", 
                i, startLine, endLine, partitionLines);
        }
        
        return partitions;
    }
    
    private FilePartition[] createJsonArrayPartitions(String filePath, int targetPartitions) throws IOException {
        // For JSON arrays, we need to parse the file to count objects
        // This is more expensive but necessary for accurate partitioning
        
        List<Long> objectPositions = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(filePath), 
                java.nio.charset.Charset.forName(encoding))) {
            
            // Find the start of the array
            String line;
            boolean foundArrayStart = false;
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("[")) {
                    foundArrayStart = true;
                    break;
                }
            }
            
            if (!foundArrayStart) {
                throw new IllegalArgumentException("JSON file does not contain a valid array");
            }
            
            // Parse and count objects (simplified - assumes one object per line after '[')
            // In production, you'd want a proper JSON streaming parser
            int objectCount = 0;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("{")) {
                    objectPositions.add((long) objectCount);
                    objectCount++;
                } else if (line.startsWith("]")) {
                    break; // End of array
                }
            }
        }
        
        if (objectPositions.isEmpty()) {
            return new FilePartition[0];
        }
        
        int totalObjects = objectPositions.size();
        int objectsPerPartition = Math.max(1, totalObjects / targetPartitions);
        int actualPartitions = (int) Math.ceil((double) totalObjects / objectsPerPartition);
        
        logger.info("JSON Array analysis: {} total objects, {} objects per partition, {} actual partitions", 
            totalObjects, objectsPerPartition, actualPartitions);
        
        FilePartition[] partitions = new FilePartition[actualPartitions];
        
        for (int i = 0; i < actualPartitions; i++) {
            int startObject = i * objectsPerPartition;
            int endObject = Math.min(startObject + objectsPerPartition - 1, totalObjects - 1);
            int partitionObjects = endObject - startObject + 1;
            
            // JSON-specific metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("format", format);
            metadata.put("startObject", startObject);
            metadata.put("endObject", endObject);
            metadata.put("recordCount", partitionObjects);
            metadata.put("encoding", encoding);
            
            partitions[i] = new FilePartition(
                i,
                filePath,
                startObject, // Use object index as offset
                partitionObjects, // Use object count as length
                metadata
            );
            
            logger.debug("JSON Partition {}: objects {}-{} ({} records)", 
                i, startObject, endObject, partitionObjects);
        }
        
        return partitions;
    }
    
    @Override
    public Iterator<JsonNode> readPartition(String filePath, FilePartition partition) {
        try {
            JsonFormat partitionFormat = partition.getMetadata("format");
            String partitionEncoding = partition.getMetadata("encoding");
            
            logger.debug("Reading JSON partition {} with format {}", 
                partition.index(), partitionFormat);
            
            if (partitionFormat == JsonFormat.JSON_LINES) {
                return readJsonLinesPartition(filePath, partition, partitionEncoding);
            } else {
                return readJsonArrayPartition(filePath, partition, partitionEncoding);
            }
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to read JSON partition " + partition.index(), e);
        }
    }
    
    private Iterator<JsonNode> readJsonLinesPartition(String filePath, FilePartition partition, String encoding) 
            throws IOException {
        Long startLine = partition.getMetadata("startLine");
        Long endLine = partition.getMetadata("endLine");
        
        List<JsonNode> partitionData = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(filePath), 
                java.nio.charset.Charset.forName(encoding))) {
            
            String line;
            long currentLine = 0;
            
            // Skip to start line
            while (currentLine < startLine && (line = reader.readLine()) != null) {
                currentLine++;
            }
            
            // Read lines for this partition
            while (currentLine <= endLine && (line = reader.readLine()) != null) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(line);
                    if (jsonNode != null) {
                        partitionData.add(jsonNode);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to parse JSON on line {}: {}", currentLine, e.getMessage());
                }
                currentLine++;
            }
        }
        
        logger.debug("Read {} JSON records for partition {}", 
            partitionData.size(), partition.index());
        
        return partitionData.iterator();
    }
    
    private Iterator<JsonNode> readJsonArrayPartition(String filePath, FilePartition partition, String encoding) 
            throws IOException {
        Integer startObject = partition.getMetadata("startObject");
        Integer endObject = partition.getMetadata("endObject");
        
        List<JsonNode> partitionData = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(filePath), 
                java.nio.charset.Charset.forName(encoding))) {
            
            // Find array start
            String line;
            boolean foundArrayStart = false;
            
            while ((line = reader.readLine()) != null) {
                if (line.trim().startsWith("[")) {
                    foundArrayStart = true;
                    break;
                }
            }
            
            if (!foundArrayStart) {
                throw new IllegalArgumentException("JSON array not found");
            }
            
            // Skip to start object
            int currentObject = 0;
            while (currentObject < startObject && (line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("{")) {
                    currentObject++;
                }
            }
            
            // Read objects for this partition
            while (currentObject <= endObject && (line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("{")) {
                    try {
                        // Read the complete JSON object (simplified - assumes single line)
                        JsonNode jsonNode = objectMapper.readTree(line.replaceAll(",$", ""));
                        if (jsonNode != null) {
                            partitionData.add(jsonNode);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to parse JSON object {}: {}", currentObject, e.getMessage());
                    }
                    currentObject++;
                } else if (line.startsWith("]")) {
                    break; // End of array
                }
            }
        }
        
        logger.debug("Read {} JSON records for partition {}", 
            partitionData.size(), partition.index());
        
        return partitionData.iterator();
    }
    
    @Override
    public String getFormatName() {
        return "JSON";
    }
    
    @Override
    public List<String> getPreferredLocations(String filePath, FilePartition partition) {
        // For local files, no specific location preference
        // In a distributed setup, this could return the nodes where the file blocks are located
        return Collections.emptyList();
    }
} 