package minispark.fileformat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * CSV format reader that creates partitions based on line ranges.
 * Each partition processes a specific range of lines from the CSV file.
 */
public class CSVReader implements FormatReader<String[]> {
    private static final Logger logger = LoggerFactory.getLogger(CSVReader.class);
    
    private final String delimiter;
    private final boolean hasHeader;
    private final String encoding;
    
    /**
     * Create a CSV reader with default settings (comma delimiter, UTF-8, no header).
     */
    public CSVReader() {
        this(",", false, "UTF-8");
    }
    
    /**
     * Create a CSV reader with custom settings.
     *
     * @param delimiter The field delimiter (e.g., ",", "\t", "|")
     * @param hasHeader Whether the first line is a header
     * @param encoding File encoding (e.g., "UTF-8", "ISO-8859-1")
     */
    public CSVReader(String delimiter, boolean hasHeader, String encoding) {
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
        this.encoding = encoding;
    }
    
    @Override
    public FilePartition[] createPartitions(String filePath, int targetPartitions) {
        try {
            logger.info("Analyzing CSV file: {} with target partitions: {}", filePath, targetPartitions);
            
            // Count total lines in the file
            long totalLines;
            try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
                totalLines = lines.count();
            }
            
            if (totalLines == 0) {
                return new FilePartition[0];
            }
            
            // Adjust for header
            long dataLines = hasHeader ? totalLines - 1 : totalLines;
            long headerLines = hasHeader ? 1 : 0;
            
            // Calculate lines per partition
            long linesPerPartition = Math.max(1, dataLines / targetPartitions);
            int actualPartitions = (int) Math.ceil((double) dataLines / linesPerPartition);
            
            logger.info("CSV analysis: {} total lines, {} data lines, {} lines per partition, {} actual partitions", 
                totalLines, dataLines, linesPerPartition, actualPartitions);
            
            FilePartition[] partitions = new FilePartition[actualPartitions];
            
            for (int i = 0; i < actualPartitions; i++) {
                long startLine = headerLines + (i * linesPerPartition);
                long endLine = Math.min(startLine + linesPerPartition - 1, totalLines - 1);
                long partitionLines = endLine - startLine + 1;
                
                // CSV-specific metadata
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("startLine", startLine);
                metadata.put("endLine", endLine);
                metadata.put("lineCount", partitionLines);
                metadata.put("delimiter", delimiter);
                metadata.put("hasHeader", hasHeader);
                metadata.put("encoding", encoding);
                metadata.put("headerLine", hasHeader ? 0L : null);
                
                partitions[i] = new FilePartition(
                    i,
                    filePath,
                    startLine, // Use line number as offset
                    partitionLines, // Use line count as length
                    metadata
                );
                
                logger.debug("CSV Partition {}: lines {}-{} ({} lines)", 
                    i, startLine, endLine, partitionLines);
            }
            
            return partitions;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to analyze CSV file: " + filePath, e);
        }
    }
    
    @Override
    public Iterator<String[]> readPartition(String filePath, FilePartition partition) {
        try {
            Long startLine = partition.getMetadata("startLine");
            Long endLine = partition.getMetadata("endLine");
            String partitionDelimiter = partition.getMetadata("delimiter");
            String partitionEncoding = partition.getMetadata("encoding");
            
            logger.debug("Reading CSV partition {} lines {}-{}", 
                partition.index(), startLine, endLine);
            
            List<String[]> partitionData = new ArrayList<>();
            
            try (BufferedReader reader = Files.newBufferedReader(
                    Paths.get(filePath), 
                    java.nio.charset.Charset.forName(partitionEncoding))) {
                
                String line;
                long currentLine = 0;
                
                // Skip to start line
                while (currentLine < startLine && (line = reader.readLine()) != null) {
                    currentLine++;
                }
                
                // Read lines for this partition
                while (currentLine <= endLine && (line = reader.readLine()) != null) {
                    String[] fields = parseCsvLine(line, partitionDelimiter);
                    partitionData.add(fields);
                    currentLine++;
                }
            }
            
            logger.debug("Read {} CSV records for partition {}", 
                partitionData.size(), partition.index());
            
            return partitionData.iterator();
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CSV partition " + partition.index(), e);
        }
    }
    
    /**
     * Parse a CSV line handling quoted fields and escaped delimiters.
     * This is a simplified parser - for production use, consider Apache Commons CSV.
     */
    private String[] parseCsvLine(String line, String delimiter) {
        if (line == null || line.trim().isEmpty()) {
            return new String[0];
        }
        
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        boolean escapeNext = false;
        
        for (char c : line.toCharArray()) {
            if (escapeNext) {
                currentField.append(c);
                escapeNext = false;
            } else if (c == '\\') {
                escapeNext = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes && line.substring(line.indexOf(c)).startsWith(delimiter)) {
                fields.add(currentField.toString().trim());
                currentField = new StringBuilder();
                // Skip delimiter characters
                for (int i = 1; i < delimiter.length(); i++) {
                    // This is simplified - would need proper delimiter handling
                }
            } else {
                currentField.append(c);
            }
        }
        
        // Add the last field
        fields.add(currentField.toString().trim());
        
        return fields.toArray(new String[0]);
    }
    
    /**
     * Get the header row if the CSV has headers.
     *
     * @param filePath Path to the CSV file
     * @return Header fields, or null if no header
     */
    public String[] getHeader(String filePath) {
        if (!hasHeader) {
            return null;
        }
        
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(filePath), 
                java.nio.charset.Charset.forName(encoding))) {
            
            String headerLine = reader.readLine();
            if (headerLine != null) {
                return parseCsvLine(headerLine, delimiter);
            }
            
        } catch (IOException e) {
            logger.warn("Failed to read CSV header from: {}", filePath, e);
        }
        
        return null;
    }
    
    @Override
    public String getFormatName() {
        return "CSV";
    }
    
    @Override
    public List<String> getPreferredLocations(String filePath, FilePartition partition) {
        // For local files, no specific location preference
        // In a distributed setup, this could return the nodes where the file blocks are located
        return Collections.emptyList();
    }
} 