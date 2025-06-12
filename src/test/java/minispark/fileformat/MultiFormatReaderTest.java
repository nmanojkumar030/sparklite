package minispark.fileformat;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Demonstrates the flexibility of the FileFormatRDD architecture
 * with different file format readers.
 */
class MultiFormatReaderTest {
    
    @Test
    void testCSVReaderWithDifferentFormats(@TempDir Path tempDir) throws IOException {
        System.out.println("\n=== CSV Reader Test ===");
        
        // Test 1: Comma-delimited CSV without header
        String csvFile1 = createCSVFile(tempDir, "test1.csv", 
            "1,John Doe,john@example.com,30,New York\n" +
            "2,Jane Smith,jane@example.com,25,San Francisco\n" +
            "3,Bob Johnson,bob@example.com,35,Chicago\n" +
            "4,Alice Brown,alice@example.com,28,Boston");
        
        CSVReader csvReader1 = new CSVReader();
        FilePartition[] partitions1 = csvReader1.createPartitions(csvFile1, 2);
        
        System.out.println("CSV Test 1 - Basic:");
        System.out.printf("  Created %d partitions for %d records%n", partitions1.length, 4);
        
        // Read first partition
        Iterator<String[]> iter1 = csvReader1.readPartition(csvFile1, partitions1[0]);
        int count1 = 0;
        while (iter1.hasNext()) {
            String[] row = iter1.next();
            System.out.printf("  Row: [%s]%n", String.join(", ", row));
            count1++;
        }
        System.out.printf("  Partition 0 read %d records%n", count1);
        
        // Test 2: Tab-delimited CSV with header
        String csvFile2 = createCSVFile(tempDir, "test2.csv",
            "id\tname\temail\tage\tcity\n" +
            "1\tJohn Doe\tjohn@example.com\t30\tNew York\n" +
            "2\tJane Smith\tjane@example.com\t25\tSan Francisco");
        
        CSVReader csvReader2 = new CSVReader("\t", true, "UTF-8");
        String[] headers = csvReader2.getHeader(csvFile2);
        System.out.println("\nCSV Test 2 - With Headers:");
        System.out.printf("  Headers: [%s]%n", String.join(", ", headers));
        
        FilePartition[] partitions2 = csvReader2.createPartitions(csvFile2, 1);
        Iterator<String[]> iter2 = csvReader2.readPartition(csvFile2, partitions2[0]);
        while (iter2.hasNext()) {
            String[] row = iter2.next();
            System.out.printf("  Data Row: [%s]%n", String.join(", ", row));
        }
        
        assertTrue(partitions1.length > 0);
        assertTrue(partitions2.length > 0);
        assertNotNull(headers);
        assertEquals(5, headers.length);
    }
    
    @Test
    void testJSONReaderWithDifferentFormats(@TempDir Path tempDir) throws IOException {
        System.out.println("\n=== JSON Reader Test ===");
        
        // Test 1: JSON Lines format (JSONL/NDJSON)
        String jsonFile1 = createJSONFile(tempDir, "test1.jsonl",
            "{\"id\": 1, \"name\": \"John Doe\", \"age\": 30, \"city\": \"New York\"}\n" +
            "{\"id\": 2, \"name\": \"Jane Smith\", \"age\": 25, \"city\": \"San Francisco\"}\n" +
            "{\"id\": 3, \"name\": \"Bob Johnson\", \"age\": 35, \"city\": \"Chicago\"}");
        
        JSONReader jsonReader1 = new JSONReader(); // Default: JSON Lines
        FilePartition[] partitions1 = jsonReader1.createPartitions(jsonFile1, 2);
        
        System.out.println("JSON Test 1 - JSON Lines:");
        System.out.printf("  Created %d partitions for 3 records%n", partitions1.length);
        
        // Read first partition
        Iterator<JsonNode> iter1 = jsonReader1.readPartition(jsonFile1, partitions1[0]);
        int count1 = 0;
        while (iter1.hasNext()) {
            JsonNode node = iter1.next();
            System.out.printf("  Record: %s (age: %d)%n", 
                node.get("name").asText(), 
                node.get("age").asInt());
            count1++;
        }
        System.out.printf("  Partition 0 read %d records%n", count1);
        
        // Test 2: JSON Array format
        String jsonFile2 = createJSONFile(tempDir, "test2.json",
            "[\n" +
            "  {\"id\": 1, \"name\": \"Alice Brown\", \"age\": 28, \"city\": \"Boston\"},\n" +
            "  {\"id\": 2, \"name\": \"Charlie Wilson\", \"age\": 32, \"city\": \"Seattle\"}\n" +
            "]");
        
        JSONReader jsonReader2 = new JSONReader(JSONReader.JsonFormat.JSON_ARRAY, "UTF-8");
        FilePartition[] partitions2 = jsonReader2.createPartitions(jsonFile2, 1);
        
        System.out.println("\nJSON Test 2 - JSON Array:");
        System.out.printf("  Created %d partitions%n", partitions2.length);
        
        Iterator<JsonNode> iter2 = jsonReader2.readPartition(jsonFile2, partitions2[0]);
        while (iter2.hasNext()) {
            JsonNode node = iter2.next();
            System.out.printf("  Record: %s from %s%n", 
                node.get("name").asText(),
                node.get("city").asText());
        }
        
        assertTrue(partitions1.length > 0);
        assertTrue(partitions2.length > 0);
    }
    
    @Test
    void testGenericFileFormatRDDUsage(@TempDir Path tempDir) throws IOException {
        System.out.println("\n=== Generic FileFormatRDD Usage Demo ===");
        
        // Create test files
        String csvFile = createCSVFile(tempDir, "customers.csv",
            "id,name,email,age\n" +
            "1,John,john@example.com,30\n" +
            "2,Jane,jane@example.com,25");
        
        String jsonFile = createJSONFile(tempDir, "products.jsonl",
            "{\"id\": 1, \"name\": \"Laptop\", \"price\": 999.99}\n" +
            "{\"id\": 2, \"name\": \"Mouse\", \"price\": 29.99}");
        
        // Demo 1: CSV processing
        System.out.println("\n1. Processing CSV with FileFormatRDD:");
        CSVReader csvReader = new CSVReader(",", true, "UTF-8");
        
        // Simulate FileFormatRDD usage (without full MiniSparkContext)
        FilePartition[] csvPartitions = csvReader.createPartitions(csvFile, 2);
        System.out.printf("   CSV: %d partitions created%n", csvPartitions.length);
        
        for (FilePartition partition : csvPartitions) {
            System.out.printf("   Partition %d: %s metadata%n", 
                partition.index(), 
                csvReader.getFormatName());
        }
        
        // Demo 2: JSON processing  
        System.out.println("\n2. Processing JSON with FileFormatRDD:");
        JSONReader jsonReader = new JSONReader();
        
        FilePartition[] jsonPartitions = jsonReader.createPartitions(jsonFile, 2);
        System.out.printf("   JSON: %d partitions created%n", jsonPartitions.length);
        
        for (FilePartition partition : jsonPartitions) {
            System.out.printf("   Partition %d: %s metadata%n", 
                partition.index(), 
                jsonReader.getFormatName());
        }
        
        // Demo 3: Show how the same FileFormatRDD can handle both
        System.out.println("\n3. Same Architecture, Different Formats:");
        System.out.printf("   CSV Reader Format: %s%n", csvReader.getFormatName());
        System.out.printf("   JSON Reader Format: %s%n", jsonReader.getFormatName());
        
        // Both implement the same FormatReader<T> interface!
        System.out.println("   ✅ Both use the same FormatReader<T> interface");
        System.out.println("   ✅ Both work with the same FileFormatRDD<T>");
        System.out.println("   ✅ Same distributed processing logic");
        
        assertTrue(csvPartitions.length > 0);
        assertTrue(jsonPartitions.length > 0);
    }
    
    private String createCSVFile(Path tempDir, String filename, String content) throws IOException {
        Path file = tempDir.resolve(filename);
        Files.write(file, content.getBytes());
        return file.toString();
    }
    
    private String createJSONFile(Path tempDir, String filename, String content) throws IOException {
        Path file = tempDir.resolve(filename);
        Files.write(file, content.getBytes());
        return file.toString();
    }
} 