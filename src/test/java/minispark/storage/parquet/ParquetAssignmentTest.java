package minispark.storage.parquet;

import minispark.storage.Record;
import minispark.storage.table.TableSchema;
import minispark.storage.parquet.assignment.TestCustomerParquetFileWriter;
import minispark.storage.parquet.assignment.SimpleParquetReader;
import minispark.storage.parquet.assignment.ParquetMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parquet Assignment Test: Workshop participants must implement SimpleParquetReader methods to pass.
 * 
 * Assignment Requirements:
 * - Implement readFooter() to extract Parquet metadata
 * - Implement selectRowGroups() for predicate pushdown optimization
 * - Implement readRowGroups() for selective I/O with column pruning
 * - Implement readWithFilter() to combine all optimizations
 * 
 * Learning Objectives:
 * - Understand how modern query engines optimize Parquet reads
 * - Experience real-world columnar storage optimization patterns
 * - Compare naive vs optimized performance approaches
 */
public class ParquetAssignmentTest {
    
    @TempDir
    Path tempDir;
    
    private String testFilePath;
    private TableSchema schema;
    private SimpleParquetReader reader;
    
    @BeforeEach
    void setUp() throws IOException {
        // Setup test file path
        testFilePath = tempDir.resolve("test-customers.parquet").toString();
        
        // Use existing schema infrastructure
        schema = TableSchema.createCustomerSchema();
        
        // Create reader instance (TODO - implement the methods)
        reader = new SimpleParquetReader(schema);
        
        // Create test file with structured row groups
        TestCustomerParquetFileWriter.createCustomersFile(testFilePath);
    }
    
    @Test
    @DisplayName("Efficient Parquet Reading with Metadata Optimization")
    public void testEfficientParquetReading() throws IOException {
        // Test Query: "Find customers with age > 35"
        final int ageThreshold = 35;
        
        // Approach 1: Naive - read entire file
        
        long startTime1 = System.currentTimeMillis();
        List<Record> allRecords = reader.readEntireFile(testFilePath);
        List<Record> naiveResults = allRecords.stream()
            .filter(r -> {
                Object ageObj = r.getValue().get("age");
                return ageObj instanceof Integer && (Integer) ageObj > ageThreshold;
            })
            .collect(Collectors.toList());
        long naiveTime = System.currentTimeMillis() - startTime1;
        
        // Approach 2: Optimized - use metadata for predicate pushdown
        long startTime2 = System.currentTimeMillis();
        
        try {
            // Use the optimized approach with integrated filtering
            List<String> columnsToRead = Arrays.asList("id", "name", "age", "city");
            List<Record> optimizedResults = reader.readWithFilter(testFilePath, "age", ageThreshold, columnsToRead);
            
            long optimizedTime = System.currentTimeMillis() - startTime2;
            
            // For educational purposes, also demonstrate the step-by-step approach
            ParquetMetadata metadata = reader.readFooter(testFilePath);
            List<Integer> relevantRowGroups = reader.selectRowGroups(metadata, "age", ageThreshold);
            
            // Validation: Results should be identical
            assertEquals(naiveResults.size(), optimizedResults.size(), 
                "Optimized approach should return same number of results");
            
            // Performance analysis
            printPerformanceComparison(metadata, relevantRowGroups, naiveTime, optimizedTime, 
                naiveResults.size(), optimizedResults.size());
            
        } catch (UnsupportedOperationException e) {
            // FAIL the test when students haven't implemented methods yet
            fail("Assignment incomplete! implement: " + e.getMessage() +
                 "\nImplement readFooter(), selectRowGroups(), readRowGroups(), and readWithFilter() methods in SimpleParquetReader");
        }
    }
    
    @Test
    @DisplayName("Query Optimization Scenarios")
    public void testDifferentQueryScenarios() throws IOException {
        List<Record> allRecords = reader.readEntireFile(testFilePath);
        
        // Scenario 1: Query that can skip most row groups
        List<Record> highAgeResults = allRecords.stream()
            .filter(r -> getAge(r) > 50)
            .collect(Collectors.toList());
        
        // Scenario 2: Query that needs multiple row groups
        List<Record> midAgeResults = allRecords.stream()
            .filter(r -> {
                int age = getAge(r);
                return age >= 25 && age <= 40;
            })
            .collect(Collectors.toList());
        
        // Scenario 3: Query that needs all row groups
        List<Record> allAgeResults = allRecords.stream()
            .filter(r -> getAge(r) > 15)
            .collect(Collectors.toList());
        
        // Verify different query selectivities
        assertTrue(highAgeResults.size() < allRecords.size(), "High age filter should be selective");
        assertTrue(allAgeResults.size() == allRecords.size(), "Low age filter should include all records");
    }
    
    // Helper methods for educational analysis
    
    private void printPerformanceComparison(ParquetMetadata metadata, List<Integer> rowGroupsRead, 
                                          long naiveTime, long optimizedTime,
                                          int naiveResultCount, int optimizedResultCount) {
        double ioReduction = (1.0 - (double)rowGroupsRead.size() / metadata.getTotalRowGroups()) * 100;
        double speedup = naiveTime > 0 ? (double)naiveTime / optimizedTime : 1.0;
        
        System.out.printf("Performance: %d/%d row groups read (%.1f%% I/O reduction), %.1fx speedup%n", 
                         rowGroupsRead.size(), metadata.getTotalRowGroups(), ioReduction, speedup);
        
        // Verify performance optimization occurred
        assertTrue(rowGroupsRead.size() < metadata.getTotalRowGroups(), 
                  "Should skip at least one row group for optimization");
        assertEquals(naiveResultCount, optimizedResultCount, 
                    "Both approaches should return same number of results");
    }
    
    private void analyzeAgeDistribution(List<Record> records, int startId, int endId, String description) {
        List<Integer> ages = records.stream()
            .filter(r -> {
                Object idObj = r.getValue().get("id");
                if (idObj instanceof String) {
                    int id = Integer.parseInt((String) idObj);
                    return id >= startId && id <= endId;
                }
                return false;
            })
            .map(this::getAge)
            .collect(Collectors.toList());
        
        // Verify we have records in this range
        assertTrue(ages.size() > 0, "Should have records in range " + startId + "-" + endId);
    }
    
    private int getAge(Record record) {
        Object ageObj = record.getValue().get("age");
        return (ageObj instanceof Integer) ? (Integer) ageObj : 0;
    }
} 