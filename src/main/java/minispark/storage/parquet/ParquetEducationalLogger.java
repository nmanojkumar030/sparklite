package minispark.storage.parquet;

import java.io.IOException;
import java.util.List;

/**
 * EDUCATIONAL: Utility class for Parquet storage educational logging.
 * 
 * Extracted from ParquetStorage to reduce toxicity:
 * - Reduces file length
 * - Decreases class coupling
 * - Improves single responsibility
 */
public class ParquetEducationalLogger {
    
    public static void logStorageCreation() {
        System.out.println("🏗️ EDUCATIONAL: Creating ParquetStorage");
        System.out.println("   📊 Columnar format optimized for analytics");
        System.out.println("   🔄 Batch-oriented writes with buffering");
        System.out.println("   📈 Excellent scan performance");
    }
    
    public static void logSingleWrite(byte[] key) {
        System.out.println("✏️ EDUCATIONAL: ParquetStorage.write()");
        System.out.println("   🔑 Key: " + new String(key));
        System.out.println("   📦 Buffering for batch efficiency");
    }
    
    public static void logBatchWrite(int recordCount) {
        System.out.println("📦 EDUCATIONAL: ParquetStorage.writeBatch()");
        System.out.println("   📊 Records: " + recordCount);
        System.out.println("   ✅ Strength: Efficient batch operations");
    }
    
    public static void logPointLookup(byte[] key) {
        System.out.println("🔍 EDUCATIONAL: ParquetStorage.read()");
        System.out.println("   🔑 Key: " + new String(key));
        System.out.println("   ⚠️ Trade-off: Slower point lookups vs excellent scans");
    }
    
    public static void logRangeScan(byte[] startKey, byte[] endKey, List<String> columns) {
        System.out.println("🔍 EDUCATIONAL: ParquetStorage.scan()");
        System.out.println("   📊 Range: [" + new String(startKey) + ", " + 
                          (endKey != null ? new String(endKey) : "END") + "]");
        System.out.println("   🎯 Columns: " + (columns != null ? columns : "ALL"));
        System.out.println("   ✅ Strength: Excellent for analytical scans");
    }
    
    public static void logDelete(byte[] key) {
        System.out.println("🗑️ EDUCATIONAL: ParquetStorage.delete()");
        System.out.println("   🔑 Key: " + new String(key));
        System.out.println("   📝 Strategy: Mark as deleted (immutable files)");
    }
    
    public static void logStorageClose() {
        System.out.println("🔒 EDUCATIONAL: Closing ParquetStorage");
        System.out.println("   💾 Flushing any buffered records");
        System.out.println("   🧹 Cleaning up resources");
    }
    
    // Parquet-specific operation logging
    
    public static void logParquetWriteStart(int recordCount, String filename) {
        System.out.println("   📝 PARQUET WRITE: Starting write operation");
        System.out.println("      Records: " + recordCount);
        System.out.println("      File: " + filename);
    }
    
    public static void logParquetWriteComplete(int recordCount, String filename) {
        System.out.println("   ✅ PARQUET WRITE: Successfully wrote " + recordCount + " records");
        System.out.println("      File: " + filename);
    }
    
    public static void logParquetWriteError(String filename, IOException e) {
        System.out.println("   ❌ PARQUET WRITE ERROR: " + e.getMessage());
        System.out.println("      File: " + filename);
    }
    
    public static void logParquetSearchStart(String filename, byte[] key) {
        System.out.println("   🔍 PARQUET SEARCH: Starting search operation");
        System.out.println("      File: " + filename);
        System.out.println("      Key: " + new String(key));
    }
    
    public static void logParquetSearchFound(String filename, byte[] key) {
        System.out.println("   ✅ PARQUET SEARCH: Found record");
        System.out.println("      File: " + filename);
        System.out.println("      Key: " + new String(key));
    }
    
    public static void logParquetSearchNotFound(String filename, byte[] key) {
        System.out.println("   ❌ PARQUET SEARCH: Record not found");
        System.out.println("      File: " + filename);
        System.out.println("      Key: " + new String(key));
    }
    
    public static void logParquetSearchError(String filename, byte[] key, IOException e) {
        System.out.println("   ❌ PARQUET SEARCH ERROR: " + e.getMessage());
        System.out.println("      File: " + filename);
        System.out.println("      Key: " + new String(key));
    }
    
    public static void logParquetScanStart(String filename, byte[] startKey, byte[] endKey, List<String> columns) {
        System.out.println("   🔍 PARQUET SCAN: Starting scan operation");
        System.out.println("      File: " + filename);
        System.out.println("      Start key: " + (startKey != null ? new String(startKey) : "BEGIN"));
        System.out.println("      End key: " + (endKey != null ? new String(endKey) : "END"));
        System.out.println("      Columns: " + (columns != null ? columns : "ALL"));
    }
    
    public static void logParquetScanComplete(String filename, int recordCount) {
        System.out.println("   ✅ PARQUET SCAN: Successfully scanned " + recordCount + " records");
        System.out.println("      File: " + filename);
    }
    
    public static void logParquetScanError(String filename, IOException e) {
        System.out.println("   ❌ PARQUET SCAN ERROR: " + e.getMessage());
        System.out.println("      File: " + filename);
    }
} 