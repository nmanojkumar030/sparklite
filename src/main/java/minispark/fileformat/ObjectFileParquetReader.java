package minispark.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import minispark.objectstore.Client;

/**
 * Enhanced Parquet format reader that creates partitions based on row groups.
 * Specialized for object store access with efficient range reads.
 * Each partition corresponds to one or more row groups in the Parquet file.
 */
public class ObjectFileParquetReader implements FormatReader<Group> {
    private static final Logger logger = LoggerFactory.getLogger(ObjectFileParquetReader.class);
    private final Configuration hadoopConf;
    private final Client objectStoreClient;
    
    /**
     * Create an ObjectFileParquetReader with object store support for efficient range reads.
     * This enables S3-style range requests for reading only the footer and specific row groups.
     */
    public ObjectFileParquetReader(Configuration hadoopConf, Client objectStoreClient) {
        this.hadoopConf = hadoopConf;
        this.objectStoreClient = objectStoreClient;
        if (objectStoreClient == null) {
            throw new IllegalArgumentException("ObjectStoreClient cannot be null for ObjectFileParquetReader");
        }
    }
    
    @Override
    public FilePartition[] createPartitions(String filePath, int targetPartitions) {
        try {
            // Use efficient range reads for object store
            logger.info("Using object store range reads for footer metadata: {}", filePath);
            
            // DETERMINISM FIX: Use async factory method instead of blocking constructor
            CompletableFuture<ObjectStoreInputFile> inputFileFuture = ObjectStoreInputFile.create(objectStoreClient, filePath);
            
            // Since this method is called from supplyAsync context with tick progression,
            // we can safely wait for the future to complete
            ObjectStoreInputFile inputFile;
            try {
                inputFile = inputFileFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException("Failed to create ObjectStoreInputFile for " + filePath, e);
            }
            
            ParquetMetadata parquetMetadata;
            // Use try-with-resources to read footer
            try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
                parquetMetadata = fileReader.getFooter();
                logger.info("Successfully read footer using range reads - file size: {} bytes", inputFile.getLength());
            }
            
            List<BlockMetaData> rowGroups = parquetMetadata.getBlocks();
            logger.info("Parquet file {} has {} row groups, target partitions: {}", 
                filePath, rowGroups.size(), targetPartitions);
            
            if (rowGroups.isEmpty()) {
                return new FilePartition[0];
            }
            
            // Strategy: Distribute row groups across partitions
            // If we have more row groups than target partitions, group multiple row groups per partition
            // If we have fewer row groups than target partitions, create one partition per row group
            
            int actualPartitions = Math.min(targetPartitions, rowGroups.size());
            FilePartition[] partitions = new FilePartition[actualPartitions];
            
            // Calculate row groups per partition
            int rowGroupsPerPartition = rowGroups.size() / actualPartitions;
            int remainingRowGroups = rowGroups.size() % actualPartitions;
            
            int currentRowGroupIndex = 0;
            for (int partitionIndex = 0; partitionIndex < actualPartitions; partitionIndex++) {
                // Some partitions get an extra row group if there's a remainder
                int rowGroupsInThisPartition = rowGroupsPerPartition + 
                    (partitionIndex < remainingRowGroups ? 1 : 0);
                
                List<Integer> rowGroupIndices = new ArrayList<>();
                long totalRows = 0;
                long startOffset = Long.MAX_VALUE;
                long endOffset = 0;
                
                for (int i = 0; i < rowGroupsInThisPartition; i++) {
                    BlockMetaData rowGroup = rowGroups.get(currentRowGroupIndex);
                    rowGroupIndices.add(currentRowGroupIndex);
                    totalRows += rowGroup.getRowCount();
                    
                    // Track the physical byte range for this partition
                    long rowGroupStart = rowGroup.getStartingPos();
                    long rowGroupEnd = rowGroupStart + rowGroup.getTotalByteSize();
                    startOffset = Math.min(startOffset, rowGroupStart);
                    endOffset = Math.max(endOffset, rowGroupEnd);
                    
                    currentRowGroupIndex++;
                }
                
                // Create partition metadata
                Map<String, Object> partitionMetadata = new HashMap<>();
                partitionMetadata.put("rowGroupIndices", rowGroupIndices);
                partitionMetadata.put("totalRows", totalRows);
                partitionMetadata.put("schema", parquetMetadata.getFileMetaData().getSchema());
                partitionMetadata.put("isObjectStore", true);
                
                long length = endOffset - startOffset;
                partitions[partitionIndex] = new FilePartition(
                    partitionIndex, filePath, startOffset, length, partitionMetadata
                );
                
                logger.debug("Created partition {} with {} row groups, {} rows, offset={}-{}, objectStore=true", 
                    partitionIndex, rowGroupsInThisPartition, totalRows, startOffset, endOffset);
            }
            
            return partitions;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Parquet metadata from " + filePath, e);
        }
    }
    
    @Override
    public Iterator<Group> readPartition(String filePath, FilePartition partition) {
        try {
            @SuppressWarnings("unchecked")
            List<Integer> rowGroupIndices = partition.getMetadata("rowGroupIndices");
            
            logger.debug("Reading partition {} with row groups {} from object store", 
                partition.index(), rowGroupIndices);
            
            List<Group> partitionData = new ArrayList<>();
            
            // Use efficient object store range reads
            readPartitionFromObjectStore(filePath, rowGroupIndices, partitionData);
            
            logger.debug("Read {} total records for partition {}", partitionData.size(), partition.index());
            return partitionData.iterator();
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Parquet partition " + partition.index(), e);
        }
    }
    
    private void readPartitionFromObjectStore(String filePath, List<Integer> rowGroupIndices, List<Group> partitionData) throws IOException {
        logger.debug("Reading from object store with range reads: {}", filePath);
        
        // DETERMINISM FIX: Use async factory method instead of blocking constructor
        CompletableFuture<ObjectStoreInputFile> inputFileFuture = ObjectStoreInputFile.create(objectStoreClient, filePath);
        
        ObjectStoreInputFile inputFile;
        try {
            inputFile = inputFileFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to create ObjectStoreInputFile for " + filePath, e);
        }
        
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            // Get the schema for creating groups
            org.apache.parquet.schema.MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
            
            // Read only the row groups assigned to this partition using range reads
            for (Integer rowGroupIndex : rowGroupIndices) {
                logger.debug("Reading row group {} using range reads for partition", rowGroupIndex);
                
                // Read specific row group - this will use range reads automatically
                org.apache.parquet.hadoop.metadata.BlockMetaData blockMetaData = 
                    fileReader.getFooter().getBlocks().get(rowGroupIndex);
                
                // Create a record reader for this row group
                org.apache.parquet.column.page.PageReadStore pageStore = fileReader.readRowGroup(rowGroupIndex);
                
                // Convert pages to records using GroupRecordConverter
                org.apache.parquet.io.MessageColumnIO columnIO = 
                    new org.apache.parquet.io.ColumnIOFactory().getColumnIO(schema);
                
                org.apache.parquet.io.RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, 
                    new org.apache.parquet.example.data.simple.convert.GroupRecordConverter(schema));
                
                // Read all records from this row group
                long rowCount = blockMetaData.getRowCount();
                for (long i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    if (group != null) {
                        partitionData.add(group);
                    }
                }
                
                logger.debug("Read {} records from row group {} using range reads", 
                    rowCount, rowGroupIndex);
            }
        }
    }
    
    @Override
    public List<String> getPreferredLocations(String filePath, FilePartition partition) {
        // For object store, you might return the regions or availability zones
        // This is a simplified implementation
        return Collections.emptyList();
    }
    
    @Override
    public String getFormatName() {
        return "parquet-objectstore";
    }
} 