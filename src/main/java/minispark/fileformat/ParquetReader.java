package minispark.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;

import java.util.concurrent.CompletableFuture;

/**
 * Parquet format reader that creates partitions based on row groups.
 * This implementation is for local filesystem access only.
 * Each partition corresponds to one or more row groups in the Parquet file.
 */
public class ParquetReader implements FormatReader<Group> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetReader.class);
    private final Configuration hadoopConf;
    
    /**
     * Create a ParquetReader for local filesystem access.
     */
    public ParquetReader() {
        this.hadoopConf = new Configuration();
    }
    
    /**
     * Create a ParquetReader for local filesystem access with custom Hadoop configuration.
     */
    public ParquetReader(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }
    
    @Override
    public CompletableFuture<FilePartition[]> createPartitions(String filePath, int targetPartitions) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Use traditional Hadoop filesystem
                logger.info("Using Hadoop filesystem for: {}", filePath);
                Path path = new Path(filePath);
                ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(hadoopConf, path);
                
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
                    
                    long length = endOffset - startOffset;
                    partitions[partitionIndex] = new FilePartition(
                        partitionIndex, filePath, startOffset, length, partitionMetadata
                    );
                    
                    logger.debug("Created partition {} with {} row groups, {} rows, offset={}-{}", 
                        partitionIndex, rowGroupsInThisPartition, totalRows, startOffset, endOffset);
                }
                
                return partitions;
                
            } catch (IOException e) {
                throw new RuntimeException("Failed to read Parquet metadata from " + filePath, e);
            }
        });
    }
    
    @Override
    public Iterator<Group> readPartition(String filePath, FilePartition partition) {
        try {
            @SuppressWarnings("unchecked")
            List<Integer> rowGroupIndices = partition.getMetadata("rowGroupIndices");
            
            logger.debug("Reading partition {} with row groups {}", 
                partition.index(), rowGroupIndices);
            
            List<Group> partitionData = new ArrayList<>();
            
            // Use traditional Hadoop filesystem
            readPartitionFromHadoop(filePath, rowGroupIndices, partitionData);
            
            logger.debug("Read {} total records for partition {}", partitionData.size(), partition.index());
            return partitionData.iterator();
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Parquet partition " + partition.index(), e);
        }
    }
    

    
    private void readPartitionFromHadoop(String filePath, List<Integer> rowGroupIndices, List<Group> partitionData) throws IOException {
        logger.debug("Reading from Hadoop filesystem: {}", filePath);
        
        Path path = new Path(filePath);
        
        // OPTIMIZED: Read only specific row groups using ParquetFileReader
        try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoopConf))) {
            
            // Get the schema for creating groups
            org.apache.parquet.schema.MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
            
            // Read only the row groups assigned to this partition
            for (Integer rowGroupIndex : rowGroupIndices) {
                logger.debug("Reading row group {} for partition {}", rowGroupIndex, rowGroupIndices);
                
                // Read specific row group
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
                
                logger.debug("Read {} records from row group {} for partition", 
                    rowCount, rowGroupIndex);
            }
        }
    }
    
    @Override
    public List<String> getPreferredLocations(String filePath, FilePartition partition) {
        // In a real implementation, you would extract block locations from HDFS
        return Collections.emptyList();
    }
    
    @Override
    public String getFormatName() {
        return "parquet";
    }
} 