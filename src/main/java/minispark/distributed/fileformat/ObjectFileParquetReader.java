package minispark.distributed.fileformat;

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

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import minispark.distributed.objectstore.Client;

import java.io.ByteArrayInputStream;

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
    public CompletableFuture<FilePartition[]> createPartitions(String filePath, int targetPartitions) {
        // Use direct object store range reads for footer metadata
        logger.info("Using direct object store range reads for footer metadata: {}", filePath);
        
        // Step 1: Get file size first
        return objectStoreClient.getObjectSize(filePath)
            .thenCompose(fileSize -> {
                logger.info("File size for {}: {} bytes", filePath, fileSize);
                
                // Step 2: Read Parquet footer using direct range read
                // Parquet footer is at the end of the file, typically last 1KB is enough for metadata
                long footerSize = Math.min(1024, fileSize); // Read last 1KB or entire file if smaller
                long footerStart = fileSize - footerSize;
                
                logger.info("Reading footer range {}-{} ({} bytes) from {}", 
                    footerStart, fileSize - 1, footerSize, filePath);
                
                return objectStoreClient.getObjectRange(filePath, footerStart, fileSize - 1)
                    .thenApply(footerBytes -> {
                        try {
                            // Step 3: Parse footer bytes to extract metadata
                            ParquetMetadata parquetMetadata = parseParquetFooter(footerBytes, fileSize);
                            
                            List<BlockMetaData> rowGroups = parquetMetadata.getBlocks();
                            logger.info("Parquet file {} has {} row groups, target partitions: {}", 
                                filePath, rowGroups.size(), targetPartitions);
                            
                            if (rowGroups.isEmpty()) {
                                return new FilePartition[0];
                            }
                            
                            // Step 4: Create partitions based on row group metadata
                            return createPartitionsFromRowGroups(filePath, rowGroups, parquetMetadata, targetPartitions);
                            
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to parse Parquet footer from " + filePath, e);
                        }
                    });
            });
    }
    
    /**
     * Parse Parquet footer bytes to extract metadata using direct byte array access.
     * This demonstrates how to read Parquet metadata without going through the full file.
     */
    private ParquetMetadata parseParquetFooter(byte[] footerBytes, long fileSize) throws IOException {
        // For educational purposes, we'll still use ParquetFileReader but with a ByteArrayInputStream
        // In a production system, you might want to implement direct footer parsing
        
        logger.debug("Parsing Parquet footer from {} bytes", footerBytes.length);
        
        // Create a temporary input stream from the footer bytes
        try (ByteArrayInputStream footerStream = new ByteArrayInputStream(footerBytes)) {
            // Use Parquet's built-in footer parsing, but note that we got the bytes via direct range read
            // This is a hybrid approach: direct range read + Parquet library parsing
            
            // Create a minimal InputFile that represents just the footer portion
            InputFile footerInputFile = new ByteArrayInputFile(footerBytes, fileSize);
            
            try (ParquetFileReader fileReader = ParquetFileReader.open(footerInputFile)) {
                ParquetMetadata metadata = fileReader.getFooter();
                logger.info("Successfully parsed footer using direct range read - {} row groups found", 
                    metadata.getBlocks().size());
                return metadata;
            }
        }
    }
    
    /**
     * Create partitions from row group metadata obtained via direct range reads.
     */
    private FilePartition[] createPartitionsFromRowGroups(String filePath, List<BlockMetaData> rowGroups, 
                                                         ParquetMetadata parquetMetadata, int targetPartitions) {
        
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
            partitionMetadata.put("directRangeRead", true); // Flag to indicate direct range read was used
            
            long length = endOffset - startOffset;
            partitions[partitionIndex] = new FilePartition(
                partitionIndex, filePath, startOffset, length, partitionMetadata
            );
            
            logger.debug("Created partition {} with {} row groups, {} rows, offset={}-{}, directRangeRead=true", 
                partitionIndex, rowGroupsInThisPartition, totalRows, startOffset, endOffset);
        }
        
        return partitions;
    }
    
    /**
     * Simple InputFile implementation for byte arrays to work with ParquetFileReader.
     * This allows us to parse footer metadata from bytes obtained via direct range reads.
     */
    private static class ByteArrayInputFile implements InputFile {
        private final byte[] data;
        private final long originalFileSize;
        
        ByteArrayInputFile(byte[] data, long originalFileSize) {
            this.data = data;
            this.originalFileSize = originalFileSize;
        }
        
        @Override
        public long getLength() {
            return originalFileSize; // Return original file size, not just footer size
        }
        
        @Override
        public SeekableInputStream newStream() {
            return new ByteArraySeekableInputStream(data, originalFileSize);
        }
    }
    
    /**
     * SeekableInputStream for byte arrays that simulates the full file size.
     * This is needed because Parquet footer parsing expects to seek to specific positions.
     */
    private static class ByteArraySeekableInputStream extends SeekableInputStream {
        private final byte[] data;
        private final long originalFileSize;
        private long position;
        
        ByteArraySeekableInputStream(byte[] data, long originalFileSize) {
            this.data = data;
            this.originalFileSize = originalFileSize;
            this.position = originalFileSize - data.length; // Start at the beginning of footer
        }
        
        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < originalFileSize - data.length || newPos > originalFileSize) {
                throw new IOException("Seek position " + newPos + " outside footer range");
            }
            this.position = newPos;
        }
        
        @Override
        public long getPos() throws IOException {
            return position;
        }
        
        @Override
        public int read() throws IOException {
            if (position >= originalFileSize) {
                return -1;
            }
            int dataIndex = (int) (position - (originalFileSize - data.length));
            if (dataIndex < 0 || dataIndex >= data.length) {
                return -1;
            }
            position++;
            return data[dataIndex] & 0xFF;
        }
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (position >= originalFileSize) {
                return -1;
            }
            
            int dataIndex = (int) (position - (originalFileSize - data.length));
            if (dataIndex < 0 || dataIndex >= data.length) {
                return -1;
            }
            
            int available = data.length - dataIndex;
            int toRead = Math.min(len, available);
            System.arraycopy(data, dataIndex, b, off, toRead);
            position += toRead;
            return toRead;
        }
        
        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (!buf.hasRemaining()) {
                return 0;
            }
            
            byte[] temp = new byte[buf.remaining()];
            int bytesRead = read(temp, 0, temp.length);
            
            if (bytesRead > 0) {
                buf.put(temp, 0, bytesRead);
            }
            
            return bytesRead;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            int totalRead = 0;
            while (totalRead < len) {
                int bytesRead = read(bytes, start + totalRead, len - totalRead);
                if (bytesRead == -1) {
                    throw new IOException("Reached EOF before reading required bytes");
                }
                totalRead += bytesRead;
            }
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                int bytesRead = read(buf);
                if (bytesRead == -1) {
                    throw new IOException("Reached EOF before filling buffer");
                }
            }
        }
        
        @Override
        public void close() throws IOException {
            // Nothing to close
        }
    }
    
    @Override
    public Iterator<Group> readPartition(String filePath, FilePartition partition) {
        @SuppressWarnings("unchecked")
        List<Integer> rowGroupIndices = partition.getMetadata("rowGroupIndices");
        
        logger.debug("Reading partition {} with row groups {} from object store", 
            partition.index(), rowGroupIndices);
        
        try {
            // For now, we need to block here since the interface doesn't support async yet
            // TODO: Consider making readPartition async as well in the future
            return readPartitionFromObjectStore(filePath, rowGroupIndices).join();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read Parquet partition " + partition.index(), e);
        }
    }
    
    private CompletableFuture<Iterator<Group>> readPartitionFromObjectStore(String filePath, List<Integer> rowGroupIndices) {
        logger.debug("Reading from object store with range reads: {}", filePath);
        
        // DETERMINISM FIX: Use async factory method instead of blocking constructor
        CompletableFuture<ObjectStoreInputFile> inputFileFuture = ObjectStoreInputFile.create(objectStoreClient, filePath);
        
        return inputFileFuture.thenApply(inputFile -> {
            try {
                List<Group> partitionData = new ArrayList<>();
                
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
                
                logger.debug("Read {} total records for partition", partitionData.size());
                return partitionData.iterator();
                
            } catch (IOException e) {
                throw new RuntimeException("Failed to read from object store", e);
            }
        });
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