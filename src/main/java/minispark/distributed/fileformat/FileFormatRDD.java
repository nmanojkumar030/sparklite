package minispark.distributed.fileformat;

import minispark.MiniSparkContext;
import minispark.distributed.rdd.MiniRDD;
import minispark.distributed.rdd.Partition;
import minispark.distributed.rdd.transformations.FilterRDD;
import minispark.distributed.rdd.transformations.MapRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Generic RDD implementation that can read different file formats.
 * This RDD delegates format-specific operations to FormatReader implementations.
 *
 * @param <T> The type of records in this RDD
 */
public class FileFormatRDD<T> implements MiniRDD<T> {
    private static final Logger logger = LoggerFactory.getLogger(FileFormatRDD.class);
    
    private final MiniSparkContext sc;
    private final String filePath;
    private final FormatReader<T> formatReader;
    private final CompletableFuture<FilePartition[]> partitionsFuture;
    
    /**
     * Create a new FileFormatRDD.
     *
     * @param sc Spark context
     * @param filePath Path to the file to read
     * @param formatReader Format-specific reader implementation
     * @param targetPartitions Desired number of partitions (hint)
     */
    public FileFormatRDD(MiniSparkContext sc, String filePath, FormatReader<T> formatReader, int targetPartitions) {
        this.sc = sc;
        this.filePath = filePath;
        this.formatReader = formatReader;
        this.partitionsFuture = formatReader.createPartitions(filePath, targetPartitions);
        
        // Log when partitions are ready (async)
        this.partitionsFuture.thenAccept(partitions -> 
            logger.info("Created FileFormatRDD for {} with {} partitions using {} reader", 
                filePath, partitions.length, formatReader.getFormatName())
        );
    }
    
    /**
     * Convenience constructor with default partition count.
     */
    public FileFormatRDD(MiniSparkContext sc, String filePath, FormatReader<T> formatReader) {
        this(sc, filePath, formatReader, DEFAULT_NUM_PARTITIONS);
    }
    
    @Override
    public Partition[] getPartitions() {
        // For now, we need to block here since the RDD interface doesn't support async getPartitions
        // TODO: Consider making the entire RDD interface async in the future
        try {
            return partitionsFuture.join();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get partitions for " + filePath, e);
        }
    }
    
    @Override
    public CompletableFuture<Iterator<T>> compute(Partition split) {
        if (!(split instanceof FilePartition)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Expected FilePartition but got " + split.getClass().getName()));
        }
        
        FilePartition filePartition = (FilePartition) split;
        logger.debug("Computing partition {} for file {} using {} reader", 
            split.index(), filePartition.getFilePath(), formatReader.getFormatName());
        
        try {
            Iterator<T> result = formatReader.readPartition(filePath, filePartition);
            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            logger.error("Failed to read partition {} from file {}: {}", 
                split.index(), filePath, e.getMessage(), e);
            return CompletableFuture.failedFuture(new RuntimeException("Failed to read partition " + split.index(), e));
        }
    }
    
    @Override
    public List<MiniRDD<?>> getDependencies() {
        return Collections.emptyList();
    }
    
    @Override
    public List<String> getPreferredLocations(Partition split) {
        if (!(split instanceof FilePartition)) {
            return Collections.emptyList();
        }
        
        FilePartition filePartition = (FilePartition) split;
        return formatReader.getPreferredLocations(filePath, filePartition);
    }
    
    @Override
    public <R> MiniRDD<R> map(Function<T, R> f) {
        return new MapRDD<>(this, f);
    }
    
    @Override
    public MiniRDD<T> filter(Predicate<T> f) {
        return new FilterRDD<>(this, f);
    }
    
    @Override
    public CompletableFuture<List<T>> collect() {
        List<CompletableFuture<Iterator<T>>> futures = new ArrayList<>();
        
        for (Partition partition : getPartitions()) {
            futures.add(compute(partition));
        }
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<T> result = new ArrayList<>();
                for (CompletableFuture<Iterator<T>> future : futures) {
                    try {
                        Iterator<T> iter = future.join();
                        while (iter.hasNext()) {
                            result.add(iter.next());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get partition result", e);
                    }
                }
                return result;
            });
    }
    
    /**
     * Get the file path being read by this RDD.
     */
    public String getFilePath() {
        return filePath;
    }
    
    /**
     * Get the format reader used by this RDD.
     */
    public FormatReader<T> getFormatReader() {
        return formatReader;
    }
} 