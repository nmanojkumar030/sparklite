package minispark.fileformat;

import minispark.MiniSparkContext;
import minispark.core.MiniRDD;
import minispark.core.Partition;
import minispark.core.transformations.FilterRDD;
import minispark.core.transformations.MapRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    private final FilePartition[] partitions;
    
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
        this.partitions = formatReader.createPartitions(filePath, targetPartitions);
        
        logger.info("Created FileFormatRDD for {} with {} partitions using {} reader", 
            filePath, partitions.length, formatReader.getFormatName());
    }
    
    /**
     * Convenience constructor with default partition count.
     */
    public FileFormatRDD(MiniSparkContext sc, String filePath, FormatReader<T> formatReader) {
        this(sc, filePath, formatReader, DEFAULT_NUM_PARTITIONS);
    }
    
    @Override
    public Partition[] getPartitions() {
        return partitions;
    }
    
    @Override
    public Iterator<T> compute(Partition split) {
        if (!(split instanceof FilePartition)) {
            throw new IllegalArgumentException("Expected FilePartition but got " + split.getClass().getName());
        }
        
        FilePartition filePartition = (FilePartition) split;
        logger.debug("Computing partition {} for file {} using {} reader", 
            split.index(), filePartition.getFilePath(), formatReader.getFormatName());
        
        try {
            return formatReader.readPartition(filePath, filePartition);
        } catch (Exception e) {
            logger.error("Failed to read partition {} from file {}: {}", 
                split.index(), filePath, e.getMessage(), e);
            throw new RuntimeException("Failed to read partition " + split.index(), e);
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
    public List<T> collect() {
        List<T> result = new ArrayList<>();
        for (Partition partition : getPartitions()) {
            Iterator<T> iter = compute(partition);
            while (iter.hasNext()) {
                result.add(iter.next());
            }
        }
        return result;
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