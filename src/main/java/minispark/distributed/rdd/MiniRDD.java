package minispark.distributed.rdd;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * MiniRDD is an abstract base class that represents a Resilient Distributed Dataset,
 * similar to Apache Spark's RDD. It provides the foundation for distributed data processing
 * operations with transformation methods that are evaluated lazily.
 *
 * @param <T> The type of elements in this RDD
 */
public interface MiniRDD<T> {
    /**
     * Default number of partitions if not specified.
     */
    int DEFAULT_NUM_PARTITIONS = 2;

    // Core RDD properties from Spark
    Partition[] getPartitions();
    CompletableFuture<Iterator<T>> compute(Partition split);
    List<MiniRDD<?>> getDependencies();
    
    // Optional properties
    List<String> getPreferredLocations(Partition split);
    
    // Transformations (lazy)
    <R> MiniRDD<R> map(Function<T, R> f);
    MiniRDD<T> filter(Predicate<T> f);
    
    // Actions (eager)
    CompletableFuture<List<T>> collect();
}

