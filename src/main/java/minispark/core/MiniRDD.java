package minispark.core;

import java.util.List;
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

    /**
     * Maps each element of this RDD using the given function.
     *
     * @param f The function to apply to each element
     * @param <R> The type of elements in the resulting RDD
     * @return A new RDD containing the mapped elements
     */
    <R> MiniRDD<R> map(Function<T, R> f);

    /**
     * Filters elements of this RDD using the given predicate.
     *
     * @param f The predicate to test each element against
     * @return A new RDD containing only the elements that satisfy the predicate
     */
    MiniRDD<T> filter(Predicate<T> f);

    /**
     * Collects all elements of this RDD into a list.
     *
     * @return A list containing all elements
     */
    List<T> collect();

    MiniRDD<T> getParent();

    Function<?, T> getTransformation();
}

