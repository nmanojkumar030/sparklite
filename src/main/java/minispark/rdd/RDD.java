package minispark.rdd;

import java.util.List;

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. Each RDD has five main
 * properties:
 * - A list of partitions
 * - A function for computing each split
 * - A list of dependencies on other RDDs
 * - Optionally, a Partitioner for key-value RDDs
 * - Optionally, a list of preferred locations to compute each split on
 */
public abstract class RDD<T> {
    private final int id;
    private final List<RDD<?>> dependencies;
    private final int numPartitions;

    protected RDD(int id, List<RDD<?>> dependencies, int numPartitions) {
        this.id = id;
        this.dependencies = dependencies;
        this.numPartitions = numPartitions;
    }

    public int getId() {
        return id;
    }

    public List<RDD<?>> getDependencies() {
        return dependencies;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Computes a partition of this RDD and returns an iterator over its elements.
     */
    public abstract List<T> compute(int partition);

    /**
     * Returns the preferred locations to compute this partition on.
     */
    public List<String> getPreferredLocations(int partition) {
        return List.of(); // Default implementation returns empty list
    }

    /**
     * Returns true if this RDD is already materialized in memory or on disk.
     */
    public boolean isCached() {
        return false; // Default implementation returns false
    }

    @Override
    public String toString() {
        return String.format("RDD[%d]", id);
    }
} 