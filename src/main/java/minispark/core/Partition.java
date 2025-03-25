package minispark.core;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Represents a partition of data in an RDD.
 *
 * @param <T> The type of data stored in this partition
 */
public class Partition<T> implements Serializable {
    private final int partitionId;
    private final Iterator<T> iterator;

    public Partition(int partitionId, Iterator<T> iterator) {
        this.partitionId = partitionId;
        this.iterator = iterator;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Iterator<T> iterator() {
        return iterator;
    }

    /**
     * Get the index of this partition.
     *
     * @return The partition index
     */
    public int index() {
        return partitionId;
    }
} 