package minispark.distributed.rdd;

import java.util.Iterator;
import java.util.List;

/**
 * A partition implementation for ParallelCollectionRDD that stores data locally.
 * Each partition contains a subset of the original collection data.
 *
 * @param <T> The type of data stored in this partition
 */
public class ParallelCollectionPartition<T> implements Partition {
    private final int partitionId;
    private final Iterator<T> iterator;

    /**
     * Create a new ParallelCollectionPartition with the given ID and data.
     *
     * @param partitionId The partition index
     * @param data The data elements for this partition
     */
    public ParallelCollectionPartition(int partitionId, List<T> data) {
        this.partitionId = partitionId;
        this.iterator = data.iterator();
    }

    /**
     * Create a new ParallelCollectionPartition with the given ID and iterator.
     *
     * @param partitionId The partition index
     * @param iterator Iterator over the data elements for this partition
     */
    public ParallelCollectionPartition(int partitionId, Iterator<T> iterator) {
        this.partitionId = partitionId;
        this.iterator = iterator;
    }

    @Override
    public int index() {
        return partitionId;
    }

    /**
     * Get the iterator over the data elements in this partition.
     *
     * @return Iterator over data elements
     */
    public Iterator<T> iterator() {
        return iterator;
    }
} 