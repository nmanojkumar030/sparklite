package minispark.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A task that executes a partition of an RDD and returns the results.
 * This task directly computes the RDD's data for a specific partition.
 *
 * @param <T> The type of data in the RDD
 */
public class RDDTask<T> extends Task<T, T> {
    private final MiniRDD<T> rdd;

    public RDDTask(int taskId, int stageId, int partitionId, MiniRDD<T> rdd) {
        super(taskId, stageId, partitionId);
        this.rdd = rdd;
    }

    @Override
    public T execute(Partition partition) {
        // Use the partition to compute RDD results
        Iterator<T> iterator = rdd.compute(partition);
        List<T> results = new ArrayList<>();
        
        // Collect all results from this partition
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        
        // Return the first result (this is a simplification)
        return results.isEmpty() ? null : results.get(0);
    }
} 