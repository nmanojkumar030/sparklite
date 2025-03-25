package minispark.core;

import java.util.ArrayList;
import java.util.List;

/**
 * A task that executes a partition of an RDD and returns the results.
 */
public class RDDTask<T> extends Task<T, T> {
    private final MiniRDD<T> rdd;

    public RDDTask(int taskId, int stageId, int partitionId, MiniRDD<T> rdd) {
        super(taskId, stageId, partitionId);
        this.rdd = rdd;
    }

    @Override
    public T execute(Partition<T> partition) {
        List<T> results = new ArrayList<>();
        rdd.compute(partition).forEachRemaining(results::add);
        return results.get(0); // For now, just return the first result
    }
} 