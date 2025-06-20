package minispark.core;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract base class for tasks that can be executed on workers.
 * A task represents a unit of work that operates on a specific partition of an RDD.
 *
 * @param <I> The input type the task operates on
 * @param <O> The output type the task produces
 */
public abstract class Task<I, O> implements Serializable {
    private final int taskId;
    private final int stageId;
    private final int partitionId;

    protected Task(int taskId, int stageId, int partitionId) {
        this.taskId = taskId;
        this.stageId = stageId;
        this.partitionId = partitionId;
    }

    /**
     * Execute the task on the given partition.
     *
     * @param partition The partition to execute on
     * @return A CompletableFuture containing the result of the task execution
     */
    public abstract CompletableFuture<O> execute(Partition partition);

    public int getTaskId() {
        return taskId;
    }

    public int getStageId() {
        return stageId;
    }

    public int getPartitionId() {
        return partitionId;
    }
} 