package minispark.scheduler;

import minispark.core.Partition;
import java.io.Serializable;

/**
 * Abstract base class for tasks that can be executed on workers.
 * A task represents a unit of work that operates on a specific partition of an RDD.
 *
 * @param <T> The input type the task operates on
 * @param <R> The output type the task produces
 */
public abstract class Task<T, R> implements Serializable {
    private static final long serialVersionUID = 1L;
    
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
     * @return The result of the task execution
     */
    public abstract R execute(Partition partition);
    
    public int getTaskId() {
        return taskId;
    }
    
    public int getStageId() {
        return stageId;
    }
    
    public int getPartitionId() {
        return partitionId;
    }
    
    @Override
    public String toString() {
        return String.format("Task(id=%d, stage=%d, partition=%d)", taskId, stageId, partitionId);
    }
} 