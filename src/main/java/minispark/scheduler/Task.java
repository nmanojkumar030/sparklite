package minispark.scheduler;

import minispark.core.Partition;
import java.io.Serializable;

/**
 * Represents a unit of work in MiniSpark. Each task processes a single partition of data.
 * Tasks are serializable so they can be sent to remote workers.
 */
public abstract class Task<T, R> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final int taskId;
    private final int stageId;
    private final int partitionId;
    
    public Task(int taskId, int stageId, int partitionId) {
        this.taskId = taskId;
        this.stageId = stageId;
        this.partitionId = partitionId;
    }
    
    /**
     * Execute this task on the given partition and return the result.
     * This method will be called on the worker node.
     */
    public abstract R execute(Partition<T> partition);
    
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