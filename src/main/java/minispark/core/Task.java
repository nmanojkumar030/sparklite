package minispark.core;

import java.io.Serializable;

public abstract class Task<I, O> implements Serializable {
    private final int taskId;
    private final int stageId;
    private final int partitionId;

    protected Task(int taskId, int stageId, int partitionId) {
        this.taskId = taskId;
        this.stageId = stageId;
        this.partitionId = partitionId;
    }

    public abstract O execute(Partition<I> partition);

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