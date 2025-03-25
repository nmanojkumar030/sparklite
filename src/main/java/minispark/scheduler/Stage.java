package minispark.scheduler;

import minispark.core.Task;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a stage of tasks that can be executed together. A stage is a set of tasks that can be
 * computed based on the same parent RDDs. Each stage contains tasks that compute partitions of the
 * same RDD.
 */
public class Stage {
    private final int stageId;
    private final int numPartitions;
    private final List<Task<?, ?>> tasks;
    private boolean isComplete;

    public Stage(int stageId, int numPartitions) {
        this.stageId = stageId;
        this.numPartitions = numPartitions;
        this.tasks = new ArrayList<>();
        this.isComplete = false;
    }

    public int getStageId() {
        return stageId;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void addTask(Task<?, ?> task) {
        tasks.add(task);
    }

    public List<Task<?, ?>> getTasks() {
        return tasks;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void setComplete(boolean complete) {
        isComplete = complete;
    }

    @Override
    public String toString() {
        return String.format("Stage(id=%d, numTasks=%d)", stageId, tasks.size());
    }
}
