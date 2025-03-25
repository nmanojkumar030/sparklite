package minispark.scheduler;

import minispark.core.Task;
import minispark.core.MiniRDD;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for stages in Spark. A stage is a set of parallel tasks that compute the partitions 
 * of an RDD using the same operation. There are two types of stages:
 * - ResultStage: For stages that compute an action's results
 * - ShuffleMapStage: For stages that produce data for a shuffle
 */
public abstract class Stage {
    private final int stageId;
    private final int numPartitions;
    private final List<Task<?, ?>> tasks;
    private boolean isComplete;
    protected final MiniRDD<?> rdd;
    private final List<Stage> parents;

    public Stage(int stageId, MiniRDD<?> rdd, int numPartitions) {
        this.stageId = stageId;
        this.rdd = rdd;
        this.numPartitions = numPartitions;
        this.tasks = new ArrayList<>();
        this.isComplete = false;
        this.parents = new ArrayList<>();
    }

    public int getStageId() {
        return stageId;
    }

    public MiniRDD<?> getRdd() {
        return rdd;
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

    public List<Stage> getParents() {
        return parents;
    }

    public void addParent(Stage parent) {
        parents.add(parent);
    }

    @Override
    public String toString() {
        return String.format("Stage(id=%d, rdd=%s, numTasks=%d)", stageId, rdd, tasks.size());
    }

    /**
     * Returns true if this stage is a shuffle map stage.
     */
    public abstract boolean isShuffleMap();
}
