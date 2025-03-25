package minispark.scheduler;

import minispark.core.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * High-level scheduler that implements stage-oriented scheduling. It computes a DAG of stages for
 * each action and submits them to the TaskScheduler in a sequence that respects their dependencies.
 * This class handles:
 * - Breaking the RDD graph into stages of tasks
 * - Determining the preferred locations for each task
 * - Handling shuffle dependencies between stages
 * - Submitting stages for execution
 */
public class DAGScheduler {
    private static final Logger logger = LoggerFactory.getLogger(DAGScheduler.class);

    private final TaskScheduler taskScheduler;
    private final Map<Integer, Stage> stages;
    private final Map<Integer, Set<Integer>> stageDependencies;
    private int nextStageId;

    public DAGScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
        this.stages = new HashMap<>();
        this.stageDependencies = new HashMap<>();
        this.nextStageId = 0;
    }

    public void addStage(Stage stage) {
        stages.put(stage.getStageId(), stage);
        stageDependencies.put(stage.getStageId(), new HashSet<>());
        logger.info("Added stage {}", stage.getStageId());
    }

    public void addStageDependency(int childStageId, int parentStageId) {
        stageDependencies.get(childStageId).add(parentStageId);
        logger.info("Added dependency: stage {} depends on stage {}", childStageId, parentStageId);
    }

    @SuppressWarnings("unchecked")
    public <T> List<CompletableFuture<T>> submitStage(Stage stage) {
        // Check if all parent stages are complete
        Set<Integer> parents = stageDependencies.get(stage.getStageId());
        for (int parentId : parents) {
            Stage parentStage = stages.get(parentId);
            if (!parentStage.isComplete()) {
                submitStage(parentStage);
            }
        }

        // Submit tasks for this stage
        List<Task<?, T>> tasks = new ArrayList<>();
        for (Task<?, ?> task : stage.getTasks()) {
            tasks.add((Task<?, T>) task);
        }
        return taskScheduler.submitTasks(tasks, stage.getNumPartitions());
    }

    public Stage createStage(int numPartitions) {
        int stageId = nextStageId++;
        Stage stage = new Stage(stageId, numPartitions);
        addStage(stage);
        return stage;
    }
} 