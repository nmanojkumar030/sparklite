package minispark.scheduler;

import minispark.core.Task;
import minispark.core.MiniRDD;
import minispark.core.RDDTask;
import minispark.core.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-level scheduler that implements stage-oriented scheduling. It computes a DAG of stages for
 * each job and submits them to the TaskScheduler in a sequence that respects their dependencies.
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
    private final Map<Object, ActiveJob> activeJobs;
    private final AtomicInteger nextStageId;
    private final AtomicInteger nextJobId;
    private final AtomicInteger nextTaskId;

    public DAGScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
        this.stages = new HashMap<>();
        this.stageDependencies = new HashMap<>();
        this.activeJobs = new HashMap<>();
        this.nextStageId = new AtomicInteger(0);
        this.nextJobId = new AtomicInteger(0);
        this.nextTaskId = new AtomicInteger(0);
    }

    public <T> List<CompletableFuture<T>> submitJob(MiniRDD<T> finalRDD, int numPartitions) {
        Object jobId = nextJobId.getAndIncrement();
        logger.info("Got job {} with {} partitions", jobId, numPartitions);

        // Create a ResultStage for this job
        ResultStage resultStage = createResultStage(finalRDD, numPartitions, jobId);
        ActiveJob<T> job = new ActiveJob<>(jobId, finalRDD, resultStage);
        activeJobs.put(jobId, job);

        // Submit the final stage and only return futures from the final stage
        submitStage(resultStage);
        return job.getFutures();
    }

    private ResultStage createResultStage(MiniRDD<?> rdd, int numPartitions, Object jobId) {
        // First, identify all shuffle dependencies and create ShuffleMapStages for them
        List<ShuffleMapStage> shuffleStages = new ArrayList<>();
        for (MiniRDD<?> dep : rdd.getDependencies()) {
            if (requiresShuffle(dep)) {
                ShuffleMapStage shuffleStage = getOrCreateShuffleMapStage(dep);
                shuffleStages.add(shuffleStage);
            }
        }

        // Create the ResultStage
        ResultStage resultStage = new ResultStage(nextStageId.getAndIncrement(), rdd, numPartitions, jobId);
        
        // Add dependencies
        for (ShuffleMapStage shuffleStage : shuffleStages) {
            addStageDependency(resultStage.getStageId(), shuffleStage.getStageId());
        }

        // Create tasks for this stage
        createTasksForStage(resultStage);

        stages.put(resultStage.getStageId(), resultStage);
        return resultStage;
    }

    private ShuffleMapStage getOrCreateShuffleMapStage(MiniRDD<?> rdd) {
        // For now, create a new stage. In a real implementation, we would cache and reuse stages
        int stageId = nextStageId.getAndIncrement();
        ShuffleMapStage stage = new ShuffleMapStage(stageId, rdd, rdd.getPartitions().length, stageId);
        
        // Create tasks for this stage
        createTasksForStage(stage);
        
        stages.put(stage.getStageId(), stage);
        return stage;
    }

    private <T> void createTasksForStage(Stage stage) {
        MiniRDD<?> rdd = stage.getRdd();
        Partition[] partitions = rdd.getPartitions();
        for (int i = 0; i < partitions.length; i++) {
            Task<?, ?> task = new RDDTask<>(nextTaskId.getAndIncrement(), stage.getStageId(), i, rdd);
            stage.addTask(task);
        }
    }

    private boolean requiresShuffle(MiniRDD<?> rdd) {
        // For now, assume any RDD requires a shuffle. In a real implementation,
        // we would check the RDD's dependencies to determine this.
        return true;
    }

    private void addStageDependency(int childStageId, int parentStageId) {
        stageDependencies.computeIfAbsent(childStageId, k -> new HashSet<>()).add(parentStageId);
        logger.info("Added dependency: stage {} depends on stage {}", childStageId, parentStageId);
    }

    @SuppressWarnings("unchecked")
    private <T> List<CompletableFuture<T>> submitStage(Stage stage) {
        // Check if all parent stages are complete
        Set<Integer> parents = stageDependencies.getOrDefault(stage.getStageId(), Collections.emptySet());
        List<CompletableFuture<T>> futures = new ArrayList<>();

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

        List<CompletableFuture<T>> stageFutures = taskScheduler.submitTasks(tasks, stage.getNumPartitions());
        
        // Only add futures to job's futures if this is a ResultStage
        if (stage instanceof ResultStage) {
            ResultStage resultStage = (ResultStage) stage;
            ActiveJob<?> job = activeJobs.get(resultStage.getJobId());
            if (job != null) {
                ((ActiveJob<T>) job).futures.addAll(stageFutures);
            }
        }
        
        futures.addAll(stageFutures);

        // When all futures complete, mark the stage as complete
        CompletableFuture.allOf(stageFutures.toArray(new CompletableFuture[0]))
            .thenRun(() -> {
                stage.setComplete(true);
                logger.info("Stage {} completed", stage.getStageId());
                if (stage instanceof ResultStage) {
                    ResultStage resultStage = (ResultStage) stage;
                    ActiveJob<?> job = activeJobs.remove(resultStage.getJobId());
                    if (job != null) {
                        logger.info("Job {} completed", job.getJobId());
                    }
                }
            });

        return futures;
    }

    private class ActiveJob<T> {
        private final Object jobId;
        private final MiniRDD<T> finalRDD;
        private final ResultStage finalStage;
        private final List<CompletableFuture<T>> futures;

        public ActiveJob(Object jobId, MiniRDD<T> finalRDD, ResultStage finalStage) {
            this.jobId = jobId;
            this.finalRDD = finalRDD;
            this.finalStage = finalStage;
            this.futures = new ArrayList<>();
        }

        public Object getJobId() {
            return jobId;
        }

        public MiniRDD<T> getFinalRDD() {
            return finalRDD;
        }

        public ResultStage getFinalStage() {
            return finalStage;
        }

        public List<CompletableFuture<T>> getFutures() {
            return futures;
        }
    }
} 