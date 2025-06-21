package minispark.distributed.scheduler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Low-level task scheduler interface. This interface is responsible for scheduling individual tasks
 * to workers and managing their execution. It receives sets of tasks from the DAGScheduler for each
 * stage and is responsible for:
 * - Sending tasks to worker nodes
 * - Running the tasks
 * - Handling retries on failures
 * - Handling straggler mitigation
 */
public interface TaskScheduler {
    /**
     * Start the task scheduler.
     */
    void start();

    /**
     * Stop the task scheduler.
     */
    void stop();

    /**
     * Submit a set of tasks from a stage to be executed.
     *
     * @param tasks The list of tasks to execute
     * @param numPartitions The number of partitions for the tasks
     * @return A list of futures representing the task results
     */
    <T> List<CompletableFuture<T>> submitTasks(List<? extends Task<?, T>> tasks, int numPartitions);

    /**
     * Get the number of worker nodes currently available.
     *
     * @return The number of available workers
     */
    int getNumWorkers();

    /**
     * Get the total number of cores available across all workers.
     *
     * @return The total number of cores
     */
    int getTotalCores();

    /**
     * Check if a worker is alive.
     *
     * @param workerId The ID of the worker to check
     * @return true if the worker is alive, false otherwise
     */
    boolean isWorkerAlive(String workerId);
} 