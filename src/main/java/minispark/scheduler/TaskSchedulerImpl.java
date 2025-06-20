package minispark.scheduler;

import minispark.core.Task;
import minispark.messages.Message;
import minispark.messages.SubmitTaskMessage;
import minispark.messages.TaskResultMessage;
import minispark.messages.WorkerRegistrationMessage;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the TaskScheduler interface that manages a pool of workers and
 * distributes tasks among them. This implementation:
 * - Maintains a pool of available workers
 * - Assigns tasks to workers based on their availability
 * - Handles task failures and retries
 * - Monitors worker health
 * - Handles worker registration and lifecycle
 * 
 * DETERMINISM NOTE: Uses LinkedHashMap instead of ConcurrentHashMap for 
 * deterministic iteration order in single-threaded execution.
 */
public class TaskSchedulerImpl implements TaskScheduler, MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulerImpl.class);

    private final NetworkEndpoint schedulerEndpoint;
    private final MessageBus messageBus;
    private final Map<String, WorkerInfo> workers = new LinkedHashMap<>();
    private final Map<Integer, CompletableFuture<?>> taskFutures = new LinkedHashMap<>();

    public static class WorkerInfo {
        final String workerId;
        final NetworkEndpoint endpoint;
        final int numCores;
        int tasksAssigned;

        WorkerInfo(String workerId, NetworkEndpoint endpoint, int numCores) {
            this.workerId = workerId;
            this.endpoint = endpoint;
            this.numCores = numCores;
            this.tasksAssigned = 0;
        }

        public String getWorkerId() {
            return workerId;
        }

        public NetworkEndpoint getEndpoint() {
            return endpoint;
        }

        public int getNumCores() {
            return numCores;
        }
    }

    public TaskSchedulerImpl(NetworkEndpoint schedulerEndpoint, MessageBus messageBus) {
        this.schedulerEndpoint = schedulerEndpoint;
        this.messageBus = messageBus;
    }

    public void start() {
        messageBus.registerHandler(schedulerEndpoint, this);
        logger.info("TaskScheduler started at endpoint {}", schedulerEndpoint);
    }

    public void stop() {
        messageBus.unregisterHandler(schedulerEndpoint);
        logger.info("TaskScheduler stopped");
    }

    @Override
    public <T> List<CompletableFuture<T>> submitTasks(List<? extends Task<?, T>> tasks, int numPartitions) {
        List<CompletableFuture<T>> futures = new ArrayList<>();
        List<WorkerInfo> availableWorkers = new ArrayList<>(workers.values());
        
        logger.info("Submitting {} tasks with {} available workers", tasks.size(), availableWorkers.size());
        
        if (availableWorkers.isEmpty()) {
            throw new IllegalStateException("No workers available to execute tasks");
        }

        int workerIndex = 0;
        for (Task<?, T> task : tasks) {
            WorkerInfo worker = availableWorkers.get(workerIndex);
            workerIndex = (workerIndex + 1) % availableWorkers.size();

            CompletableFuture<T> future = new CompletableFuture<>();
            taskFutures.put(task.getTaskId(), future);

            SubmitTaskMessage submitMessage = new SubmitTaskMessage(task);
            messageBus.send(submitMessage, schedulerEndpoint, worker.endpoint);
            worker.tasksAssigned++;

            futures.add(future);
            logger.info("Submitted task {} to worker {} at {}", task.getTaskId(), worker.workerId, worker.endpoint);
        }

        return futures;
    }

    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        logger.debug("TaskScheduler received message of type {} from {}", message.getType(), sender);
        
        if (message instanceof TaskResultMessage) {
            TaskResultMessage resultMessage = (TaskResultMessage) message;
            logger.info("Received task result message from {} for task {} (stage={})", 
                sender, resultMessage.getTaskId(), resultMessage.getStageId());
            handleTaskResult(resultMessage);
        } else if (message instanceof WorkerRegistrationMessage) {
            WorkerRegistrationMessage regMessage = (WorkerRegistrationMessage) message;
            logger.info("Received worker registration from {} with {} cores at {}", 
                regMessage.getWorkerId(), regMessage.getNumCores(), regMessage.getEndpoint());
            
            WorkerInfo workerInfo = new WorkerInfo(
                regMessage.getWorkerId(),
                regMessage.getEndpoint(),
                regMessage.getNumCores()
            );
            workers.put(regMessage.getWorkerId(), workerInfo);
            logger.info("Worker {} registered with {} cores at {}", 
                regMessage.getWorkerId(), regMessage.getNumCores(), regMessage.getEndpoint());
        } else {
            logger.warn("TaskScheduler received unknown message type: {}", message.getType());
        }
    }

    private void handleTaskResult(TaskResultMessage resultMessage) {
        logger.debug("Processing task result for task {} (stage={})", 
            resultMessage.getTaskId(), resultMessage.getStageId());
            
        @SuppressWarnings("unchecked")
        CompletableFuture<Object> future = (CompletableFuture<Object>) taskFutures.get(resultMessage.getTaskId());
        
        if (future != null) {
            try {
                if (resultMessage.getError() != null) {
                    logger.error("Task {} failed: {}", resultMessage.getTaskId(), 
                        resultMessage.getError().getMessage(), resultMessage.getError());
                    future.completeExceptionally(resultMessage.getError());
                } else if (!resultMessage.isSuccess()) {
                    logger.error("Task {} failed without explicit error", resultMessage.getTaskId());
                    future.completeExceptionally(
                        new RuntimeException("Task execution failed"));
                } else if (resultMessage.getResult() != null) {
                    Object result = resultMessage.getResult();
                    logger.debug("Completing future for task {} with result: {}", 
                        resultMessage.getTaskId(), result);
                    future.complete(result);
                    logger.info("Task {} completed successfully with result: {}", 
                        resultMessage.getTaskId(), result);
                } else {
                    logger.error("Task {} completed but no result was provided", resultMessage.getTaskId());
                    future.completeExceptionally(
                        new RuntimeException("Task completed but no result was provided"));
                }
            } catch (ClassCastException e) {
                logger.error("Task {} failed: Failed to cast result to expected type", 
                    resultMessage.getTaskId(), e);
                future.completeExceptionally(e);
            } finally {
                // Only remove the future after it has been completed
                if (future.isDone()) {
                    taskFutures.remove(resultMessage.getTaskId());
                }
            }
        } else {
            logger.warn("Received result for unknown task {}", resultMessage.getTaskId());
        }
    }

    public void removeWorker(String workerId) {
        WorkerInfo workerInfo = workers.remove(workerId);
        if (workerInfo != null) {
            logger.info("Worker {} removed", workerId);
            // TODO: Handle task failures and retries for tasks assigned to this worker
        }
    }

    public int getNumWorkers() {
        return workers.size();
    }

    public int getTotalCores() {
        return workers.values().stream()
                .mapToInt(w -> w.numCores)
                .sum();
    }

    public boolean isWorkerAlive(String workerId) {
        return workers.containsKey(workerId);
    }
} 