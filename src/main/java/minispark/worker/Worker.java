package minispark.worker;

import minispark.core.Partition;
import minispark.core.BasePartition;
import minispark.core.Task;
import minispark.messages.Message;
import minispark.messages.SubmitTaskMessage;
import minispark.messages.TaskResultMessage;
import minispark.messages.WorkerRegistrationMessage;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a worker node in the MiniSpark cluster. Each worker is responsible for:
 * - Executing tasks assigned to it
 * - Managing its own thread pool for parallel task execution
 * - Reporting task completion and failures back to the scheduler
 */
public class Worker implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final String workerId;
    private final NetworkEndpoint endpoint;
    private final NetworkEndpoint schedulerEndpoint;
    private final int numCores;
    private final MessageBus messageBus;
    // Note: In deterministic simulation, we don't use thread pools
    // private final ExecutorService executorService;

    public Worker(String workerId, NetworkEndpoint endpoint, NetworkEndpoint schedulerEndpoint, int numCores, MessageBus messageBus) {
        this.workerId = workerId;
        this.endpoint = endpoint;
        this.schedulerEndpoint = schedulerEndpoint;
        this.numCores = numCores;
        this.messageBus = messageBus;
        // Note: In deterministic simulation, we execute tasks synchronously with tick progression
        // this.executorService = Executors.newFixedThreadPool(numCores);
    }

    public void start() {
        messageBus.registerHandler(endpoint, this);
        // Send registration message to scheduler
        WorkerRegistrationMessage registrationMessage = new WorkerRegistrationMessage(workerId, endpoint, numCores);
        messageBus.send(registrationMessage, endpoint, schedulerEndpoint);
        logger.info("Worker {} started and registered with scheduler", workerId);
    }

    public void stop() {
        messageBus.unregisterHandler(endpoint);
        // Note: No executor service to shutdown in deterministic simulation
        // executorService.shutdown();
        logger.info("Worker {} stopped", workerId);
    }

    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        logger.debug("Worker {} received message of type {} from {}", workerId, message.getType(), sender);
        
        if (message instanceof SubmitTaskMessage) {
            SubmitTaskMessage taskMessage = (SubmitTaskMessage) message;
            Task<?, ?> task = taskMessage.getTask();
            
            logger.info("Worker {} received task {} (stage={}, partition={}) from {}", 
                workerId, task.getTaskId(), task.getStageId(), task.getPartitionId(), sender);
            
            // Execute task synchronously with tick progression (deterministic simulation)
            try {
                logger.debug("Worker {} starting execution of task {}", workerId, task.getTaskId());
                
                @SuppressWarnings("unchecked")
                Task<Object, Object> typedTask = (Task<Object, Object>) task;
                
                // Get the correct partition from the RDD instead of creating a BasePartition
                Partition partition;
                if (task instanceof minispark.core.RDDTask) {
                    minispark.core.RDDTask<?> rddTask = (minispark.core.RDDTask<?>) task;
                    try {
                        // Use reflection to get the RDD from the task
                        java.lang.reflect.Field rddField = minispark.core.RDDTask.class.getDeclaredField("rdd");
                        rddField.setAccessible(true);
                        minispark.core.MiniRDD<?> rdd = (minispark.core.MiniRDD<?>) rddField.get(rddTask);
                        
                        // Get the actual partition from the RDD
                        Partition[] partitions = rdd.getPartitions();
                        if (task.getPartitionId() < partitions.length) {
                            partition = partitions[task.getPartitionId()];
                            logger.debug("Worker {} using actual partition {} of type {}", 
                                workerId, partition.index(), partition.getClass().getSimpleName());
                        } else {
                            logger.warn("Worker {} partition index {} out of bounds, using BasePartition", 
                                workerId, task.getPartitionId());
                            partition = new BasePartition(task.getPartitionId());
                        }
                    } catch (Exception e) {
                        logger.warn("Worker {} failed to get RDD partition, using BasePartition: {}", 
                            workerId, e.getMessage());
                        partition = new BasePartition(task.getPartitionId());
                    }
                } else {
                    // For non-RDD tasks, use BasePartition
                    partition = new BasePartition(task.getPartitionId());
                }
                
                logger.debug("Worker {} executing task {} with partition {} ({})", 
                    workerId, task.getTaskId(), partition.index(), partition.getClass().getSimpleName());
                
                // Execute task - this may return futures that need tick progression
                Object result = executeTaskWithTickProgression(typedTask, partition);
                
                logger.info("Worker {} completed task {} with result: {}", workerId, task.getTaskId(), result);
                sendTaskResult(task, result, null);
            } catch (Exception e) {
                logger.error("Worker {} failed to execute task {}: {}", workerId, task.getTaskId(), e.getMessage(), e);
                sendTaskResult(task, null, e);
            }
        } else {
            logger.warn("Worker {} received unknown message type: {}", workerId, message.getType());
        }
    }
    
    /**
     * Executes a task with tick progression for any futures that need to complete.
     * This is the key method that enables deterministic simulation by driving
     * the MessageBus tick loop until all async operations complete.
     */
    private Object executeTaskWithTickProgression(Task<Object, Object> task, Partition partition) {
        // Execute the task - this now returns a CompletableFuture
        CompletableFuture<Object> future = task.execute(partition);
        
        logger.debug("Worker {} task {} returned future, driving ticks until completion", workerId, task.getTaskId());
        
        // Drive ticks until the future completes
        while (!future.isDone()) {
            messageBus.tick();
            
            // Brief yield to avoid tight CPU loop
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task execution interrupted", e);
            }
        }
        
        try {
            Object result = future.get();
            logger.debug("Worker {} task {} future completed with result", workerId, task.getTaskId());
            return result;
        } catch (Exception e) {
            logger.error("Worker {} task {} future failed: {}", workerId, task.getTaskId(), e.getMessage());
            throw new RuntimeException("Task future failed", e);
        }
    }

    private void sendTaskResult(Task<?, ?> task, Object result, Exception error) {
        TaskResultMessage resultMessage = new TaskResultMessage(task.getTaskId(), task.getStageId(), result, error);
        logger.info("Worker {} sending {} result for task {} to {}", 
            workerId, error == null ? "success" : "failure", task.getTaskId(), schedulerEndpoint);
        messageBus.send(resultMessage, endpoint, schedulerEndpoint);
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

    /**
     * Represents the result of a task execution, including any errors that occurred.
     */
    public static class TaskResult<T> {
        private final int taskId;
        private final T result;
        private final Exception error;

        public TaskResult(int taskId, T result, Exception error) {
            this.taskId = taskId;
            this.result = result;
            this.error = error;
        }

        public int getTaskId() {
            return taskId;
        }

        public T getResult() {
            return result;
        }

        public Exception getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }
} 