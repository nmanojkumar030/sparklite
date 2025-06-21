package minispark.distributed.worker;

import minispark.distributed.rdd.Partition;
import minispark.distributed.scheduler.RDDTask;
import minispark.distributed.scheduler.Task;
import minispark.distributed.messages.Message;
import minispark.distributed.messages.SubmitTaskMessage;
import minispark.distributed.messages.TaskResultMessage;
import minispark.distributed.messages.WorkerRegistrationMessage;
import minispark.distributed.network.MessageBus;
import minispark.distributed.network.NetworkEndpoint;
import minispark.distributed.rdd.MiniRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a worker node in the MiniSpark cluster. Each worker is responsible for:
 * - Executing tasks assigned to it
 * - Managing its own thread pool for parallel task execution
 * - Reporting task completion and failures back to the scheduler
 * 
 * BACK-PRESSURE: Workers now have bounded task queues to prevent unbounded memory growth
 * and exercise realistic failure paths when workers are overloaded.
 */
public class Worker implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final String workerId;
    private final NetworkEndpoint endpoint;
    private final NetworkEndpoint schedulerEndpoint;
    private final int numCores;
    private final MessageBus messageBus;
    
    // BACK-PRESSURE: Bounded task queue to prevent memory issues
    private final int maxQueueSize;
    private final Queue<PendingTask> taskQueue;
    private int activeTasks = 0;
    
    // Note: In deterministic simulation, we don't use thread pools
    // private final ExecutorService executorService;

    /**
     * Represents a task waiting to be executed.
     */
    private static class PendingTask {
        final Task<?, ?> task;
        final NetworkEndpoint sender;
        
        PendingTask(Task<?, ?> task, NetworkEndpoint sender) {
            this.task = task;
            this.sender = sender;
        }
    }

    public Worker(String workerId, NetworkEndpoint endpoint, NetworkEndpoint schedulerEndpoint, int numCores, MessageBus messageBus) {
        this(workerId, endpoint, schedulerEndpoint, numCores, messageBus, 16); // Default queue size (not based on cores)
    }
    
    /**
     * Constructor with configurable queue size for back-pressure control.
     * 
     * @param maxQueueSize Maximum number of tasks that can be queued (back-pressure limit)
     */
    public Worker(String workerId, NetworkEndpoint endpoint, NetworkEndpoint schedulerEndpoint, 
                  int numCores, MessageBus messageBus, int maxQueueSize) {
        this.workerId = workerId;
        this.endpoint = endpoint;
        this.schedulerEndpoint = schedulerEndpoint;
        this.numCores = numCores;
        this.messageBus = messageBus;
        this.maxQueueSize = maxQueueSize;
        this.taskQueue = new LinkedList<>();
        
        logger.info("Worker {} initialized with max queue size {} (cores parameter ignored in single-threaded simulation)", 
            workerId, maxQueueSize);
        
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
            
            // BACK-PRESSURE: Check if we can accept more tasks
            if (!canAcceptTask()) {
                logger.warn("Worker {} rejecting task {} - queue full (active: {}, queued: {}, max: {})", 
                    workerId, task.getTaskId(), activeTasks, taskQueue.size(), maxQueueSize);
                
                // Send failure result to indicate overload
                Exception overloadError = new RuntimeException(
                    String.format("Worker %s overloaded: %d active tasks, %d queued (max %d)", 
                        workerId, activeTasks, taskQueue.size(), maxQueueSize));
                sendTaskResult(task, null, overloadError);
                return;
            }
            
            // Queue the task for execution
            taskQueue.offer(new PendingTask(task, sender));
            logger.debug("Worker {} queued task {}, queue size: {}", workerId, task.getTaskId(), taskQueue.size());
            
            // Process tasks from queue
            processTaskQueue();
            
        } else {
            logger.warn("Worker {} received unknown message type: {}", workerId, message.getType());
        }
    }
    
    /**
     * BACK-PRESSURE: Check if worker can accept more tasks based on current load.
     */
    private boolean canAcceptTask() {
        int totalLoad = activeTasks + taskQueue.size();
        boolean canAccept = totalLoad < maxQueueSize;
        
        if (!canAccept) {
            logger.debug("Worker {} at capacity: {} active + {} queued >= {} max", 
                workerId, activeTasks, taskQueue.size(), maxQueueSize);
        }
        
        return canAccept;
    }
    
    /**
     * Process tasks from the queue.
     * Since execution is single-threaded in the simulation, we process one task at a time.
     */
    private void processTaskQueue() {
        // In single-threaded simulation, we only need to check if we have capacity
        // and no active tasks (since we can't actually run tasks in parallel)
        while (!taskQueue.isEmpty() && activeTasks == 0) {
            PendingTask pendingTask = taskQueue.poll();
            activeTasks++;
            
            logger.debug("Worker {} starting task {} (active: {}, queued: {})", 
                workerId, pendingTask.task.getTaskId(), activeTasks, taskQueue.size());
            
            try {
                executeTaskAsync(pendingTask);
            } catch (Exception e) {
                activeTasks--; // Decrement on immediate failure
                logger.error("Worker {} failed to start task {}: {}", 
                    workerId, pendingTask.task.getTaskId(), e.getMessage(), e);
                sendTaskResult(pendingTask.task, null, e);
                // Continue processing queue after failure
                processTaskQueue();
            }
        }
    }
    
    /**
     * Execute a task asynchronously and handle completion.
     */
    private void executeTaskAsync(PendingTask pendingTask) {
        Task<?, ?> task = pendingTask.task;
        
        try {
            @SuppressWarnings("unchecked")
            Task<Object, Object> typedTask = (Task<Object, Object>) task;
            
            // Get the correct partition from the RDD instead of creating a BasePartition
            Partition partition = getPartitionForTask(task);
            
            logger.debug("Worker {} executing task {} with partition {} ({})", 
                workerId, task.getTaskId(), partition.index(), partition.getClass().getSimpleName());
            
            // Execute task - this returns a future
            CompletableFuture<?> future = task.execute(partition);
            future.whenComplete((result, throwable) -> {
                // FIXED: Decrement task count when future actually completes
                activeTasks--;
                logger.info("Worker {} completed task {} with result: {}", workerId, task.getTaskId(), result);
                
                if (throwable != null) {
                    sendTaskResult(task, result, throwable);
                } else {
                    sendTaskResult(task, result, null);
                }
                
                // Process more tasks from queue now that this one is truly complete
                processTaskQueue();
            });

        } catch (Exception e) {
            // FIXED: Only decrement on immediate synchronous failure
            activeTasks--;
            logger.error("Worker {} failed to execute task {}: {}", workerId, task.getTaskId(), e.getMessage(), e);
            sendTaskResult(task, null, e);
            // Process more tasks after synchronous failure
            processTaskQueue();
        }
    }
    
    /**
     * Get the appropriate partition for a task.
     */
    private Partition getPartitionForTask(Task<?, ?> task) {
        if (task instanceof RDDTask) {
            RDDTask<?> rddTask = (RDDTask<?>) task;
            
            // ENCAPSULATION FIX: Use proper getter instead of reflection
            // This eliminates reflection, improves performance, and makes dependencies explicit
            MiniRDD<?> rdd = rddTask.getRdd();
            
            // Get the actual partition from the RDD
            Partition[] partitions = rdd.getPartitions();
            if (task.getPartitionId() < partitions.length) {
                Partition partition = partitions[task.getPartitionId()];
                logger.debug("Worker {} using actual partition {} of type {}", 
                    workerId, partition.index(), partition.getClass().getSimpleName());
                return partition;
            } else {
                logger.warn("Worker {} partition index {} out of bounds, using BasePartition", 
                    workerId, task.getPartitionId());
                return new BasePartition(task.getPartitionId());
            }
        } else {
            // For non-RDD tasks, use BasePartition
            return new BasePartition(task.getPartitionId());
        }
    }
    
    /**
     * Executes a task with tick progression for any futures that need to complete.
     * This is the key method that enables deterministic simulation by driving
     * the MessageBus tick loop until all async operations complete.
     * 
     * DETERMINISM NOTE: Removed Thread.sleep() to avoid yielding to OS scheduler
     * which breaks determinism under load. Pure tick progression ensures 
     * deterministic execution order.
     */
    private Object executeTaskWithTickProgression(Task<Object, Object> task, Partition partition) {
        // Execute the task - this now returns a CompletableFuture
        CompletableFuture<Object> future = task.execute(partition);
        
        logger.debug("Worker {} task {} returned future, driving ticks until completion", workerId, task.getTaskId());

        try {
            Object result = future.get();
            logger.debug("Worker {} task {} future completed with result", workerId, task.getTaskId());
            return result;
        } catch (Exception e) {
            logger.error("Worker {} task {} future failed: {}", workerId, task.getTaskId(), e.getMessage());
            throw new RuntimeException("Task future failed", e);
        }
    }

    private void sendTaskResult(Task<?, ?> task, Object result, Throwable error) {
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
     * BACK-PRESSURE: Get current queue statistics for monitoring.
     */
    public WorkerStats getStats() {
        return new WorkerStats(activeTasks, taskQueue.size(), maxQueueSize);
    }
    
    /**
     * BACK-PRESSURE: Worker statistics for monitoring and load balancing.
     */
    public static class WorkerStats {
        private final int activeTasks;
        private final int queuedTasks;
        private final int maxQueueSize;
        
        public WorkerStats(int activeTasks, int queuedTasks, int maxQueueSize) {
            this.activeTasks = activeTasks;
            this.queuedTasks = queuedTasks;
            this.maxQueueSize = maxQueueSize;
        }
        
        public int getActiveTasks() { return activeTasks; }
        public int getQueuedTasks() { return queuedTasks; }
        public int getMaxQueueSize() { return maxQueueSize; }
        public int getTotalLoad() { return activeTasks + queuedTasks; }
        public double getLoadPercentage() { return (double) getTotalLoad() / maxQueueSize * 100; }
        
        public boolean isOverloaded() { return getTotalLoad() >= maxQueueSize; }
        public boolean isNearCapacity() { return getLoadPercentage() > 80; }
        
        @Override
        public String toString() {
            return String.format("WorkerStats{active=%d, queued=%d, max=%d, load=%.1f%%}", 
                activeTasks, queuedTasks, maxQueueSize, getLoadPercentage());
        }
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