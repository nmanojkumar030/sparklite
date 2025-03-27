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
    private final ExecutorService executorService;

    public Worker(String workerId, NetworkEndpoint endpoint, NetworkEndpoint schedulerEndpoint, int numCores, MessageBus messageBus) {
        this.workerId = workerId;
        this.endpoint = endpoint;
        this.schedulerEndpoint = schedulerEndpoint;
        this.numCores = numCores;
        this.messageBus = messageBus;
        this.executorService = Executors.newFixedThreadPool(numCores);
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
        executorService.shutdown();
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
            
            executorService.submit(() -> {
                try {
                    logger.debug("Worker {} starting execution of task {} with thread {}", 
                        workerId, task.getTaskId(), Thread.currentThread().getName());
                    
                    @SuppressWarnings("unchecked")
                    Task<Object, Object> typedTask = (Task<Object, Object>) task;
                    Partition partition = new BasePartition(task.getPartitionId());
                    
                    logger.debug("Worker {} created partition {} for task {}", 
                        workerId, partition.index(), task.getTaskId());
                    
                    Object result = typedTask.execute(partition);
                    
                    logger.info("Worker {} completed task {} with result: {}", workerId, task.getTaskId(), result);
                    sendTaskResult(task, result, null);
                } catch (Exception e) {
                    logger.error("Worker {} failed to execute task {}: {}", workerId, task.getTaskId(), e.getMessage(), e);
                    sendTaskResult(task, null, e);
                }
            });
        } else {
            logger.warn("Worker {} received unknown message type: {}", workerId, message.getType());
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