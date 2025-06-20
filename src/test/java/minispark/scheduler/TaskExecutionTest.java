package minispark.scheduler;

import minispark.core.Partition;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import minispark.util.SimulationRunner;
import minispark.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class TaskExecutionTest {
    private MessageBus messageBus;
    private NetworkEndpoint schedulerEndpoint;
    private TaskSchedulerImpl taskScheduler;
    private Worker worker1;
    private Worker worker2;

    @BeforeEach
    void setUp() {
        messageBus = new MessageBus();
        schedulerEndpoint = new NetworkEndpoint("localhost", 8080);
        taskScheduler = new TaskSchedulerImpl(schedulerEndpoint, messageBus);

        // Create and start workers
        NetworkEndpoint worker1Endpoint = new NetworkEndpoint("localhost", 8081);
        NetworkEndpoint worker2Endpoint = new NetworkEndpoint("localhost", 8082);
        worker1 = new Worker("worker1", worker1Endpoint, schedulerEndpoint, 2, messageBus);
        worker2 = new Worker("worker2", worker2Endpoint, schedulerEndpoint, 2, messageBus);

        // Start all components
        messageBus.start();
        taskScheduler.start();
        worker1.start();
        worker2.start();

        // Wait for workers to register using tick progression
        SimulationRunner.runUntil(messageBus,
            () -> taskScheduler.getNumWorkers() >= 2, 
            java.time.Duration.ofSeconds(5));
    }

    @AfterEach
    void tearDown() {
        worker1.stop();
        worker2.stop();
        taskScheduler.stop();
        messageBus.stop();
    }

    @Test
    void shouldExecuteTasksAndReturnResults() throws Exception {
        // Create test tasks
        TestTask task1 = new TestTask(1, 1, 0, 5);
        TestTask task2 = new TestTask(2, 1, 1, 10);
        List<TestTask> tasks = Arrays.asList(task1, task2);

        // Submit tasks
        List<CompletableFuture<Integer>> futures = taskScheduler.submitTasks(tasks, 1);
        assertEquals(2, futures.size());

        // Wait for results with tick progression
        SimulationRunner.runUntil(messageBus, () -> futures.get(0).isDone(), java.time.Duration.ofSeconds(5));
        SimulationRunner.runUntil(messageBus, () -> futures.get(1).isDone(), java.time.Duration.ofSeconds(5));
        
        Integer result1 = futures.get(0).get();
        Integer result2 = futures.get(1).get();

        // Verify results
        assertEquals(5, result1);
        assertEquals(10, result2);
    }

    @Test
    void shouldHandleTaskFailures() throws Exception {
        // Create a task that will fail
        TestTask failingTask = new TestTask(1, 1, 0, -1); // Negative input will cause failure
        List<TestTask> tasks = Arrays.asList(failingTask);

        // Submit task
        List<CompletableFuture<Integer>> futures = taskScheduler.submitTasks(tasks, 1);
        assertEquals(1, futures.size());

        // Wait for completion with tick progression
        SimulationRunner.runUntil(messageBus, () -> futures.get(0).isDone(), java.time.Duration.ofSeconds(5));

        // Verify that the task fails with the expected exception
        Exception exception = assertThrows(Exception.class, () -> {
            futures.get(0).get();
        });
        
        // The original IllegalArgumentException is now wrapped in ExecutionException:
        // ExecutionException -> IllegalArgumentException
        Throwable cause = exception.getCause(); // RuntimeException
        assertTrue(cause instanceof IllegalArgumentException);
    }

    /**
     * A test task that simply returns its input value or throws an exception if input is negative.
     */
    private static class TestTask extends minispark.core.Task<Void, Integer> {
        private final int input;

        TestTask(int taskId, int stageId, int partitionId, int input) {
            super(taskId, stageId, partitionId);
            this.input = input;
        }

        @Override
        public CompletableFuture<Integer> execute(Partition partition) {
            if (input < 0) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Input cannot be negative"));
            }
            return CompletableFuture.completedFuture(input);
        }
    }
} 