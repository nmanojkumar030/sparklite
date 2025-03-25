package minispark.scheduler;

import minispark.core.MiniRDD;
import minispark.core.Partition;
import minispark.core.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class DAGSchedulerTest {
    private DAGScheduler dagScheduler;
    private TestTaskScheduler taskScheduler;

    @BeforeEach
    void setUp() {
        taskScheduler = new TestTaskScheduler();
        dagScheduler = new DAGScheduler(taskScheduler);
    }

    @Test
    void shouldCreateAndExecuteSimpleJob() {
        // Create a simple RDD with 2 partitions
        TestRDD rdd = new TestRDD(2);
        List<CompletableFuture<Integer>> futures = dagScheduler.submitJob(rdd, 2);
        
        assertEquals(2, futures.size());
        assertEquals(1, taskScheduler.getSubmittedStages().size());
        
        // Complete the futures with some values
        taskScheduler.completeNextStageWithValues(List.of(1, 2));
        
        // Verify results
        List<Integer> results = futures.stream()
            .map(CompletableFuture::join)
            .toList();
        assertEquals(List.of(1, 2), results);
    }

    @Test
    void shouldHandleMultiStageJob() {
        // Create an RDD with a shuffle dependency
        TestRDD baseRdd = new TestRDD(2);
        TestShuffleRDD shuffleRdd = new TestShuffleRDD(baseRdd, 2);
        
        List<CompletableFuture<Integer>> futures = dagScheduler.submitJob(shuffleRdd, 2);
        
        assertEquals(2, futures.size());
        assertEquals(2, taskScheduler.getSubmittedStages().size());
        
        // Complete the shuffle map stage
        taskScheduler.completeNextStageWithValues(List.of(10, 20));
        
        // Complete the result stage
        taskScheduler.completeNextStageWithValues(List.of(30, 40));
        
        // Verify final results
        List<Integer> results = futures.stream()
            .map(CompletableFuture::join)
            .toList();
        assertEquals(List.of(30, 40), results);
    }

    // Test implementation of TaskScheduler that allows us to control task completion
    private static class TestTaskScheduler implements TaskScheduler {
        private final List<List<Task<?, ?>>> submittedStages = new ArrayList<>();
        private final List<List<CompletableFuture<?>>> stageFutures = new ArrayList<>();
        private boolean isStarted = false;

        @Override
        public <T> List<CompletableFuture<T>> submitTasks(List<? extends Task<?, T>> tasks, int numPartitions) {
            submittedStages.add((List) tasks);
            List<CompletableFuture<T>> futures = new ArrayList<>();
            for (int i = 0; i < numPartitions; i++) {
                futures.add(new CompletableFuture<>());
            }
            stageFutures.add((List) futures);
            return futures;
        }

        @Override
        public void start() {
            isStarted = true;
        }

        @Override
        public void stop() {
            isStarted = false;
        }

        @Override
        public int getNumWorkers() {
            return 2; // Return a fixed number for testing
        }

        @Override
        public int getTotalCores() {
            return 4; // Return a fixed number for testing
        }

        @Override
        public boolean isWorkerAlive(String workerId) {
            return true; // Always return true for testing
        }

        public List<List<Task<?, ?>>> getSubmittedStages() {
            return submittedStages;
        }

        public void completeNextStageWithValues(List<?> values) {
            List<CompletableFuture<?>> futures = stageFutures.get(0);
            stageFutures.remove(0);
            for (int i = 0; i < futures.size(); i++) {
                ((CompletableFuture) futures.get(i)).complete(values.get(i));
            }
        }
    }

    // Simple test RDD that returns its partition index as the value
    private static class TestRDD implements MiniRDD<Integer> {
        private final int numPartitions;

        public TestRDD(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public Partition[] getPartitions() {
            Partition[] partitions = new Partition[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = new Partition<>(i, List.of(i).iterator());
            }
            return partitions;
        }

        @Override
        public Iterator<Integer> compute(Partition split) {
            return List.of(split.index()).iterator();
        }

        @Override
        public List<MiniRDD<?>> getDependencies() {
            return List.of();
        }

        @Override
        public List<String> getPreferredLocations(Partition split) {
            return List.of();
        }

        @Override
        public <R> MiniRDD<R> map(Function<Integer, R> f) {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public MiniRDD<Integer> filter(java.util.function.Predicate<Integer> f) {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public List<Integer> collect() {
            throw new UnsupportedOperationException("Not implemented for test");
        }
    }

    // Test RDD that simulates a shuffle dependency
    private static class TestShuffleRDD extends TestRDD {
        private final TestRDD parent;

        public TestShuffleRDD(TestRDD parent, int numPartitions) {
            super(numPartitions);
            this.parent = parent;
        }

        @Override
        public List<MiniRDD<?>> getDependencies() {
            return List.of(parent);
        }
    }
} 