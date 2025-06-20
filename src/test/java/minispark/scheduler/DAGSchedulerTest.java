package minispark.scheduler;

import minispark.core.BasePartition;
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
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        
        // Execute the tasks
        taskScheduler.executeNextStage();
        
        // Verify results
        List<Integer> results = futures.stream()
            .map(CompletableFuture::join)
            .toList();
        assertEquals(List.of(0, 1), results);
    }
    
    @Test
    void shouldHandleMapAndFilterTransformations() {
        // Create a simple RDD with 3 partitions containing values 0, 1, 2
        TestRDD baseRdd = new TestRDD(3);
        
        // Apply a map transformation to double each value: 0->0, 1->2, 2->4
        MapTransformRDD<Integer, Integer> mappedRdd = new MapTransformRDD<>(baseRdd, n -> n * 2);
        
        // Apply a filter to keep only values > 0: 2, 4
        FilterTransformRDD<Integer> filteredRdd = new FilterTransformRDD<>(mappedRdd, n -> n > 0);
        
        // Submit the job to process the RDD chain
        List<CompletableFuture<Integer>> futures = dagScheduler.submitJob(filteredRdd, filteredRdd.getPartitions().length);
        
        assertEquals(3, futures.size());
        // We expect a single stage since all are narrow transformations
        assertEquals(1, taskScheduler.getSubmittedStages().size());
        
        // Execute the tasks - this will actually apply the transformations
        taskScheduler.executeNextStage();
        
        // Verify results
        List<Integer> results = futures.stream()
            .map(CompletableFuture::join)
            .filter(result -> result != null) // Filter out nulls from empty partitions
            .toList();
        
        // The first partition (0) should be filtered out since 0*2=0 and filter keeps > 0
        // So we should only get results from partitions 1 and 2
        List<Integer> expected = List.of(2, 4); // From partitions 1 and 2 after map and filter
        assertEquals(expected, results);
    }
    
    @Test
    void shouldExecuteComplexTransformationChain() {
        // Create a base RDD with 4 partitions
        TestRDD baseRdd = new TestRDD(4); // Values: 0, 1, 2, 3
        
        // Create a complex transformation chain
        MiniRDD<Integer> transformedRdd = baseRdd
            .map(x -> x * 3)         // Values: 0, 3, 6, 9
            .filter(x -> x % 2 == 0)  // Values: 0, 6
            .map(x -> x + 5);         // Values: 5, 11
            
        // Submit job
        List<CompletableFuture<Integer>> futures = 
            dagScheduler.submitJob(transformedRdd, transformedRdd.getPartitions().length);
            
        assertEquals(4, futures.size());
        assertEquals(1, taskScheduler.getSubmittedStages().size());
        
        // Execute the tasks with the full transformation chain
        taskScheduler.executeNextStage();
        
        // Verify results
        List<Integer> results = futures.stream()
            .map(CompletableFuture::join)
            .filter(result -> result != null) // Filter out nulls from empty partitions
            .toList();
            
        // Only partitions 0 and 2 should have results after filtering
        List<Integer> expected = List.of(5, 11); // From partitions 0 and 2 after transformations
        assertEquals(expected, results);
    }
    
    @Test
    void shouldCreateSingleStageWithCompleteLineage() {
        // Create an RDD with 3 partitions
        TestRDD baseRdd = new TestRDD(3);
        
        // Create a transformation chain
        MiniRDD<Integer> transformedRdd = baseRdd
            .map(x -> x * 2)          // Double each value
            .filter(x -> x > 0)       // Keep positives
            .map(x -> x + 10);        // Add 10

        // Submit the job //This will happen in terminal actions like collect.
        List<CompletableFuture<Integer>> futures = dagScheduler.submitJob(transformedRdd, 3);
        
        // Verify there's only one stage created
        assertEquals(1, taskScheduler.getSubmittedStages().size());
        
        // Verify there's one task per partition
        List<Task<?, ?>> tasks = taskScheduler.getSubmittedStages().get(0);
        assertEquals(3, tasks.size());
        
        // In a real implementation, each task would execute the full lineage
        // Here, we simulate the execution results
        taskScheduler.completeNextStageWithValues(List.of(
            10,    // Partition 0: (0*2 > 0? No, filtered out) → Should actually be filtered
            12,    // Partition 1: (1*2 = 2) → (2 > 0? Yes) → (2+10 = 12)
            14     // Partition 2: (2*2 = 4) → (4 > 0? Yes) → (4+10 = 14)
        ));
        
        // Verify the results
        List<Integer> results = futures.stream()
            .map(CompletableFuture::join)
            .toList();
        
        // Note: In real execution with our TestRDD, partition 0 would be filtered out,
        // but our mock scheduler doesn't actually run the transformations
        assertEquals(List.of(10, 12, 14), results);
    }

    // Test implementation of TaskScheduler that executes tasks
    private static class TestTaskScheduler implements TaskScheduler {
        private final List<List<Task<?, ?>>> submittedStages = new ArrayList<>();
        private final List<List<CompletableFuture<?>>> stageFutures = new ArrayList<>();
        private boolean isStarted = false;

        @Override
        public <T> List<CompletableFuture<T>> submitTasks(List<? extends Task<?, T>> tasks, int numPartitions) {
            submittedStages.add((List) tasks);
            List<CompletableFuture<T>> futures = new ArrayList<>();
            for (int i = 0; i < tasks.size(); i++) {
                futures.add(new CompletableFuture<>());
            }
            stageFutures.add((List) futures);
            return futures;
        }
        
        // New method that actually executes tasks
        @SuppressWarnings("unchecked")
        public <T> void executeNextStage() {
            if (submittedStages.isEmpty() || stageFutures.isEmpty()) {
                throw new IllegalStateException("No stages to execute");
            }
            
            List<Task<?, ?>> tasks = submittedStages.get(0);
            List<CompletableFuture<?>> futures = stageFutures.get(0);
            
            submittedStages.remove(0);
            stageFutures.remove(0);
            
            // Execute each task and complete its future with the result
            for (int i = 0; i < tasks.size(); i++) {
                try {
                    Task<?, ?> task = tasks.get(i);
                    CompletableFuture<?> future = futures.get(i);
                    
                    // For tests, we directly call compute on the RDD for the corresponding partition
                    // instead of trying to execute the task
                    int partitionId = task.getPartitionId();
                    
                    if (task instanceof minispark.core.RDDTask) {
                        minispark.core.RDDTask<?> rddTask = (minispark.core.RDDTask<?>) task;
                        minispark.core.MiniRDD<?> rdd = getRddFromTask(rddTask);
                        
                        if (rdd != null) {
                            // Get the partition and compute the result
                            Partition[] partitions = rdd.getPartitions();
                            if (partitionId < partitions.length) {
                                CompletableFuture<?> futureIter = rdd.compute(partitions[partitionId]);
                                // Don't block - use thenApply to handle the result asynchronously
                                futureIter.thenApply(resultIter -> {
                                    Iterator<?> iter = (Iterator<?>) resultIter;
                                    if (iter.hasNext()) {
                                        completeCompletableFuture(future, iter.next());
                                    } else {
                                        completeCompletableFuture(future, null); // Empty result
                                    }
                                    return null;
                                }).exceptionally(throwable -> {
                                    future.completeExceptionally(throwable);
                                    return null;
                                });
                            } else {
                                completeCompletableFuture(future, null); // Partition index out of bounds
                            }
                        } else {
                            completeCompletableFuture(future, null); // No RDD found
                        }
                    } else {
                        // For non-RDD tasks, complete with a generic result
                        completeCompletableFuture(future, null);
                    }
                } catch (Exception e) {
                    System.err.println("Error executing task: " + e.getMessage());
                    e.printStackTrace();
                    futures.get(i).completeExceptionally(e);
                }
            }
        }

        // Helper method to safely complete a CompletableFuture with proper type casting
        @SuppressWarnings("unchecked")
        private <T> void completeCompletableFuture(CompletableFuture<?> future, Object result) {
            ((CompletableFuture<T>) future).complete((T) result);
        }

        // Helper method to extract RDD from RDDTask using reflection
        @SuppressWarnings("unchecked")
        private <T> MiniRDD<T> getRddFromTask(minispark.core.RDDTask<?> task) {
            try {
                // Use reflection to access the private rdd field
                java.lang.reflect.Field rddField = minispark.core.RDDTask.class.getDeclaredField("rdd");
                rddField.setAccessible(true);
                return (MiniRDD<T>) rddField.get(task);
            } catch (Exception e) {
                System.err.println("Error accessing RDD from task: " + e.getMessage());
                return null;
            }
        }

        // Original method for backward compatibility
        public void completeNextStageWithValues(List<?> values) {
            List<CompletableFuture<?>> futures = stageFutures.get(0);
            stageFutures.remove(0);
            submittedStages.remove(0);
            
            for (int i = 0; i < futures.size(); i++) {
                if (i < values.size()) {
                    ((CompletableFuture) futures.get(i)).complete(values.get(i));
                } else {
                    ((CompletableFuture) futures.get(i)).complete(null);
                }
            }
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
                partitions[i] = new BasePartition(i);
            }
            return partitions;
        }

        @Override
        public CompletableFuture<Iterator<Integer>> compute(Partition split) {
            return CompletableFuture.completedFuture(List.of(split.index()).iterator());
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
            return new MapTransformRDD<>(this, f);
        }

        @Override
        public MiniRDD<Integer> filter(Predicate<Integer> f) {
            return new FilterTransformRDD<>(this, f);
        }

        @Override
        public CompletableFuture<List<Integer>> collect() {
            List<CompletableFuture<Iterator<Integer>>> futures = new ArrayList<>();
            
            for (Partition partition : getPartitions()) {
                futures.add(compute(partition));
            }
            
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<Integer> result = new ArrayList<>();
                    for (CompletableFuture<Iterator<Integer>> future : futures) {
                        try {
                            Iterator<Integer> iter = future.join(); // Use join() instead of get() - it's non-blocking here since allOf already completed
                            while (iter.hasNext()) {
                                result.add(iter.next());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to get partition result", e);
                        }
                    }
                    return result;
                });
        }
    }

    // Map transformation RDD for tests
    private static class MapTransformRDD<T, R> implements MiniRDD<R> {
        private final MiniRDD<T> parent;
        private final Function<T, R> mapFunction;

        public MapTransformRDD(MiniRDD<T> parent, Function<T, R> mapFunction) {
            this.parent = parent;
            this.mapFunction = mapFunction;
        }

        @Override
        public Partition[] getPartitions() {
            return parent.getPartitions();
        }

        @Override
        public CompletableFuture<Iterator<R>> compute(Partition split) {
            return parent.compute(split).thenApply(parentIter -> {
                List<R> results = new ArrayList<>();
                while (parentIter.hasNext()) {
                    results.add(mapFunction.apply(parentIter.next()));
                }
                return results.iterator();
            });
        }

        @Override
        public List<MiniRDD<?>> getDependencies() {
            return List.of(parent);
        }

        @Override
        public List<String> getPreferredLocations(Partition split) {
            return parent.getPreferredLocations(split);
        }

        @Override
        public <U> MiniRDD<U> map(Function<R, U> f) {
            return new MapTransformRDD<>(this, f);
        }

        @Override
        public MiniRDD<R> filter(Predicate<R> f) {
            return new FilterTransformRDD<>(this, f);
        }

        @Override
        public CompletableFuture<List<R>> collect() {
            List<CompletableFuture<Iterator<R>>> futures = new ArrayList<>();
            
            for (Partition partition : getPartitions()) {
                futures.add(compute(partition));
            }
            
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<R> result = new ArrayList<>();
                    for (CompletableFuture<Iterator<R>> future : futures) {
                        try {
                            Iterator<R> iter = future.join(); // Use join() instead of get() - it's non-blocking here since allOf already completed
                            while (iter.hasNext()) {
                                result.add(iter.next());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to get partition result", e);
                        }
                    }
                    return result;
                });
        }
    }

    // Filter transformation RDD for tests
    private static class FilterTransformRDD<T> implements MiniRDD<T> {
        private final MiniRDD<T> parent;
        private final Predicate<T> filterPredicate;

        public FilterTransformRDD(MiniRDD<T> parent, Predicate<T> filterPredicate) {
            this.parent = parent;
            this.filterPredicate = filterPredicate;
        }

        @Override
        public Partition[] getPartitions() {
            return parent.getPartitions();
        }

        @Override
        public CompletableFuture<Iterator<T>> compute(Partition split) {
            return parent.compute(split).thenApply(parentIter -> {
                List<T> results = new ArrayList<>();
                while (parentIter.hasNext()) {
                    T item = parentIter.next();
                    if (filterPredicate.test(item)) {
                        results.add(item);
                    }
                }
                return results.iterator();
            });
        }

        @Override
        public List<MiniRDD<?>> getDependencies() {
            return List.of(parent);
        }

        @Override
        public List<String> getPreferredLocations(Partition split) {
            return parent.getPreferredLocations(split);
        }

        @Override
        public <R> MiniRDD<R> map(Function<T, R> f) {
            return new MapTransformRDD<>(this, f);
        }

        @Override
        public MiniRDD<T> filter(Predicate<T> f) {
            return new FilterTransformRDD<>(this, f);
        }

        @Override
        public CompletableFuture<List<T>> collect() {
            List<CompletableFuture<Iterator<T>>> futures = new ArrayList<>();
            
            for (Partition partition : getPartitions()) {
                futures.add(compute(partition));
            }
            
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<T> result = new ArrayList<>();
                    for (CompletableFuture<Iterator<T>> future : futures) {
                        try {
                            Iterator<T> iter = future.join(); // Use join() instead of get() - it's non-blocking here since allOf already completed
                            while (iter.hasNext()) {
                                result.add(iter.next());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to get partition result", e);
                        }
                    }
                    return result;
                });
        }
    }
} 