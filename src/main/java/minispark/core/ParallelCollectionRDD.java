package minispark.core;

import minispark.MiniSparkContext;
import minispark.core.transformations.MapRDD;
import minispark.core.transformations.FilterRDD;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A simple RDD that represents data already in memory.
 * The data is distributed evenly across partitions.
 */
public class ParallelCollectionRDD<T> implements MiniRDD<T> {
    private final MiniSparkContext sc;
    private final List<T> data;
    private final int numPartitions;

    public ParallelCollectionRDD(MiniSparkContext sc, List<T> data, int numPartitions) {
        this.sc = sc;
        this.data = new ArrayList<>(data);
        this.numPartitions = numPartitions;
    }

    @Override
    public Partition[] getPartitions() {
        Partition[] result = new Partition[numPartitions];
        int itemsPerPartition = (int) Math.ceil((double) data.size() / numPartitions);

        for (int i = 0; i < numPartitions; i++) {
            int start = i * itemsPerPartition;
            int end = Math.min(start + itemsPerPartition, data.size());
            List<T> partitionData = data.subList(start, end);
            result[i] = new ParallelCollectionPartition<>(i, partitionData);
        }

        return result;
    }

    @Override
    public CompletableFuture<Iterator<T>> compute(Partition split) {
        if (!(split instanceof ParallelCollectionPartition)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid partition type: " + 
                split.getClass().getName() + ", expected: ParallelCollectionPartition"));
        }
        
        @SuppressWarnings("unchecked")
        ParallelCollectionPartition<T> partition = (ParallelCollectionPartition<T>) split;
        return CompletableFuture.completedFuture(partition.iterator());
    }

    @Override
    public List<MiniRDD<?>> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return Collections.emptyList();
    }

    @Override
    public <R> MiniRDD<R> map(Function<T, R> f) {
        return new MapRDD<>(this, f);
    }

    @Override
    public MiniRDD<T> filter(Predicate<T> f) {
        return new FilterRDD<>(this, f);
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
                        Iterator<T> iter = future.join();
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