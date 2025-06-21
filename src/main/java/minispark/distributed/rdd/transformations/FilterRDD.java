package minispark.distributed.rdd.transformations;

import minispark.distributed.rdd.MiniRDD;
import minispark.distributed.rdd.Partition;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterRDD<T> implements MiniRDD<T> {
    private final MiniRDD<T> parent;
    private final Predicate<T> predicate;
    private final Partition[] partitions;

    public FilterRDD(MiniRDD<T> parent, Predicate<T> f) {
        this.parent = parent;
        this.predicate = f;
        this.partitions = parent.getPartitions();
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    @Override
    public CompletableFuture<Iterator<T>> compute(Partition split) {
        return parent.compute(split).thenApply(parentIter -> {
            return new Iterator<T>() {
                private T nextElement = null;
                private boolean hasNext = false;

                private void findNext() {
                    while (!hasNext && parentIter.hasNext()) {
                        T element = parentIter.next();
                        if (predicate.test(element)) {
                            nextElement = element;
                            hasNext = true;
                        }
                    }
                }

                @Override
                public boolean hasNext() {
                    if (!hasNext) {
                        findNext();
                    }
                    return hasNext;
                }

                @Override
                public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    hasNext = false;
                    return nextElement;
                }
            };
        });
    }

    @Override
    public List<MiniRDD<?>> getDependencies() {
        return Collections.singletonList(parent);
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return parent.getPreferredLocations(split);
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