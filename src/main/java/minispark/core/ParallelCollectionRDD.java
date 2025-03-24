package minispark.core;

import minispark.MiniSparkContext;
import minispark.core.transformations.MapRDD;
import minispark.core.transformations.FilterRDD;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public class ParallelCollectionRDD<T> implements MiniRDD<T> {
    private final MiniSparkContext sc;
    private final List<T> data;
    private final int numSlices;
    private Partition[] partitions;

    public ParallelCollectionRDD(MiniSparkContext sc, List<T> data, int numSlices) {
        this.sc = sc;
        this.data = data;
        this.numSlices = numSlices;
        this.partitions = computePartitions();
    }

    private Partition[] computePartitions() {
        Partition[] result = new Partition[numSlices];
        for (int i = 0; i < numSlices; i++) {
            result[i] = new CollectionPartition(i);
        }
        return result;
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    @Override
    public Iterator<T> compute(Partition split) {
        CollectionPartition partition = (CollectionPartition) split;
        int start = partition.index() * (data.size() / numSlices);
        int end = (partition.index() + 1) * (data.size() / numSlices);
        if (partition.index() == numSlices - 1) {
            end = data.size();
        }
        return data.subList(start, end).iterator();
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
    public List<T> collect() {
        List<T> result = new ArrayList<>();
        for (Partition partition : getPartitions()) {
            Iterator<T> iter = compute(partition);
            while (iter.hasNext()) {
                result.add(iter.next());
            }
        }
        return result;
    }

    private class CollectionPartition implements Partition {
        private final int idx;

        CollectionPartition(int idx) {
            this.idx = idx;
        }

        @Override
        public int index() {
            return idx;
        }
    }
} 