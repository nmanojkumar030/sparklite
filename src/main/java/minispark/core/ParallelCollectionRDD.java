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
    private final int numPartitions;

    public ParallelCollectionRDD(MiniSparkContext sc, List<T> data, int numPartitions) {
        this.sc = sc;
        this.data = new ArrayList<>(data);
        this.numPartitions = numPartitions;
    }

    @Override
    public Partition[] getPartitions() {
        @SuppressWarnings("unchecked")
        Partition[] result = new Partition[numPartitions];
        int itemsPerPartition = (int) Math.ceil((double) data.size() / numPartitions);

        for (int i = 0; i < numPartitions; i++) {
            int start = i * itemsPerPartition;
            int end = Math.min(start + itemsPerPartition, data.size());
            List<T> partitionData = data.subList(start, end);
            result[i] = new Partition<>(i, partitionData.iterator());
        }

        return result;
    }

    @Override
    public Iterator<T> compute(Partition split) {
        if (!(split instanceof Partition)) {
            throw new IllegalArgumentException("Invalid partition type");
        }
        @SuppressWarnings("unchecked")
        Partition<T> typedSplit = (Partition<T>) split;
        return typedSplit.iterator();
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
} 