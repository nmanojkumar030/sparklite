package minispark.core;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import minispark.core.transformations.MapRDD;
import minispark.core.transformations.FilterRDD;

public class ParallelCollectionRDD<T> implements MiniRDD<T> {
    private final List<T> data;

    public ParallelCollectionRDD(List<T> data) {
        this.data = data;
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
        return data;
    }

    @Override
    public MiniRDD<T> getParent() {
        return null; // This is the root RDD
    }

    @Override
    public Function<?, T> getTransformation() {
        return null; // No transformation for root RDD
    }
} 