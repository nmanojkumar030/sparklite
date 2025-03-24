package minispark.core.transformations;

import minispark.core.MiniRDD;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterRDD<T> implements MiniRDD<T> {
    private final MiniRDD<T> parent;
    private final Predicate<T> predicate;

    public FilterRDD(MiniRDD<T> parent, Predicate<T> predicate) {
        this.parent = parent;
        this.predicate = predicate;
    }

    @Override
    public <R> MiniRDD<R> map(Function<T, R> f) {
        return new MapRDD<>(this, f);
    }

    @Override
    public MiniRDD<T> filter(Predicate<T> f) {
        return new FilterRDD<>(this, predicate.and(f));
    }

    @Override
    public List<T> collect() {
        List<T> parentData = parent.collect();
        List<T> result = new ArrayList<>();
        for (T item : parentData) {
            if (predicate.test(item)) {
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public MiniRDD<T> getParent() {
        return parent;
    }

    @Override
    public Function<?, T> getTransformation() {
        return null; // Filter doesn't transform types
    }
} 