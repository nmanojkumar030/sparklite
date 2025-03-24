package minispark.core.transformations;

import minispark.core.MiniRDD;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class MapRDD<T, R> implements MiniRDD<R> {
    private final MiniRDD<T> parent;
    private final Function<T, R> transformation;

    public MapRDD(MiniRDD<T> parent, Function<T, R> transformation) {
        this.parent = parent;
        this.transformation = transformation;
    }

    @Override
    public <U> MiniRDD<U> map(Function<R, U> f) {
        return new MapRDD<>(this, f);
    }

    @Override
    public MiniRDD<R> filter(Predicate<R> f) {
        return new FilterRDD<>(this, f);
    }

    @Override
    public List<R> collect() {
        List<T> parentData = parent.collect();
        List<R> result = new ArrayList<>(parentData.size());
        for (T item : parentData) {
            result.add(transformation.apply(item));
        }
        return result;
    }

    @Override
    public MiniRDD<R> getParent() {
        return (MiniRDD<R>) parent;
    }

    @Override
    public Function<?, R> getTransformation() {
        return (Function<?, R>) transformation;
    }
} 