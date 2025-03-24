package minispark;

import minispark.core.MiniRDD;
import minispark.core.ParallelCollectionRDD;
import java.util.List;

public class MiniSparkContext {
    private final int defaultParallelism;

    public MiniSparkContext(int defaultParallelism) {
        this.defaultParallelism = defaultParallelism;
    }

    public <T> MiniRDD<T> parallelize(List<T> data) {
        return parallelize(data, defaultParallelism);
    }

    public <T> MiniRDD<T> parallelize(List<T> data, int numSlices) {
        return new ParallelCollectionRDD<>(this, data, numSlices);
    }

    public int defaultParallelism() {
        return defaultParallelism;
    }
} 