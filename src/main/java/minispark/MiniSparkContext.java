package minispark;

import minispark.distributed.rdd.MiniRDD;
import minispark.distributed.rdd.ParallelCollectionRDD;
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