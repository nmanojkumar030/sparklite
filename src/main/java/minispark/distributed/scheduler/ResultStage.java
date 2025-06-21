package minispark.distributed.scheduler;

import minispark.distributed.rdd.MiniRDD;

/**
 * ResultStage is the final stage in a job that applies a function to compute the result of an action.
 * For example, count() or collect() will run a ResultStage that executes the final computation
 * on an RDD's partitions.
 */
public class ResultStage extends Stage {
    private final Object jobId;

    public ResultStage(int stageId, MiniRDD<?> rdd, int numPartitions, Object jobId) {
        super(stageId, rdd, numPartitions);
        this.jobId = jobId;
    }

    public Object getJobId() {
        return jobId;
    }

    @Override
    public boolean isShuffleMap() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("ResultStage(id=%d, rdd=%s, jobId=%s)", getStageId(), getRdd(), jobId);
    }
} 