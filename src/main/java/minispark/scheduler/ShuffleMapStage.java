package minispark.scheduler;

import minispark.core.MiniRDD;

/**
 * ShuffleMapStage is a stage that produces data for a shuffle. It writes map output files that can
 * be fetched by reduce tasks in subsequent stages. The results of a ShuffleMapStage will generally
 * be reused by multiple child stages.
 */
public class ShuffleMapStage extends Stage {
    private final int shuffleId;
    private boolean outputLocationsAvailable;

    public ShuffleMapStage(int stageId, MiniRDD<?> rdd, int numPartitions, int shuffleId) {
        super(stageId, rdd, numPartitions);
        this.shuffleId = shuffleId;
        this.outputLocationsAvailable = false;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public boolean isOutputLocationsAvailable() {
        return outputLocationsAvailable;
    }

    public void setOutputLocationsAvailable(boolean available) {
        this.outputLocationsAvailable = available;
    }

    @Override
    public boolean isShuffleMap() {
        return true;
    }

    @Override
    public String toString() {
        return String.format("ShuffleMapStage(id=%d, rdd=%s, shuffleId=%d)", getStageId(), getRdd(), shuffleId);
    }
} 