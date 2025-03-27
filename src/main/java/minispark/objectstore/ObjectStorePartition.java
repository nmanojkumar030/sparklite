package minispark.objectstore;

import minispark.core.Partition;

/**
 * A specialized partition for ObjectStoreRDD that stores information about
 * which object store keys should be accessed for this partition.
 */
public class ObjectStorePartition implements Partition {
    private final int partitionId;
    private final String baseKey;

    /**
     * Create a new ObjectStorePartition with the given ID and base key.
     *
     * @param partitionId The partition index
     * @param baseKey The base key prefix for objects in this partition
     */
    public ObjectStorePartition(int partitionId, String baseKey) {
        this.partitionId = partitionId;
        this.baseKey = baseKey;
    }

    @Override
    public int index() {
        return partitionId;
    }

    /**
     * Get the base key prefix for this partition.
     *
     * @return The base key prefix
     */
    public String getBaseKey() {
        return baseKey;
    }
    
    @Override
    public String toString() {
        return "ObjectStorePartition(" + partitionId + ", " + baseKey + ")";
    }
} 