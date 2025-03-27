package minispark.core;

import java.io.Serializable;

/**
 * An identifier for a partition in an RDD.
 * Each RDD implementation can have its own Partition implementation
 * that contains the necessary information to identify and access the data.
 */
public interface Partition extends Serializable {
    /**
     * Get the index of this partition within its parent RDD.
     *
     * @return The partition index
     */
    int index();
} 