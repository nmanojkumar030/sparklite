package minispark.distributed.objectstore;

import minispark.MiniSparkContext;
import minispark.distributed.rdd.MiniRDD;
import minispark.distributed.rdd.Partition;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

import minispark.distributed.rdd.transformations.FilterRDD;
import minispark.distributed.rdd.transformations.MapRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An RDD implementation that reads data from an object store.
 * It partitions objects based on their keys using a hash function.
 */
public class ObjectStoreRDD implements MiniRDD<byte[]> {
    private final MiniSparkContext sc;
    private final Client objectStoreClient;
    private final String baseKey;
    private final int numPartitions;
    private final Partition[] partitions;
    private static final Logger logger = LoggerFactory.getLogger(ObjectStoreRDD.class);

    public ObjectStoreRDD(MiniSparkContext sc, Client objectStoreClient, String key, int numPartitions) {
        this.sc = sc;
        this.objectStoreClient = objectStoreClient;
        this.baseKey = key;
        this.numPartitions = numPartitions;
        this.partitions = createPartitions();
    }

    private Partition[] createPartitions() {
        Partition[] result = new Partition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            result[i] = new ObjectStorePartition(i, baseKey);
        }
        return result;
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    @Override
    public CompletableFuture<Iterator<byte[]>> compute(Partition split) {
        logger.debug("Computing partition {} asynchronously", split.index());
        ObjectStorePartition partition = new ObjectStorePartition(split.index(), baseKey);
        return computePartitionAsync(partition);
    }

    private CompletableFuture<Iterator<byte[]>> computePartitionAsync(ObjectStorePartition partition) {
        // First, list all objects with the base key prefix
        return objectStoreClient.listObjects(partition.getBaseKey())
            .thenCompose(allKeys -> {
                logger.debug("Found {} keys with prefix {}", allKeys.size(), partition.getBaseKey());

                // Filter keys for this partition based on hash
                List<String> partitionKeys = new ArrayList<>();
                for (String key : allKeys) {
                    if (Math.abs(hash(key) % numPartitions) == partition.index()) {
                        partitionKeys.add(key);
                        logger.debug("Key {} assigned to partition {}", key, partition.index());
                    }
                }
                logger.debug("Partition {} has {} keys", partition.index(), partitionKeys.size());

                // Create futures for reading all partition keys
                List<CompletableFuture<byte[]>> dataFutures = new ArrayList<>();
                for (String key : partitionKeys) {
                    CompletableFuture<byte[]> dataFuture = objectStoreClient.getObject(key)
                        .handle((data, throwable) -> {
                            if (throwable != null) {
                                logger.warn("Failed to read object with key {}: {}", key, throwable.getMessage());
                                return null;
                            }
                            if (data != null && data.length > 0) {
                                logger.debug("Successfully read data for key {}", key);
                                return data;
                            }
                            return null;
                        });
                    dataFutures.add(dataFuture);
                }

                // Combine all futures and return iterator
                return CompletableFuture.allOf(dataFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        List<byte[]> partitionData = new ArrayList<>();
                        for (CompletableFuture<byte[]> future : dataFutures) {
                            try {
                                byte[] data = future.join();
                                if (data != null) {
                                    partitionData.add(data);
                                }
                            } catch (Exception e) {
                                logger.warn("Error getting future result: {}", e.getMessage());
                            }
                        }
                        return partitionData.iterator();
                    });
            })
            .exceptionally(throwable -> {
                logger.error("Failed to read data from ObjectStore", throwable);
                throw new RuntimeException("Failed to read data from ObjectStore", throwable);
            });
    }

    @Override
    public List<MiniRDD<?>> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        // For now, we don't have information about data locality
        return Collections.emptyList();
    }

    @Override
    public <R> MiniRDD<R> map(Function<byte[], R> f) {
        return new MapRDD<>(this, f);
    }

    @Override
    public MiniRDD<byte[]> filter(Predicate<byte[]> f) {
        return new FilterRDD<>(this, f);
    }

    @Override
    public CompletableFuture<List<byte[]>> collect() {
        List<CompletableFuture<Iterator<byte[]>>> futures = new ArrayList<>();
        
        // Start all partition computations asynchronously
        for (Partition partition : getPartitions()) {
            futures.add(compute(partition));
        }
        
        // Combine all futures and return the final result
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<byte[]> result = new ArrayList<>();
                for (CompletableFuture<Iterator<byte[]>> future : futures) {
                    try {
                        Iterator<byte[]> iter = future.join();
                        while (iter.hasNext()) {
                            result.add(iter.next());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get partition result", e);
                    }
                }
                return result;
            });
    }

    private long hash(String key) {
        // Using MurmurHash for better distribution
        byte[] data = key.getBytes();
        long seed = 0x1234ABCD;
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;
        long h = seed ^ (data.length * m);

        for (int i = 0; i < data.length; i++) {
            h = (h + (data[i] & 0xFF)) * m;
            h ^= h >>> r;
        }

        h *= m;
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        return h;
    }
} 