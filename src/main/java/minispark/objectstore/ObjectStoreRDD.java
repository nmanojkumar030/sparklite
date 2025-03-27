package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.core.MiniRDD;
import minispark.core.Partition;
import java.util.*;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import minispark.objectstore.serialization.ObjectStoreSerializer;

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
    public Iterator<byte[]> compute(Partition split) {
        ObjectStorePartition partition;
        if (split instanceof ObjectStoreRDD.ObjectStorePartition) {
            partition = (ObjectStorePartition) split;
        } else {
            // Create a new partition with the same ID if we received a generic Partition
            partition = new ObjectStorePartition(split.getPartitionId(), baseKey);
            logger.debug("Created new ObjectStorePartition from generic partition with ID {}", split.getPartitionId());
        }
        
        try {
            // List all objects with the base key prefix
            List<String> allKeys = objectStoreClient.listObjects(baseKey).get();
            logger.debug("Found {} keys with prefix {}", allKeys.size(), baseKey);
            
            // Filter keys for this partition based on hash
            List<String> partitionKeys = new ArrayList<>();
            for (String key : allKeys) {
                if (Math.abs(hash(key) % numPartitions) == partition.getPartitionId()) {
                    partitionKeys.add(key);
                    logger.debug("Key {} assigned to partition {}", key, partition.getPartitionId());
                }
            }
            logger.debug("Partition {} has {} keys", partition.getPartitionId(), partitionKeys.size());
            
            // Read data for this partition's keys
            List<byte[]> partitionData = new ArrayList<>();
            for (String key : partitionKeys) {
                try {
                    byte[] data = objectStoreClient.getObject(key).get();
                    if (data != null && data.length > 0) {
                        partitionData.add(data);
                        logger.debug("Successfully read data for key {}", key);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to read object with key {}: {}", key, e.getMessage());
                }
            }
            
            return partitionData.iterator();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read data from ObjectStore", e);
        }
    }

    @Override
    public List<MiniRDD<?>> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        // For now, we don't implement data locality
        return Collections.emptyList();
    }


    @Override
    public <R> MiniRDD<R> map(java.util.function.Function<byte[], R> f) {
        return new MappedRDD<>(this, f);
    }

    @Override
    public MiniRDD<byte[]> filter(java.util.function.Predicate<byte[]> f) {
        return new FilteredRDD<>(this, f);
    }

    @Override
    public List<byte[]> collect() {
        List<byte[]> result = new ArrayList<>();
        for (Partition partition : getPartitions()) {
            Iterator<byte[]> iter = compute(partition);
            while (iter.hasNext()) {
                result.add(iter.next());
            }
        }
        return result;
    }

    public static class ObjectStorePartition extends Partition<byte[]> {
        private final String baseKey;

        public ObjectStorePartition(int partitionId, String baseKey) {
            super(partitionId, null);
            this.baseKey = baseKey;
        }

        public String getBaseKey() {
            return baseKey;
        }
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

    private static class MappedRDD<R, T> implements MiniRDD<R> {
        private final MiniRDD<T> parent;
        private final java.util.function.Function<T, R> f;
        private final Partition[] partitions;

        public MappedRDD(MiniRDD<T> parent, java.util.function.Function<T, R> f) {
            this.parent = parent;
            this.f = f;
            this.partitions = parent.getPartitions();
        }

        @Override
        public Partition[] getPartitions() {
            return partitions;
        }

        @Override
        public Iterator<R> compute(Partition split) {
            Iterator<T> parentIter = parent.compute(split);
            return new Iterator<R>() {
                @Override
                public boolean hasNext() {
                    return parentIter.hasNext();
                }

                @Override
                public R next() {
                    return f.apply(parentIter.next());
                }
            };
        }

        @Override
        public List<MiniRDD<?>> getDependencies() {
            return Collections.singletonList(parent);
        }

        @Override
        public List<String> getPreferredLocations(Partition split) {
            return parent.getPreferredLocations(split);
        }

        @Override
        public <R2> MiniRDD<R2> map(java.util.function.Function<R, R2> f) {
            return new MappedRDD<>(this, f);
        }

        @Override
        public MiniRDD<R> filter(java.util.function.Predicate<R> f) {
            return new FilteredRDD<>(this, f);
        }

        @Override
        public List<R> collect() {
            List<R> result = new ArrayList<>();
            for (Partition partition : getPartitions()) {
                Iterator<R> iter = compute(partition);
                while (iter.hasNext()) {
                    result.add(iter.next());
                }
            }
            return result;
        }
    }

    private static class FilteredRDD<T> implements MiniRDD<T> {
        private final MiniRDD<T> parent;
        private final java.util.function.Predicate<T> f;
        private final Partition[] partitions;

        public FilteredRDD(MiniRDD<T> parent, java.util.function.Predicate<T> f) {
            this.parent = parent;
            this.f = f;
            this.partitions = parent.getPartitions();
        }

        @Override
        public Partition[] getPartitions() {
            return partitions;
        }

        @Override
        public Iterator<T> compute(Partition split) {
            Iterator<T> parentIter = parent.compute(split);
            return new Iterator<T>() {
                private T next = null;
                private boolean hasNext = false;

                @Override
                public boolean hasNext() {
                    if (!hasNext) {
                        while (parentIter.hasNext()) {
                            next = parentIter.next();
                            if (f.test(next)) {
                                hasNext = true;
                                return true;
                            }
                        }
                        return false;
                    }
                    return true;
                }

                @Override
                public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    hasNext = false;
                    return next;
                }
            };
        }

        @Override
        public List<MiniRDD<?>> getDependencies() {
            return Collections.singletonList(parent);
        }

        @Override
        public List<String> getPreferredLocations(Partition split) {
            return parent.getPreferredLocations(split);
        }

        @Override
        public <R> MiniRDD<R> map(java.util.function.Function<T, R> f) {
            return new MappedRDD<>(this, f);
        }

        @Override
        public MiniRDD<T> filter(java.util.function.Predicate<T> f) {
            return new FilteredRDD<>(this, f);
        }

        @Override
        public List<T> collect() {
            List<T> result = new ArrayList<>();
            for (Partition partition : getPartitions()) {
                Iterator<T> iter = compute(partition);
                while (iter.hasNext()) {
                    result.add(iter.next());
                }
            }
            return result;
        }
    }
} 