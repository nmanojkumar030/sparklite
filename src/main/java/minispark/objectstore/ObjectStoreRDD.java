package minispark.objectstore;

import minispark.MiniSparkContext;
import minispark.core.MiniRDD;
import minispark.core.Partition;
import java.util.*;

public class ObjectStoreRDD<T> implements MiniRDD<T> {
    private final MiniSparkContext sc;
    private final Client objectStoreClient;
    private final String baseKey;
    private final int numPartitions;
    private final Partition[] partitions;
    private Map<String, byte[]> cachedData;

    public ObjectStoreRDD(MiniSparkContext sc, Client objectStoreClient, String key, int numPartitions) {
        this.sc = sc;
        this.objectStoreClient = objectStoreClient;
        this.baseKey = key;
        this.numPartitions = numPartitions;
        this.partitions = createPartitions();
        this.cachedData = new HashMap<>();
    }

    private Partition[] createPartitions() {
        Partition[] result = new Partition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            // Create a unique key for each partition by appending the partition ID
            String partitionKey = baseKey + "-part-" + i;
            result[i] = new ObjectStorePartition(i, partitionKey);
        }
        return result;
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    @Override
    public Iterator<T> compute(Partition split) {
        if (!(split instanceof ObjectStorePartition)) {
            throw new IllegalArgumentException("Invalid partition type");
        }
        ObjectStorePartition partition = (ObjectStorePartition) split;
        
        try {
            // Read data only once and cache it
            if (!cachedData.containsKey(partition.getKey())) {
                cachedData.put(partition.getKey(), objectStoreClient.getObject(partition.getKey()).get());
            }
            
            byte[] data = cachedData.get(partition.getKey());
            if (data == null || data.length == 0) {
                return Collections.emptyIterator();
            }
            
            // For now, we'll just convert the byte array to a single string
            // In a real implementation, we would need proper deserialization
            return Collections.singletonList((T) new String(data)).iterator();
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
    public <R> MiniRDD<R> map(java.util.function.Function<T, R> f) {
        throw new UnsupportedOperationException("Map operation not implemented yet");
    }

    @Override
    public MiniRDD<T> filter(java.util.function.Predicate<T> f) {
        throw new UnsupportedOperationException("Filter operation not implemented yet");
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

    private static class ObjectStorePartition extends Partition<String> {
        private final String key;

        public ObjectStorePartition(int partitionId, String key) {
            super(partitionId, null);
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }
} 