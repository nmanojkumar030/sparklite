package minispark.objectstore;

import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class HashRing {
    private static final Logger logger = LoggerFactory.getLogger(HashRing.class);
    private static final int VIRTUAL_NODES_PER_SERVER = 100;
    private final ConcurrentSkipListMap<Long, NetworkEndpoint> ring;
    private final ConcurrentHashMap<NetworkEndpoint, Set<Long>> serverToPoints;

    public HashRing() {
        this.ring = new ConcurrentSkipListMap<>();
        this.serverToPoints = new ConcurrentHashMap<>();
    }

    public void addServer(NetworkEndpoint server) {
        logger.info("Adding server {} to hash ring", server);
        Set<Long> points = new HashSet<>();
        
        for (int i = 0; i < VIRTUAL_NODES_PER_SERVER; i++) {
            String virtualNode = server.toString() + "#" + i;
            long hash = hash(virtualNode);
            ring.put(hash, server);
            points.add(hash);
        }
        
        serverToPoints.put(server, points);
    }

    public void removeServer(NetworkEndpoint server) {
        logger.info("Removing server {} from hash ring", server);
        Set<Long> points = serverToPoints.remove(server);
        if (points != null) {
            points.forEach(ring::remove);
        }
    }

    public NetworkEndpoint getServerForKey(String key) {
        long hash = hash(key);
        Map.Entry<Long, NetworkEndpoint> entry = ring.ceilingEntry(hash);
        
        if (entry == null) {
            // If we're at the end of the ring, wrap around to the beginning
            entry = ring.firstEntry();
        }
        
        NetworkEndpoint server = entry.getValue();
        logger.debug("Key {} mapped to server {}", key, server);
        return server;
    }

    public List<NetworkEndpoint> getServersForKey(String key, int count) {
        List<NetworkEndpoint> servers = new ArrayList<>();
        long hash = hash(key);
        
        // Get the first server
        Map.Entry<Long, NetworkEndpoint> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        
        // Add servers until we have enough
        while (servers.size() < count) {
            NetworkEndpoint server = entry.getValue();
            if (!servers.contains(server)) {
                servers.add(server);
            }
            
            // Get next server
            entry = ring.higherEntry(entry.getKey());
            if (entry == null) {
                entry = ring.firstEntry();
            }
            
            // If we've gone full circle, break
            if (entry.getKey() == hash) {
                break;
            }
        }
        
        logger.debug("Key {} mapped to {} servers: {}", key, servers.size(), servers);
        return servers;
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

    public Set<NetworkEndpoint> getServers() {
        return new HashSet<>(serverToPoints.keySet());
    }
} 