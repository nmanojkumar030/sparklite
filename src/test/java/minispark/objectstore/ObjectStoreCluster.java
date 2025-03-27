package minispark.objectstore;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manages a cluster of ObjectStore servers and a client for testing purposes.
 */
public class ObjectStoreCluster implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ObjectStoreCluster.class);
    
    private final MessageBus messageBus;
    private final Client client;
    private final List<ServerNode> serverNodes;
    
    public ObjectStoreCluster(MessageBus messageBus, Path tempDir, int numServers) {
        this.messageBus = messageBus;
        
        // Create server nodes
        serverNodes = new ArrayList<>();
        for (int i = 0; i < numServers; i++) {
            ServerNode node = new ServerNode(
                "server-" + i,
                new LocalStorageNode(tempDir.resolve("server" + i).toString()),
                new NetworkEndpoint("localhost", 8082 + i)
            );
            node.server = new Server(node.id, node.storage, messageBus, node.endpoint);
            serverNodes.add(node);
        }

        // Create client with all server endpoints
        NetworkEndpoint clientEndpoint = new NetworkEndpoint("localhost", 8081);
        client = new Client(messageBus, clientEndpoint, 
            serverNodes.stream()
                .map(node -> node.endpoint)
                .collect(Collectors.toList()));
    }
    
    public void start() {
        // Server nodes are already registered with MessageBus in their constructor
        logger.info("Started ObjectStore cluster with {} servers", serverNodes.size());
    }
    
    public void stop() {
        // No need to stop servers as they don't have a stop method
        logger.info("Stopped ObjectStore cluster");
    }
    
    @Override
    public void close() {
        stop();
    }
    
    public Client getClient() {
        return client;
    }
    
    public List<ServerNode> getServerNodes() {
        return serverNodes;
    }
    
    public ServerNode getServerForKey(String key) {
        NetworkEndpoint targetServer = client.getTargetServer(key);
        return serverNodes.stream()
            .filter(node -> node.endpoint.equals(targetServer))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No server found for key: " + key));
    }
    
    public int countObjectsForServer(LocalStorageNode storageNode) {
        try {
            return storageNode.listObjects("").size();
        } catch (Exception e) {
            return 0;
        }
    }
    
    public static class ServerNode {
        final String id;
        final LocalStorageNode storage;
        final NetworkEndpoint endpoint;
        Server server;

        ServerNode(String id, LocalStorageNode storage, NetworkEndpoint endpoint) {
            this.id = id;
            this.storage = storage;
            this.endpoint = endpoint;
        }
    }
} 