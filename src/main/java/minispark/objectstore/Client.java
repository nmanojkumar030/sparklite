package minispark.objectstore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.UUID;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import minispark.messages.*;
import minispark.objectstore.serialization.ObjectStoreSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class Client implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final MessageBus messageBus;
    private final NetworkEndpoint clientEndpoint;
    private final HashRing hashRing;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> pendingRequests;

    public Client(MessageBus messageBus, NetworkEndpoint clientEndpoint, List<NetworkEndpoint> serverEndpoints) {
        this.messageBus = messageBus;
        this.clientEndpoint = clientEndpoint;
        this.hashRing = new HashRing();
        this.pendingRequests = new ConcurrentHashMap<>();
        messageBus.registerHandler(clientEndpoint, this);
        
        // Initialize hash ring with servers
        for (NetworkEndpoint server : serverEndpoints) {
            hashRing.addServer(server);
        }
    }

    public void addServer(NetworkEndpoint server) {
        hashRing.addServer(server);
    }

    public void removeServer(NetworkEndpoint server) {
        hashRing.removeServer(server);
    }

    public NetworkEndpoint getTargetServer(String key) {
        return hashRing.getServerForKey(key);
    }

    public CompletableFuture<Void> putObject(String key, byte[] data) {
        logger.debug("Sending PUT_OBJECT for key {}", key);
        CompletableFuture<Object> future = new CompletableFuture<>();
        String correlationId = UUID.randomUUID().toString();
        pendingRequests.put(correlationId, future);
        PutObjectMessage message = new PutObjectMessage(key, data, true, correlationId);
        NetworkEndpoint targetServer = getTargetServer(key);
        messageBus.send(message, clientEndpoint, targetServer);
        return future.thenAccept(response -> {
            PutObjectResponseMessage resp = (PutObjectResponseMessage) response;
            if (!resp.isSuccess()) {
                throw new RuntimeException(resp.getErrorMessage());
            }
            logger.debug("PUT_OBJECT successful for key {}", key);
        });
    }

    public CompletableFuture<byte[]> getObject(String key) {
        logger.debug("Sending GET_OBJECT for key {}", key);
        CompletableFuture<Object> future = new CompletableFuture<>();
        String correlationId = UUID.randomUUID().toString();
        pendingRequests.put(correlationId, future);
        GetObjectMessage message = new GetObjectMessage(key, correlationId);
        NetworkEndpoint targetServer = getTargetServer(key);
        messageBus.send(message, clientEndpoint, targetServer);
        return future.thenApply(response -> {
            GetObjectResponseMessage resp = (GetObjectResponseMessage) response;
            if (!resp.isSuccess()) {
                throw new RuntimeException("Failed to retrieve object: " + key);
            }
            logger.debug("GET_OBJECT successful for key {}", key);
            return resp.getData();
        });
    }

    public CompletableFuture<Void> deleteObject(String key) {
        logger.debug("Sending DELETE_OBJECT for key {}", key);
        CompletableFuture<Object> future = new CompletableFuture<>();
        String correlationId = UUID.randomUUID().toString();
        pendingRequests.put(correlationId, future);
        DeleteObjectMessage message = new DeleteObjectMessage(key, correlationId);
        NetworkEndpoint targetServer = getTargetServer(key);
        messageBus.send(message, clientEndpoint, targetServer);
        return future.thenAccept(response -> {
            DeleteObjectResponseMessage resp = (DeleteObjectResponseMessage) response;
            if (!resp.isSuccess()) {
                throw new RuntimeException(resp.getErrorMessage());
            }
            logger.debug("DELETE_OBJECT successful for key {}", key);
        });
    }

    public CompletableFuture<List<String>> listObjects(String prefix) {
        logger.debug("Sending LIST_OBJECTS for prefix {}", prefix);
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        
        // Query all servers in parallel
        for (NetworkEndpoint server : hashRing.getServers()) {
            CompletableFuture<Object> future = new CompletableFuture<>();
            // Create unique correlation ID for each request
            String correlationId = UUID.randomUUID().toString();
            pendingRequests.put(correlationId, future);
            ListObjectsMessage message = new ListObjectsMessage(prefix, correlationId);
            messageBus.send(message, clientEndpoint, server);
            
            futures.add(future.thenApply(response -> {
                ListObjectsResponseMessage resp = (ListObjectsResponseMessage) response;
                if (!resp.isSuccess()) {
                    logger.warn("Failed to list objects from server {}: {}", server, resp.getErrorMessage());
                    return Collections.emptyList();
                }
                return resp.getObjects();
            }));
        }
        
        // Combine results from all servers
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList()));
    }

    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        String correlationId = message.getCorrelationId();
        if (correlationId == null) {
            logger.warn("Received message with null correlationId: {}", message.getType());
            return;
        }

        CompletableFuture<Object> future = pendingRequests.remove(correlationId);
        if (future != null) {
            future.complete(message);
        } else {
            logger.warn("No pending request found for correlationId: {}", correlationId);
        }
    }

    private String createRequestKey(String operation, String key) {
        return operation + ":" + key;
    }
} 