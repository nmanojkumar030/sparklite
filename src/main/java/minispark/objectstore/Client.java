package minispark.objectstore;

import minispark.messages.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        serverEndpoints.forEach(hashRing::addServer);
        this.pendingRequests = new ConcurrentHashMap<>();
        messageBus.registerHandler(clientEndpoint, this);
        logger.info("Client registered with MessageBus at endpoint {}", clientEndpoint);
    }

    private String createRequestKey(String messageType, String key) {
        return messageType + ":" + key;
    }

    private NetworkEndpoint getTargetServer(String key) {
        NetworkEndpoint server = hashRing.getServerForKey(key);
        logger.debug("Key '{}' mapped to server {}", key, server);
        return server;
    }

    public CompletableFuture<Void> putObject(String key, byte[] data) {
        logger.debug("Sending PUT_OBJECT for key {}", key);
        CompletableFuture<Object> future = new CompletableFuture<>();
        String requestKey = createRequestKey("PUT_OBJECT", key);
        pendingRequests.put(requestKey, future);
        PutObjectMessage message = new PutObjectMessage(key, data, true);
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
        String requestKey = createRequestKey("GET_OBJECT", key);
        pendingRequests.put(requestKey, future);
        GetObjectMessage message = new GetObjectMessage(key);
        NetworkEndpoint targetServer = getTargetServer(key);
        messageBus.send(message, clientEndpoint, targetServer);
        return future.thenApply(response -> {
            GetObjectResponseMessage resp = (GetObjectResponseMessage) response;
            if (!resp.isSuccess()) {
                throw new RuntimeException(resp.getErrorMessage());
            }
            logger.debug("GET_OBJECT successful for key {}", key);
            return resp.getData();
        });
    }

    public CompletableFuture<Void> deleteObject(String key) {
        logger.debug("Sending DELETE_OBJECT for key {}", key);
        CompletableFuture<Object> future = new CompletableFuture<>();
        String requestKey = createRequestKey("DELETE_OBJECT", key);
        pendingRequests.put(requestKey, future);
        DeleteObjectMessage message = new DeleteObjectMessage(key);
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
        logger.debug("Sending LIST_OBJECTS with prefix {}", prefix);
        CompletableFuture<Object> future = new CompletableFuture<>();
        String requestKey = createRequestKey("LIST_OBJECTS", prefix);
        pendingRequests.put(requestKey, future);
        ListObjectsMessage message = new ListObjectsMessage(prefix);
        // For list operations, we can use any server since we need to aggregate results
        NetworkEndpoint targetServer = hashRing.getServers().stream().findFirst().orElseThrow();
        messageBus.send(message, clientEndpoint, targetServer);
        return future.thenApply(response -> {
            ListObjectsResponseMessage resp = (ListObjectsResponseMessage) response;
            if (!resp.isSuccess()) {
                throw new RuntimeException(resp.getErrorMessage());
            }
            logger.debug("LIST_OBJECTS successful, found {} objects", resp.getObjects().size());
            return resp.getObjects();
        });
    }

    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        logger.debug("Received message of type {} from {}", message.getType(), sender);
        CompletableFuture<Object> future = null;
        String requestKey = null;
        String key = null;

        if (message instanceof PutObjectResponseMessage) {
            PutObjectResponseMessage response = (PutObjectResponseMessage) message;
            key = response.getKey();
            requestKey = createRequestKey("PUT_OBJECT", key);
        } else if (message instanceof GetObjectResponseMessage) {
            GetObjectResponseMessage response = (GetObjectResponseMessage) message;
            key = response.getKey();
            requestKey = createRequestKey("GET_OBJECT", key);
        } else if (message instanceof DeleteObjectResponseMessage) {
            DeleteObjectResponseMessage response = (DeleteObjectResponseMessage) message;
            key = response.getKey();
            requestKey = createRequestKey("DELETE_OBJECT", key);
        } else if (message instanceof ListObjectsResponseMessage) {
            ListObjectsResponseMessage response = (ListObjectsResponseMessage) message;
            requestKey = createRequestKey("LIST_OBJECTS", "");
        }

        if (requestKey != null) {
            future = pendingRequests.remove(requestKey);
            if (future != null) {
                future.complete(message);
                logger.debug("Completed future for message type {} with key {}", message.getType(), key);
            } else {
                logger.warn("No pending request found for message type {} with key {}", message.getType(), key);
            }
        } else {
            logger.warn("Received unknown message type: {}", message.getType());
        }
    }
} 