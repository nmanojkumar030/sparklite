package minispark.network;

import minispark.messages.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final MessageBus messageBus;
    private final NetworkEndpoint clientEndpoint;
    private final NetworkEndpoint serverEndpoint;
    private final ConcurrentHashMap<String, CompletableFuture<Object>> pendingRequests;

    public Client(MessageBus messageBus, NetworkEndpoint clientEndpoint, NetworkEndpoint serverEndpoint) {
        this.messageBus = messageBus;
        this.clientEndpoint = clientEndpoint;
        this.serverEndpoint = serverEndpoint;
        this.pendingRequests = new ConcurrentHashMap<>();
        messageBus.registerHandler(clientEndpoint, this);
        logger.info("Client registered with MessageBus at endpoint {}", clientEndpoint);
    }

    public CompletableFuture<Void> putObject(String key, byte[] data) {
        logger.debug("Sending PUT_OBJECT for key {}", key);
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(key, future);
        PutObjectMessage message = new PutObjectMessage(key, data);
        messageBus.send(message, clientEndpoint, serverEndpoint);
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
        pendingRequests.put(key, future);
        GetObjectMessage message = new GetObjectMessage(key);
        messageBus.send(message, clientEndpoint, serverEndpoint);
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
        pendingRequests.put(key, future);
        DeleteObjectMessage message = new DeleteObjectMessage(key);
        messageBus.send(message, clientEndpoint, serverEndpoint);
        return future.thenAccept(response -> {
            DeleteObjectResponseMessage resp = (DeleteObjectResponseMessage) response;
            if (!resp.isSuccess()) {
                throw new RuntimeException(resp.getErrorMessage());
            }
            logger.debug("DELETE_OBJECT successful for key {}", key);
        });
    }

    public CompletableFuture<List<String>> listObjects() {
        logger.debug("Sending LIST_OBJECTS");
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put("list", future);
        ListObjectsMessage message = new ListObjectsMessage();
        messageBus.send(message, clientEndpoint, serverEndpoint);
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

        if (message instanceof PutObjectResponseMessage) {
            PutObjectResponseMessage response = (PutObjectResponseMessage) message;
            future = pendingRequests.remove(response.getKey());
        } else if (message instanceof GetObjectResponseMessage) {
            GetObjectResponseMessage response = (GetObjectResponseMessage) message;
            future = pendingRequests.remove(response.getKey());
        } else if (message instanceof DeleteObjectResponseMessage) {
            DeleteObjectResponseMessage response = (DeleteObjectResponseMessage) message;
            future = pendingRequests.remove(response.getKey());
        } else if (message instanceof ListObjectsResponseMessage) {
            ListObjectsResponseMessage response = (ListObjectsResponseMessage) message;
            future = pendingRequests.remove("list");
        }

        if (future != null) {
            future.complete(message);
            logger.debug("Completed future for message type {}", message.getType());
        } else {
            logger.warn("No pending request found for message type {}", message.getType());
        }
    }
} 