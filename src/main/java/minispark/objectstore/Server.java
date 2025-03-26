package minispark.objectstore;

import minispark.messages.*;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Server implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final NetworkEndpoint endpoint;
    private final LocalStorageNode localStorage;
    private final MessageBus messageBus;
    private final String serverId;

    public Server(String serverId, LocalStorageNode localStorage, MessageBus messageBus, NetworkEndpoint endpoint) {
        this.serverId = serverId;
        this.localStorage = localStorage;
        this.messageBus = messageBus;
        this.endpoint = endpoint;
        messageBus.registerHandler(endpoint, this);
        logger.info("Server registered with MessageBus at endpoint {}", endpoint);
    }

    public NetworkEndpoint getEndpoint() {
        return endpoint;
    }

    public String getServerId() {
        return serverId;
    }

    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        logger.debug("Received message of type {} from {}", message.getType(), sender);
        try {
            switch (message.getType()) {
                case PUT_OBJECT:
                    handlePutObject((PutObjectMessage) message, sender);
                    break;
                case GET_OBJECT:
                    handleGetObject((GetObjectMessage) message, sender);
                    break;
                case DELETE_OBJECT:
                    handleDeleteObject((DeleteObjectMessage) message, sender);
                    break;
                case LIST_OBJECTS:
                    handleListObjects((ListObjectsMessage) message, sender);
                    break;
                case LIST_OBJECTS_WITH_METADATA:
                    handleListObjectsWithMetadata((ListObjectsWithMetadataMessage) message, sender);
                    break;
                case LIST_FROM:
                    handleListFrom((ListFromMessage) message, sender);
                    break;
                default:
                    // Handle unknown message type
                    break;
            }
        } catch (IOException e) {
            logger.error("Error handling message {}: {}", message.getType(), e.getMessage());
            sendErrorResponse(message, sender, e.getMessage());
        }
    }

    private void handlePutObject(PutObjectMessage message, NetworkEndpoint sender) throws IOException {
        logger.debug("Handling PUT_OBJECT for key {}", message.getKey());
        localStorage.putObjectWithLock(message.getKey(), message.getData(), message.isOverwrite());
        PutObjectResponseMessage response = new PutObjectResponseMessage(message.getKey(), true, null, message.getCorrelationId());
        messageBus.send(response, endpoint, sender);
        logger.debug("Sent PUT_OBJECT_RESPONSE for key {} to {}", message.getKey(), sender);
    }

    private void handleGetObject(GetObjectMessage message, NetworkEndpoint sender) throws IOException {
        logger.debug("Handling GET_OBJECT for key {}", message.getKey());
        byte[] data = localStorage.getObject(message.getKey());
        GetObjectResponseMessage response = new GetObjectResponseMessage(message.getKey(), data, true, null, message.getCorrelationId());
        messageBus.send(response, endpoint, sender);
        logger.debug("Sent GET_OBJECT_RESPONSE for key {} to {}", message.getKey(), sender);
    }

    private void handleDeleteObject(DeleteObjectMessage message, NetworkEndpoint sender) throws IOException {
        logger.debug("Handling DELETE_OBJECT for key {}", message.getKey());
        localStorage.deleteObject(message.getKey());
        DeleteObjectResponseMessage response = new DeleteObjectResponseMessage(message.getKey(), true, null, message.getCorrelationId());
        messageBus.send(response, endpoint, sender);
        logger.debug("Sent DELETE_OBJECT_RESPONSE for key {} to {}", message.getKey(), sender);
    }

    private void handleListObjects(ListObjectsMessage message, NetworkEndpoint sender) throws IOException {
        logger.debug("Handling LIST_OBJECTS with prefix {}", message.getPrefix());
        List<String> objects = localStorage.listObjects(message.getPrefix());
        ListObjectsResponseMessage response = new ListObjectsResponseMessage(objects, true, null, message.getPrefix(), message.getCorrelationId());
        messageBus.send(response, endpoint, sender);
        logger.debug("Sent LIST_OBJECTS_RESPONSE with {} objects to {}", objects.size(), sender);
    }

    private void handleListObjectsWithMetadata(ListObjectsWithMetadataMessage message, NetworkEndpoint sender) {
        logger.debug("Handling LIST_OBJECTS_WITH_METADATA with prefix {}", message.getPrefix());
        try {
            List<FileStatus> objects = localStorage.listObjectsWithMetadata(message.getPrefix());
            ListObjectsWithMetadataResponseMessage response = new ListObjectsWithMetadataResponseMessage(objects, true, null);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent LIST_OBJECTS_WITH_METADATA_RESPONSE with {} objects to {}", objects.size(), sender);
        } catch (Exception e) {
            logger.error("Error handling LIST_OBJECTS_WITH_METADATA: {}", e.getMessage());
            ListObjectsWithMetadataResponseMessage response = new ListObjectsWithMetadataResponseMessage(null, false, e.getMessage());
            messageBus.send(response, endpoint, sender);
        }
    }

    private void handleListFrom(ListFromMessage message, NetworkEndpoint sender) {
        logger.debug("Handling LIST_FROM with prefix {} and recursive {}", message.getPrefix(), message.isRecursive());
        try {
            List<FileStatus> objects = localStorage.listFrom(message.getPrefix(), message.isRecursive());
            ListFromResponseMessage response = new ListFromResponseMessage(objects, true, null);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent LIST_FROM_RESPONSE with {} objects to {}", objects.size(), sender);
        } catch (Exception e) {
            logger.error("Error handling LIST_FROM: {}", e.getMessage());
            ListFromResponseMessage response = new ListFromResponseMessage(null, false, e.getMessage());
            messageBus.send(response, endpoint, sender);
        }
    }

    private void sendErrorResponse(Message originalMessage, NetworkEndpoint sender, String errorMessage) {
        Message errorResponse;
        if (originalMessage instanceof PutObjectMessage) {
            errorResponse = new PutObjectResponseMessage(((PutObjectMessage) originalMessage).getKey(), false, errorMessage, originalMessage.getCorrelationId());
        } else if (originalMessage instanceof GetObjectMessage) {
            errorResponse = new GetObjectResponseMessage(((GetObjectMessage) originalMessage).getKey(), null, false, errorMessage, originalMessage.getCorrelationId());
        } else if (originalMessage instanceof DeleteObjectMessage) {
            errorResponse = new DeleteObjectResponseMessage(((DeleteObjectMessage) originalMessage).getKey(), false, errorMessage, originalMessage.getCorrelationId());
        } else if (originalMessage instanceof ListObjectsMessage) {
            errorResponse = new ListObjectsResponseMessage(null, false, errorMessage, ((ListObjectsMessage) originalMessage).getPrefix(), originalMessage.getCorrelationId());
        } else if (originalMessage instanceof ListObjectsWithMetadataMessage) {
            errorResponse = new ListObjectsWithMetadataResponseMessage(null, false, errorMessage);
        } else if (originalMessage instanceof ListFromMessage) {
            errorResponse = new ListFromResponseMessage(null, false, errorMessage);
        } else {
            logger.error("Unknown message type for error response: {}", originalMessage.getType());
            return;
        }
        messageBus.send(errorResponse, endpoint, sender);
    }
} 