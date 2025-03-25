package minispark.network;

import minispark.messages.*;
import minispark.objectstore.LocalStorageNode;
import java.io.UncheckedIOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final LocalStorageNode storageNode;
    private final MessageBus messageBus;
    private final NetworkEndpoint serverEndpoint;

    public Server(LocalStorageNode storageNode, MessageBus messageBus, NetworkEndpoint serverEndpoint) {
        this.storageNode = storageNode;
        this.messageBus = messageBus;
        this.serverEndpoint = serverEndpoint;
        messageBus.registerHandler(serverEndpoint, this);
        logger.info("Server registered with MessageBus at endpoint {}", serverEndpoint);
    }

    @Override
    public void handleMessage(Message message, NetworkEndpoint sender) {
        logger.debug("Received message of type {} from {}", message.getType(), sender);
        try {
            if (message instanceof PutObjectMessage) {
                handlePutObject((PutObjectMessage) message, sender);
            } else if (message instanceof GetObjectMessage) {
                handleGetObject((GetObjectMessage) message, sender);
            } else if (message instanceof DeleteObjectMessage) {
                handleDeleteObject((DeleteObjectMessage) message, sender);
            } else if (message instanceof ListObjectsMessage) {
                handleListObjects((ListObjectsMessage) message, sender);
            }
        } catch (Exception e) {
            logger.error("Error handling message: {}", e.getMessage(), e);
        }
    }

    private void handlePutObject(PutObjectMessage message, NetworkEndpoint sender) {
        try {
            logger.debug("Handling PUT_OBJECT for key {}", message.getKey());
            storageNode.putObject(message.getKey(), message.getData());
            PutObjectResponseMessage response = new PutObjectResponseMessage(message.getKey(), true, null);
            messageBus.send(response, serverEndpoint, sender);
            logger.debug("Sent PUT_OBJECT_RESPONSE for key {}", message.getKey());
        } catch (Exception e) {
            logger.error("Error handling PUT_OBJECT for key {}: {}", message.getKey(), e.getMessage(), e);
            PutObjectResponseMessage response = new PutObjectResponseMessage(message.getKey(), false, e.getMessage());
            messageBus.send(response, serverEndpoint, sender);
        }
    }

    private void handleGetObject(GetObjectMessage message, NetworkEndpoint sender) {
        try {
            logger.debug("Handling GET_OBJECT for key {}", message.getKey());
            byte[] data = storageNode.getObject(message.getKey());
            GetObjectResponseMessage response = new GetObjectResponseMessage(message.getKey(), data, true, null);
            messageBus.send(response, serverEndpoint, sender);
            logger.debug("Sent GET_OBJECT_RESPONSE for key {}", message.getKey());
        } catch (Exception e) {
            logger.error("Error handling GET_OBJECT for key {}: {}", message.getKey(), e.getMessage(), e);
            GetObjectResponseMessage response = new GetObjectResponseMessage(message.getKey(), null, false, e.getMessage());
            messageBus.send(response, serverEndpoint, sender);
        }
    }

    private void handleDeleteObject(DeleteObjectMessage message, NetworkEndpoint sender) {
        try {
            logger.debug("Handling DELETE_OBJECT for key {}", message.getKey());
            storageNode.deleteObject(message.getKey());
            DeleteObjectResponseMessage response = new DeleteObjectResponseMessage(message.getKey(), true, null);
            messageBus.send(response, serverEndpoint, sender);
            logger.debug("Sent DELETE_OBJECT_RESPONSE for key {}", message.getKey());
        } catch (Exception e) {
            logger.error("Error handling DELETE_OBJECT for key {}: {}", message.getKey(), e.getMessage(), e);
            DeleteObjectResponseMessage response = new DeleteObjectResponseMessage(message.getKey(), false, e.getMessage());
            messageBus.send(response, serverEndpoint, sender);
        }
    }

    private void handleListObjects(ListObjectsMessage message, NetworkEndpoint sender) {
        try {
            logger.debug("Handling LIST_OBJECTS");
            List<String> objects = storageNode.listObjects();
            ListObjectsResponseMessage response = new ListObjectsResponseMessage(objects, true, null);
            messageBus.send(response, serverEndpoint, sender);
            logger.debug("Sent LIST_OBJECTS_RESPONSE with {} objects", objects.size());
        } catch (Exception e) {
            logger.error("Error handling LIST_OBJECTS: {}", e.getMessage(), e);
            ListObjectsResponseMessage response = new ListObjectsResponseMessage(null, false, e.getMessage());
            messageBus.send(response, serverEndpoint, sender);
        }
    }
} 