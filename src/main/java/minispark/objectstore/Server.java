package minispark.objectstore;

import minispark.messages.*;
import minispark.network.MessageBus;
import minispark.network.NetworkEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Server implements MessageBus.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final NetworkEndpoint endpoint;
    private final LocalStorageNode localStorage;
    private final MessageBus messageBus;

    public Server(LocalStorageNode localStorage, MessageBus messageBus, NetworkEndpoint endpoint) {
        this.localStorage = localStorage;
        this.messageBus = messageBus;
        this.endpoint = endpoint;
        messageBus.registerHandler(endpoint, this);
        logger.info("Server registered with MessageBus at endpoint {}", endpoint);
    }

    public NetworkEndpoint getEndpoint() {
        return endpoint;
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
            logger.error("Error handling message {} from {}: {}", message.getType(), sender, e.getMessage(), e);
            sendErrorResponse(message, sender, e.getMessage());
        }
    }

    private void handlePutObject(PutObjectMessage message, NetworkEndpoint sender) {
        logger.debug("Handling PUT_OBJECT for key {}", message.getKey());
        try {
            localStorage.putObject(message.getKey(), message.getData());
            PutObjectResponseMessage response = new PutObjectResponseMessage(message.getKey(), true, null);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent PUT_OBJECT_RESPONSE for key {} to {}", message.getKey(), sender);
        } catch (RuntimeException e) {
            logger.error("Error handling PUT_OBJECT for key {}: {}", message.getKey(), e.getMessage());
            PutObjectResponseMessage response = new PutObjectResponseMessage(message.getKey(), false, e.getMessage());
            messageBus.send(response, endpoint, sender);
        }
    }

    private void handleGetObject(GetObjectMessage message, NetworkEndpoint sender) {
        logger.debug("Handling GET_OBJECT for key {}", message.getKey());
        try {
            byte[] data = localStorage.getObject(message.getKey());
            GetObjectResponseMessage response = new GetObjectResponseMessage(message.getKey(), data, true, null);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent GET_OBJECT_RESPONSE for key {} to {}", message.getKey(), sender);
        } catch (Exception e) {
            logger.error("Error handling GET_OBJECT for key {}: {}", message.getKey(), e.getMessage());
            GetObjectResponseMessage response = new GetObjectResponseMessage(message.getKey(), null, false, e.getMessage());
            messageBus.send(response, endpoint, sender);
        }
    }

    private void handleDeleteObject(DeleteObjectMessage message, NetworkEndpoint sender) {
        logger.debug("Handling DELETE_OBJECT for key {}", message.getKey());
        try {
            localStorage.deleteObject(message.getKey());
            DeleteObjectResponseMessage response = new DeleteObjectResponseMessage(message.getKey(), true, null);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent DELETE_OBJECT_RESPONSE for key {} to {}", message.getKey(), sender);
        } catch (Exception e) {
            logger.error("Error handling DELETE_OBJECT for key {}: {}", message.getKey(), e.getMessage());
            DeleteObjectResponseMessage response = new DeleteObjectResponseMessage(message.getKey(), false, e.getMessage());
            messageBus.send(response, endpoint, sender);
        }
    }

    private void handleListObjects(ListObjectsMessage message, NetworkEndpoint sender) {
        logger.debug("Handling LIST_OBJECTS");
        try {
            List<String> objects = localStorage.listObjects();
            ListObjectsResponseMessage response = new ListObjectsResponseMessage(objects, true, null);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent LIST_OBJECTS_RESPONSE with {} objects to {}", objects.size(), sender);
        } catch (Exception e) {
            logger.error("Error handling LIST_OBJECTS: {}", e.getMessage());
            ListObjectsResponseMessage response = new ListObjectsResponseMessage(null, false, e.getMessage());
            messageBus.send(response, endpoint, sender);
        }
    }

    private void sendErrorResponse(Message message, NetworkEndpoint sender, String errorMessage) {
        if (message instanceof PutObjectMessage) {
            PutObjectMessage m = (PutObjectMessage) message;
            PutObjectResponseMessage response = new PutObjectResponseMessage(m.getKey(), false, errorMessage);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent error response for PUT_OBJECT with key {} to {}", m.getKey(), sender);
        } else if (message instanceof GetObjectMessage) {
            GetObjectMessage m = (GetObjectMessage) message;
            GetObjectResponseMessage response = new GetObjectResponseMessage(m.getKey(), null, false, errorMessage);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent error response for GET_OBJECT with key {} to {}", m.getKey(), sender);
        } else if (message instanceof DeleteObjectMessage) {
            DeleteObjectMessage m = (DeleteObjectMessage) message;
            DeleteObjectResponseMessage response = new DeleteObjectResponseMessage(m.getKey(), false, errorMessage);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent error response for DELETE_OBJECT with key {} to {}", m.getKey(), sender);
        } else if (message instanceof ListObjectsMessage) {
            ListObjectsResponseMessage response = new ListObjectsResponseMessage(null, false, errorMessage);
            messageBus.send(response, endpoint, sender);
            logger.debug("Sent error response for LIST_OBJECTS to {}", sender);
        }
    }
} 