package minispark.network.messages;

import java.io.Serializable;

/**
 * Base class for all messages exchanged between nodes in the MiniSpark cluster.
 */
public abstract class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String messageId;
    private final MessageType type;

    protected Message(String messageId, MessageType type) {
        this.messageId = messageId;
        this.type = type;
    }

    public String getMessageId() {
        return messageId;
    }

    public MessageType getType() {
        return type;
    }

    public enum MessageType {
        SUBMIT_TASK,
        TASK_RESULT,
        WORKER_HEARTBEAT,
        WORKER_REGISTRATION,
        WORKER_DEREGISTRATION
    }
} 