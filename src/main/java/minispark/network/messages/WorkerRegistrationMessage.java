package minispark.network.messages;

import minispark.network.NetworkEndpoint;

/**
 * Message sent from a worker to the scheduler to register itself in the cluster.
 */
public class WorkerRegistrationMessage extends Message {
    private static final long serialVersionUID = 1L;
    
    private final String workerId;
    private final NetworkEndpoint endpoint;
    private final int numCores;

    public WorkerRegistrationMessage(String messageId, String workerId, NetworkEndpoint endpoint, int numCores) {
        super(messageId, MessageType.WORKER_REGISTRATION);
        this.workerId = workerId;
        this.endpoint = endpoint;
        this.numCores = numCores;
    }

    public String getWorkerId() {
        return workerId;
    }

    public NetworkEndpoint getEndpoint() {
        return endpoint;
    }

    public int getNumCores() {
        return numCores;
    }
} 