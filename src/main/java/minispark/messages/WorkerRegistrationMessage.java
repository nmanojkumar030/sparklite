package minispark.messages;

import minispark.network.NetworkEndpoint;

public class WorkerRegistrationMessage extends Message {
    private final String workerId;
    private final NetworkEndpoint endpoint;
    private final int numCores;

    public WorkerRegistrationMessage(String workerId, NetworkEndpoint endpoint, int numCores) {
        super(MessageType.WORKER_REGISTRATION);
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