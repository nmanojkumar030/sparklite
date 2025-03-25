package minispark.network.messages;

import minispark.scheduler.Task;

/**
 * Message sent from the scheduler to a worker to submit a task for execution.
 */
public class SubmitTaskMessage extends Message {
    private static final long serialVersionUID = 1L;
    
    private final Task<?, ?> task;
    private final int stageId;

    public SubmitTaskMessage(String messageId, Task<?, ?> task, int stageId) {
        super(messageId, MessageType.SUBMIT_TASK);
        this.task = task;
        this.stageId = stageId;
    }

    public Task<?, ?> getTask() {
        return task;
    }

    public int getStageId() {
        return stageId;
    }
} 