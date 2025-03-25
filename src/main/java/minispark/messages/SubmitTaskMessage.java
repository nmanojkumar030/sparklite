package minispark.messages;

import minispark.core.Task;

public class SubmitTaskMessage extends Message {
    private final Task<?, ?> task;

    public SubmitTaskMessage(Task<?, ?> task) {
        super(MessageType.SUBMIT_TASK);
        this.task = task;
    }

    public Task<?, ?> getTask() {
        return task;
    }
} 