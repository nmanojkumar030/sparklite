package minispark.network.messages;

/**
 * Message sent from a worker to the scheduler containing the result of a task execution.
 */
public class TaskResultMessage extends Message {
    private static final long serialVersionUID = 1L;
    
    private final int taskId;
    private final int stageId;
    private final Object result;
    private final Exception error;

    public TaskResultMessage(String messageId, int taskId, int stageId, Object result, Exception error) {
        super(messageId, MessageType.TASK_RESULT);
        this.taskId = taskId;
        this.stageId = stageId;
        this.result = result;
        this.error = error;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getStageId() {
        return stageId;
    }

    public Object getResult() {
        return result;
    }

    public Exception getError() {
        return error;
    }

    public boolean isSuccess() {
        return error == null;
    }
} 