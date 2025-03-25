package minispark.messages;

public class TaskResultMessage extends Message {
    private final int taskId;
    private final int stageId;
    private final Object result;
    private final Exception error;

    public TaskResultMessage(int taskId, int stageId, Object result, Exception error) {
        super(MessageType.TASK_RESULT);
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