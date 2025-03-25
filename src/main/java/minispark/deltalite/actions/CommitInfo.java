package minispark.deltalite.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.HashMap;

/**
 * Action that represents commit information.
 */
public class CommitInfo implements Action {
    private final long timestamp;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final String operationMetrics;

    @JsonCreator
    public CommitInfo(
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("operation") String operation,
        @JsonProperty("operationParameters") Map<String, String> operationParameters,
        @JsonProperty("operationMetrics") String operationMetrics
    ) {
        this.timestamp = timestamp;
        this.operation = operation;
        this.operationParameters = operationParameters;
        this.operationMetrics = operationMetrics;
    }

    @Override
    @JsonProperty("type")
    public String getType() {
        return "commitInfo";
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("operation")
    public String getOperation() {
        return operation;
    }

    @JsonProperty("operationParameters")
    public Map<String, String> getOperationParameters() {
        return operationParameters;
    }

    @JsonProperty("operationMetrics")
    public String getOperationMetrics() {
        return operationMetrics;
    }
} 