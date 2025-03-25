package minispark.deltalite.actions;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base interface for all Delta actions.
 * Actions represent operations that can be performed on a Delta table.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", include = JsonTypeInfo.As.EXISTING_PROPERTY)
public interface Action {
    /**
     * Get the type of this action.
     * @return The action type as a string
     */
    String getType();
} 