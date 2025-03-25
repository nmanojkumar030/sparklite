package minispark.deltalite;

import minispark.deltalite.actions.Action;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for transactions in Delta Lake.
 * Provides the foundation for optimistic concurrency control.
 */
public abstract class Transaction {
    protected final String tablePath;
    protected final List<Action> actions;

    protected Transaction(String tablePath) {
        this.tablePath = tablePath;
        this.actions = new ArrayList<>();
    }

    /**
     * Adds an action to this transaction.
     *
     * @param action the action to add
     * @return this transaction for chaining
     */
    protected Transaction addAction(Action action) {
        actions.add(action);
        return this;
    }

    /**
     * Gets all actions in this transaction.
     *
     * @return the list of actions
     */
    protected List<Action> getActions() {
        return actions;
    }

    /**
     * Commits this transaction.
     *
     * @return this transaction for chaining
     * @throws IOException if an I/O error occurs
     */
    public abstract Transaction commit() throws IOException;
} 