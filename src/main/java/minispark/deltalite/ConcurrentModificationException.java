package minispark.deltalite;

/**
 * Exception thrown when a concurrent modification is detected during a transaction.
 */
public class ConcurrentModificationException extends RuntimeException {
    
    public ConcurrentModificationException(String message) {
        super(message);
    }
    
    public ConcurrentModificationException(String message, Throwable cause) {
        super(message, cause);
    }
} 