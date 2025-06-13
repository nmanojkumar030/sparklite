package minispark.storage.btree.page;

/**
 * Represents a key-value pair in a B+Tree page.
 */
public class Element {
    private final byte[] key;
    private final byte[] value;
    private final boolean hasOverflow;
    
    /**
     * Creates a new element.
     *
     * @param key The key
     * @param value The value
     * @param hasOverflow Whether the value has overflow pages
     */
    public Element(byte[] key, byte[] value, boolean hasOverflow) {
        this.key = key;
        this.value = value;
        this.hasOverflow = hasOverflow;
    }
    
    /**
     * Gets the key.
     *
     * @return The key
     */
    public byte[] key() {
        return key;
    }
    
    /**
     * Gets the value.
     *
     * @return The value
     */
    public byte[] value() {
        return value;
    }
    
    /**
     * Checks if the value has overflow pages.
     *
     * @return true if the value has overflow pages
     */
    public boolean hasOverflow() {
        return hasOverflow;
    }
    
    /**
     * Gets the overflow page ID if the value has overflow pages.
     *
     * @return The overflow page ID
     */
    public long overflowPageId() {
        if (!hasOverflow) {
            throw new IllegalStateException("Element does not have overflow pages");
        }
        return java.nio.ByteBuffer.wrap(value).getLong();
    }
} 