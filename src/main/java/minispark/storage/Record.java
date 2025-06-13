package minispark.storage;

import java.util.Map;

/**
 * Represents a record in the storage engine.
 * A record consists of a key and a value map containing field names and their values.
 */
public class Record {
    private final byte[] key;
    private final Map<String, Object> value;

    /**
     * Creates a new record.
     *
     * @param key The record key
     * @param value The record value as a map of field names to values
     */
    public Record(byte[] key, Map<String, Object> value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Gets the record key.
     *
     * @return The record key
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Gets the record value.
     *
     * @return The record value as a map of field names to values
     */
    public Map<String, Object> getValue() {
        return value;
    }
} 