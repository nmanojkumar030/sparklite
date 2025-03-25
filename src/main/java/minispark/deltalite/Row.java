package minispark.deltalite;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Row {
    private final Map<String, Object> values;

    public Row() {
        this.values = new HashMap<>();
    }

    public Row(Map<String, Object> values) {
        this.values = new HashMap<>(values);
    }

    public Object get(String fieldName) {
        return values.get(fieldName);
    }

    public void set(String fieldName, Object value) {
        values.put(fieldName, value);
    }

    public Map<String, Object> getAllValues() {
        return new HashMap<>(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
} 