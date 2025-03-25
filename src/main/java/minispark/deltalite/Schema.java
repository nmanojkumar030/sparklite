package minispark.deltalite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents the schema of a Delta table.
 */
public class Schema {
    private final List<Field> fields;

    @JsonCreator
    public Schema(@JsonProperty("fields") List<Field> fields) {
        this.fields = fields != null ? fields : new ArrayList<>();
    }

    public Schema() {
        this(new ArrayList<>());
    }

    public void addField(Field field) {
        fields.add(field);
    }

    @JsonProperty("fields")
    public List<Field> getFields() {
        return fields;
    }

    public static class Field {
        private final String name;
        private final String type;
        private final boolean nullable;

        @JsonCreator
        public Field(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("nullable") boolean nullable
        ) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }

        @JsonProperty("name")
        public String getName() {
            return name;
        }

        @JsonProperty("type")
        public String getType() {
            return type;
        }

        @JsonProperty("nullable")
        public boolean isNullable() {
            return nullable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return Objects.equals(name, field.name) && Objects.equals(type, field.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }
    }

    public enum Type {
        STRING,
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        BINARY
    }
} 