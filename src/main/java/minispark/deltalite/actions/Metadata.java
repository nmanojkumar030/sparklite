package minispark.deltalite.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import minispark.deltalite.Schema;
import java.util.Map;
import java.util.HashMap;

/**
 * Action that represents table metadata.
 */
public class Metadata implements Action {
    private final String id;
    private final String name;
    private final String description;
    private final Schema schema;
    private final Map<String, String> partitionColumns;
    private final Map<String, String> configuration;

    @JsonCreator
    public Metadata(
        @JsonProperty("id") String id,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("schema") Schema schema,
        @JsonProperty("partitionColumns") Map<String, String> partitionColumns,
        @JsonProperty("configuration") Map<String, String> configuration
    ) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.schema = schema;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
    }

    @Override
    @JsonProperty("type")
    public String getType() {
        return "metadata";
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("schema")
    public Schema getSchema() {
        return schema;
    }

    @JsonProperty("partitionColumns")
    public Map<String, String> getPartitionColumns() {
        return partitionColumns;
    }

    @JsonProperty("configuration")
    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public String getKey() {
        return "metadata";
    }

    public String getValue() {
        return String.format("id=%s,name=%s,description=%s", id, name, description);
    }
} 