package minispark.deltalite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.core.type.TypeReference;
import minispark.deltalite.actions.Action;
import minispark.deltalite.actions.AddFile;
import minispark.deltalite.actions.Metadata;
import minispark.deltalite.actions.RemoveFile;
import minispark.deltalite.actions.CommitInfo;

import java.util.List;

/**
 * Utility class for JSON serialization and deserialization.
 */
public class JsonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    
    static {
        // Register subtypes for polymorphic deserialization
        MAPPER.registerSubtypes(
            new NamedType(AddFile.class, "add"),
            new NamedType(RemoveFile.class, "remove"),
            new NamedType(Metadata.class, "metadata"),
            new NamedType(CommitInfo.class, "commitInfo")
        );
    }
    
    /**
     * Serializes an object to JSON.
     *
     * @param obj the object to serialize
     * @return the JSON string
     */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }
    
    /**
     * Deserializes a JSON string to an object.
     *
     * @param json the JSON string
     * @return the deserialized object
     */
    public static Action fromJson(String json) {
        try {
            return MAPPER.readValue(json.trim(), Action.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize from JSON: " + json, e);
        }
    }
    
    /**
     * Deserializes a JSON array string to a list of actions.
     *
     * @param json the JSON array string
     * @return the list of deserialized actions
     */
    public static List<Action> fromJsonArray(String json) {
        try {
            return MAPPER.readValue(json.trim(), new TypeReference<List<Action>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize array from JSON: " + json, e);
        }
    }
} 