package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonFieldRenamer extends ScalarFunction {
    public static final String NAME = "RENAME_JSON_FIELD";

    private static final Logger LOGGER = LogManager.getLogger();

    // initialized lazily to be serializable
    private transient ObjectMapper objectMapper;

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    public String eval(String json, String oldName, String newName) {
        if (json == null || oldName == null || newName == null || oldName.equals(newName)) {
            return json;
        }

        try {
            ObjectMapper mapper = getObjectMapper();
            JsonNode rootNode = mapper.readTree(json);

            // This UDF only operates on top-level fields of a JSON object
            if (rootNode.isObject()) {
                ObjectNode objectNode = (ObjectNode) rootNode;
                if (objectNode.has(oldName)) {
                    // Get the value, remove the old field, and add the new field
                    JsonNode value = objectNode.get(oldName);
                    objectNode.remove(oldName);
                    objectNode.set(newName, value);
                }
                return mapper.writeValueAsString(objectNode);
            } else {
                // If it's not a JSON object, we can't rename a field. Return as is.
                LOGGER.warn("Input string is not a JSON object. Returning original value.");
                return json;
            }
        } catch (Exception e) {
            LOGGER.error("Error renaming JSON field '{}' to '{}'. Returning original value. Error: {}", oldName, newName, e.getMessage());
            // In case of error, return the original json to avoid data loss
            return json;
        }
    }
}
