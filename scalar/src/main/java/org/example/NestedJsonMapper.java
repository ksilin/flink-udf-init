package org.example;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

public class NestedJsonMapper extends ScalarFunction {
    
    public static final String NAME = "nested_json_mapper";
    private static final Logger LOGGER = LogManager.getLogger();
    
    // initialized lazily to be serializable
    private transient ObjectMapper objectMapper;
    
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }
    
    public String eval(String jsonInput, @DataTypeHint("MAP<STRING, STRING>") Map<String, String> fieldMappings) {
        if (jsonInput == null || jsonInput.trim().isEmpty()) {
            LOGGER.debug("Input is null or empty");
            return null;
        }
        
        if (fieldMappings == null || fieldMappings.isEmpty()) {
            LOGGER.debug("Field mappings are null or empty, returning input unchanged");
            return jsonInput;
        }
        
        try {
            ObjectMapper mapper = getObjectMapper();
            JsonNode inputNode = mapper.readTree(jsonInput);
            JsonNode transformedNode = transformJsonNode(inputNode, fieldMappings);
            return mapper.writeValueAsString(transformedNode);
        } catch (Exception e) {
            LOGGER.error("Error processing JSON input: {}", e.getMessage(), e);
            return null;
        }
    }
    
    private JsonNode transformJsonNode(JsonNode node, Map<String, String> fieldMappings) {
        if (node == null) {
            return null;
        }
        
        if (node.isObject()) {
            return transformObjectNode((ObjectNode) node, fieldMappings);
        } else if (node.isArray()) {
            return transformArrayNode((ArrayNode) node, fieldMappings);
        } else {
            // Primitive values (string, number, boolean, null) are returned as-is
            return node;
        }
    }
    
    private JsonNode transformObjectNode(ObjectNode objectNode, Map<String, String> fieldMappings) {
        ObjectNode result = getObjectMapper().createObjectNode();
        
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String originalKey = field.getKey();
            JsonNode value = field.getValue();
            
            // Apply field mapping if exists, otherwise keep original key
            String mappedKey = fieldMappings.getOrDefault(originalKey, originalKey);
            
            // Recursively transform the value
            JsonNode transformedValue = transformJsonNode(value, fieldMappings);
            
            result.set(mappedKey, transformedValue);
        }
        
        return result;
    }
    
    private JsonNode transformArrayNode(ArrayNode arrayNode, Map<String, String> fieldMappings) {
        ArrayNode result = getObjectMapper().createArrayNode();
        
        for (JsonNode element : arrayNode) {
            JsonNode transformedElement = transformJsonNode(element, fieldMappings);
            result.add(transformedElement);
        }
        
        return result;
    }
}