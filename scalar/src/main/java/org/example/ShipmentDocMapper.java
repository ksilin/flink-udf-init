package org.example;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ShipmentDocMapper extends ScalarFunction {
    
    public static final String NAME = "shipment_doc_mapper";
    private static final Logger LOGGER = LogManager.getLogger();
    
    private static final Map<String, String> FIELD_MAPPINGS = createFieldMappings();
    
    // Transient ObjectMapper - initialized lazily to avoid Flink serialization issues
    private transient ObjectMapper objectMapper;
    
    private static Map<String, String> createFieldMappings() {
        Map<String, String> mappings = new HashMap<>();
        // Top-level array key mapping
        mappings.put("/AMS/YBRV_PMO07", "shipmentDocument");
        
        // Shipment level mappings
        mappings.put("MANDT", "mandt");
        mappings.put("SHIPMENT_NUMBER", "shipmentNumber");
        mappings.put("DELIVERY_TYPE", "deliveryType");
        mappings.put("APPOINTMENT_DATE", "appointmentDate");
        mappings.put("APPOINTMENT_TIME", "appointmentTime");
        mappings.put("SHIPPING_POINT", "shippingPoint");
        mappings.put("SHIP_TO", "shippToParty");
        mappings.put("DELIVERY_CREATED_ON", "createdOn");
        mappings.put("LIPS", "deliveryItems");
        
        // Delivery items level mappings
        mappings.put("DELIVERY_NUMBER", "deliveryNumber");
        mappings.put("DELIVERY_ITEM", "deliveryItem");
        mappings.put("MATERIAL_NUMBER", "material");
        mappings.put("MATERIAL_DESCRIPTION", "description");
        mappings.put("QUANTITY", "deliveryQuantity");
        mappings.put("UNIT_OF_MEASURE", "baseUnit");
        mappings.put("FISCAL_NOTE", "fiscalNote");
        mappings.put("EIKP", "trackage");
        
        // Trackage level mappings
        mappings.put("TRACKAGE_ID", "trackageId");
        mappings.put("LFA1", "shipment");
        
        // Shipment level mappings
        mappings.put("CARRIER_CNPJ", "carrierCNPJ");
        mappings.put("VTTK", "changeDocument");
        
        // Change document level mappings
        mappings.put("CARRIER_ID", "carrierId");
        mappings.put("SHIPMENT_CREATED_ON", "shipmentCreatedOn");
        mappings.put("SHIPMENT_DATE", "shipmentDate");
        mappings.put("SHIPMENT_TIME", "shipmentTime");
        mappings.put("VEHICLE_TYPE", "vehicleType");
        
        return mappings;
    }
    
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }
    
    public String eval(String jsonInput) {
        if (jsonInput == null || jsonInput.trim().isEmpty()) {
            LOGGER.debug("Input is null or empty");
            return null;
        }
        
        try {
            ObjectMapper mapper = getObjectMapper();
            JsonNode inputNode = mapper.readTree(jsonInput);
            JsonNode transformedNode = transformJsonNode(inputNode);
            return mapper.writeValueAsString(transformedNode);
        } catch (Exception e) {
            LOGGER.error("Error processing JSON input: {}", e.getMessage(), e);
            return null;
        }
    }
    
    private JsonNode transformJsonNode(JsonNode node) {
        if (node == null) {
            return null;
        }
        
        if (node.isObject()) {
            return transformObjectNode((ObjectNode) node);
        } else if (node.isArray()) {
            return transformArrayNode((ArrayNode) node);
        } else {
            // Primitive values (string, number, boolean, null) are returned as-is
            return node;
        }
    }
    
    private JsonNode transformObjectNode(ObjectNode objectNode) {
        ObjectNode result = getObjectMapper().createObjectNode();
        
        Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String originalKey = field.getKey();
            JsonNode value = field.getValue();
            
            // Apply field mapping if exists, otherwise keep original key
            String mappedKey = FIELD_MAPPINGS.getOrDefault(originalKey, originalKey);
            
            // Recursively transform the value
            JsonNode transformedValue = transformJsonNode(value);
            
            result.set(mappedKey, transformedValue);
        }
        
        return result;
    }
    
    private JsonNode transformArrayNode(ArrayNode arrayNode) {
        ArrayNode result = getObjectMapper().createArrayNode();
        
        for (JsonNode element : arrayNode) {
            JsonNode transformedElement = transformJsonNode(element);
            result.add(transformedElement);
        }
        
        return result;
    }
}