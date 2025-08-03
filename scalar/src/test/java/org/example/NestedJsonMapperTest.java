package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class NestedJsonMapperTest {
    
    private final NestedJsonMapper mapper = new NestedJsonMapper();
    
    @Test
    public void testWithSimpleShipmentDocument() {
        // Test with the same mappings as ShipmentDocMapper
        Map<String, String> shipmentMappings = createShipmentMappings();
        
        String sourceJson = """
            {
              "/AMS/YBRV_PMO07": [
                {
                  "MANDT": "103",
                  "SHIPMENT_NUMBER": "0004604557",
                  "DELIVERY_TYPE": "ZTPR"
                }
              ]
            }
            """;
        
        String expectedJson = """
            {
              "shipmentDocument": [
                {
                  "mandt": "103",
                  "shipmentNumber": "0004604557",
                  "deliveryType": "ZTPR"
                }
              ]
            }
            """;
        
        String result = mapper.eval(sourceJson, shipmentMappings);
        assertJsonEquals(expectedJson, result);
    }

    @Test
    public void testWithCompleteShipmentDocument() {
        // This test is migrated from the deleted ShipmentDocMapperTest to ensure
        // that the generic NestedJsonMapper can handle the complex, real-world scenario.
        Map<String, String> shipmentMappings = createShipmentMappings();

        String sourceJson = """
            {
              "/AMS/YBRV_PMO07": [
                {
                  "MANDT": "103",
                  "SHIPMENT_NUMBER": "0004604557",
                  "DELIVERY_TYPE": "ZTPR",
                  "APPOINTMENT_DATE": "0000-00-00",
                  "APPOINTMENT_TIME": "00:00:00",
                  "SHIPPING_POINT": "BR18",
                  "SHIP_TO": "0000733101",
                  "DELIVERY_CREATED_ON": "2024-07-24",
                  "LIPS": [
                    {
                      "DELIVERY_NUMBER": "0850026493",
                      "DELIVERY_ITEM": "000010",
                      "MATERIAL_NUMBER": "000000000090004706",
                      "MATERIAL_DESCRIPTION": "ESTRADO PALLET RETORNAVEL",
                      "QUANTITY": "10.0",
                      "UNIT_OF_MEASURE": "EA",
                      "FISCAL_NOTE": "",
                      "EIKP": [
                        {
                          "TRACKAGE_ID": "",
                          "LFA1": [
                            {
                              "CARRIER_CNPJ": "57705097000155",
                              "VTTK": [
                                {
                                  "CARRIER_ID": "0100205740",
                                  "SHIPMENT_CREATED_ON": "2024-07-24",
                                  "SHIPMENT_DATE": "2024-07-24",
                                  "SHIPMENT_TIME": "09:57:00",
                                  "VEHICLE_TYPE": "BR004"
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
              ]
            }
            """;

        String expectedJson = """
            {
              "shipmentDocument": [
                {
                  "mandt": "103",
                  "shipmentNumber": "0004604557",
                  "deliveryType": "ZTPR",
                  "appointmentDate": "0000-00-00",
                  "appointmentTime": "00:00:00",
                  "shippingPoint": "BR18",
                  "shippToParty": "0000733101",
                  "createdOn": "2024-07-24",
                  "deliveryItems": [
                    {
                      "deliveryNumber": "0850026493",
                      "deliveryItem": "000010",
                      "material": "000000000090004706",
                      "description": "ESTRADO PALLET RETORNAVEL",
                      "deliveryQuantity": "10.0",
                      "baseUnit": "EA",
                      "fiscalNote": "",
                      "trackage": [
                        {
                          "trackageId": "",
                          "shipment": [
                            {
                              "carrierCNPJ": "57705097000155",
                              "changeDocument": [
                                {
                                  "carrierId": "0100205740",
                                  "shipmentCreatedOn": "2024-07-24",
                                  "shipmentDate": "2024-07-24",
                                  "shipmentTime": "09:57:00",
                                  "vehicleType": "BR004"
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
              ]
            }
            """;

        String result = mapper.eval(sourceJson, shipmentMappings);
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testWithCustomMappings() {
        // Test with different field mappings
        Map<String, String> customMappings = new HashMap<>();
        customMappings.put("firstName", "givenName");
        customMappings.put("lastName", "familyName");
        customMappings.put("email", "emailAddress");
        
        String sourceJson = """
            {
              "firstName": "John",
              "lastName": "Doe",
              "email": "john.doe@example.com",
              "age": 30
            }
            """;
        
        String expectedJson = """
            {
              "givenName": "John",
              "familyName": "Doe",
              "emailAddress": "john.doe@example.com",
              "age": 30
            }
            """;
        
        String result = mapper.eval(sourceJson, customMappings);
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testWithNestedObjects() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("user_info", "userDetails");
        mappings.put("first_name", "firstName");
        mappings.put("contact_info", "contacts");
        mappings.put("phone_number", "phoneNumber");
        
        String sourceJson = """
            {
              "user_info": {
                "first_name": "Jane",
                "age": 25,
                "contact_info": {
                  "phone_number": "123-456-7890"
                }
              }
            }
            """;
        
        String expectedJson = """
            {
              "userDetails": {
                "firstName": "Jane",
                "age": 25,
                "contacts": {
                  "phoneNumber": "123-456-7890"
                }
              }
            }
            """;
        
        String result = mapper.eval(sourceJson, mappings);
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testWithArrays() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("employees", "staff");
        mappings.put("emp_id", "employeeId");
        mappings.put("dept", "department");
        
        String sourceJson = """
            {
              "employees": [
                {
                  "emp_id": "E001",
                  "name": "Alice",
                  "dept": "Engineering"
                },
                {
                  "emp_id": "E002",
                  "name": "Bob",
                  "dept": "Marketing"
                }
              ]
            }
            """;
        
        String expectedJson = """
            {
              "staff": [
                {
                  "employeeId": "E001",
                  "name": "Alice",
                  "department": "Engineering"
                },
                {
                  "employeeId": "E002",
                  "name": "Bob",
                  "department": "Marketing"
                }
              ]
            }
            """;
        
        String result = mapper.eval(sourceJson, mappings);
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testWithEmptyMappings() {
        Map<String, String> emptyMappings = new HashMap<>();
        
        String sourceJson = """
            {
              "field1": "value1",
              "field2": "value2"
            }
            """;
        
        // Should return input unchanged when mappings are empty
        String result = mapper.eval(sourceJson, emptyMappings);
        assertEquals(sourceJson, result);
    }
    
    @Test
    public void testWithNullMappings() {
        String sourceJson = """
            {
              "field1": "value1",
              "field2": "value2"
            }
            """;
        
        // Should return input unchanged when mappings are null
        String result = mapper.eval(sourceJson, null);
        assertEquals(sourceJson, result);
    }
    
    @Test
    public void testWithPartialMappings() {
        Map<String, String> partialMappings = new HashMap<>();
        partialMappings.put("old_field", "new_field");
        // unmapped_field should remain unchanged
        
        String sourceJson = """
            {
              "old_field": "value1",
              "unmapped_field": "value2"
            }
            """;
        
        String expectedJson = """
            {
              "new_field": "value1",
              "unmapped_field": "value2"
            }
            """;
        
        String result = mapper.eval(sourceJson, partialMappings);
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testNullInput() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("field", "mappedField");
        
        String result = mapper.eval(null, mappings);
        assertNull(result, "Null input should return null");
    }
    
    @Test
    public void testEmptyInput() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("field", "mappedField");
        
        String result = mapper.eval("", mappings);
        assertNull(result, "Empty input should return null");
    }
    
    @Test
    public void testInvalidJson() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("field", "mappedField");
        
        String invalidJson = "{ invalid json }";
        String result = mapper.eval(invalidJson, mappings);
        assertNull(result, "Invalid JSON should return null");
    }
    
    @Test
    public void testComplexNestedStructure() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("level1", "levelOne");
        mappings.put("level2", "levelTwo");
        mappings.put("items", "itemList");
        mappings.put("item_id", "itemId");
        mappings.put("sub_items", "subItems");
        mappings.put("sub_id", "subId");
        
        String sourceJson = """
            {
              "level1": {
                "level2": {
                  "items": [
                    {
                      "item_id": "A001",
                      "name": "Item A",
                      "sub_items": [
                        {
                          "sub_id": "S001",
                          "value": "Sub 1"
                        }
                      ]
                    }
                  ]
                }
              }
            }
            """;
        
        String expectedJson = """
            {
              "levelOne": {
                "levelTwo": {
                  "itemList": [
                    {
                      "itemId": "A001",
                      "name": "Item A",
                      "subItems": [
                        {
                          "subId": "S001",
                          "value": "Sub 1"
                        }
                      ]
                    }
                  ]
                }
              }
            }
            """;
        
        String result = mapper.eval(sourceJson, mappings);
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testEmptyArrays() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("data", "processedData");
        
        String sourceJson = """
            {
              "data": []
            }
            """;
        
        String expectedJson = """
            {
              "processedData": []
            }
            """;
        
        String result = mapper.eval(sourceJson, mappings);
        assertJsonEquals(expectedJson, result);
    }
    
    private void assertJsonEquals(String expected, String actual) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode expectedNode = mapper.readTree(expected);
            com.fasterxml.jackson.databind.JsonNode actualNode = mapper.readTree(actual);
            assertEquals(expectedNode, actualNode,
                "JSON structures should be equal.\nExpected: " + expected + "\nActual: " + actual);
        } catch (Exception e) {
            fail("Failed to parse or compare JSON strings.", e);
        }
    }
    
    private Map<String, String> createShipmentMappings() {
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
    
    /**
     * Test that the UDF can be serialized and deserialized successfully.
     * This is critical for Flink's distributed runtime where UDFs need to be 
     * sent to different nodes in the cluster.
     */
    @Test
    public void testSerializationDeserializationFreshInstance() throws Exception {
        // Test serialization of a fresh instance (no state initialized)
        NestedJsonMapper originalFunction = new NestedJsonMapper();
        
        // Serialize the function
        byte[] serializedData = serializeObject(originalFunction);
        
        // Deserialize the function
        NestedJsonMapper deserializedFunction = deserializeObject(serializedData, NestedJsonMapper.class);
        
        // Verify the deserialized function works correctly
        assertNotNull(deserializedFunction, "Deserialized function should not be null");
        
        // Test that the deserialized function behaves the same as original
        Map<String, String> testMappings = new HashMap<>();
        testMappings.put("old_field", "new_field");
        
        String testInput = """
            {
              "old_field": "test_value",
              "unchanged": "stays_same"
            }
            """;
        
        String expected = """
            {
              "new_field": "test_value",
              "unchanged": "stays_same"
            }
            """;
        
        String result = deserializedFunction.eval(testInput, testMappings);
        assertJsonEquals(expected, result);
    }
    
    /**
     * Test serialization of a UDF that has been used (state initialized).
     */
    @Test
    public void testSerializationDeserializationAfterUse() throws Exception {
        // Create and use the function to initialize its internal state
        NestedJsonMapper originalFunction = new NestedJsonMapper();
        
        Map<String, String> initMappings = new HashMap<>();
        initMappings.put("test_field", "mapped_field");
        
        // Initialize internal state by calling eval
        String initInput = """
            {
              "test_field": "value"
            }
            """;
        originalFunction.eval(initInput, initMappings);
        
        // Now serialize the function with initialized state
        byte[] serializedData = serializeObject(originalFunction);
        
        // Deserialize the function
        NestedJsonMapper deserializedFunction = deserializeObject(serializedData, NestedJsonMapper.class);
        
        // Verify the deserialized function works correctly
        assertNotNull(deserializedFunction, "Deserialized function should not be null");
        
        // Test that the deserialized function behaves correctly
        Map<String, String> testMappings = new HashMap<>();
        testMappings.put("employees", "staff");
        testMappings.put("emp_id", "employeeId");
        
        String testInput = """
            {
              "employees": [
                {
                  "emp_id": "E001",
                  "name": "Alice"
                }
              ]
            }
            """;
        
        String expected = """
            {
              "staff": [
                {
                  "employeeId": "E001",
                  "name": "Alice"
                }
              ]
            }
            """;
        
        String result = deserializedFunction.eval(testInput, testMappings);
        assertJsonEquals(expected, result);
    }
    
    /**
     * Test multiple serialization/deserialization cycles to ensure no state corruption.
     */
    @Test
    public void testMultipleSerializationCycles() throws Exception {
        NestedJsonMapper function = new NestedJsonMapper();
        
        Map<String, String> testMappings = new HashMap<>();
        testMappings.put("source_field", "target_field");
        
        String testInput = """
            {
              "source_field": "test_value",
              "other": "unchanged"
            }
            """;
        
        String expected = """
            {
              "target_field": "test_value",
              "other": "unchanged"
            }
            """;
        
        // Perform multiple serialization cycles
        for (int i = 0; i < 3; i++) {
            // Use the function
            String result = function.eval(testInput, testMappings);
            assertJsonEquals(expected, result);
            
            // Serialize and deserialize
            byte[] serializedData = serializeObject(function);
            function = deserializeObject(serializedData, NestedJsonMapper.class);
            
            // Verify it still works after deserialization
            result = function.eval(testInput, testMappings);
            assertJsonEquals(expected, result);
        }
    }
    
    /**
     * Test that serialization works with null and empty inputs.
     */
    @Test
    public void testSerializationWithEdgeCases() throws Exception {
        NestedJsonMapper originalFunction = new NestedJsonMapper();
        
        Map<String, String> testMappings = new HashMap<>();
        testMappings.put("field", "mappedField");
        
        // Test with null input first
        String nullResult = originalFunction.eval(null, testMappings);
        assertNull(nullResult);
        
        // Test with empty input
        String emptyResult = originalFunction.eval("", testMappings);
        assertNull(emptyResult);
        
        // Now serialize after processing edge cases
        byte[] serializedData = serializeObject(originalFunction);
        NestedJsonMapper deserializedFunction = deserializeObject(serializedData, NestedJsonMapper.class);
        
        // Verify edge cases still work correctly
        assertNull(deserializedFunction.eval(null, testMappings));
        assertNull(deserializedFunction.eval("", testMappings));
        
        // Verify normal operation still works
        String normalResult = deserializedFunction.eval("{\"field\": \"value\"}", testMappings);
        assertJsonEquals("{\"mappedField\": \"value\"}", normalResult);
    }
    
    /**
     * Helper method to serialize any object to byte array.
     */
    private byte[] serializeObject(Object obj) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        }
    }
    
    /**
     * Helper method to deserialize byte array back to object.
     */
    @SuppressWarnings("unchecked")
    private <T> T deserializeObject(byte[] data, Class<T> clazz) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        }
    }
}