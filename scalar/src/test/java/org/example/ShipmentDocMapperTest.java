package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ShipmentDocMapperTest {
    
    private final ShipmentDocMapper mapper = new ShipmentDocMapper();
    
    @Test
    public void testCompleteTransformation() {
        // Source JSON example
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
        
        // Expected target JSON
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
        
        String result = mapper.eval(sourceJson);
        
        // Parse both JSON strings and compare structure
        assertNotNull(result, "Result should not be null");
        assertJsonEquals(expectedJson, result);
    }
    
    @Test
    public void testSimpleFieldRenaming() {
        String simpleJson = """
            {
              "MANDT": "103",
              "SHIPMENT_NUMBER": "0004604557"
            }
            """;
        
        String result = mapper.eval(simpleJson);
        
        String expectedSimple = """
            {
              "mandt": "103",
              "shipmentNumber": "0004604557"
            }
            """;
        
        assertJsonEquals(expectedSimple, result);
    }
    
    @Test
    public void testTopLevelArrayRenaming() {
        String arrayJson = """
            {
              "/AMS/YBRV_PMO07": [
                {
                  "MANDT": "103"
                }
              ]
            }
            """;
        
        String result = mapper.eval(arrayJson);
        
        String expectedArray = """
            {
              "shipmentDocument": [
                {
                  "mandt": "103"
                }
              ]
            }
            """;
        
        assertJsonEquals(expectedArray, result);
    }
    
    @Test
    public void testNestedArrays() {
        String nestedJson = """
            {
              "LIPS": [
                {
                  "DELIVERY_NUMBER": "123",
                  "EIKP": [
                    {
                      "TRACKAGE_ID": "456"
                    }
                  ]
                }
              ]
            }
            """;
        
        String result = mapper.eval(nestedJson);
        
        String expectedNested = """
            {
              "deliveryItems": [
                {
                  "deliveryNumber": "123",
                  "trackage": [
                    {
                      "trackageId": "456"
                    }
                  ]
                }
              ]
            }
            """;
        
        assertJsonEquals(expectedNested, result);
    }
    
    @Test
    public void testDeepNesting() {
        String deepJson = """
            {
              "LIPS": [
                {
                  "EIKP": [
                    {
                      "LFA1": [
                        {
                          "CARRIER_CNPJ": "12345",
                          "VTTK": [
                            {
                              "CARRIER_ID": "67890"
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
        
        String result = mapper.eval(deepJson);
        
        String expectedDeep = """
            {
              "deliveryItems": [
                {
                  "trackage": [
                    {
                      "shipment": [
                        {
                          "carrierCNPJ": "12345",
                          "changeDocument": [
                            {
                              "carrierId": "67890"
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
        
        assertJsonEquals(expectedDeep, result);
    }
    
    @Test
    public void testEmptyArrays() {
        String emptyArrayJson = """
            {
              "LIPS": []
            }
            """;
        
        String result = mapper.eval(emptyArrayJson);
        
        String expectedEmpty = """
            {
              "deliveryItems": []
            }
            """;
        
        assertJsonEquals(expectedEmpty, result);
    }
    
    @Test
    public void testNullInput() {
        String result = mapper.eval(null);
        assertNull(result, "Null input should return null");
    }
    
    @Test
    public void testEmptyInput() {
        String result = mapper.eval("");
        assertNull(result, "Empty input should return null");
    }
    
    @Test
    public void testInvalidJson() {
        String invalidJson = "{ invalid json }";
        String result = mapper.eval(invalidJson);
        assertNull(result, "Invalid JSON should return null");
    }
    
    @Test
    public void testUnknownFields() {
        String unknownFieldJson = """
            {
              "UNKNOWN_FIELD": "value",
              "MANDT": "103"
            }
            """;
        
        String result = mapper.eval(unknownFieldJson);
        
        String expectedUnknown = """
            {
              "UNKNOWN_FIELD": "value",
              "mandt": "103"
            }
            """;
        
        assertJsonEquals(expectedUnknown, result);
    }
    
    @Test
    public void testMultipleArrayElements() {
        String multipleElementsJson = """
            {
              "LIPS": [
                {
                  "DELIVERY_NUMBER": "123"
                },
                {
                  "DELIVERY_NUMBER": "456"
                }
              ]
            }
            """;
        
        String result = mapper.eval(multipleElementsJson);
        
        String expectedMultiple = """
            {
              "deliveryItems": [
                {
                  "deliveryNumber": "123"
                },
                {
                  "deliveryNumber": "456"
                }
              ]
            }
            """;
        
        assertJsonEquals(expectedMultiple, result);
    }
    
    private void assertJsonEquals(String expected, String actual) {
        // Remove whitespace and compare JSON strings
        String normalizedExpected = expected.replaceAll("\\s+", "");
        String normalizedActual = actual.replaceAll("\\s+", "");
        
        assertEquals(normalizedExpected, normalizedActual, 
            "JSON structures should be equal.\nExpected: " + expected + "\nActual: " + actual);
    }
    
    /**
     * Test that the UDF can be serialized and deserialized successfully.
     * This is critical for Flink's distributed runtime where UDFs need to be 
     * sent to different nodes in the cluster.
     * 
     * REUSABLE: Copy this test to any ScalarFunction test class and change the 
     * class name in the generic type and constructor.
     */
    @Test
    public void testSerializationDeserializationFreshInstance() throws Exception {
        // Test serialization of a fresh instance (no state initialized)
        ShipmentDocMapper originalFunction = new ShipmentDocMapper();
        
        // Serialize the function
        byte[] serializedData = serializeObject(originalFunction);
        
        // Deserialize the function
        ShipmentDocMapper deserializedFunction = deserializeObject(serializedData, ShipmentDocMapper.class);
        
        // Verify the deserialized function works correctly
        assertNotNull(deserializedFunction, "Deserialized function should not be null");
        
        // Test that the deserialized function behaves the same as original
        String testInput = """
            {
              "MANDT": "103",
              "SHIPMENT_NUMBER": "0004604557"
            }
            """;
        
        String expected = """
            {
              "mandt": "103",
              "shipmentNumber": "0004604557"
            }
            """;
        
        String result = deserializedFunction.eval(testInput);
        assertJsonEquals(expected, result);
    }
    
    /**
     * Test serialization of a UDF that has been used (state initialized).
     * This simulates the case where a UDF instance is serialized after 
     * it has already processed some data.
     */
    @Test
    public void testSerializationDeserializationAfterUse() throws Exception {
        // Create and use the function to initialize its internal state
        ShipmentDocMapper originalFunction = new ShipmentDocMapper();
        
        // Initialize internal state by calling eval
        String initInput = """
            {
              "MANDT": "103",
              "SHIPMENT_NUMBER": "0004604557"
            }
            """;
        originalFunction.eval(initInput);
        
        // Now serialize the function with initialized state
        byte[] serializedData = serializeObject(originalFunction);
        
        // Deserialize the function
        ShipmentDocMapper deserializedFunction = deserializeObject(serializedData, ShipmentDocMapper.class);
        
        // Verify the deserialized function works correctly
        assertNotNull(deserializedFunction, "Deserialized function should not be null");
        
        // Test that the deserialized function behaves correctly
        String testInput = """
            {
              "LIPS": [
                {
                  "DELIVERY_NUMBER": "123",
                  "EIKP": [
                    {
                      "TRACKAGE_ID": "456"
                    }
                  ]
                }
              ]
            }
            """;
        
        String expected = """
            {
              "deliveryItems": [
                {
                  "deliveryNumber": "123",
                  "trackage": [
                    {
                      "trackageId": "456"
                    }
                  ]
                }
              ]
            }
            """;
        
        String result = deserializedFunction.eval(testInput);
        assertJsonEquals(expected, result);
    }
    
    /**
     * Test multiple serialization/deserialization cycles to ensure no state corruption.
     */
    @Test
    public void testMultipleSerializationCycles() throws Exception {
        ShipmentDocMapper function = new ShipmentDocMapper();
        
        String testInput = """
            {
              "MANDT": "103",
              "SHIPMENT_NUMBER": "0004604557"
            }
            """;
        
        String expected = """
            {
              "mandt": "103",
              "shipmentNumber": "0004604557"
            }
            """;
        
        // Perform multiple serialization cycles
        for (int i = 0; i < 3; i++) {
            // Use the function
            String result = function.eval(testInput);
            assertJsonEquals(expected, result);
            
            // Serialize and deserialize
            byte[] serializedData = serializeObject(function);
            function = deserializeObject(serializedData, ShipmentDocMapper.class);
            
            // Verify it still works after deserialization
            result = function.eval(testInput);
            assertJsonEquals(expected, result);
        }
    }
    
    /**
     * Test ObjectMapper serialization directly to verify if it's the issue.
     */
    @Test
    public void testObjectMapperSerialization() throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        
        try {
            byte[] serializedData = serializeObject(mapper);
            com.fasterxml.jackson.databind.ObjectMapper deserializedMapper = 
                deserializeObject(serializedData, com.fasterxml.jackson.databind.ObjectMapper.class);
            
            // Test that the deserialized ObjectMapper still works
            String testJson = "{\"test\": \"value\"}";
            com.fasterxml.jackson.databind.JsonNode node = deserializedMapper.readTree(testJson);
            assertEquals("value", node.get("test").asText());
            
        } catch (Exception e) {
            // ObjectMapper is not serializable - this is the issue!
            System.out.println("ObjectMapper serialization failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            throw e;
        }
    }

    /**
     * Test that serialization works with null and empty inputs.
     */
    @Test
    public void testSerializationWithEdgeCases() throws Exception {
        ShipmentDocMapper originalFunction = new ShipmentDocMapper();
        
        // Test with null input first
        String nullResult = originalFunction.eval(null);
        assertNull(nullResult);
        
        // Test with empty input
        String emptyResult = originalFunction.eval("");
        assertNull(emptyResult);
        
        // Now serialize after processing edge cases
        byte[] serializedData = serializeObject(originalFunction);
        ShipmentDocMapper deserializedFunction = deserializeObject(serializedData, ShipmentDocMapper.class);
        
        // Verify edge cases still work correctly
        assertNull(deserializedFunction.eval(null));
        assertNull(deserializedFunction.eval(""));
        
        // Verify normal operation still works
        String normalResult = deserializedFunction.eval("{\"MANDT\": \"103\"}");
        assertJsonEquals("{\"mandt\": \"103\"}", normalResult);
    }
    
    /**
     * Helper method to serialize any object to byte array.
     * 
     * @param obj The object to serialize
     * @return Byte array containing the serialized object
     * @throws Exception If serialization fails
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
     * 
      * @param data The byte array containing serialized object
     * @param clazz The class type to deserialize to
     * @return The deserialized object
     * @throws Exception If deserialization fails
     */
    @SuppressWarnings("unchecked")
    private <T> T deserializeObject(byte[] data, Class<T> clazz) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        }
    }
}