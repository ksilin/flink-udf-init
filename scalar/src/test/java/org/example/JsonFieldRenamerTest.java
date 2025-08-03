package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JsonFieldRenamerTest {

    private final JsonFieldRenamer renamer = new JsonFieldRenamer();

    @Test
    public void testRenameField() {
        String sourceJson = "{\"name\":\"John\",\"age\":30}";
        String expectedJson = "{\"firstName\":\"John\",\"age\":30}";
        String result = renamer.eval(sourceJson, "name", "firstName");
        assertJsonEquals(expectedJson, result);
    }

    @Test
    public void testRenameNonExistentField() {
        String sourceJson = "{\"name\":\"John\",\"age\":30}";
        String result = renamer.eval(sourceJson, "address", "homeAddress");
        assertJsonEquals(sourceJson, result);
    }

    @Test
    public void testShouldNotReplaceValue() {
        // This test explicitly confirms the original bug is fixed.
        // The string "old_field" in the description should NOT be replaced.
        String sourceJson = "{\"description\":\"this is my old_field\",\"old_field\":\"value\"}";
        String expectedJson = "{\"description\":\"this is my old_field\",\"new_field\":\"value\"}";
        String result = renamer.eval(sourceJson, "old_field", "new_field");
        assertJsonEquals(expectedJson, result);
    }

    @Test
    public void testWithComplexValue() {
        String sourceJson = "{\"data\":{\"nested_key\":\"nested_value\"},\"age\":30}";
        String expectedJson = "{\"payload\":{\"nested_key\":\"nested_value\"},\"age\":30}";
        String result = renamer.eval(sourceJson, "data", "payload");
        assertJsonEquals(expectedJson, result);
    }

    @Test
    public void testNullJson() {
        assertNull(renamer.eval(null, "old", "new"));
    }

    @Test
    public void testNullOldName() {
        String sourceJson = "{\"name\":\"John\"}";
        assertEquals(sourceJson, renamer.eval(sourceJson, null, "new"));
    }

    @Test
    public void testNullNewName() {
        String sourceJson = "{\"name\":\"John\"}";
        assertEquals(sourceJson, renamer.eval(sourceJson, "old", null));
    }

    @Test
    public void testSameOldAndNewName() {
        String sourceJson = "{\"name\":\"John\"}";
        assertEquals(sourceJson, renamer.eval(sourceJson, "name", "name"));
    }

    @Test
    public void testInvalidJson() {
        String invalidJson = "{ not json }";
        assertEquals(invalidJson, renamer.eval(invalidJson, "old", "new"));
    }

    @Test
    public void testJsonArrayInput() {
        // The UDF should not modify non-object JSON structures
        String jsonArray = "[{\"name\":\"John\"}]";
        assertEquals(jsonArray, renamer.eval(jsonArray, "name", "firstName"));
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        JsonFieldRenamer originalFunction = new JsonFieldRenamer();

        // Use it once to initialize state if needed
        originalFunction.eval("{\"key\":\"value\"}", "key", "newKey");

        byte[] serializedData = serializeObject(originalFunction);
        JsonFieldRenamer deserializedFunction = deserializeObject(serializedData, JsonFieldRenamer.class);

        assertNotNull(deserializedFunction);

        String sourceJson = "{\"name\":\"Jane\",\"age\":25}";
        String expectedJson = "{\"firstName\":\"Jane\",\"age\":25}";
        String result = deserializedFunction.eval(sourceJson, "name", "firstName");
        assertJsonEquals(expectedJson, result);
    }

    private void assertJsonEquals(String expected, String actual) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode expectedNode = mapper.readTree(expected);
            com.fasterxml.jackson.databind.JsonNode actualNode = mapper.readTree(actual);
            assertEquals(expectedNode, actualNode);
        } catch (Exception e) {
            fail("Failed to parse or compare JSON strings.", e);
        }
    }

    private byte[] serializeObject(Object obj) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeObject(byte[] data, Class<T> clazz) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        }
    }
}
