package org.example;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vehicle.objects.VehicleObjects;

import static org.junit.jupiter.api.Assertions.*;

class VehicleStayProtobufSerializerTest {

    private VehicleStayProtobufSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new VehicleStayProtobufSerializer();
    }

    @Test
    void testSerializeCompleteVehicleStay() throws Exception {
        // Create test data that matches the expected Row structure
        
        // Create Object row
        Row objectRow = Row.of(
            "test-alias",           // alias
            "test.namespace",       // namespace  
            "2024-01-01T10:00:00Z", // created
            "2024-01-01T10:30:00Z", // updated
            "correlation-123"       // correlation_id
        );

        // Create Vehicle row
        Row vehicleRow = Row.of(
            "VIN123456789",         // vehicle_identification_number
            "ABC-123",              // vehicle_license_plate
            "John Doe",             // customer_name
            "SUV",                  // vehicle_type
            "Lane-1",               // lane
            "Model-X",              // vehicle_model
            "2024-01-01T09:00:00Z", // last_transited_entered
            "ALT-ID-456"            // alternative_vehicle_identifier
        );

        // Create Trackable Object row
        Row trackableObjectRow = Row.of(
            "trackable-alias",      // alias
            "trackable.namespace",  // namespace
            "2024-01-01T09:45:00Z", // created
            "2024-01-01T10:15:00Z", // updated
            "track-correlation-456" // correlation_id
        );

        // Create Trackable Vehicle row
        Row trackableVehicleRow = Row.of(
            trackableObjectRow,     // object
            "2024-01-01T10:00:00Z", // omlox_sync_ts
            "tenant-123",           // tenant_alias
            vehicleRow              // vehicle
        );

        // Create complete VehicleStay row
        Row vehicleStayRow = Row.of(
            objectRow,                      // object (index 0)
            trackableVehicleRow,            // trackable_vehicle (index 1)
            "tenant-123",                   // tenant_alias (index 2)
            "2024-01-01T08:30:00Z",        // entry_identified_date_time (index 3)
            "Entry-Lane-A",                 // entry_identified_lane (index 4)
            "Lane-A",                       // entry_lane (index 5)
            "2024-01-01T08:45:00Z",        // entry_date_time (index 6)
            "Lane-B",                       // exit_lane (index 7)
            "2024-01-01T12:30:00Z",        // exit_date_time (index 8)
            "2024-01-01T12:35:00Z",        // exit_identified_date_time (index 9)
            "Exit-Lane-B",                  // exit_identified_lane (index 10)
            true,                           // customer_is_waiting (index 11)
            "Maintenance",                  // vehicle_visit_reason (index 12)
            false,                          // external (index 13)
            "2024-01-01T12:40:00Z",        // exit_external_drive_date_time (index 14)
            "External-Lane-C",              // exit_external_drive_lane (index 15)
            "2024-01-01T08:25:00Z",        // entry_external_drive_date_time (index 16)
            "External-Lane-A"               // entry_external_drive_lane (index 17)
        );

        // Execute serialization
        byte[] result = serializer.eval(vehicleStayRow);

        // Verify result is not null
        assertNotNull(result, "Serialized result should not be null");
        assertTrue(result.length > 0, "Serialized result should have content");

        // Deserialize to verify correctness
        VehicleObjects.VehicleStay deserializedVehicleStay = VehicleObjects.VehicleStay.parseFrom(result);

        // Verify Object fields
        assertTrue(deserializedVehicleStay.hasObject(), "VehicleStay should have object");
        VehicleObjects.Object obj = deserializedVehicleStay.getObject();
        assertEquals("test-alias", obj.getAlias());
        assertEquals("test.namespace", obj.getNamespace());
        assertEquals("2024-01-01T10:00:00Z", obj.getCreated());
        assertEquals("2024-01-01T10:30:00Z", obj.getUpdated());
        assertEquals("correlation-123", obj.getCorrelationId());

        // Verify Trackable Vehicle fields
        assertTrue(deserializedVehicleStay.hasTrackableVehicle(), "VehicleStay should have trackable vehicle");
        VehicleObjects.Trackable trackableVehicle = deserializedVehicleStay.getTrackableVehicle();
        assertEquals("2024-01-01T10:00:00Z", trackableVehicle.getOmloxSyncTs());
        assertEquals("tenant-123", trackableVehicle.getTenantAlias());
        
        assertTrue(trackableVehicle.hasObject(), "Trackable should have object");
        VehicleObjects.Object trackableObj = trackableVehicle.getObject();
        assertEquals("trackable-alias", trackableObj.getAlias());
        assertEquals("trackable.namespace", trackableObj.getNamespace());

        assertTrue(trackableVehicle.hasVehicle(), "Trackable should have vehicle");
        VehicleObjects.Trackable.Vehicle vehicle = trackableVehicle.getVehicle();
        assertEquals("VIN123456789", vehicle.getVehicleIdentificationNumber());
        assertEquals("ABC-123", vehicle.getVehicleLicensePlate());
        assertEquals("John Doe", vehicle.getCustomerName());
        assertEquals("SUV", vehicle.getVehicleType());
        assertEquals("Lane-1", vehicle.getLane());
        assertEquals("Model-X", vehicle.getVehicleModel());
        assertEquals("2024-01-01T09:00:00Z", vehicle.getLastTransitedEntered());
        assertEquals("ALT-ID-456", vehicle.getAlternativeVehicleIdentifier());

        // Verify VehicleStay specific fields
        assertEquals("tenant-123", deserializedVehicleStay.getTenantAlias());
        assertEquals("2024-01-01T08:30:00Z", deserializedVehicleStay.getEntryIdentifiedDateTime());
        assertEquals("Entry-Lane-A", deserializedVehicleStay.getEntryIdentifiedLane());
        assertEquals("Lane-A", deserializedVehicleStay.getEntryLane());
        assertEquals("2024-01-01T08:45:00Z", deserializedVehicleStay.getEntryDateTime());
        assertEquals("Lane-B", deserializedVehicleStay.getExitLane());
        assertEquals("2024-01-01T12:30:00Z", deserializedVehicleStay.getExitDateTime());
        assertEquals("2024-01-01T12:35:00Z", deserializedVehicleStay.getExitIdentifiedDateTime());
        assertEquals("Exit-Lane-B", deserializedVehicleStay.getExitIdentifiedLane());
        assertTrue(deserializedVehicleStay.getCustomerIsWaiting());
        assertEquals("Maintenance", deserializedVehicleStay.getVehicleVisitReason());
        
        assertTrue(deserializedVehicleStay.hasExternal(), "VehicleStay should have external field");
        assertFalse(deserializedVehicleStay.getExternal().getValue());
        
        assertEquals("2024-01-01T12:40:00Z", deserializedVehicleStay.getExitExternalDriveDateTime());
        assertEquals("External-Lane-C", deserializedVehicleStay.getExitExternalDriveLane());
        assertEquals("2024-01-01T08:25:00Z", deserializedVehicleStay.getEntryExternalDriveDateTime());
        assertEquals("External-Lane-A", deserializedVehicleStay.getEntryExternalDriveLane());
    }

    @Test
    void testSerializeMinimalVehicleStay() throws Exception {
        // Test with minimal required fields (some fields can be null/empty)
        Row objectRow = Row.of("alias", "namespace", "", "", "");
        Row vehicleRow = Row.of("VIN123", "ABC-123", "", "", "", "", "", "");
        Row trackableObjectRow = Row.of("t-alias", "t-namespace", "", "", "");
        Row trackableVehicleRow = Row.of(trackableObjectRow, "", "tenant", vehicleRow);

        Row vehicleStayRow = Row.of(
            objectRow,           // object
            trackableVehicleRow, // trackable_vehicle
            "tenant",           // tenant_alias
            "",                 // entry_identified_date_time
            "",                 // entry_identified_lane
            "",                 // entry_lane
            "",                 // entry_date_time
            "",                 // exit_lane
            "",                 // exit_date_time
            "",                 // exit_identified_date_time
            "",                 // exit_identified_lane
            false,              // customer_is_waiting
            "",                 // vehicle_visit_reason
            null,               // external (null)
            "",                 // exit_external_drive_date_time
            "",                 // exit_external_drive_lane
            "",                 // entry_external_drive_date_time
            ""                  // entry_external_drive_lane
        );

        byte[] result = serializer.eval(vehicleStayRow);
        assertNotNull(result);
        
        VehicleObjects.VehicleStay deserializedVehicleStay = VehicleObjects.VehicleStay.parseFrom(result);
        assertEquals("alias", deserializedVehicleStay.getObject().getAlias());
        assertEquals("VIN123", deserializedVehicleStay.getTrackableVehicle().getVehicle().getVehicleIdentificationNumber());
        assertFalse(deserializedVehicleStay.getCustomerIsWaiting());
        assertFalse(deserializedVehicleStay.hasExternal()); // Should not be set when null
    }

    @Test
    void testSerializeNullVehicleStay() {
        byte[] result = serializer.eval(null);
        assertNull(result, "Result should be null when input is null");
    }

    @Test
    void testSerializeWithNullSubObjects() throws Exception {
        // Test with null object and trackable_vehicle
        Row vehicleStayRow = Row.of(
            null,               // object (null)
            null,               // trackable_vehicle (null)
            "tenant",           // tenant_alias
            "",                 // entry_identified_date_time
            "",                 // entry_identified_lane
            "",                 // entry_lane
            "",                 // entry_date_time
            "",                 // exit_lane
            "",                 // exit_date_time
            "",                 // exit_identified_date_time
            "",                 // exit_identified_lane
            false,              // customer_is_waiting
            "",                 // vehicle_visit_reason
            null,               // external
            "",                 // exit_external_drive_date_time
            "",                 // exit_external_drive_lane
            "",                 // entry_external_drive_date_time
            ""                  // entry_external_drive_lane
        );

        byte[] result = serializer.eval(vehicleStayRow);
        assertNotNull(result);
        
        VehicleObjects.VehicleStay deserializedVehicleStay = VehicleObjects.VehicleStay.parseFrom(result);
        assertFalse(deserializedVehicleStay.hasObject());
        assertFalse(deserializedVehicleStay.hasTrackableVehicle());
        assertEquals("tenant", deserializedVehicleStay.getTenantAlias());
    }

    @Test
    void testSerializeWithExternalTrue() throws Exception {
        Row objectRow = Row.of("alias", "namespace", "", "", "");
        Row vehicleRow = Row.of("VIN123", "ABC-123", "", "", "", "", "", "");
        Row trackableObjectRow = Row.of("t-alias", "t-namespace", "", "", "");
        Row trackableVehicleRow = Row.of(trackableObjectRow, "", "tenant", vehicleRow);

        Row vehicleStayRow = Row.of(
            objectRow, trackableVehicleRow, "tenant", "", "", "", "", "", "", "", "",
            true,               // customer_is_waiting
            "Service",          // vehicle_visit_reason
            true,               // external (true)
            "", "", "", ""
        );

        byte[] result = serializer.eval(vehicleStayRow);
        assertNotNull(result);
        
        VehicleObjects.VehicleStay deserializedVehicleStay = VehicleObjects.VehicleStay.parseFrom(result);
        assertTrue(deserializedVehicleStay.getCustomerIsWaiting());
        assertEquals("Service", deserializedVehicleStay.getVehicleVisitReason());
        assertTrue(deserializedVehicleStay.hasExternal());
        assertTrue(deserializedVehicleStay.getExternal().getValue());
    }

    @Test
    void testSerializeErrorHandling() {
        // Test with malformed row (fewer fields than expected)
        Row incompleteRow = Row.of("tenant"); // Only one field instead of 18

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            serializer.eval(incompleteRow);
        });
        
        assertTrue(exception.getMessage().contains("Failed to serialize VehicleStay to protobuf"));
    }

    @Test
    void testRoundTripSerialization() throws Exception {
        // Test that we can serialize and deserialize successfully
        Row objectRow = Row.of("alias-123", "vehicle.stay", "2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z", "corr-123");
        Row vehicleRow = Row.of("VIN987654321", "XYZ-789", "Jane Smith", "Sedan", "Lane-5", "Model-Y", "2024-01-01T09:30:00Z", "ALT-789");
        Row trackableObjectRow = Row.of("track-alias-789", "track.vehicle", "2024-01-01T09:00:00Z", "2024-01-01T10:30:00Z", "track-corr-789");
        Row trackableVehicleRow = Row.of(trackableObjectRow, "2024-01-01T10:15:00Z", "tenant-456", vehicleRow);

        Row vehicleStayRow = Row.of(
            objectRow, trackableVehicleRow, "tenant-456",
            "2024-01-01T07:45:00Z", "Entry-Lane-X", "Lane-X", "2024-01-01T08:00:00Z",
            "Lane-Y", "2024-01-01T14:30:00Z", "2024-01-01T14:35:00Z", "Exit-Lane-Y",
            false, "Inspection", true,
            "2024-01-01T14:40:00Z", "External-Y", "2024-01-01T07:40:00Z", "External-X"
        );

        // First serialization
        byte[] serialized1 = serializer.eval(vehicleStayRow);
        assertNotNull(serialized1);

        // Parse back
        VehicleObjects.VehicleStay parsed = VehicleObjects.VehicleStay.parseFrom(serialized1);
        assertNotNull(parsed);

        // Verify key fields
        assertEquals("alias-123", parsed.getObject().getAlias());
        assertEquals("VIN987654321", parsed.getTrackableVehicle().getVehicle().getVehicleIdentificationNumber());
        assertEquals("Jane Smith", parsed.getTrackableVehicle().getVehicle().getCustomerName());
        assertEquals("Inspection", parsed.getVehicleVisitReason());
        assertFalse(parsed.getCustomerIsWaiting());
        assertTrue(parsed.getExternal().getValue());

        // Second serialization using toByteArray()
        byte[] serialized2 = parsed.toByteArray();
        
        // Both should produce identical results
        assertArrayEquals(serialized1, serialized2, "Both serialization methods should produce identical results");
    }
}