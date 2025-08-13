package org.example;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vehicle.objects.VehicleObjects;

/**
 * Type-specific UDF for serializing VehicleStay Flink Row to protobuf bytes.
 * This UDF is designed for optimal performance with compile-time type safety.
 */
public class VehicleStayProtobufSerializer extends ScalarFunction {
    
    public static final String NAME = "VEHICLE_STAY_PROTOBUF_SERIALIZE";
    
    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Serializes a VehicleStay Row to protobuf bytes.
     * 
     * Row structure matches the VehicleStay protobuf message:
     * - object: ROW<alias STRING, namespace STRING, created STRING, updated STRING, correlation_id STRING>
     * - trackable_vehicle: ROW<...> (Trackable structure)
     * - tenant_alias: STRING
     * - entry_identified_date_time: STRING
     * - entry_identified_lane: STRING
     * - entry_lane: STRING
     * - entry_date_time: STRING
     * - exit_lane: STRING
     * - exit_date_time: STRING
     * - exit_identified_date_time: STRING
     * - exit_identified_lane: STRING
     * - customer_is_waiting: BOOLEAN
     * - vehicle_visit_reason: STRING
     * - external: BOOLEAN (can be null)
     * - exit_external_drive_date_time: STRING
     * - exit_external_drive_lane: STRING
     * - entry_external_drive_date_time: STRING
     * - entry_external_drive_lane: STRING
     */
    public byte[] eval(
        @DataTypeHint("ROW<" +
            "object ROW<alias STRING, namespace STRING, created STRING, updated STRING, correlation_id STRING>, " +
            "trackable_vehicle ROW<" +
                "object ROW<alias STRING, namespace STRING, created STRING, updated STRING, correlation_id STRING>, " +
                "omlox_sync_ts STRING, " +
                "tenant_alias STRING, " +
                "vehicle ROW<vehicle_identification_number STRING, vehicle_license_plate STRING, customer_name STRING, vehicle_type STRING, lane STRING, vehicle_model STRING, last_transited_entered STRING, alternative_vehicle_identifier STRING>" +
            ">, " +
            "tenant_alias STRING, " +
            "entry_identified_date_time STRING, " +
            "entry_identified_lane STRING, " +
            "entry_lane STRING, " +
            "entry_date_time STRING, " +
            "exit_lane STRING, " +
            "exit_date_time STRING, " +
            "exit_identified_date_time STRING, " +
            "exit_identified_lane STRING, " +
            "customer_is_waiting BOOLEAN, " +
            "vehicle_visit_reason STRING, " +
            "external BOOLEAN, " +
            "exit_external_drive_date_time STRING, " +
            "exit_external_drive_lane STRING, " +
            "entry_external_drive_date_time STRING, " +
            "entry_external_drive_lane STRING" +
        ">")
        Row vehicleStayRow
    ) {
        if (vehicleStayRow == null) {
            LOGGER.warn("VehicleStay row is null, returning null");
            return null;
        }

        try {
            VehicleObjects.VehicleStay.Builder builder = VehicleObjects.VehicleStay.newBuilder();

            // Extract object field (index 0)
            Row objectRow = vehicleStayRow.getFieldAs(0);
            if (objectRow != null) {
                VehicleObjects.Object.Builder objectBuilder = VehicleObjects.Object.newBuilder();
                setStringField(objectBuilder::setAlias, objectRow, 0);
                setStringField(objectBuilder::setNamespace, objectRow, 1);
                setStringField(objectBuilder::setCreated, objectRow, 2);
                setStringField(objectBuilder::setUpdated, objectRow, 3);
                setStringField(objectBuilder::setCorrelationId, objectRow, 4);
                builder.setObject(objectBuilder.build());
            }

            // Extract trackable_vehicle field (index 1)
            Row trackableVehicleRow = vehicleStayRow.getFieldAs(1);
            if (trackableVehicleRow != null) {
                VehicleObjects.Trackable.Builder trackableBuilder = VehicleObjects.Trackable.newBuilder();
                
                // Trackable object (index 0)
                Row trackableObjectRow = trackableVehicleRow.getFieldAs(0);
                if (trackableObjectRow != null) {
                    VehicleObjects.Object.Builder trackableObjectBuilder = VehicleObjects.Object.newBuilder();
                    setStringField(trackableObjectBuilder::setAlias, trackableObjectRow, 0);
                    setStringField(trackableObjectBuilder::setNamespace, trackableObjectRow, 1);
                    setStringField(trackableObjectBuilder::setCreated, trackableObjectRow, 2);
                    setStringField(trackableObjectBuilder::setUpdated, trackableObjectRow, 3);
                    setStringField(trackableObjectBuilder::setCorrelationId, trackableObjectRow, 4);
                    trackableBuilder.setObject(trackableObjectBuilder.build());
                }
                
                // omlox_sync_ts (index 1)
                setStringField(trackableBuilder::setOmloxSyncTs, trackableVehicleRow, 1);
                
                // tenant_alias (index 2)
                setStringField(trackableBuilder::setTenantAlias, trackableVehicleRow, 2);
                
                // vehicle (index 3)
                Row vehicleRow = trackableVehicleRow.getFieldAs(3);
                if (vehicleRow != null) {
                    VehicleObjects.Trackable.Vehicle.Builder vehicleBuilder = VehicleObjects.Trackable.Vehicle.newBuilder();
                    setStringField(vehicleBuilder::setVehicleIdentificationNumber, vehicleRow, 0);
                    setStringField(vehicleBuilder::setVehicleLicensePlate, vehicleRow, 1);
                    setStringField(vehicleBuilder::setCustomerName, vehicleRow, 2);
                    setStringField(vehicleBuilder::setVehicleType, vehicleRow, 3);
                    setStringField(vehicleBuilder::setLane, vehicleRow, 4);
                    setStringField(vehicleBuilder::setVehicleModel, vehicleRow, 5);
                    setStringField(vehicleBuilder::setLastTransitedEntered, vehicleRow, 6);
                    setStringField(vehicleBuilder::setAlternativeVehicleIdentifier, vehicleRow, 7);
                    trackableBuilder.setVehicle(vehicleBuilder.build());
                }
                
                builder.setTrackableVehicle(trackableBuilder.build());
            }

            // Set all remaining string and boolean fields
            setStringField(builder::setTenantAlias, vehicleStayRow, 2);
            setStringField(builder::setEntryIdentifiedDateTime, vehicleStayRow, 3);
            setStringField(builder::setEntryIdentifiedLane, vehicleStayRow, 4);
            setStringField(builder::setEntryLane, vehicleStayRow, 5);
            setStringField(builder::setEntryDateTime, vehicleStayRow, 6);
            setStringField(builder::setExitLane, vehicleStayRow, 7);
            setStringField(builder::setExitDateTime, vehicleStayRow, 8);
            setStringField(builder::setExitIdentifiedDateTime, vehicleStayRow, 9);
            setStringField(builder::setExitIdentifiedLane, vehicleStayRow, 10);
            
            // customer_is_waiting (boolean, index 11)
            Boolean customerIsWaiting = vehicleStayRow.getFieldAs(11);
            if (customerIsWaiting != null) {
                builder.setCustomerIsWaiting(customerIsWaiting);
            }
            
            setStringField(builder::setVehicleVisitReason, vehicleStayRow, 12);
            
            // external (boolean with wrapper, index 13)
            Boolean external = vehicleStayRow.getFieldAs(13);
            if (external != null) {
                builder.setExternal(com.google.protobuf.BoolValue.of(external));
            }
            
            setStringField(builder::setExitExternalDriveDateTime, vehicleStayRow, 14);
            setStringField(builder::setExitExternalDriveLane, vehicleStayRow, 15);
            setStringField(builder::setEntryExternalDriveDateTime, vehicleStayRow, 16);
            setStringField(builder::setEntryExternalDriveLane, vehicleStayRow, 17);

            return builder.build().toByteArray();

        } catch (Exception e) {
            LOGGER.error("Failed to serialize VehicleStay Row to protobuf bytes: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize VehicleStay to protobuf", e);
        }
    }

    /**
     * Helper method to safely set string fields from Row
     */
    private void setStringField(java.util.function.Consumer<String> setter, Row row, int index) {
        if (row != null && index < row.getArity()) {
            String value = row.getFieldAs(index);
            if (value != null && !value.isEmpty()) {
                setter.accept(value);
            }
        }
    }
}