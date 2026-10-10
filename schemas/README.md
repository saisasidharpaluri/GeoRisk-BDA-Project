# Shared event contracts

The `v1` JSON Schemas define producer and consumer interfaces. All event timestamps are UTC ISO 8601. Vessel coordinates use separate `latitude` and `longitude` fields; GeoJSON hazard coordinates follow GeoJSON longitude/latitude order. Breaking changes require a new schema version and agreement from all four workstream owners.

Python producers and processors should call `georisk.contracts.validate_event`
before accepting or publishing an event. It uses the checked-in schemas and
enforces UTC `date-time` values. Invalid records should be quarantined by the
streaming layer rather than silently accepted. Hazard polygon coordinates use
GeoJSON longitude/latitude order and must contain at least one ring.
