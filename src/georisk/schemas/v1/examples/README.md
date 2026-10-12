# Contract examples

Files use `<contract>__<case>.json`. All examples are packaged with georisk and
checked by `tests/test_contract_examples.py` using the public validator.

The `valid/` directory demonstrates basic telemetry, Polygon and MultiPolygon
weather alerts, alerted and non-alerted risk events, and a density metric.

The `invalid/` directory documents these rejected inputs:

| File | Rejection reason |
|---|---|
| `vessel_telemetry__missing_speed.json` | Required speed_knots is absent. |
| `weather_alert__unclosed_ring.json` | The last ring position differs from the first. |
| `risk_event__invalid_severity.json` | A matched hazard has severity 6, above the maximum of 5. |
| `density_metric__negative_count.json` | unique_vessel_count cannot be negative. |
