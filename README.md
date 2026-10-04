# GeoRisk-Spark

GeoRisk-Spark is a semester project exploring how a streaming data pipeline can help logistics teams spot vessel exposure to marine hazards and congestion. It uses synthetic data and H3 cells for a repeatable demonstration. It is an academic prototype, not an operational navigation or safety system.

## First foundation

The current foundation reads a small vessel telemetry sample and a GeoJSON storm polygon, maps positions and polygon coverage to H3 cells, flags matching vessels, and writes inspectable JSON Lines output. It runs locally and does not create AWS resources.

### Requirements

- Python 3.10 or newer
- Install the single dependency with `python -m pip install -r requirements.txt`

### Run

```powershell
python src/georisk/foundation.py --telemetry data/sample/vessel-telemetry.jsonl --hazards data/sample/weather-alerts.geojson --output output/data/risk-events.jsonl
```

Review `output/data/risk-events.jsonl`. The sample storm is deliberately large enough that its interior H3 cells cover the sample vessel locations. Resolution 7 is the default; use `--resolution 8` to compare a finer grid.

## Project roadmap

The semester roadmap, four-person work split, architecture, data contracts, milestones, cloud plan, validation criteria, and demo guide are in [the team project guide](output/pdf/GeoRisk-Spark-Team-Project-Guide.pdf).

## Cloud direction

The planned AWS path uses Amazon Kinesis Data Streams for telemetry and weather-alert events, AWS Glue Spark streaming for spatial processing, Amazon S3 with Parquet for the data lake, and AWS Glue Data Catalog plus Athena for SQL analytics. Cloud resources are added in later milestones and should only run during development or demos. The initial foundation is local and has no AWS bill.

## Important spatial note

H3 is a discrete spatial index. The prototype covers each storm polygon with H3 cells and checks whether a vessel's cell is in that covered set. This is a fast coarse spatial filter, not exact polygon containment. Boundary-sensitive production decisions would need an additional geometry check and operational validation.
