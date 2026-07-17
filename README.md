# Solar Irradiance

[![CI](https://github.com/cluttazi/solar-irradiance/actions/workflows/ci.yml/badge.svg)](https://github.com/cluttazi/solar-irradiance/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.11-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-2.1-e25a1c.svg)](https://spark.apache.org/)

An Apache Spark data pipeline that ingests solar radiation (NSRDB) and weather station data into Hive tables and prepares it as a clean feature set for machine-learning models that predict solar irradiance.

## Pipeline Architecture

The build is an sbt multi-project, with one module per pipeline stage:

```
retrieval  ──►  preparation  ──►  processing
    │               │                 │
    └───────────────┴────────┬────────┘
                          commons
```

- **`commons`** – shared model: Hive table definitions (stations, station measurements, weather data) and dataset locations
- **`retrieval`** – `Ingester` creates the Hive tables from the raw NSRDB station metadata, per-station measurement files, and NOAA weather station data (filtered by state and station class)
- **`preparation`** – `Preparer` joins station metadata with measurements and filters out sentinel/null readings (zenith, azimuth, irradiance modelled values, etc.), producing a clean dataset of date, location, elevation and irradiance features
- **`processing`** – final filtering/feature selection for the ML stage

## Tech Stack

- Scala 2.11
- Apache Spark 2.1 (Core, SQL, Hive)
- ScalaTest 3.0
- sbt multi-project build, with `sbt-assembly` for fat JARs

## Getting Started

### Prerequisites

- JDK 8
- [sbt](https://www.scala-sbt.org/)
- The [NSRDB](https://nsrdb.nrel.gov/) station/measurement datasets and NOAA weather station data downloaded locally

### Configure the Dataset Location

Point the pipeline at your local dataset root in
`commons/src/main/scala/com/chrisluttazi/solarirradiance/commons/model/HiveTable.scala`
(`RootPath.root`), which expects the layout:

```
<root>/Stations/NSRDB_StationsMeta.csv
<root>/Stations/data/                     # per-station NSRDB measurement files
<root>/Weather/Stations/weatherstations.csv
<root>/Weather/data/                      # weather station observations
```

### Build & Test

```bash
sbt compile          # compile all modules
sbt test             # runs the ingestion/preparation suites (requires the datasets above)
```

## Continuous Integration

Every push and pull request compiles all modules (main and test sources) on JDK 8 via [GitHub Actions](.github/workflows/ci.yml). The test suites themselves need the local datasets, so they are run manually.

## License

This project is licensed under the MIT License – see [LICENSE](LICENSE) for details.
