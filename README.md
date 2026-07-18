# Solar Irradiance

[![CI](https://github.com/cluttazi/solar-irradiance/actions/workflows/ci.yml/badge.svg)](https://github.com/cluttazi/solar-irradiance/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.13-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.2-e25a1c.svg)](https://spark.apache.org/)

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

- Scala 2.13
- Apache Spark 4.2 (Core, SQL, Hive)
- ScalaTest 3.2
- sbt 1.11 multi-project build, with `sbt-assembly` for fat JARs and scalafmt for formatting

## Getting Started

### Prerequisites

- JDK 17 or 21 (Spark 4 requires Java 17+)
- [sbt](https://www.scala-sbt.org/) 1.x (the version in `project/build.properties` is downloaded automatically)
- The [NSRDB](https://nsrdb.nrel.gov/) station/measurement datasets and NOAA weather station data downloaded locally

### Configure the Dataset Location

Point the pipeline at your local dataset root with either the
`SOLAR_IRRADIANCE_DATA_ROOT` environment variable or the
`solar.irradiance.data.root` JVM system property (the property wins), e.g.

```bash
export SOLAR_IRRADIANCE_DATA_ROOT=/path/to/datasets
```

The root directory is expected to have the layout:

```
<root>/Stations/NSRDB_StationsMeta.csv
<root>/Stations/data/                     # per-station NSRDB measurement files
<root>/Weather/Stations/weatherstations.csv
<root>/Weather/data/                      # weather station observations
```

### Build & Test

```bash
sbt compile          # compile all modules
sbt commons/test     # unit tests (no Spark session or datasets needed)
sbt test             # also runs the ingestion/preparation suites (requires the datasets above)
sbt scalafmtCheckAll # formatting check
```

## Continuous Integration

Every push and pull request checks formatting, compiles all modules (main and test sources) and runs the dataset-independent unit tests on JDK 21 via [GitHub Actions](.github/workflows/ci.yml). The Spark ingestion/preparation suites need the local datasets, so they are run manually.

## License

This project is licensed under the MIT License – see [LICENSE](LICENSE) for details.
