# Modernization Plan — solar-irradiance

Date: 2026-07-18 · Branch: `claude/repo-modernization-audit-6se2sn`

## 1. Audit

### Project shape

- Language/build: Scala, sbt multi-project (`commons`, `retrieval`,
  `preparation`, `processing`; `processing` has no sources yet).
- ~300 LOC of Scala: Hive table definitions (`commons`), a Hive ingestion job
  (`retrieval/Ingester`), a feature-preparation job (`preparation/Preparer`),
  plus two ScalaTest suites that require local NSRDB/NOAA datasets.
- CI: `.github/workflows/ci.yml` — checkout@v4, setup-java@v4 (temurin 8),
  sbt/setup-sbt@v1, `sbt compile test:compile`. Tests are not run in CI
  (datasets unavailable) — that constraint remains true.

### Toolchain / dependency inventory

| Component | Current | Latest stable | Status |
|---|---|---|---|
| sbt | 0.13.13 (2016) | 1.11.7 | EOL; **cannot bootstrap** in this environment (0.13 repos blocked / not on Maven Central) |
| Scala | 2.11.7 (2015) | 2.13.18 (2.x line) | EOL since 2017; does not run on modern JDKs |
| JDK target | 8 | 21 (only JDK installed here) | Spark 2.1 cannot run on Java 21 |
| Apache Spark (core/sql/hive) | 2.1.1 (2017) | 4.2.0 | EOL; multiple CVEs in 2.x line (e.g. CVE-2018-8024, CVE-2018-17190, CVE-2019-10099, CVE-2020-9480, CVE-2022-33891 affects <3.3, CVE-2023-22946 <3.4); log4j 1.x transitively (CVE-2019-17571, CVE-2021-4104) |
| scalactic / scalatest | 3.0.1 | 3.2.20 | Old; `FlatSpec`/`Matchers` moved packages in 3.1+ |
| sbt-assembly | 0.14.4 | 2.3.1 | sbt 0.13-only version |
| sbt-dependency-graph | 0.8.2 | built into sbt ≥1.4 (`dependencyTree`) | plugin obsolete |
| Artima resolver | present | — | only needed for the (commented-out) supersafe plugin; drop |

### Baseline verification (2026-07-18)

- `sbt compile`: **won't run** — launcher cannot resolve sbt 0.13.13
  (typesafe/scala-sbt ivy repos return 403 via proxy; 0.13 not on Maven
  Central).
- `sbt test`: won't run (same reason). Even on a period-correct toolchain the
  suites require local datasets and `RootPath.root` is `???`
  (`NotImplementedError` placeholder), so tests could never pass unedited.
- Lint/format: none configured.

### Real bugs found

1. `commons/.../HiveTable.scala`: `RootPath.root = ???` — throws
   `NotImplementedError` on first use; the dataset root can only be set by
   editing source. Fix: read from `SOLAR_IRRADIANCE_DATA_ROOT` env var /
   `solar.irradiance.data.root` system property with a clear error message.
2. `PreparerTest` labels `stations_data` as "solar_data" (asserts on the raw
   ingest table instead of the `solar_data` table the Preparer creates).

## 2. Prioritized checklist

- [x] **P0 Toolchain**: sbt 0.13.13 → 1.11.7 (`project/build.properties`);
      Scala 2.11.7 → 2.13.18; JDK target 21.
- [x] **P0 Spark 2.1 → 4.2** (Spark 4 is Scala 2.13-only, Java 17/21):
      codebase is tiny (<300 LOC of DataFrame-free Spark SQL), so the major
      migration is attempted rather than deferred. Includes: log4j 1.x
      `Logger` → slf4j (log4j 1.x removed from Spark 3.3+), ScalaTest 3.0 →
      3.2 package moves (`AnyFlatSpec`, `matchers.should.Matchers`),
      Scala 2.13 postfix/procedure-syntax cleanups, `Test / fork` +
      `--add-opens` JVM flags for Java 21.
- [x] **P0 Plugins**: sbt-assembly 2.3.1; drop sbt-dependency-graph
      (use built-in `dependencyTree`); drop Artima resolver.
- [x] **P1 Bug fixes** (with regression tests where testable without Spark):
      configurable dataset root; PreparerTest table mix-up.
- [x] **P1 Best practices**: scalafmt (sbt-scalafmt + `.scalafmt.conf`,
      enforced in CI); README refresh (JDK 21, Scala 2.13, Spark 4.2, dataset
      root configuration).
- [x] **P2 CI**: JDK 8 → 21 (temurin), `test:compile` → `Test/compile`
      (sbt 1.x syntax), add scalafmt check, keep sbt/coursier caching.

## 3. Done vs Deferred

_Filled in at the end of the modernization pass — see bottom of file._
