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

### Done

- sbt 0.13.13 → **1.11.7**; Scala 2.11.7 → **2.13.18**; JDK target 8 → **21**
  (`-release 17`, matching Spark 4's minimum).
- Spark 2.1.1 → **4.2.0** (core/sql/hive), including the full migration:
  log4j 1.x → slf4j loggers, `Test / fork` with the JDK 17+ `--add-opens`
  set, ScalaTest 3.0 → **3.2.20** (`AnyFlatSpec` / `matchers.should.Matchers`).
- Scala 2.13 syntax modernization: no postfix operators, no procedure
  syntax, explicit types on implicits, explicit `()` on side-effecting calls.
- Plugins: sbt-assembly **2.3.1**; sbt-dependency-graph → built-in
  `addDependencyTreePlugin`; dropped unused Artima resolver.
- Bug fixes: configurable dataset root (`solar.irradiance.data.root` /
  `SOLAR_IRRADIANCE_DATA_ROOT`) replacing the `???` placeholder, with new
  dataset-independent unit tests in `commons` (3 tests, all pass);
  `PreparerTest` now asserts on the tables the Preparer actually creates
  (`solar_data`, `weather_data_clean`).
- scalafmt 3.11.4 via sbt-scalafmt 2.5.5 (2.6.x needs sbt ≥ 1.12); whole
  codebase formatted; check enforced in CI.
- README refreshed (toolchain, dataset-root configuration, build/test docs).
- CI: JDK 8 → 21 (temurin), sbt 1.x task syntax, added format check and
  `commons/test`; kept checkout@v4 / setup-java@v4 / sbt-setup@v1 / sbt cache.
- Security: leaves the EOL Spark 2.1.1 line (CVE-2018-8024, CVE-2018-17190,
  CVE-2019-10099, CVE-2020-9480, CVE-2022-33891, CVE-2023-22946, …) and the
  transitive log4j 1.x (CVE-2019-17571, CVE-2021-4104) behind; Spark 4.2.0
  had no known unpatched CVEs at time of writing.

### Deferred

- **Running the Spark ingestion/preparation suites**: they require the local
  NSRDB/NOAA datasets (unchanged constraint; CI compiles + runs unit tests
  only). They compile, boot a local Spark 4 session on JDK 21, and fail
  fast with a clear configuration error instead of `NotImplementedError`.
- **ANSI SQL mode**: Spark 4 enables `spark.sql.ansi.enabled` by default;
  the pipeline's Hive-style queries were written under Spark 2 semantics.
  Verify against real datasets before relying on outputs (sentinel-value
  comparisons should be unaffected, but casts of malformed CSV fields now
  error instead of returning NULL).
- **`processing` module** has no sources; left as-is.
- **Semantic oddity (pre-existing, unchanged)**: `weatherDataUnclean`
  (table `weather_data`) is populated from *solar* station files in
  `Ingester`; renaming would change on-disk table names, so it is only
  noted here.
- Scala 3 / sbt 2 migration: out of scope for a conservative pass.

## 4. Summary (PR-description style)

**What changed**: the build is brought from a 2016/2017 stack (sbt 0.13,
Scala 2.11, Spark 2.1, JDK 8) that can no longer be resolved or run, to a
current one (sbt 1.11.7, Scala 2.13.18, Spark 4.2.0, JDK 21) with scalafmt,
dataset-root configuration via env var/system property, two real bug fixes,
new unit tests, and CI that checks formatting, compiles everything, and runs
the dataset-independent tests on JDK 21.

**Risks**: Spark 2 → 4 is a four-major-version jump; the SQL surface used
here is small (Hive DDL + `LOAD DATA` + `input_file_name`/`regexp_extract`/
`array_contains`) and compiles clean, but the ingestion/preparation flows
could not be executed end-to-end without the proprietary datasets — ANSI
mode and Hive-metastore behavior differences should be validated on a real
dataset run. `RootPath.root` changed from a `val` (that always threw) to a
`def`; `ingest`/`prepare` gained explicit `()` parameter lists.

**Untouched**: pipeline SQL/business logic, table names and schemas, module
layout, `processing` placeholder module, LICENSE, .editorconfig, .gitignore.
