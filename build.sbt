ThisBuild / organization := "com.chrisluttazi.solarirradiance"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

val sparkVersion = "4.2.0"
val scalatestVersion = "3.2.20"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-release", "17"),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.scalactic" %% "scalactic" % scalatestVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  ),
  // Spark 4 on JDK 17+ needs the JVM module system opened up (see
  // org.apache.spark.launcher.JavaModuleOptions); tests fork so the
  // options apply.
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "--enable-native-access=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false"
  )
)

lazy val commons = project
  .settings(
    commonSettings
  )

lazy val retrieval = project
  .dependsOn(commons)
  .settings(
    commonSettings
  )

lazy val preparation = project
  .dependsOn(commons)
  .settings(
    commonSettings
  )

lazy val processing = project
  .dependsOn(commons)
  .settings(
    commonSettings
  )

lazy val root = (project in file("."))
  .aggregate(commons, retrieval, preparation, processing)
