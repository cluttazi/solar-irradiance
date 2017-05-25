lazy val commonSettings = Seq(
  organization := "com.chrisluttazi.solarirradiance",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.7",
  resolvers ++= Seq(
    "Artima Maven Repository" at "http://repo.artima.com/releases"
  ),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.1",
    "org.apache.spark" %% "spark-sql" % "2.1.1",
    "org.apache.spark" %% "spark-hive" % "2.1.1",
    "org.scalactic" %% "scalactic" % "3.0.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
)

lazy val commons = project
  .settings(
    commonSettings
    // other settings
  )

lazy val retrieval = project
  .dependsOn(commons)
  .settings(
    commonSettings
    // other settings
  )

lazy val preparation = project
  .dependsOn(commons)
  .settings(
    commonSettings
    // other settings
  )

lazy val processing = project
  .dependsOn(commons)
  .settings(
    commonSettings
    // other settings
  )

//lazy val streaming = project
//  .dependsOn(commons)
//  .settings(
//    commonSettings
//    // other settings
//  )

lazy val root = (project in file(".")).
  aggregate(commons, retrieval, preparation, processing, streaming)
