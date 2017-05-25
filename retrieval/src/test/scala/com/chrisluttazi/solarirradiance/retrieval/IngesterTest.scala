package com.chrisluttazi.solarirradiance.retrieval

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest._

class IngesterTest extends FlatSpec with Matchers {
  implicit val spark = SparkSession
    .builder()
    .appName("Retriever test")
    .config("spark.sql.warehouse.dir", "file:${system:user.dir}/hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  implicit val logger: Logger = Logger getLogger (this getClass)

  "A retriever" should "create tables" in {

    new Ingester ingest
    val stations = spark sql s"SELECT * FROM stations"
    val stations_data = spark sql s"SELECT * FROM stations_data"
    val weather_data = spark sql s"SELECT * FROM weather_data"
    stations.count should not be 0L
    stations_data.count should not be 0L
    weather_data.count should not be 0L

    logger info s"Stations count: ${stations count}"
    logger info s"Stations Data count: ${stations_data count}"
    logger info s"Weather Data count: ${weather_data count}"
  }

}
