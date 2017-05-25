package com.chrisluttazi.solarirradiance.preparation

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class PreparerTest extends FlatSpec with Matchers {
  implicit val spark = SparkSession
    .builder()
    .appName("Retriever test")
    .config("spark.sql.warehouse.dir", "file:${system:user.dir}/hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  implicit val logger: Logger = Logger getLogger (this getClass)

  "The preparation process" should "create tables" in {

    new Preparer prepare
    val solar_data = spark sql "SELECT * FROM stations_data"
    val weather_data = spark sql "SELECT * FROM weather_data"

    solar_data.count should not be 0
    weather_data.count should not be 0

    logger info s"Solar Data count: ${solar_data count}"
    logger info s"Weather Data count: ${weather_data count}"
  }

}