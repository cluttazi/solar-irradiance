package com.chrisluttazi.solarirradiance.preparation

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class PreparerTest extends AnyFlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("Retriever test")
    .config("spark.sql.warehouse.dir", "file:${system:user.dir}/hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  implicit val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "The preparation process" should "create tables" in {

    new Preparer().prepare()
    val solar_data = spark.sql("SELECT * FROM solar_data")
    val weather_data = spark.sql("SELECT * FROM weather_data_clean")

    solar_data.count() should not be 0
    weather_data.count() should not be 0

    logger.info(s"Solar Data count: ${solar_data.count()}")
    logger.info(s"Weather Data count: ${weather_data.count()}")
  }

}
