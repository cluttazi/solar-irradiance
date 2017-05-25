package com.chrisluttazi.solarirradiance.retrieval

import java.io.File

import com.chrisluttazi.solarirradiance.commons.model._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class Ingester(implicit logger: Logger, spark: SparkSession) extends Serializable {

  import spark.sql

  val stateCode: String = "AZ"
  val force: Boolean = false

  def ingest {
    createTable(stations)
    val stationsDataValue: HiveTable = stationsData
    stationsDataValue.files = spark
      .sql(s"SELECT usaf FROM  ${stations name} WHERE class < 3 AND st='$stateCode'")
      .rdd
      .map(
        _.toString
          .replace("[", "")
          .replace("]", ""))
      .toLocalIterator
    createTable(stationsDataValue)
    createTable(weatherStations)
    createTable(weatherStationsData)

    sql(s"CREATE TABLE IF NOT EXISTS ${weatherDataUnclean name} AS SELECT * FROM (" +
      s"select regexp_extract(input_file_name(), '[0-9]{6}',0) as usaf, * from ${stationsData name}" +
      s")")

    sql(s"CREATE TABLE IF NOT EXISTS ${weatherData name} AS SELECT * FROM (" +
      s"SELECT ws.latitude, ws.longitude, ws.elevation, wsd.date, wsd.element, wsd.value "
      + s"FROM ${weatherStationsData name} as wsd "
      + s"LEFT JOIN ${weatherStations name} as ws ON (wsd.id = ws.id) "
      + s"WHERE ws.state ='$stateCode'" +
      s")")
  }

  private def createTable(hiveTable: HiveTable)(implicit spark: SparkSession) = {
    import spark.sql
    if (sql(s"SHOW TABLES LIKE '${hiveTable name}'").collect().length == 0 || force) {
      //work with solar stations
      sql(s"DROP TABLE IF EXISTS ${hiveTable name}")
      sql(s"CREATE TABLE ${hiveTable name} (" +
        hiveTable.metadata +
        s") row format delimited fields terminated by ',' " +
        s"${hiveTable header}")
      if ( {
        hiveTable files
      } isEmpty) {
        sql(s"LOAD DATA LOCAL INPATH '${hiveTable path}' " +
          s"OVERWRITE INTO TABLE ${hiveTable name}")
      }
      else {
        for (file <- hiveTable files) {
          sql(s"LOAD DATA LOCAL INPATH " +
            s"'${hiveTable path}$file*' " +
            s"INTO TABLE ${hiveTable name}")
        }
      }
    }
  }

  object IOUtils extends Serializable {
    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }
  }

}