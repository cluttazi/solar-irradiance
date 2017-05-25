package com.chrisluttazi.solarirradiance.commons.model

class RootPath(path: String) {
  // Datasources location
  val root: String = ??? // s"/???/datasets"

  override def toString = s"$root$path"
}

object stationsRoot extends RootPath("/Stations")

object weatherRoot extends RootPath("/Weather")

class HiveTable(name_ : String,
                path_ : String = "",
                metadata_ : String = "",
                ignoreHeaders_ : Boolean = false,
                files_ : Iterator[String] = Iterator.empty) {
  val name: String = name_
  val path: String = path_
  val metadata: String = metadata_
  var files: Iterator[String] = files_

  def header: String = {
    if (ignoreHeaders_) {
      "tblproperties(\"skip.header.line.count\"=\"1\")"
    }
    else {
      ""
    }
  }
}

object stations extends HiveTable(
  name_ = s"stations",
  path_ = s"$stationsRoot/NSRDB_StationsMeta.csv",
  metadata_ = s"" +
    s"usaf int, class int, solar int," +
    s"station string, st string, nsrdb_lat double," +
    s"nsrdb_lon double, nsrdb_elev double, time_zone int," +
    s"ish_lat double, ish_lon double, ish_elev double")

object stationsData extends HiveTable(
  name_ = s"stations_data",
  path_ = s"$stationsRoot/data/",
  metadata_ = s"" +
    s"date string, time string, zenith double," +
    s"azimuth double, etr double, etrn double," +
    s"glomod double, glomodunc double, glomodsource double," +
    s"dirmod double, dirmodunc double, dirmodsource double," +
    s"difmod double, difmodunc double, difmodsource double," +
    s"measglo double, measgloflg double, measdir double," +
    s"measdirflg double, measdif double, measdifflg double," +
    s"totcc double, precipwat double, precipwatflg double," +
    s"aod double, aodflg double",
  ignoreHeaders_ = true
)

object stationsDataId extends HiveTable(
  name_ = s"stations_data_filename")

object solarData extends HiveTable(
  name_ = s"solar_data")

object weatherStations extends HiveTable(
  name_ = s"weather_stations",
  path_ = s"$weatherRoot/Stations/weatherstations.csv",
  metadata_ = s"" +
    s"id string, latitude double, longitude double," +
    s"elevation double, state string, name string")

object weatherStationsData extends HiveTable(
  name_ = s"weather_stations_data",
  path_ = s"$weatherRoot/data/*",
  metadata_ = s"" +
    s"id string, date int, element string," +
    s"value double, mflag string, qflag string," +
    s"sflag string, obstime string")

object weatherDataUnclean extends HiveTable(
  name_ = s"weather_data")

object weatherData extends HiveTable(
  name_ = s"weather_data_clean")
