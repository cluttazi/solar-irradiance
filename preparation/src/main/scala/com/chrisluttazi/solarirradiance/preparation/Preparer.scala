package com.chrisluttazi.solarirradiance.preparation

import com.chrisluttazi.solarirradiance.commons.model.{stations, stationsDataId, weatherData, weatherDataUnclean}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class Preparer(implicit logger: Logger, spark: SparkSession) extends Serializable {

  def prepare {
    import spark.sql

    val nullValue = "null"
    val queryStringBuilder: StringBuilder = new StringBuilder
    queryStringBuilder append s"SELECT "
    queryStringBuilder append s"d.date, "
    queryStringBuilder append s"(s.nsrdb_lat) latitude, (s.nsrdb_lon) longitude, (s.nsrdb_elev) elevation, "
    queryStringBuilder append s"(d.etr) sumwatts, "
    queryStringBuilder append s"d.precipwat, d.aod "
    // FROM CLAUSE
    queryStringBuilder append s"FROM ${stationsDataId name} AS d INNER JOIN ${stations name} AS s ON ( d.usaf = s.usaf ) "
    // WHERE clause start
    queryStringBuilder append s"WHERE d.usaf is not $nullValue AND "
    queryStringBuilder append s"d.date is not $nullValue AND "
    queryStringBuilder append s"d.time is not $nullValue AND "
    queryStringBuilder append s"d.zenith != 99.0 AND d.zenith is not $nullValue AND "
    queryStringBuilder append s"d.azimuth != -99.0 AND d.azimuth is not $nullValue AND "
    queryStringBuilder append s"d.etr is not $nullValue AND "
    queryStringBuilder append s"d.etrn is not $nullValue AND "
    queryStringBuilder append s"d.glomod != -9900 AND d.glomod is not $nullValue AND "
    queryStringBuilder append s"d.glomodunc != -9900 AND d.glomodunc is not $nullValue AND "
    queryStringBuilder append s"d.glomodsource != 99 AND d.glomodsource is not $nullValue AND "
    queryStringBuilder append s"d.dirmod != -9900 AND d.dirmod is not $nullValue AND "
    queryStringBuilder append s"d.dirmodunc != -9900 AND d.dirmodunc is not $nullValue AND "
    queryStringBuilder append s"d.dirmodsource != 99 AND d.dirmodsource is not $nullValue AND "
    queryStringBuilder append s"d.difmod != -9900 AND d.difmod is not $nullValue AND "
    queryStringBuilder append s"d.difmodunc != -9900 AND d.difmodunc is not $nullValue AND "
    queryStringBuilder append s"d.difmodsource != 99 AND d.difmodsource is not $nullValue AND "
    queryStringBuilder append s"d.precipwat != 99.0 AND d.precipwat is not $nullValue AND "
    queryStringBuilder append s"d.precipwatflg is not $nullValue AND "
    queryStringBuilder append s"d.aod != -9900.0 AND d.aod is not $nullValue AND "
    queryStringBuilder append s"d.aodflg != 99 AND d.aodflg is not $nullValue"
    // WHERE clause end
    sql(s"CREATE TABLE IF NOT EXISTS solar_data AS SELECT * FROM (${queryStringBuilder toString})")

    queryStringBuilder.clear()

    //    //filtering by the weather features that are most significative
    queryStringBuilder append s"SELECT * FROM ${weatherDataUnclean name} "
    queryStringBuilder append s"WHERE array_contains(split('"
    queryStringBuilder append s"TMAX,TMIN,ACMC,ACMH,ACSC,ACSH,DAEV,DAPR,"
    queryStringBuilder append s"EVAP,MDEV,MDPR,PGTM,PSUN,SN,SX,TAVG,TOBS,TSUN,WT,WV,"
    queryStringBuilder append s"WT01,WT02,WT03,WT04,WT05,WT06,WT07,"
    queryStringBuilder append s"WT08,WT09,WT10,WT11,WT12,WT13,WT14,"
    queryStringBuilder append s"WT15,WT16,WT17,WT18,WT19,WT20,WT21,WT22,"
    queryStringBuilder append s"SN01,SN02,SN03,SN04,SN05,SN06,SN07,SN11,SN12,SN13,"
    queryStringBuilder append s"SN14,SN15,SN16,SN17,SN21,SN22,SN23,SN24,SN25,SN26,"
    queryStringBuilder append s"SN27,SN31,SN32,SN33,SN34,SN35,SN36,SN37,SN41,SN42,"
    queryStringBuilder append s"SN43,SN44,SN45,SN46,SN47,SN51,SN52,SN53,SN54,SN55,"
    queryStringBuilder append s"SN56,SN57,SN61,SN62,SN63,SN64,SN65,SN66,SN67,SN71,"
    queryStringBuilder append s"SN72,SN73,SN74,SN75,SN76,SN77,SN81,SN82,SN83,SN84,SN85,SN86,SN87,"
    queryStringBuilder append s"SN01,SN02,SN03,SN04,SN05,SN06,SN07,SN11,SN12,SN13,"
    queryStringBuilder append s"SN14,SN15,SN16,SN17,SN21,SN22,SN23,SN24,SN25,SN26,"
    queryStringBuilder append s"SN27,SN31,SN32,SN33,SN34,SN35,SN36,SN37,SN41,SN42,"
    queryStringBuilder append s"SX43,SX44,SX45,SX46,SX47,SX51,SX52,SX53,SX54,SX55,"
    queryStringBuilder append s"SX56,SX57,SX61,SX62,SX63,SX64,SX65,SX66,SX67,SX71,"
    queryStringBuilder append s"SX72,SX73,SX74,SX75,SX76,SX77,SX81,SX82,SX83,SX84,SX85,SX86,SX87"
    queryStringBuilder append s"',','),element)"

    sql(s"CREATE TABLE IF NOT EXISTS ${weatherData name} AS SELECT * FROM (${queryStringBuilder toString})")


  }
}
