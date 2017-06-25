import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{CountVectorizer, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object WeatherDS {

  val GlobalTemperatures = "GlobalTemperatures.csv"
  val GlobalLandTemperaturesByCountry = "GlobalLandTemperaturesByCountry.csv"
  val GlobalLandTemperaturesByCity = "GlobalLandTemperaturesByCity.csv"

  val DateTimeFormat = "yyyy-MM-dd"

  /**
    * The argument should be path to files.
    *
    * @param args arguments.
    */
  def main(args: Array[String]): Unit = {
    makeDS(args(0))
  }

  def makeDS(pathToFiles: String) {
    val spark = SparkSession
      .builder()
      .appName("WeatherExample")
      .getOrCreate()

    // this is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    val format = new java.text.SimpleDateFormat(DateTimeFormat)

    val (globalTemperaturesHeader, globalTemperaturesRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalTemperatures)
    val globalTemperaturesDF = globalTemperaturesRdd
      .map(array =>
        Tuple9[String, Double, Double, Double, Double, Double, Double, Double, Double](
          array(0),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          if (array(3).isEmpty) null.asInstanceOf[Double] else array(3).toDouble,
          if (array(4).isEmpty) null.asInstanceOf[Double] else array(4).toDouble,
          if (array(5).isEmpty) null.asInstanceOf[Double] else array(5).toDouble,
          if (array(6).isEmpty) null.asInstanceOf[Double] else array(6).toDouble,
          if (array(7).isEmpty) null.asInstanceOf[Double] else array(7).toDouble,
          if (array(8).isEmpty) null.asInstanceOf[Double] else array(8).toDouble))
      .toDF(globalTemperaturesHeader: _*)

    val (globalLandTemperaturesByCountryHeader, globalLandTemperaturesByCountryRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalLandTemperaturesByCountry)
    val globalLandTemperaturesByCountryDF = globalLandTemperaturesByCountryRdd
      .map(array =>
        Tuple4[String, Double, Double, String](
          array(0),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          array(3)))
      .toDF(globalLandTemperaturesByCountryHeader: _*)

    val (globalLandTemperaturesByCityHeader, globalLandTemperaturesByCityRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalLandTemperaturesByCity)
    val globalLandTemperaturesByCityDF = globalLandTemperaturesByCityRdd
      .map(array =>
        Tuple7[String, Double, Double, String, String, String, String](
          array(0),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          array(3), array(4), array(5), array(6)))
      .toDF(globalLandTemperaturesByCityHeader: _*)

    var tempDF = globalLandTemperaturesByCityDF.select("dt", "AverageTemperature", "City", "Country")
      .map(row => (row.getAs[String](0).substring(0, 4),
        if(row(1) == null) null.asInstanceOf[Double] else row.getDouble(1),
        row.getAs[String](2), row.getAs[String](3))).toDF("dt", "AverageTemperature", "City", "Country")

    // first element for join
    var yearCityCountryDF = tempDF
      .select("dt", "City", "Country")
      .dropDuplicates().toDF("Year", "City", "Country")

    spark.stop()
  }

  def readFile(sparkContext: SparkContext, pathToFiles: String, fileName: String): (Array[String], RDD[Array[String]]) = {
    val rowData = sparkContext.textFile(if (pathToFiles.endsWith("/")) pathToFiles + fileName
    else pathToFiles + "/" + fileName)
      .map(line => line.split(",", -1).map(_.trim))
    val header = rowData.first
    (header, rowData.filter(_(0) != header(0)))
  }
}