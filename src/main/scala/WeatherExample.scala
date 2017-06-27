import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{CountVectorizer, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object WeatherExample {

  val GlobalTemperatures = "GlobalTemperatures.csv"
  val GlobalLandTemperaturesByCountry = "GlobalLandTemperaturesByCountry.csv"
  val GlobalLandTemperaturesByCity = "GlobalLandTemperaturesByCity.csv"

  /**
    * The argument should be path to files.
    *
    * @param args arguments.
    */
  def main(args: Array[String]): Unit = {
    make(args(0), args(1))
  }

  def make(pathToFiles: String, pathToSave: String) {
    val spark = SparkSession
      .builder()
      .appName("WeatherExample")
      .getOrCreate()

    // this is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    val scales = Seq("City", "Country", "Land")
    val periods = Seq("Year", "Decade", "Century")
    val reduceMethods = Seq("min", "max", "avg")

    var tempDF = getGlobalLandTemperaturesByCityDF(spark, pathToFiles).select("dt", "AverageTemperature", "City", "Country")
      .map(row => (row.getAs[String](0).substring(0, 4),
        if (row(1) == null) null.asInstanceOf[Double] else row.getDouble(1),
        row.getAs[String](2), row.getAs[String](3))).toDF("Year", "AverageTemperature", "City", "Country")

    // first element for join
    var resultDF = tempDF
      .select("Year", "City", "Country")
      .map(row => (row.getAs[String](0).substring(0, 4),
        row.getAs[String](0).substring(0, 3),
        row.getAs[String](0).substring(0, 2),
        row.getAs[String](1),
        row.getAs[String](2)
      ))
      .dropDuplicates().toDF("Year", "Decade", "Century", "City", "Country")

    for (scale <- scales) {
      var minSourceDataFrame: DataFrame = null
      var maxSourceDataFrame: DataFrame = null
      var avgSourceDataFrame: DataFrame = null
      for(period <- periods) {
        if (period == "Year") {
          if (scale == "Country") tempDF = getGlobalLandTemperaturesByCountryDF(spark, pathToFiles)
            .select("dt", "AverageTemperature", "Country")
          else if (scale == "Land") tempDF = getGlobalTemperaturesDF(spark, pathToFiles)
            .select("dt", "LandAverageTemperature")
          minSourceDataFrame = tempDF
          maxSourceDataFrame = tempDF
          avgSourceDataFrame = tempDF
        }
        for (reduceMethod <- reduceMethods) {
          val dataFrame = inScaleBy(scale, spark,
            if (reduceMethod == "min") minSourceDataFrame
            else if (reduceMethod == "max") maxSourceDataFrame
            else avgSourceDataFrame,
            period, reduceMethod)
          if (reduceMethod == "min") minSourceDataFrame = dataFrame
          else if (reduceMethod == "max") maxSourceDataFrame = dataFrame
          else avgSourceDataFrame = dataFrame
          resultDF = resultDF.join(dataFrame, getJoinColumns(scale) :+ period)
        }
      }
    }

    resultDF.drop("Century", "Decade")
    resultDF.show()

    resultDF.write.format("parquet").save(
      if (pathToSave.endsWith("/")) pathToSave + "weather.parquet"
      else pathToSave + "/" + "weather.parquet")

    spark.stop()
  }

  def readFile(sparkContext: SparkContext, pathToFiles: String, fileName: String): (Array[String], RDD[Array[String]]) = {
    val rowData = sparkContext.textFile(if (pathToFiles.endsWith("/")) pathToFiles + fileName
    else pathToFiles + "/" + fileName)
      .map(line => line.split(",", -1).map(_.trim))
    val header = rowData.first
    (header, rowData.filter(_ (0) != header(0)))
  }

  def substrIndexFrom(period: String): Int = period match {
    case "Year" => 4
    case "Decade" => 3
    case "Century" => 2
    case _ => throw new IllegalArgumentException
  }

  def reduce(method: String, a: Double, b: Double): Double = method match {
    case "min" => math.min(a, b)
    case "max" => math.max(a, b)
    case "avg" => if (a == null) b else if (b == null) a else (a + b) / 2
    case _ => throw new IllegalArgumentException
  }

  def inScaleBy(scale: String,
                spark: SparkSession,
                sourceDataFrame: DataFrame,
                period: String,
                reduceMethod: String): DataFrame = scale match {
    case "City" => inCityBy(spark, sourceDataFrame, period, reduceMethod)
    case "Country" => inCountryBy(spark, sourceDataFrame, period, reduceMethod)
    case "Land" => inLandBy(spark, sourceDataFrame, period, reduceMethod)
    case _ => throw new IllegalArgumentException
  }

  def inLandBy(spark: SparkSession,
               sourceDataFrame: DataFrame,
               period: String,
               reduceMethod: String): DataFrame = {
    import spark.implicits._

    sourceDataFrame.rdd.map(row => (row.getAs[String](0).substring(0, substrIndexFrom(period)),
      if (row(1) == null) null.asInstanceOf[Double] else row.getDouble(1)))
      .reduceByKey((a, b) =>reduce(reduceMethod, a, b))
      .toDF(period, reduceMethod + "InLandBy" + period)
  }

  def inCountryBy(spark: SparkSession,
               sourceDataFrame: DataFrame,
               period: String,
               reduceMethod: String): DataFrame = {
    import spark.implicits._
    sourceDataFrame.rdd.map(row => ((row.getAs[String](0).substring(0, substrIndexFrom(period))
      , row.getAs[String](2)),
      if (row(1) == null) null.asInstanceOf[Double] else row.getDouble(1)))
      .reduceByKey((a, b) => if (a == null) b else if (b == null) a else (a + b) / 2)
      .map(tuple => (tuple._1._1, tuple._1._2, tuple._2))
      .toDF(period, "Country", reduceMethod + "InCountryBy" + period)
  }

  def inCityBy(spark: SparkSession,
               sourceDataFrame: DataFrame,
               period: String,
               reduceMethod: String): DataFrame = {
    import spark.implicits._
    sourceDataFrame.rdd.map(row => ((row.getAs[String](0).substring(0, substrIndexFrom(period)),
      row.getAs[String](2), row.getAs[String](3)),
      if (row(1) == null) null.asInstanceOf[Double] else row.getDouble(1)))
      .reduceByKey((a, b) => if (a == null) b else if (b == null) a else (a + b) / 2)
      .map(tuple => (tuple._1._1, tuple._1._2, tuple._1._3, tuple._2))
      .toDF(period, "City", "Country", reduceMethod + "InCityBy" + period)
  }

  def getJoinColumns(scale: String): Seq[String] = scale match {
    case "City" => Seq("City", "Country")
    case "Country" => Seq("Country")
    case "Land" => Seq()
    case _ => throw new IllegalArgumentException
  }
  def getGlobalTemperaturesDF(spark: SparkSession, pathToFiles: String): DataFrame = {
    import spark.implicits._

    val (globalTemperaturesHeader, globalTemperaturesRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalTemperatures)
    globalTemperaturesRdd
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
  }

  def getGlobalLandTemperaturesByCountryDF(spark: SparkSession, pathToFiles: String): DataFrame = {
    import spark.implicits._

    val (globalLandTemperaturesByCountryHeader, globalLandTemperaturesByCountryRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalLandTemperaturesByCountry)
    globalLandTemperaturesByCountryRdd
      .map(array =>
        Tuple4[String, Double, Double, String](
          array(0),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          array(3)))
      .toDF(globalLandTemperaturesByCountryHeader: _*)
  }

  def getGlobalLandTemperaturesByCityDF(spark: SparkSession, pathToFiles: String): DataFrame = {
    import spark.implicits._

    val (globalLandTemperaturesByCityHeader, globalLandTemperaturesByCityRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalLandTemperaturesByCity)
    globalLandTemperaturesByCityRdd
      .map(array =>
        Tuple7[String, Double, Double, String, String, String, String](
          array(0),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          array(3), array(4), array(5), array(6)))
      .toDF(globalLandTemperaturesByCityHeader: _*)
  }
}