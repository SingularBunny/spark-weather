import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{CountVectorizer, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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
        Tuple9[java.sql.Date, Double, Double, Double, Double, Double, Double, Double, Double](
          new java.sql.Date(new java.text.SimpleDateFormat(DateTimeFormat).parse(array(0)).getTime),
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
    val globalLandTemperaturesByCountryDF = globalTemperaturesRdd
      .map(array =>
        Tuple4[java.sql.Date, Double, Double, String](
          new java.sql.Date(new java.text.SimpleDateFormat(DateTimeFormat).parse(array(0)).getTime),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          array(3)))
      .toDF(globalLandTemperaturesByCountryHeader: _*)

    val (globalLandTemperaturesByCityHeader, globalLandTemperaturesByCityRdd) =
      readFile(spark.sparkContext, pathToFiles, GlobalLandTemperaturesByCity)
    val globalLandTemperaturesByCityDF = globalTemperaturesRdd
      .map(array =>
        Tuple7[java.sql.Date, Double, Double, String, String, String, String](
          new java.sql.Date(new java.text.SimpleDateFormat(DateTimeFormat).parse(array(0)).getTime),
          if (array(1).isEmpty) null.asInstanceOf[Double] else array(1).toDouble,
          if (array(2).isEmpty) null.asInstanceOf[Double] else array(2).toDouble,
          array(3), array(4), array(5), array(6)))
      .toDF(globalLandTemperaturesByCityHeader: _*)
    //    val regexTokenizer = new RegexTokenizer()
    //      .setInputCol("text")
    //      .setOutputCol("words")
    //      .setPattern("\\W")
    //    val wordsData = regexTokenizer.transform(rowData)
    //
    //    val countVectorizer = new CountVectorizer()
    //      .setInputCol("words").setOutputCol("rawFeatures")
    //    val featurizedDataModel = countVectorizer.fit(wordsData)
    //
    //    val featurizedData = featurizedDataModel.transform(wordsData)
    //
    //    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //    val idfModel = idf.fit(featurizedData)
    //
    //    val rescaledData = idfModel.transform(featurizedData)
    //    rescaledData.select("id", "features").show(false)
    //
    //    val schema = StructType(Seq(StructField("id", StringType),
    //      StructField("index", IntegerType),
    //      StructField("value", DoubleType)))
    //
    //    val data = rescaledData.rdd.map(row => {
    //      val id = row.getAs[String]("id")
    //      val buffer = ListBuffer[(Int, List[(String, Double)])]()
    //      row.getAs[SparseVector]("features")
    //        .foreachActive((index, value) => {
    //          buffer += Tuple2(index, List(Tuple2(id, value)))
    //        })
    //      buffer.toList
    //    })
    //      .flatMap(tuple => tuple)
    //      .reduceByKey((list1, list2) => (list1 ::: list2).sortWith(_._2 > _._2).take(relevantNum))
    //      .map(tuple => Tuple2(featurizedDataModel.vocabulary(tuple._1), tuple._2))
    //      .toDF("word", "relevants")
    //    data.show(false)
    //
    //    data.write.json(pathToSave)
    //    spark.stop()
  }

  def readFile(sparkContext: SparkContext, pathToFiles: String, fileName: String): (Array[String], RDD[Array[String]]) = {
    val rowData = sparkContext.textFile(if (pathToFiles.endsWith("/")) pathToFiles + fileName
    else pathToFiles + "/" + fileName)
      .map(line => line.split(",", -1).map(_.trim))
    val header = rowData.first

    (header, rowData.filter(_ (0) != header(0)))
  }
}