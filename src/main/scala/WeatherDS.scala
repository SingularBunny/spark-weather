import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{CountVectorizer, _}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object WeatherDS {

  val GlobalTemperatures = "GlobalTemperatures.csv"
  val GlobalLandTemperaturesByCountry = "GlobalLandTemperaturesByCountry.csv"
  val GlobalLandTemperaturesByCity = "GlobalLandTemperaturesByCity.csv"

  /**
    * The argument should be path to files.
    *
    * @param args arguments.
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WeatherExample")
    makeDS(conf, args(0))
  }

  def makeDS(conf: SparkConf, pathToFiles: String) {
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val rowData = sc.textFile(if (pathToFiles.endsWith("/")) pathToFiles + GlobalTemperatures
    else pathToFiles + "/" + GlobalTemperatures)
      .map(line => line.split(",").map(_.trim))
    val header = rowData.first
    val df = rowData.filter(_(0) != header(0))
      // TODO stoped here. Should prase data to objects.
      .map(array => Tuple9(array(0), array(1).toLowerCase))
      .toDF(header: _*)

    df.show()
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
}