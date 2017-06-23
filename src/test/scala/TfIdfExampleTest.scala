import org.apache.spark.{SparkConf, SparkContext}

object TfIdfExampleTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    WeatherDS.makeDS("src/test/resources")

  }
}