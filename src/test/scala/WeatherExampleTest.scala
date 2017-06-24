import org.apache.spark.{SparkConf, SparkContext}

object WeatherExampleTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WeatherExample").setMaster("local")
    val sc = new SparkContext(conf)


    WeatherDS.makeDS("src/test/resources")

  }
}