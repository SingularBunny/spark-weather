import org.apache.spark.{SparkConf, SparkContext}

object WeatherExampleTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")

    WeatherDS.makeDS(conf, "src/test/resources")

  }
}