import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scalax.file.Path
import scala.util.Try

object WeatherExampleSpec extends FlatSpec with BeforeAndAfter{

  val PathToFiles = "src/test/resources"
  val PathToSave = "weather"

  before {
    val path = Path (PathToSave)
    Try(path.deleteRecursively(continueOnFailure = false))
  }

  "The app" should "read SCV files, create DatFrame and store it in FS or HDFS" in {
    val path = Path (PathToSave)
    val conf = new SparkConf().setAppName("WeatherExample").setMaster("local")
    val sc = new SparkContext(conf)

    Weather.make(PathToFiles, PathToSave)

    assert(path.children().isEmpty)
  }
}