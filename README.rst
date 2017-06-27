Spark weather example
=======================

Build with command:
sbt package

Then run:

YOUR_SPARK_HOME/bin/spark-submit \
  --class "WeatherExample" \
  --master local[4] \
  target/scala-2.11/spark-weather_2.11-1.0.jar \
  path/to/files/with/weather/stats \
  path/where/parquet/should/be/stored

