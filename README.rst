Spark weather example
=======================
To synchronize the logic the average data from GlobalTemperatures.csv is used because other files didn't contain min and max values.

Build with command:
sbt package

Then run:

YOUR_SPARK_HOME/bin/spark-submit \
  --class "WeatherExample" \
  --master local[4] \
  target/scala-2.11/spark-weather_2.11-1.0.jar \
  path/to/files/with/weather/stats \
  path/where/parquet/should/be/stored

