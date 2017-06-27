name := "spark-weather"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-mllib" % "2.1.1",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3" % "test",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3" % "test"
)

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

ideaExcludeFolders += ".idea"
ideaExcludeFolders += ".idea_modules"