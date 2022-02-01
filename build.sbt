ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "P1"
  )
//// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3"
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.3"
//
//// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.3"
//
//libraryDependencies += "com.typesafe" % "config" % "1.3.0"
//libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"
//libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

//SMALLER SET
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"