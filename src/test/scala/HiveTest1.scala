import org.apache.spark.sql.SparkSession

object HiveTest1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HiveTest1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    val sampleSeq = Seq((1, "spark"), (2, "Big Data"))

    val df = spark.createDataFrame(sampleSeq).toDF("Course id", "course name")
    df.show()
    df.write.format("csv").save("sampleSeq")

  }
}