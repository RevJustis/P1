import org.apache.spark.sql.SparkSession

object HiveTest2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HiveTest2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    spark.sql("create table newone1(id Int,name String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone1")
    spark.sql("SELECT * FROM newone1").show()
  }
}