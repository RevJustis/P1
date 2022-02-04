import org.apache.spark.sql.SparkSession
import scala.io.StdIn.{readLine, readByte, readChar}

import Utilities._

object P1 {
  val op1 = List[String](
    "Scenario 1",
    "Scenario 2",
    "Scenario 3",
    "Scenario 4",
    "Scenario 5",
    "Scenario 6"
  )
  val s1 = List[String]( // Implement with type 1 if possible
    "Total Number of Consumers on Branch 1",
    "Total Number of Consumers on Branch 2"
  )
  val s2 = List[String](
    "Most Consumed Beverage on Branch 1",
    "Most Consumed Beverage on Branch 2",
    "Average Consumed Beverage on Branch 2"
  )
  val s3 = List[String](
    "What beverages are available on Branch10",
    "What beverages are available on Branch8",
    "What beverages are available on Branch1?",
    "Beverages available at Branch4 and Branch7"
  )
  val s4 = List[String](
    "Show Partitions for Scenario 3",
    "Show Views for Scenario 3"
  )
  val s5 = List[String](
    "Add a Note o a Table",
    "Remove a Row From a Table"
  )
  val s6 = List[String](
    "Rank Diversity of Orders from Branch 1",
    "Rank All Branches by Diversity"
  )

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .enableHiveSupport()
    .getOrCreate()

// A word count example
//  val lines = spark.sparkContext.parallelize(
//    Seq("Spark Intellij Idea Scala test one",
//      "Spark Intellij Idea Scala test two",
//      "Spark Intellij Idea Scala test three"))
//  val counts = lines
//    .flatMap(line => line.split(" "))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)

  def main(args: Array[String]): Unit = {
    //counts.foreach(println)

//    println("Welcome to DataStuff, where we have some queries for you!")
//    val menu = new MyMenu(op1)
//    menu.printMenu()
//    print("Option: ")
//    val in = chooseN(6)
//
//    val option = menu.selectOption(in)
//    println(option)

    //<editor-fold desc="DROP commands">
    //spark.sql("DROP TABLE IF EXISTS branchbevs")
    //</editor-fold>
    //<editor-fold desc="Creates branch_a-c tables">
    spark.sql("CREATE TABLE IF NOT EXISTS branch_a (bev STRING, branch STRING)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS branch_b (bev STRING, branch STRING)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS branch_c (bev STRING, branch STRING)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //</editor-fold>

    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE branch_a")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branch_b")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branch_c")

    spark.sql("CREATE TABLE all_branch AS " +
      "SELECT * FROM branch_a UNION SELECT * FROM branch_b UNION SELECT * FROM branch_c")

    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")

    // bevs common between BranchA and ConscountA
    spark.sql("SELECT DISTINCT cons_a.bev FROM branch_a INNER JOIN cons_a ON cons_a.bev = branch_a.bev").show()

  }

  def totCons(branch: Byte): Unit ={ // Total consumers for Branch1

  }

  def tot(): Unit ={

  }
}