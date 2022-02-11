import org.apache.spark.sql.SparkSession

import scala.io.StdIn.{readByte, readChar, readLine}
import Utilities._
import org.apache.spark.sql.functions.col
import java.io.{File, PrintWriter}
import scala.Console.println

object P1 {
// some code that was for making dataframes, not working at the moment, stick to tables!!!
//  case class BevBr(bev: String, branch: String)
//  case class BevCons(bev: String, count: String)
//  val a = sc.textFile("/input/Bev_BranchA.txt")
//  val b = a.map(_.split(","))
//  val df = b.map(attributes => BevBr(attributes(0), attributes(1).trim)).toDF()

  val op1 = List[String](
    "Scenario 1",
    "Scenario 2",
    "Scenario 3",
    "Scenario 4",
    "Scenario 5",
    "Scenario 6",
    "End Program"
  )

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    junk(spark)
    println("Welcome to DataStuff, where we have some queries for you!")
    var in: Byte = 0
    var option: String = ""
    val menu = new MyMenu(op1)
    var continue = true

    while (continue) {
      menu.printMenu()
      print("Option: ")
      in = chooseN(7)
      option = menu.selectOption(in)

      option match {
        case "Scenario 1" =>
          println("Total for Branch 1")
          val df = spark.sql("SELECT * FROM constot1")
          df.show()
          println("Total for Branch 2")
          spark.sql(s"SELECT 2 AS branch, SUM(count) AS ConsBranch2 FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev").show()
          readLine("Press Enter to view menu")
        case "Scenario 2" =>
          println("Most consumed beverage on branch 1")
          spark.sql("SELECT b1bevs.bev, count FROM b1bevs INNER JOIN cons_abc AS c ON c.bev = b1bevs.bev " +
                      "ORDER BY count DESC LIMIT 1").show()
          println("Least consumed beverage on branch 2")
          spark.sql("SELECT b2bevs.bev, count FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev " +
                      "ORDER BY count LIMIT 1").show()
          println("Average beverage consumption of Branch 2")
          spark.sql("SELECT AVG(count) FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev").show()
          readLine("Press Enter to view menu")
        case "Scenario 3" =>
          println("Available beverages on branch 9")
          spark.sql("SELECT * FROM all_branch WHERE branch = 'Branch9'").show()
          println("Beverages in both Branch 4 and Branch 7")
          spark.sql("SELECT bev FROM all_branch WHERE branch = 'Branch4' INTERSECT SELECT bev FROM all_branch WHERE branch = 'Branch7'").show()
          readLine("Press Enter to view menu")
        case "Scenario 4" => // Create a "partiion,View" for scenario 3
          println("Create a partition for Scenario 3")
          spark.sql("SELECT * FROM Partitioned_abc WHERE branches = 'Branch9'").show
          spark.sql("describe formatted Partitioned_abc").show()
          readLine("Press Enter to view menu")
          /* //some alternate partition code testing
          val df = spark.sql("SELECT bev, branch FROM all_branch")
          df.repartition(9, col("branch")).where("branch == 'Branch5'").show()
           */
        case "Scenario 5" => // Add note to a table
          val note = readLine("In this scenario you get to add a note to the branch_a table!\nPlease enter your note here: ")
          spark.sql(s"ALTER TABLE branch_a SET TBLPROPERTIES('notes' = '$note', 'comment' = '$note')")
          spark.sql("SHOW TBLPROPERTIES branch_a").show()
          // Delete a row from a table
          var t = readLine("Now you get to delete some stuff.\nWhich beverage do you want to delete from BranchA?: ")
          while (spark.sql(s"SELECT * FROM branch_a WHERE bev = '$t'").count() == 0) {
            t = readLine("Sorry, but that beverage isn't in the data, try again: ")
          }
          spark.sql(s"CREATE TABLE row AS SELECT * FROM branch_a WHERE bev = '$t' LIMIT 1")
          spark.sql("CREATE TABLE branch_a_del AS " +
            "SELECT branch_a.bev, branch_a.branch FROM branch_a " +
            "LEFT JOIN row " +
            "ON branch_a.bev = row.bev " +
            "WHERE row.bev IS NULL")
          spark.sql("INSERT OVERWRITE TABLE branch_a select * from branch_a_del")
          spark.sql("DROP TABLE branch_a_del")
          spark.sql("DROP TABLE row")
          spark.sql("SELECT * FROM branch_a").show(999)
          readLine("Press Enter to view menu")
        case "Scenario 6" =>
          println("My future query:\n")
          println("The Diversity Rank gives an indication of how diverse a branches orders are. For reverence, perfect\n" +
            "diversity (every customer has a unique order) among 10 total drinks types sold will cause Diversity Rank\n" +
            "= 10, perfect diversity among 25 total drink types sold will cause Diversity Rank 25. At half diversity\n" +
            "(10 different drinks ordered by 20 different people for example, then the rank will be 5. As the number\n" +
            "of customers grows while number of drinks ordered stays the same or shrinks, the rank will get closer and\n" +
            "closer to 0 (but never reach 0). But, if every single item on the menu was ordered, diversity no longer\n" +
            "has significant weight and to indicate this the rank will be made zero. This means you both know how many\n" +
            "items on the menu were ordered, how diverse your customers orders are, and whether or not this metric\n" +
            "contextually valuable. When was the last time business data told you when it is and isn't useful?")
          val df = spark.sql("SELECT constotall.branch AS Branch, ((bevTot / consTot) * (bevTot % 54))  AS Diversity_Rank " +
                               "FROM constotall " +
                               "INNER JOIN bevTotAll ON constotall.branch = bevTotAll.branch")
          df.show
          df.coalesce(1).write.format("csv").option("header",true).mode("overwrite").save("hdfs://localhost:9000/user/justis/future.csv")
          readLine("Press Enter to view menu")
        case "End Program" => continue = false
      }
    }
    spark.close
    end
  }
}
