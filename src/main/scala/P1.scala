import org.apache.spark.sql.SparkSession
import scala.io.StdIn.{readByte, readChar, readLine}
import Utilities._
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

  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    //<editor-fold desc="DROP commands">
    //spark.sql("DROP TABLE IF EXISTS branchbevs")
    //spark.sql("DROP TABLE IF EXISTS cons_a")
    //spark.sql("DROP TABLE IF EXISTS cons_b")
    //spark.sql("DROP TABLE IF EXISTS cons_c")
    //spark.sql("DROP TABLE IF EXISTS cons_aXb")
    //</editor-fold>

    //<editor-fold desc="Creates tables">
    spark.sql("CREATE TABLE IF NOT EXISTS branch_a (bev STRING, branch STRING)" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS branch_b (bev STRING, branch STRING)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS branch_c (bev STRING, branch STRING)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS cons_b (bev STRING, count INT)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    spark.sql("CREATE TABLE IF NOT EXISTS cons_c (bev STRING, count INT)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    //</editor-fold>
    //<editor-fold desc="Loads into tables">
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE branch_a")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branch_b")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branch_c")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE cons_b")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE cons_c")
    //</editor-fold
    //<editor-fold desc="Create table from query and MISC">
//    Intersection of cons_a and cons_b \\
    spark.sql("CREATE TABLE IF NOT EXISTS cons_aXb AS SELECT * FROM cons_a INTERSECT SELECT * FROM cons_b")
    spark.sql("CREATE TABLE IF NOT EXISTS all_branch AS SELECT * FROM branch_a UNION SELECT * FROM branch_b UNION SELECT * FROM branch_c")
    spark.sql("CREATE TABLE IF NOT EXISTS cons_abc AS SELECT * FROM cons_a UNION SELECT * FROM cons_b UNION SELECT * FROM cons_c")
    spark.sql("CREATE TABLE IF NOT EXISTS b1bevs AS SELECT bev FROM all_branch WHERE branch = 'Branch1'")
    spark.sql("CREATE TABLE IF NOT EXISTS b2bevs AS SELECT bev FROM all_branch WHERE branch = 'Branch2'")

//    bevs common between BranchA and ConscountA \\
//      spark.sql("SELECT branch_a.branch, cons_a.bev, cons_a.count FROM branch_a " +
//        "INNER JOIN cons_a ON cons_a.bev = branch_a.bev ORDER BY branch_a.branch, cons_a.bev, cons_a.count").show()
      //</editor-fold>
    println("Welcome to DataStuff, where we have some queries for you!")
    val menu = new MyMenu(op1)
    menu.printMenu()
    print("Option: ")
    val in = chooseN(6)
    val option = menu.selectOption(in)

    option match {
      case "Scenario 1" => // Total consumers for branch 1
        spark.sql("SELECT SUM(count) AS ConsBranch1 FROM b1bevs INNER JOIN cons_abc AS c ON c.bev = b1bevs.bev").show()
        spark.sql("SELECT SUM(count) AS ConsBranch2 FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev").show()
      case "Scenario 2" =>
        println("Most consumed beverage on branch 1")
        spark.sql("SELECT b1bevs.bev, count FROM b1bevs INNER JOIN cons_abc AS c ON c.bev = b1bevs.bev ORDER BY count DESC LIMIT 1").show()
        println("Least consumed beverage on branch 2")
        spark.sql("SELECT b2bevs.bev, count FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev ORDER BY count LIMIT 1").show()
        println("Average consumed beverage of Branch 2")
        spark.sql("SELECT AVG(count) FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev").show()
      case "Scenario 3" => // Available beverages on branch 10
        spark.sql("SELECT * FROM b10bevs").show()
      case "Scenario 4" => // Create a "partiion,View" for scenario 3
          spark.sql("CREATE TABLE IF NOT EXISTS Partitioned(bev STRING) COMMENT 'A PARTITIONED BRANCH TABLE' PARTITIONED BY (branches STRING)")
          spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")// TODO USE THIS FOR A MORE COMPACT DELETE
          spark.sql("INSERT OVERWRITE TABLE Partitioned PARTITION(branches) SELECT bev,branch from all_branch")
          spark.sql("SELECT * FROM Partitioned WHERE branches = 'Branch9'").show
      case "Scenario 5" => // Add note to a table
        val note = readLine("In this scenario you get to add a note to a table!\nPlease enter your note here: ")
        spark.sql(s"ALTER TABLE branch_a SET TBLPROPERTIES('notes' = '$note')")
        spark.sql("SHOW TBLPROPERTIES branch_a").show()
        // Delete a row from a table
        var t = readLine("Now you get to delete some stuff.\nWhich beverage do you want to delete from BranchA?: ")
        while (spark.sql(s"SELECT * FROM branch_a WHERE bev = '$t'").count() == 0) {
          t = readLine("Sorry, but that beverage isn't in the data, try again: ")
        }
        spark.sql(s"CREATE TABLE IF NOT EXISTS row AS SELECT * FROM branch_a WHERE bev = '$t' LIMIT 1")
        spark.sql("CREATE TABLE IF NOT EXISTS branch_a_del AS " +
          "SELECT branch_a.bev, branch_a.branch FROM branch_a " +
          "LEFT JOIN row " +
          "ON branch_a.bev = row.bev " +
          "WHERE row.bev IS NULL")
        spark.sql("INSERT OVERWRITE TABLE branch_a select * from branch_a_del")
        spark.sql("DROP TABLE branch_a_del")
        spark.sql("SELECT * FROM branch_a").show(9999)
//        //create copy table
//        spark1.sql("CREATE TABLE newone2_copy LIKE newone2")
//        //load data into copy table except deleted item
//        spark1.sql("INSERT INTO newone2_copy SELECT * FROM newone2 WHERE id NOT IN (SELECT id FROM newone2 WHERE id=23)")
//        //overwrite copy table to original table
//        spark1.sql("INSERT OVERWRITE TABLE newone2 SELECT * FROM newone2_copy")
//        //drop copy table
//        spark1.sql("DROP TABLE newone2_copy")
      case "Scenario 6" =>// TODO currently just a place to dump test queries. Should be My Query: Variety and Diversity
        spark.sql("SELECT * FROM all_branch").show
    }
    spark.close()
  }

  def totCons(branch: Byte): Unit ={ // Total consumers for Branch1

  }

  def tot(): Unit ={

  }
}