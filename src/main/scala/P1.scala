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
  val s1 = List[String]( // Implement with type 1 if possible
    "Total Number of on Branch 1",
    "Total Number of on Branch 2"
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
    //<editor-fold desc="Set configuration">
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")// TODO USE THIS FOR A MORE COMPACT DELETE
    //</editor-fold>
    //<editor-fold desc="DROP commands">
    //spark.sql("DROP TABLE IF EXISTS branchbevs")
    //spark.sql("DROP TABLE IF EXISTS cons_a")
    //spark.sql("DROP TABLE IF EXISTS cons_b")
    //spark.sql("DROP TABLE IF EXISTS cons_c")
    //spark.sql("DROP TABLE IF EXISTS cons_aXb")
    spark.sql("DROP TABLE IF EXISTS constot1")
    spark.sql("DROP TABLE IF EXISTS constot2")
    spark.sql("DROP TABLE IF EXISTS constot3")
    spark.sql("DROP TABLE IF EXISTS constot4")
    spark.sql("DROP TABLE IF EXISTS constot5")
    spark.sql("DROP TABLE IF EXISTS constot6")
    spark.sql("DROP TABLE IF EXISTS constot7")
    spark.sql("DROP TABLE IF EXISTS constot8")
    spark.sql("DROP TABLE IF EXISTS constot9")
    spark.sql("DROP TABLE IF EXISTS constotall")
    spark.sql("DROP TABLE IF EXISTS cons_tot_all")
    //</editor-fold>

    //<editor-fold desc="Creates tables">
//    spark.sql("CREATE TABLE IF NOT EXISTS branch_a (bev STRING, branch STRING)" +
//        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS branch_b (bev STRING, branch STRING)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS branch_c (bev STRING, branch STRING)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_a (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_b (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_c (bev STRING, count INT)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//    spark.sql("CREATE TABLE IF NOT EXISTS Partitioned_abc(bev STRING) PARTITIONED BY (branches STRING)")

    //</editor-fold>
    //<editor-fold desc="Loads into tables">
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE branch_a")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branch_b")
//    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branch_c")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE cons_a")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE cons_b")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE cons_c")
//    spark.sql("INSERT OVERWRITE TABLE Partitioned_abc PARTITION(branches) SELECT bev,branch from all_branch")

    //</editor-fold
    //<editor-fold desc="Create table from query and MISC">
//    Intersection of cons_a and cons_b \\
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_aXb AS SELECT * FROM cons_a INTERSECT SELECT * FROM cons_b")
//    spark.sql("CREATE TABLE IF NOT EXISTS all_branch AS SELECT * FROM branch_a UNION SELECT * FROM branch_b UNION SELECT * FROM branch_c")
//    spark.sql("CREATE TABLE IF NOT EXISTS cons_abc AS SELECT * FROM cons_a UNION SELECT * FROM cons_b UNION SELECT * FROM cons_c")
//    for (x <- 1 to 9) {
//      spark.sql(s"CREATE TABLE IF NOT EXISTS b${x}bevs AS SELECT bev FROM all_branch WHERE branch = 'Branch$x'")
//    }
//    for (x <- 1 to 9) {
//      spark.sql(s"CREATE TABLE IF NOT EXISTS bevTot$x AS SELECT $x AS branch, COUNT(bev) AS bevTot FROM " +
//        s"b${x}bevs")
//    }
//    for (x <- 1 to 9) {
//      val c = spark.sql(s"SELECT $x AS branch, SUM(count) AS cons FROM " +
//      s"b${x}bevs INNER JOIN cons_abc AS c ON c.bev = b${x}bevs.bev").collect()
//      val pw = new PrintWriter(new File(s"input/dumb$x.txt" ))
//      pw.write((c(0)(0)).toString + ',' + c(0)(1) + '\n')
//      pw.close()
//    }
//    for (x <- 1 to 9) {
//      spark.sql(s"CREATE TABLE IF NOT EXISTS consTot$x (branch INT, consTot INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
//      spark.sql(s"LOAD DATA LOCAL INPATH 'input/dumb$x.txt' OVERWRITE INTO TABLE consTot$x")
//    }
//    var s = "CREATE TABLE IF NOT EXISTS bevTotAll AS SELECT * FROM bevTot1 "
//    for (x <- 2 to 9) {
//      s = s + s"UNION SELECT * FROM bevTot$x "
//    }
//    spark.sql(s)

//    var s2 = "CREATE TABLE IF NOT EXISTS constotall AS SELECT * FROM constot1 "
//    for (x <- 2 to 9) {
//      s2 = s2 + s"UNION SELECT * FROM constot$x "
//    }
//    spark.sql(s2)
    //    bevs common between BranchA and ConscountA \\
//      spark.sql("SELECT branch_a.branch, cons_a.bev, cons_a.count FROM branch_a " +
//        "INNER JOIN cons_a ON cons_a.bev = branch_a.bev ORDER BY branch_a.branch, cons_a.bev, cons_a.count").show()
      //</editor-fold>
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
          spark.sql("SELECT * FROM totCon1").show()
          println("Total for Branch 2")
          spark.sql(s"SELECT 2 AS branch, SUM(count) AS ConsBranch2 FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev").show()
        case "Scenario 2" =>
          println("Most consumed beverage on branch 1")
          spark.sql("SELECT b1bevs.bev, count FROM b1bevs INNER JOIN cons_abc AS c ON c.bev = b1bevs.bev " +
            "ORDER BY count DESC LIMIT 1").show()
          println("Least consumed beverage on branch 2")
          spark.sql("SELECT b2bevs.bev, count FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev " +
            "ORDER BY count LIMIT 1").show()
          println("Average consumed beverage of Branch 2")
          spark.sql("SELECT AVG(count) FROM b2bevs INNER JOIN cons_abc AS c ON c.bev = b2bevs.bev").show()
        case "Scenario 3" =>
          println("Available beverages on branch 10")
          spark.sql("SELECT * FROM all_branch WHERE branch = 'Branch9'").show()
        case "Scenario 4" => // Create a "partiion,View" for scenario 3
          println("Create a partition for Scenario 3")
          spark.sql("SELECT * FROM Partitioned_abc WHERE branches = 'Branch9'").show
          spark.sql("describe formatted Partitioned_abc").show()
          /* //some alternate partition code testing
          val df = spark.sql("SELECT bev, branch FROM all_branch")
          println("Below is a partition ??????????????")
          println(df.repartition(9, col("branch")).where("branch == 'Branch5'").count())
          println("Create a partition for Scenario 3 REDUX")
          println(spark.sql("SELECT * FROM Partitioned_abc").where("branches == 'Branch5'").count)
           */
        case "Scenario 5" => // Add note to a table
          val note = readLine("In this scenario you get to add a note to the branch_a table!\nPlease enter your note here: ")
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
          spark.sql("SELECT constotall.branch, ((bevTot / consTot) * (bevTot % 54)) AS Diversity_Rank  FROM constotall " +
            "INNER JOIN bevTotAll ON constotall.branch = bevTotAll.branch").show
        case "End Program" => continue = false
      }
    }
    spark.close
    end
  }
}