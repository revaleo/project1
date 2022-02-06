import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

import scala.Console.{BLACK, CYAN_B, GREEN, RESET, YELLOW, YELLOW_B, println}
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.io.StdIn.readLine


object project1 extends App {
    System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()
  //branch_x tables column: "product", "branch"
  //cons_x tables column: "product", "consumer"
  //column name is case sensitive in spark.sql statement!

   val df1 = spark.sql("select * from branch_a where branch='Branch1'" )
  //branchA+branch1
   val df2 = spark.sql("select product,sum(consumer) as sumcon from consa group by product")
  //consa sum by product
   val df3 = df1.join(df2,df1("product")=== df2("product"),"inner")
  //crossed a for branch1 (by product)
   val total1 = df3.agg(functions.sum("sumcon")).first.get(0)
  //scen1 Q1

  val df4 = spark.sql("select * from branch_a where branch='Branch2'" )
  //branchA+branch2
  val df5 = spark.sql("select * from branch_c where branch='Branch2'" )
  //branchC+branch2
  val df6 = spark.sql("select product,sum(consumer) as sumcon from consc group by product")
  //consc sum by product
  val df7 = df4.join(df2,df4("product")=== df2("product"),"inner")
  //crossed a for branch2
  val df8 = df5.join(df6,df5("product")=== df6("product"),"inner")
  //crossed c for branch2
  val df9 = df7.union(df8)
  //combined result from a and c for branch2
  val total2 = df9.agg(functions.sum("sumcon")).first.get(0)
  //scen1 Q2
def scenario1:Unit={
  println("The total customers for branch1 is:"+total1)
  println("The total customers for branch2 is:"+total2)
}

  //scen2 Q1,Q2,Q3
  val mostbev= df3.orderBy(desc("sumcon")).first.get(0)

  val leastbev= df9.orderBy(asc("sumcon")).first.get(0)
  val avgbev= df9.orderBy(asc("sumcon")).take(df9.count().toInt/2+1).last.get(0)

  def scenario2:Unit={
  println("The most consumed beverage on Branch1 is:" + mostbev)
  println("The least consumed beverage on Branch2 is:" + leastbev)
  println("The average consumed beverage on Branch2 is:" + avgbev)
}
  //scen3
  //there is  no branch10, and branch1 only in branch_a, branch8 only in branch_B, nothing in branch_c)
  //likewise, there is no branch4 or 7 from Branch_A, 7 in B, 4 and 7 in C
  val df10 = spark.sql("Select * from branch_b where branch ='Branch8'")
  //product available in branch8
  val df11 = df1.join(df10,df1("product")=== df10("product"),"inner")
  //products available both in branch 1 and 8, (there's no branch10)
  val df12 = spark.sql("Select product from branch_b where branch='Branch7' group by product")
  //7 in B
  val df13 = spark.sql("select product from branch_c where branch='Branch7' group by product")
  //7 in C
  val df14 = spark.sql("select product from branch_c where branch='Branch4' group by product")
  //4 in C
  val df15 = df12.join(df13,df12("product")=== df13("product"),"inner").join(df14,df12("product")=== df14("product"),"inner")
  //join all 3


  def scenario3:Unit={
    println("The products available both in branch 1 and 8 (no branch10) are:\n" + df11.select("branch_a.product").collectAsList.mkString)
    println("The common beverages available in Branch4 and Branch7 are:\n" + df15.select("branch_b.product").collectAsList.mkString)
  }
  //Scen4
  def scenario4:Unit={
    //df11.select("branch_b.branch","branch_b.product").write.partitionBy("product").saveAsTable("product_part")
    //partitioned table already created for df11, only showing the partitioned table
    spark.sql("select * from product_part").show
    //creating view for df15 and showing
    df15.createOrReplaceTempView("Aview")
    spark.sql("select * from aview").show
  }
  def scenario5:Unit={
    spark.sql("Alter table product_part set tblproperties('Comment'='this is a comment', 'note'='This is a note')")
    spark.sql("Show tblproperties product_part").show(10)
    val del=spark.sql("select * from product_part")
    del.show()
   // del.write.saveAsTable("deleting")
   // spark.sql("delete from deleting where product='Special_Espresso'").show
    // "DELETE is only supported with v2 tables." issue

  }
  def scenario6a:Unit= {
    val df16 = spark.sql("Select * from consa union select * from consb union select * from consc")
  // df16 is the combined df from consumers count a,b,c
    val df17 = df16.groupBy("product").sum().sort(sum("consumer").desc)
  // df17 is sum for each product in descending order
    val df18 = df17.withColumn("brew",split(col("product"),"_").getItem(0)).withColumn("flavor",split(col("product"),"_").getItem(1)).drop("product")
    df18.show
  //df18 split product into brew and flavor
    val df21=df18.groupBy("brew").sum().sort(desc("sum(sum(consumer))"))
    df21.show
    val df22=df18.groupBy("flavor").sum().sort(desc("sum(sum(consumer))"))
    df22.show
    println(s"${GREEN}Conclusion: The 'cappuccino' is the best selling flavor with huge statistic significance, while the 'Large' is the best selling brew without statistic significance.${RESET}")
    df21.write.option("header","true").csv("output/scenario6a/brew/")
    df22.write.option("header","true").csv("output/scenario6a/flavor/")

  }
  def scenario6b:Unit={
    val df19 = spark.sql("Select * from branch_a union select * from branch_b union select * from branch_c")
    //df19 is the combined df from branch a,b,c
    val df20 = df19.groupBy("branch").count().sort(desc("count"))
    df20.show
    df19.groupBy("product").count().sort(desc("count")).show
    println(s"${YELLOW}Disclaimer: Due to the ambiguity of the conscount data, total consumers for each branch cannot be calculated because the same beverage can come from more than one branch, and the proportion of that beverage consumption from a certain branch is unknown.${RESET}" )
    println(s"${GREEN}Conclusion: The branch7 sells the most types of beverages and branch1 sells the least types of beverages.${RESET}")
    df20.write.option("header","true").csv("output/scenario6b")

  }

  //spark.sql("create database project1")
  //spark.sql("CREATE TABLE ConsC (product STRING, consumer INT) row format delimited fields terminated by ','")
  //spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/input/Bev_ConscountC.txt' INTO TABLE ConsC")
  //spark.sql("show tables").show()

    //Beginning of the program
  println(f"${CYAN_B}${BLACK} Welcome to the Coffee shop query database, please make queries....${RESET}\n")


  var loop1=1
    while(loop1 == 1) {
      println(s"\nPlease select scenario below:")
      println("Press 1 for Problem Scenario 1\nWhat is the total number of consumers for Branch1?.")
      println("What is the number of consumers for the Branch2?")
      println("Press 2 for Problem Scenario 2\nWhat is the most consumed beverage on Branch1")
      println("What is the least consumed beverage on Branch2")
      println("What is the Average consumed beverage of Branch2")
      println("Press 3 for Problem Scenario 3\nWhat are the beverages available on Branch10, Branch8, and Branch1?")
      println("What are the comman beverages available in Branch4,Branch7?")
      println("Press 4 for Problem Scenario 4\nCreate a partition,View for the scenario3.")
      println("Press 5 for Problem Scenario 5")
      println("""Alter the table properties to add "note","comment"""")
      println("Remove a row from the any Scenario.")
      println("Press 6 for Problem Scenario 6a\nAdd future query")
      println("Press 7 for Problem Scenario 6b\nAdd future query")

      println("Press 8 to end")
      var i = readLine().toInt
      i match {
        case 1 =>scenario1
          Thread.sleep(5000)
        case 2 =>scenario2
          Thread.sleep(5000)
        case 3 =>scenario3
          Thread.sleep(5000)
        case 4 =>scenario4
          Thread.sleep(5000)
        case 5 =>scenario5
          Thread.sleep(5000)
        case 6 =>scenario6a
          Thread.sleep(5000)
        case 7 =>scenario6b
          Thread.sleep(5000)

        case 8 => loop1 = 0
          println(f"${YELLOW_B}${BLACK}Thanks for using this program, good bye!${RESET}")

      }
    }
 }