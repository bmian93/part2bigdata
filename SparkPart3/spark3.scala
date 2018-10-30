import org.apache.spark._
import org.apache.spark.sql.Row

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.log4j._



object spark3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:/Users/Bilal/Downloads/" +
        "ConsumerComplaints.csv")


    //df.show()


    //P1Q4Apply Union operation on the dataset and order the output by Company Name alphabetically.
    val df1 = df.limit(5)
    val df2 = df.limit(10)
    val unionDf = df1.union(df2)

    unionDf.orderBy("Company").show()


    df.createOrReplaceTempView("consumer")

    //P1Q5Use Groupby Query based on Zip Codes.

    val zipgroup = spark.sql("select count(Company) from consumer GROUP BY ZipCode ")

    zipgroup.show()


    //Aggregate Max and Average
    val MaxDF = spark.sql("select Max(ComplaintID) from consumer")
    MaxDF.show()

    val AvgDF = spark.sql("select Avg(ComplaintID) from consumer")

    AvgDF.show()

    //P2Q1 Join the dataframe using sql

    val df3 = df.limit(50)
    val df4 = df.limit(80)

    df3.createOrReplaceTempView("left")
    df4.createOrReplaceTempView("right")


    val joinSQl = spark.sql("select left.ProductName,right.Company FROM left,right where left.ComplaintID = " +
      "right.ComplaintID")
    joinSQl.show()

    //P2Q2 13th Row from DataFrame
    val df13th = df.take(13).last

    print(df13th)


    //P1Q3 Count the number of repeated record in data
    val DupDF = spark.sql("select count(*), company, ZipCode from consumer group by ZipCode, company having count(*)>1")

    DupDF.show()





  }
}