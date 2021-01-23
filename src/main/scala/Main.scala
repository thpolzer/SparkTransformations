import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager

import sparkconnection.SparkConfiguration
import recordtypes.Measures

object Main extends App {

  val sparkcontext = new SparkConfiguration(args(0),args(1))
  val sc = sparkcontext.sparkconfig._1
  val sqlcontext = sparkcontext.sparkconfig._2
  val rddFromFile = sc.textFile("hdfs://hadoopserver:9000/hdpuser/inputdata/inputdata_timeseries.csv").filter(row => !(row.split(",")(0).equals("\"TYPE\"")))
  val schemaInferredRDD = rddFromFile
      .map(_.split(","))
      .map (m => Measures(
        m(0).toString.replace("\"",""),
        m(1).toString,
        m(2).toInt,
        m(3).toString,
        m(4).toFloat,
        m(5).toString.replace("\"","").toFloat))

  // calculate average allocation per year per station
  val aggregateRDD = schemaInferredRDD
    .map (mydata => ((mydata.station,mydata.calyear),mydata.allocation))
    .reduceByKey((x,y) => (x+y)/2)
    .sortByKey()
 //   .saveAsTextFile("hdfs://hadoopserver:9000/hdpuser/output/aggregatedAllocations.txt")

  import sqlcontext.implicits._
  val jdbcDF = aggregateRDD.toDF()

  val dataframe_mysql = sqlcontext
    .read.format("jdbc")
    .option("url", "jdbc:mysql://hadoopserver:3306/testuser?characterEncoding=utf8&useSSL=false&useUnicode=true")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "mytable")
    .option("user", "hiveuser")
    .option("password", "09.Rudi.Voeller").load()
  dataframe_mysql.show()

  val a = 1

}


