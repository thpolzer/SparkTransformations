import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import sparkconnection.SparkConfiguration

object Main extends App {
 // System.setProperty("HADOOP_USER_NAME","hdpuser")
  val sc = new SparkConfiguration(args(0),args(1)).sparkconfig._1
 // print(sc.sparkUser)

  val rddFromFile = sc.textFile("hdfs://hadoopserver:9000/hdpuser/inputdata/inputdata_timeseries.csv")

 // val rddFromFile1 = rddFromFile.collect()

  val rdd = sc.parallelize(List(1,2,3,4,5,6)).cache()
  val t1= rdd.map(x => x*2)
  val result = t1.collect()
  val a = 1
 println ("Karl")

}
