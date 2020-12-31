import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, HIVE_TYPE_STRING, IntegerType}
import sparkconnection.SparkConfiguration
import org.apache.spark.rdd.RDD
import rddtypes.Measures

object Main extends App {
 // System.setProperty("HADOOP_USER_NAME","hdpuser")
 val sparkConf = new SparkConfiguration(args(0),args(1))
 val sc = sparkConf.sparkconfig._1
 val spark = sparkConf.sparkconfig._2
 // print(sc.sparkUser)

 //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 //import sqlContext.implicits

//  val rddFromFile = sc.textFile("hdfs://hadoopserver:9000/hdpuser/inputdata/inputdata_timeseries.csv")

//sql operations on spark
/* import spark.implicits._
 val rddMetrics = spark.read.option("header","true")
   .csv("hdfs://hadoopserver:9000/hdpuser/inputdata/inputdata_timeseries.csv")
   .withColumn("typ", 'TYPE)
   .withColumn("station", 'STATION)
   .withColumn("calendaryear", 'CALENDARYEAR.cast(IntegerType))
   .withColumn("day", 'DAY)
   .withColumn("allocation", 'ALLOCATION.cast(DoubleType))
   .withColumn("capacity", 'CAPACITY.cast(DoubleType))
   .as[Measures]*/

    val metrics = sc.textFile("hdfs://hadoopserver:9000/hdpuser/inputdata/inputdata_timeseries.csv")
      .map(row => row.split(",")) // each line will be converted into an Array
      .filter(row => !(row(1).equals("\"STATION\""))) // remove Header
      .map (row => Measures(row(0).toString,row(1).toString,row(2).toInt,row(3).toString,row(4).toFloat/100, row(5).toFloat/100))

    val avgAllocationByCalendaryear = metrics
     .map(measure => (measure.typ,measure.allocation.toDouble))
     .reduceByKey((v1,v2) => (v1+v2)/2)
     .collect()

    avgAllocationByCalendaryear.foreach(println)

 sc.stop()

}
