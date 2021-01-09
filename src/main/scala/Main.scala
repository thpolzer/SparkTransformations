import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import sparkconnection.SparkConfiguration
import recordtypes.Measures

object Main extends App {

  val sc = new SparkConfiguration(args(0),args(1)).sparkconfig._1
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

  val rddFromFile1 = schemaInferredRDD.collect()

  // calculate average of capacity per year pear station
  val aggregateRDD = schemaInferredRDD.map (mydata => ((mydata.station,mydata.calyear),mydata.allocation))
  val check = aggregateRDD.collect()

  val reducedRDD = aggregateRDD.reduceByKey((x,y) => (x+y)/2)
    //.map(x=>(x._1,x._2._1,x._2._2))

  val debug = reducedRDD.collect()
  val a = 3






}
