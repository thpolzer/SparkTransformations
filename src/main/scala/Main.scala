import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main extends App {

  private val sparkConf = new SparkConf().setAppName("Text extraction").setMaster("local")
  private val sc = new SparkContext(sparkConf)
  private val sparksql = SparkSession.builder().appName("App1").getOrCreate()

  def sparkconfig = Tuple3(sparkConf,sc,sparksql)

  val rddFromFile = sc.textFile("hdfs://hadoopserver:9000/hdpuser/inputdata/inputdata_timeseries.csv").collect()

  val rdd = sc.parallelize(List(1,2,3,4,5,6)).cache()
  val t1= rdd.map(x => x*2)
  val result = t1.collect()
  val a = 1


}
