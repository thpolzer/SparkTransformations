package sparkconnection

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class SparkConfiguration (masternode:String, appName:String){

  private val sparkConf = new SparkConf().setAppName(appName).setMaster(masternode) //.set("spark.driver.host","192.168.56.101")
  private val sc = new SparkContext(sparkConf)
  private val sparksql = SparkSession.builder().appName(this.appName).getOrCreate()

  def sparkconfig = Tuple2(sc,sparksql)

}
