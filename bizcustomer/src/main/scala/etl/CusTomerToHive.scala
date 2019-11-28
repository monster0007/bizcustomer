package etl
import until.MysqlUntil.getFromMysql
import org.apache.spark.sql.SparkSession

/*
把客户表从Mysql到Hive
 */
object CusTomerToHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("local[1]").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    val url="jdbc:mysql://10.1.24.209:3306/"
    //val customer=getFromMysql(spark,url,)
  }
}
