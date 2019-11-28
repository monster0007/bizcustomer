package kpi_product
import until.DateUntil.{getDate,getYesterdayMonth}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Goods_Fact {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("product_fact").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("error")
    val month=getYesterdayMonth()//昨天所属的月份
    val res=spark.sql(
      """
        |select id,sdate,goodsid,customerid,accountid,productnumber,amount,pardate,
        |case when amount/productnumber<=0.1 then 1
        |else 0
        |end as is_gift
        |from sys_customer.dw_sale_fact
        |where pardate=
      """.stripMargin+month)
    res.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv("hdfs://nameservice1/user/hive/warehouse/bus_kpi.db/product_sale_fact/pardate="+month)
  }
}
