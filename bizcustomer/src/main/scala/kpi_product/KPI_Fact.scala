package kpi_product

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object KPI_Fact {
  def getYesterday():String = {
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KPI_Fact")
    val ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val day = getYesterday()
    println(day)
    ss.sparkContext.setLogLevel("error")
    ss.sql("use sys_customer")
    val d = ss.sql("select * from dw_sale_fact where sdate = '"+ day +"'")
    d.createTempView("goods_sale")
    ss.sql("use bus_kpi")
    val sale_fact = ss.sql("select f.id,f.goodsid,g.nameid,f.customerid,f.amount,f.sdate,f.pardate,c.provincename,c.cityname " +
      "from goods_sale f " +
      "left join kpi_customer c " +
      "on f.customerid = c.customerid " +
      "left join kpi_goods g " +
      "on f.goodsid = g.product_id " +
      "where g.is_own = 1" )
    sale_fact.repartition(1).write.mode("append").option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv("hdfs://nameservice1/user/hive/warehouse/bus_kpi.db/kpi_sale_fact")
  }
}
