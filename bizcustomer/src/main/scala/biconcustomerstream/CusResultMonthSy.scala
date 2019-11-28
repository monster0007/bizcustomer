package biconcustomerstream

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object CusResultMonthSy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    val month=getYesterdayMonth()
    val mon=month.replace("-","")
      val customreDf=getResult(spark,month)
      val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/sys_cus_sy_"+mon
      customreDf.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv(path)

  }

  def getResult(spark: SparkSession,month:String): DataFrame = {
    val mon=month.replace("-","")
    spark.sql(
      """
        |msck repair table sys_customer.dm_custom_month_sy
      """.stripMargin)
    spark.sql(
      """
        |create table if not exists sys_customer.sys_cus_sy_"""
        .stripMargin+mon+"""
        |(customerid bigint,
        |createtime date,
        |leasttime date,
        |addflag int,
        |norflag int,
        |delflag int,
        |cusattr int,
        |selfflag int,
        |accountid bigint,
        |consumlevel int,
        |customname string,
        |customtype string,
        |address string,
        |contactname string,
        |phonenum string,
        |provincename string,
        |cityname string
        |)row format delimited fields terminated by ','
      """.stripMargin
    )
    val data=spark.sql(
      """
        |select m.customerid,m.createtime,m.leasttime,m.addflag,m.norflag,m.delflag,m.cusattr,m.selfflag,m.accountid,
        |case when m.delflag=1 then 0
        |else c.consumelevel
        |end as consumlevel,
        |ifnull(i.customname,"其他"),ifnull(i.customtype,"其他"),ifnull(i.address,"其他"),ifnull(i.contactname,"其他"),ifnull(i.phone,"其他") phonenum,
        |ifnull(i.provincename,"其他"),ifnull(i.cityname,"其他")
        |from sys_customer.dm_custom_month_sy m
        |join sys_customer.cus_consume_sy c on m.customerid=c.customerid and m.selfflag=c.is_own and m.accountid=c.accountid
        |left join sys_customer.customer_info i on m.customerid=i.id
        |where c.month=""".stripMargin+"\""+month+"\""+ " and m.month="+"\""+month+"\"")
    data
      }

  /*
   获取昨天所属的月份
    */
  def getYesterdayMonth():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  }
