package biconcustomerstream

import java.text.SimpleDateFormat
import java.util.Calendar

import biconcustomerstream.DateUntil.{getDate, getYesterdayMonth}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CusResultMonthHz {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    val month=getYesterdayMonth()
    val mon=month.replace("-","")
    val customreDf=getResult(spark,month)
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/sys_cus_hz_"+mon
    customreDf.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv(path)
  }

  def getResult(spark: SparkSession,month:String): DataFrame = {
    val mon=month.replace("-","")
    spark.sql(
      """
        |msck repair table sys_customer.dm_custom_month
      """.stripMargin)
    spark.sql(
      """
        |create table if not exists sys_customer.sys_cus_hz_"""
        .stripMargin+mon+"""
        |(customerid bigint,
        |createtime date,
        |leasttime date,
        |addflag int,
        |norflag int,
        |delflag int,
        |cusattr int,
        |selfflag int,
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
        |select m.customerid,m.createtime,m.leasttime,m.addflag,m.norflag,m.delflag,m.cusattr,m.selfflag,
        |case when m.delflag=1 then 0
        |else c.consumelevel
        |end as consumlevel,
        |ifnull(i.customname,"其他"),ifnull(i.customtype,"其他"),ifnull(i.address,"其他"),ifnull(i.contactname,"其他"),ifnull(i.phone,"其他") phonenum,
        |ifnull(i.provincename,"其他"),ifnull(i.cityname,"其他")
        |from sys_customer.dm_custom_month m
        |join sys_customer.cus_consume c on m.customerid=c.customerid and m.selfflag=c.is_own
        |left join sys_customer.customer_info i on m.customerid=i.id
        |where c.month=""".stripMargin+"\""+month+"\""+ " and m.month="+"\""+month+"\"")
    data
      }



  }
