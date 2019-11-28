package biconcustomer

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerResult {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    val num=args(0).toString
    val date=num.replace("-","")
    val customreDf=getResult(spark,num)
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/sys_customer_"+date
    customreDf.repartition(1).write.mode("overwrite").csv(path)
    createResView(spark,date)
  }

  def getResult(spark: SparkSession,num:String): DataFrame = {
//      val data=spark.sql(
//        """
//          |select dc.customerid,dc.createtime,dc.addflag,dc.norflag,dc.delflag,dc.currenttime dealtime,cc.consumelevel consumlevel,
//          |ci.customname,ci.customtype,ci.address,ci.contactname,ci.phone phonenum,ci.provincename,ci.cityname
//          |from sys_customer.dm_custom dc
//          |join sys_customer.cus_consume cc on dc.customerid=cc.customerid
//          |join sys_customer.customer_info ci on dc.customerid=ci.id
//          |where dc.currenttime="2019-04-01"
//          |and cc.month="2019-04"
//        """.stripMargin)
    val date=num.replace("-","")
    spark.sql(
      """
        |create table if not exists sys_customer.sys_customer_"""
        .stripMargin+date+"""
        |(customerid bigint,
        |createtime date,
        |addflag int,
        |norflag int,
        |delflag int,
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
        |select dc.customerid,dc.createtime,dc.addflag,dc.norflag,dc.delflag,cc.consumelevel consumlevel,
        |ifnull(ci.customname,"其他"),ifnull(ci.customtype,"其他"),ifnull(ci.address,"其他"),ifnull(ci.contactname,"其他"),ifnull(ci.phone,"其他") phonenum,ifnull(ci.provincename,"其他"),ifnull(ci.cityname,"其他")
        |from sys_customer.dm_custom dc
        |left join sys_customer.cus_consume cc on dc.customerid=cc.customerid
        |left join sys_customer.customer_info ci on dc.customerid=ci.id
        |where cc.month="2019-05"
        |and dc.currenttime=
      """.stripMargin+"\""+num+"\"")
    data
      }
  def createResView(spark:SparkSession,date:String): Unit ={
    val viewName="sys_fact_cus_"+date
    val tableName="sys_customer_"+date
    spark.sql(
      """
        |create view sys_customer.""".stripMargin+viewName+
        """
        |as select
        |s1.id,
        |s1.sdate,
        |s1.goodsid,
        |s1.customerid,
        |s1.accountid,
        |s1.productnumber,
        |s1.amount,
        |s2.createtime,
        |s2.addflag ,
        |s2.norflag ,
        |s2.delflag ,
        |s2.consumlevel ,
        |s2.customname ,
        |s2.customtype ,
        |s2.address ,
        |s2.contactname ,
        |s2.phonenum ,
        |s2.provincename ,
        |s2.cityname,
        |s3.business_name,
        |s4.goodsname,
        |s4.vendorname,
        |s4.spec,
        |s4.approvedno,
        |s4.classname
        |from sys_customer.sys_customer_salefact s1
        |join  sys_customer.sys_customer_bus s3 on s1.accountid=s3.accountid
        |join sys_customer.SYS_CUSTOMER_GOODS s4 on s1.goodsid=s4.goodsid
        |join sys_customer.
      """.stripMargin+tableName+" s2 on s1.customerid=s2.customerid")
  }
  }
