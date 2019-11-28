package biconcustomer

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerResultCopy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    val num=args(0).toString
    val customreDf=getResult(spark,num)
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/sys_customer_customer/"
    customreDf.repartition(1).write.mode("overwrite").csv(path)
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
    spark.sql(
      """
        |drop table sys_customer_customer;
      """.stripMargin)
    spark.sql(
      """
        |create table
      """.stripMargin)
    val data=spark.sql(
      """
        |select dc.customerid,dc.createtime,dc.addflag,dc.norflag,dc.delflag,dc.currenttime dealtime,cc.consumelevel consumlevel,
        |ci.customname,ci.customtype,ci.address,ci.contactname,ci.phone phonenum,ci.provincename,ci.cityname
        |from sys_customer.dm_custom dc
        |join sys_customer.cus_consume cc on dc.customerid=cc.customerid
        |join sys_customer.customer_info ci on dc.customerid=ci.id
        |where cc.month="2019-04"
        |and dc.currenttime=
      """.stripMargin+"\""+num+"\"")
    data
      }
  }
