package repair

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerAnalyse19to18 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
      for(i<-Range(118,2,-1)){
        val today = getDate(i)
        val yestoday = getDate(i + 1)
        //      val todayData = getTodayData(spark, today)//job1
        //     todayData.show()
        //      val baseData = getCusFromHive(spark,yestoday)//job1
        //     baseData.show()
        //      val timeDf=updateCusromerWithTime(spark, baseData,todayData)//job1
        //
        //      val path1="hdfs://bd198:8020/user/hive/warehouse/sys_customer.db/dw_custom/curretntime="+today
        //      timeDf.repartition(1).write.mode("overwrite").csv(path1)
        //      timeDf.show()
        val resDf = updateCustomerWithFlag(spark,today)
        val path2 = "hdfs://bd198:8020/user/hive/warehouse/sys_customer.db/dm_custom/curretntime=" + today
        resDf.repartition(1).write.mode("overwrite").csv(path2)
      }
  }

  def getCusFromHive(spark:SparkSession,yestoday:String): DataFrame ={
    spark.sql(
      """
        |msck repair table sys_customer.dw_custom
      """.stripMargin)
    val cusData=spark.sql(
      """
        |select * from sys_customer.dw_custom where curretntime =
      """.stripMargin+"\""+yestoday+"\"")
    cusData
  }
  def getDate(day:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    var date = dateFormat.format(cal.getTime())
    date
  }
  def getTodayData(spark:SparkSession,today:String):DataFrame={
    val todayData=spark.sql(
      """
        |select customerid,sdate dealdate from sys_customer.sys_customer_salefact sf
        |join sys_customer.sys_customer_goods sg on sf.goodsid=sg.goodsid
        |where sf.sdate =
      """.stripMargin++"\""+today+"\""+

      """
        |group by customerid,sdate
      """.stripMargin
    )
    todayData
  }
  def updateCusromerWithTime(spark:SparkSession,baseData:DataFrame,todayData:DataFrame)={
    //计算当天购买了的客户
      val updateCustomer=baseData.join(todayData,baseData("customerid")===todayData("customerid")).select(baseData("customerid"),baseData("createtime"),baseData("leasttime").alias("lasttime"),todayData("dealdate").alias("leasttime"))
    //计算当天未购买的客户
    val stayCustomer=baseData.join(todayData,baseData("customerid")===todayData("customerid"),"left").filter(todayData("customerid").isNull).select(baseData("customerid"),baseData("createtime"),baseData("lasttime"),baseData("leasttime"))
    //当天新增客户
    val addCustomer=todayData.join(baseData,todayData("customerid")===baseData("customerid"),"left").filter(baseData("customerid").isNull).select(todayData("customerid"),todayData("dealdate").alias("createtime"),todayData("dealdate").alias("lasttime"),todayData("dealdate").alias("leasttime"))
    //当天的时间更新结果
    val todayCustomer=addCustomer.union(updateCustomer).union(stayCustomer)
    todayCustomer
//    todayCustomer.createOrReplaceTempView("customer")
//    spark.sql(
//      """
//        |insert into sys_customer.dw_custom partition(curretntime=
//      """.stripMargin+"\""+today+"\""+")"+
//      """
//        |select t.*,-1 from customer t
//      """.stripMargin
//    )
  }
  def updateCustomerWithFlag(spark:SparkSession,today:String):DataFrame ={
     val data=spark.sql(
       """
         |select customerid,
         |case
         |when createtime="2019-12-31" then "2018-12-31"
         |else createtime
         |end as createtime,
         |lasttime,leasttime,flag,curretntime
         | from sys_customer.dw_custom where curretntime=
       """.stripMargin+"\""+today+"\"")
    data.createOrReplaceTempView("customer_flag")
      val flagDf=spark.sql(
        """
          |select customerid,createtime,lasttime,leasttime,curretntime,
          |case
          |when datediff(leasttime,lasttime)>90 and datediff(curretntime,leasttime)=0 then 1
          |when datediff(leasttime,lasttime)=0 and datediff(curretntime,leasttime)=0 then 1
          |else 0
          |end as addflag,
          |case
          |when datediff(curretntime,leasttime)<=90 then 1
          |else 0
          |end as norflag,
          |case
          |when datediff(curretntime,leasttime)>90 then 1
          |else 0
          |end as delflag
          |from customer_flag
        """.stripMargin).select("customerid","createtime","lasttime","leasttime","addflag","norflag","delflag")
    flagDf
//          val flagDf=spark.sql(
//            """
//              |select customerid,createtime,lasttime,leasttime,
//            """.stripMargin+"\""+today+"\""+
//
//            """
//              | as curretntime,
//              |case
//              |when datediff(leasttime,lasttime)>90 and datediff(curretntime,leasttime)=0 then 0
//              |when datediff(leasttime,lasttime)=0 and datediff(curretntime,leasttime)=0 then 0
//              |when datediff(curretntime,leasttime)>90 then 2
//              |else 1
//              |end as flag
//              |from customer_flag
//            """.stripMargin
//          ).select("customerid","createtime","lasttime","flag")

  }
}
