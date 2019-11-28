package repair

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerAnalyseReally {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    for(i<-Range(85,70,-1)) {
      val today = getDate(i)
      val yestoday=getDate(i+1)
      val todayData = getTodayData(spark, today)
      val baseData = getCusFromHive(spark,yestoday)
      val timeDf=updateCusromerWithTime(spark, baseData,todayData)
      val path1="hdfs://bd198:8020/user/hive/warehouse/lyb_customer.db/lyb_custom/curretntime="+today
      timeDf.repartition(1).write.mode("overwrite").csv(path1)
      Thread.sleep(1000)
      val resDf=updateCustomerWithFlag(spark,timeDf,today)
      val path2="hdfs://bd198:8020/user/hive/warehouse/lyb_customer.db/lyb_custom_flag/curretntime="+today
      resDf.repartition(1).write.mode("overwrite").csv(path2)
      Thread.sleep(1000)
    }
  }

  def getCusFromHive(spark:SparkSession,yestoday:String): DataFrame ={
    val cusData=spark.sql(
      """
        |select * from lyb_customer.lyb_custom where curretntime =
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
        |select customerid,sdate dealdate from lyb_customer.sale_fact sf
        |join lyb_customer.self_goods sg on sf.goodsid=sg.goodsid
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
  }
  def updateCustomerWithFlag(spark:SparkSession,timedf:DataFrame,today:String):DataFrame ={
     val data=spark.sql(
       """
         |select * from lyb_customer.lyb_custom where curretntime=
       """.stripMargin+"\""+today+"\"")
    data.createOrReplaceTempView("customer_flag")
      val flagDf=spark.sql(
        """
          |select customerid,createtime,lasttime,leasttime,curretntime,
          |case
          |when datediff(leasttime,lasttime)>90 and datediff(curretntime,leasttime)=0 then 0
          |when datediff(leasttime,lasttime)=0 and datediff(curretntime,leasttime)=0 then 0
          |when datediff(curretntime,leasttime)>90 then 2
          |else 1
          |end as flag
          |from customer_flag
        """.stripMargin).select("customerid","createtime","lasttime","leasttime","flag")
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
