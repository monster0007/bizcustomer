package biconcustomer

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 每天计算最新的客户购买时间，
  * 同时把新增客户的销售水平的标追加到本月的消费水平上面
  */
object CustomerAnalyseDay {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")

        val today = getDate(11)
        val yestoday = getDate(12)
        val month=getMonth(2)
        val todayData = getTodayData(spark, today)
        val baseData = getCusFromHive(spark,yestoday)
        val levelPath="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/cus_consume/month=" + month
        val timeDf=updateCusromerWithTime(spark, baseData,todayData,levelPath)
        val path1="hdfs://bd198:8020/user/hive/warehouse/sys_customer.db/dw_custom/curretntime=" + today//购买时间存储路径
        timeDf.repartition(1).write.mode("overwrite").csv(path1)
        val resDf = updateCustomerWithFlag(spark,today)
        val path2 = "hdfs://bd198:8020/user/hive/warehouse/sys_customer.db/dm_custom/curretntime=" + today//打标后存储路径
        resDf.repartition(1).write.mode("overwrite").csv(path2)
  }

  /**
    * 获取当前前一天的数据
    * @param spark
    * @param yestoday
    * @return
    */
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

  /**
    * 获取时间
    * @param day
    * @return
    */
  def getDate(day:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    var date = dateFormat.format(cal.getTime())
    date
  }

  /**
    * 获取月份
    * @param month
    * @return
    */
  def getMonth(month: Int): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -month)
    var mon = dateFormat.format(cal.getTime())
    mon
  }

  /**
    * 获取当天数据,包括当天消费金额
    * @param spark
    * @param today
    * @return
    */
  def getTodayData(spark:SparkSession,today:String):DataFrame={
    val todayData=spark.sql(
      """
        |select customerid,sdate dealdate,sum(amount) consumemoney from sys_customer.sys_customer_salefact sf
        |where sf.sdate =
      """.stripMargin++"\""+today+"\""+
      """
        |group by customerid,sdate
      """.stripMargin
    )
    todayData
  }

  /**
    * 更新最近购买时间以及新增客户
    * @param spark
    * @param baseData
    * @param todayData
    * @return
    */
  def updateCusromerWithTime(spark:SparkSession,baseData:DataFrame,todayData:DataFrame,path:String):DataFrame={
    //计算当天购买了的客户
      val updateCustomer=baseData.join(todayData,baseData("customerid")===todayData("customerid")).select(baseData("customerid"),baseData("createtime"),baseData("leasttime").alias("lasttime"),todayData("dealdate").alias("leasttime"))
    //计算当天未购买的客户
    val stayCustomer=baseData.join(todayData,baseData("customerid")===todayData("customerid"),"left").filter(todayData("customerid").isNull).select(baseData("customerid"),baseData("createtime"),baseData("lasttime"),baseData("leasttime"))
    //当天新增客户
    val addCustomer=todayData.join(baseData,todayData("customerid")===baseData("customerid"),"left").filter(baseData("customerid").isNull).select(todayData("customerid"),todayData("dealdate").alias("createtime"),todayData("dealdate").alias("lasttime"),todayData("dealdate").alias("leasttime"))
    //计算当天的新增客户消费层次，追加到上个月计算出来的消费层次
    val addCustomerLevel=todayData.join(baseData,todayData("customerid")===baseData("customerid"),"left").filter(baseData("customerid").isNull).select(todayData("customerid"),todayData("consumemoney"))
    addCustomerLevel.createOrReplaceTempView("addcustomerlevel")
    val addLevel=spark.sql(
      """
        |select customerid,
        |case
        |when consumemoney<10000 then 1
        |when 10000<consumemoney and consumemoney<100000 then 2
        |when 100000<consumemoney and consumemoney<1000000 then 3
        |else 4
        |end as consumelevel
        |from addcustomerlevel
      """.stripMargin)
    addLevel.repartition(1).write.mode("append").csv(path)//把新增的客户消费水平追加到这个月的消费水平计算结果
    val todayCustomer=addCustomer.union(updateCustomer).union(stayCustomer)//当天的时间更新结果
    todayCustomer
  }

  /**
    * 根据时间给客户打标
    * @param spark
    * @param today
    * @return
    */
  def updateCustomerWithFlag(spark:SparkSession,today:String):DataFrame ={
    val data=spark.sql(
      """
        |select customerid,
        |case
        |when createtime="2019-12-31" then "2018-12-31"
        |else createtime
        |end as createtime,
        |lasttime,leasttime,curretntime
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

//     val data=spark.sql(
//       """
//         |select customerid,createtime,lasttime,leasttime,addflag,norflag,delflag,
//         |case
//         |when consum_money<10000 then 1
//         |when 10000<consum_money and consum_money<100000 then 2
//         |when 100000<consum_money and consum_money<1000000 then 3
//         |else 4
//         |end as consumelevel
//         |from sys_customer.dm_custom_cus
//         |where curretntime=
//       """.stripMargin+"\""+today+"\"")
  }


}
