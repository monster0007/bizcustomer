package biconcustomersy

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerAnalyseDayInitHzTwoAccount {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("CustomerAnalyseDayInitSy").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")//14
    val start:Int = args(0).toInt
    val end:Int = args(1).toInt

    for(i<-Range(start,end,-1)){
        val today = getDate(i)
        val yestoday = getDate(i + 1)//前一天
        val ninetyDay=getDate(i+90)//90天前
        val todayData = getTodayData(spark, today)
        val baseData = getCusFromHive(spark,yestoday)
        val basePath="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/"
        val levelPath=basePath+"cus_consume_sy/month=2019-08"
        val timeDf=updateCusromerWithTime(spark, baseData,todayData,levelPath)
        val path1=basePath+"dw_custom_sy/currenttime="+today
        timeDf.repartition(1).write.mode("overwrite").csv(path1)
        val resDf = updateCustomerWithFlag(spark,today)
        val path2 = basePath+"dm_custom_sy/currenttime=" + today
        resDf.repartition(1).write.mode("overwrite").csv(path2)
      }
  }

  /*
  获取昨天的dw表
   */
  def getCusFromHive(spark:SparkSession,yestoday:String): DataFrame ={
    val cusData=spark.sql(
      """
        |select * from sys_customer.dw_custom_sy where currenttime =
      """.stripMargin+"\""+yestoday+"\"")
    cusData
  }

  /*
  获取日期
   */
  def getDate(day:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    var date = dateFormat.format(cal.getTime())
    date
  }

  /*
  获取当天的消费客户和金额,按照是否自有分组
   */
  def getTodayData(spark:SparkSession,today:String):DataFrame={
    val todayData=spark.sql(
      """
        |select customerid,sdate dealdate,sum(amount) consumemoney,is_own,accountid from sys_customer.sys_customer_salefact f
        |join sys_customer.pcd_goods g
        |on f.goodsid=g.goodsid
        |where f.sdate =
      """.stripMargin++"\""+today+"\""+
      """
        |group by customerid,sdate,is_own,accountid
      """.stripMargin
    )
    todayData
  }


  def updateCusromerWithTime(spark:SparkSession,baseData:DataFrame,todayData:DataFrame,path:String):DataFrame={
    //计算当天购买了的客户,更新最近时间
      val updateCustomer=baseData.join(todayData,Seq("customerid","is_own","accountid")).select(baseData("customerid"),baseData("createtime"),baseData("leasttime").alias("lasttime"),todayData("dealdate").alias("leasttime"),baseData("is_own"),baseData("accountid"))
    //计算当天未购买的客户，保持
    val stayCustomer=baseData.join(todayData,Seq("customerid","is_own","accountid"),"left").filter(todayData("customerid").isNull).select(baseData("customerid"),baseData("createtime"),baseData("lasttime"),baseData("leasttime"),baseData("is_own"),baseData("accountid"))
    //当天的新增客户，加入
    val addCustomer=todayData.join(baseData,Seq("customerid","is_own","accountid"),"left").filter(baseData("customerid").isNull).select(todayData("customerid"),todayData("dealdate").alias("createtime"),todayData("dealdate").alias("lasttime"),todayData("dealdate").alias("leasttime"),todayData("is_own"),todayData("accountid"))
    //计算当天的新增客户消费层次，追加到上个月计算出来的消费层次
    val addCustomerLevel=todayData.join(baseData,Seq("customerid","is_own","accountid"),"left").filter(baseData("customerid").isNull).select(todayData("customerid"),todayData("consumemoney"),todayData("is_own"),todayData("accountid"))
    addCustomerLevel.createOrReplaceTempView("addcustomerlevel")
    val addLevel=spark.sql(
      """
        |select customerid,
        |case
        |when consumemoney<10000 then 1
        |when 10000<consumemoney and consumemoney<100000 then 2
        |when 100000<consumemoney and consumemoney<1000000 then 3
        |else 4
        |end as consumelevel,is_own,accountid
        |from addcustomerlevel
      """.stripMargin)
    addLevel.repartition(1).write.mode("append").csv(path)//把新增的客户消费水平追加到这个月的消费水平计算结果

    //当天的时间更新结果
    val todayCustomer=addCustomer.union(updateCustomer).union(stayCustomer)
    todayCustomer
  }

  /*
  把客户的时间整合到一条数据上，然后根据时间给客户打标
   */
  def updateCustomerWithFlag(spark:SparkSession,today:String):DataFrame ={
    spark.sql(
      """
        |msck repair table sys_customer.dw_custom_sy
      """.stripMargin)
    val data=spark.sql(
      """
        |select customerid,createtime,lasttime,leasttime,is_own,accountid,currenttime
        | from sys_customer.dw_custom_sy where currenttime=
      """.stripMargin+"\""+today+"\"")
    data.createOrReplaceTempView("dw_cus_sy")

    //交叉客户及时间
    val crossCus=spark.sql(
      """
        |select t1.customerid,t1.createtime o_createtime,t1.lasttime o_lasttime,t1.leasttime o_leasttime,
        |t2.createtime c_createtime,t2.lasttime c_lasttime,t2.leasttime c_leasttime,t1.accountid,t1.currenttime from
        |(select * from dw_cus_sy where is_own=1) t1 join
        |(select * from dw_cus_sy where is_own=0) t2 on t1.customerid=t2.customerid and t1.accountid=t2.accountid
      """.stripMargin)
    //纯自有客户及时间
    val ownCus=spark.sql(
      """
        |select t1.customerid,t1.createtime o_createtime,t1.lasttime o_lasttime,t1.leasttime o_leasttime,
        |t2.createtime c_createtime,t2.lasttime c_lasttime,t2.leasttime c_leasttime,t1.accountid,t1.currenttime from
        |(select * from dw_cus_sy where is_own=1) t1 left join
        |(select * from dw_cus_sy where is_own=0) t2 on t1.customerid=t2.customerid and t1.accountid=t2.accountid where t2.customerid is null
      """.stripMargin)
    //纯竞品客户
    val comCus=spark.sql(
      """
        |select t2.customerid,t1.createtime o_createtime,t1.lasttime o_lasttime,t1.leasttime o_leasttime,
        |t2.createtime c_createtime,t2.lasttime c_lasttime,t2.leasttime c_leasttime,t2.accountid,t2.currenttime from
        |(select * from dw_cus_sy where is_own=1) t1 right join
        |(select * from dw_cus_sy where is_own=0) t2 on t1.customerid=t2.customerid and t1.accountid=t2.accountid where t1.customerid is null
      """.stripMargin)
    //带了自有产品和竞品购买时间的客户
   val cusWithTime=crossCus.union(ownCus).union(comCus)
    println(cusWithTime.count()+"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    cusWithTime.createOrReplaceTempView("dm_cus_sy")
    //交叉客户打标,交叉客户用的是自有商品购买的时间
    val crossCusFlag=spark.sql(
      """
        |select customerid,o_createtime,o_lasttime,o_leasttime,
        |case
        |when  datediff(o_leasttime,o_lasttime)>90 and datediff(currenttime,o_leasttime)=0 then 1
        |when datediff(o_leasttime,o_lasttime)=0 and datediff(currenttime,o_leasttime)=0 then 1
        |else 0
        |end as addflag,
        |case
        |when datediff(currenttime,o_leasttime)<=90 and datediff(o_leasttime,o_lasttime)>=0  then 1
        |else 0
        |end as norflag,
        |case
        |when datediff(currenttime,o_leasttime)>90 then 1
        |else 0
        |end as delflag,2 cusatrr,1 selfflag,accountid
        |from dm_cus_sy where o_createtime is not null and c_createtime is not null
        |and datediff(currenttime,o_leasttime)<=90 and datediff(currenttime,c_leasttime)<=90
      """.stripMargin)
    //纯自有客户打标,90天内只买了自有产品的客户(竞品为空或90天没买,且90天内买了自有产品)
    val ownCusFlag=spark.sql(
      """
        |select customerid,o_createtime,o_lasttime,o_leasttime,
        |case
        |when  datediff(o_leasttime,o_lasttime)>90 and datediff(currenttime,o_leasttime)=0 then 1
        |when datediff(o_leasttime,o_lasttime)=0 and datediff(currenttime,o_leasttime)=0 then 1
        |else 0
        |end as addflag,
        |case
        |when datediff(currenttime,o_leasttime)<=90 and datediff(o_leasttime,o_lasttime)>=0  then 1
        |else 0
        |end as norflag,
        |case
        |when datediff(currenttime,o_leasttime)>90 then 1
        |else 0
        |end as delflag,1 cusatrr,1 selfflag,accountid
        |from dm_cus_sy where (c_createtime is null or datediff(currenttime,c_leasttime)>90)
        |and datediff(currenttime,o_leasttime)<=90
      """.stripMargin)
    //竞品客户打标，90天内只买了竞品的客户(曾买过自有，但90天没买了；和纯竞品客户，没买过自有的)
    val comCusFlag1=spark.sql(
      """
        |select customerid,o_createtime,o_lasttime,o_leasttime,
        |0 addflag,
        |0 norflag,
        |1 delflag,
        |3 cusatrr,
        |1 selfflag,
        |accountid
        |from dm_cus_sy where o_createtime is not null and datediff(currenttime,o_leasttime)>90
        |and datediff(currenttime,c_leasttime)<=90
      """.stripMargin)
    val comCusFlag2=spark.sql(
      """
        |select customerid,c_createtime,c_lasttime,c_leasttime,
        |0 addflag,
        |1 norflag,
        |0 delflag,
        |3 cusatrr,
        |0 selfflag,
        |accountid
        |from dm_cus_sy where o_createtime is null and datediff(currenttime,c_leasttime)<=90
      """.stripMargin)
    //90天内啥都没买过，但曾经只买过自有产品
    val overNity1=spark.sql(
      """
        |select customerid,o_createtime,o_lasttime,o_leasttime,
        |0 addflag,
        |0 norflag,
        |1 delflag,
        |1 cusatrr,
        |1 selfflag,
        |accountid
        |from dm_cus_sy where o_createtime is not null and c_createtime is null and datediff(currenttime,o_leasttime)>90
      """.stripMargin)
    //90天内啥都没买但曾经两个都买过
    val overNity2=spark.sql(
      """
        |select customerid,o_createtime,o_lasttime,o_leasttime,
        |0 addflag,
        |0 norflag,
        |1 delflag,
        |2 cusatrr,
        |1 selfflag,
        |accountid
        |from dm_cus_sy where o_createtime is not null and c_createtime is not null and datediff(currenttime,o_leasttime)>90 and datediff(currenttime,c_leasttime)>90
      """.stripMargin)
    //90天内啥都没买过，曾经也没买过自有(自有为null,且竞品购买时间大于90天)
    val overNityCom=spark.sql(
      """
        |select customerid,c_createtime,c_lasttime,c_leasttime,
        |0 addflag,
        |0 norflag,
        |1 delflag,
        |3 cusatrr,
        |0 selfflag,
        |accountid
        |from dm_cus_sy where o_createtime is null and datediff(currenttime,c_leasttime)>90
      """.stripMargin)

    val res=crossCusFlag.union(ownCusFlag).union(comCusFlag1).union(comCusFlag2).union(overNity1).union(overNity2).union(overNityCom)
    res
  }
}
