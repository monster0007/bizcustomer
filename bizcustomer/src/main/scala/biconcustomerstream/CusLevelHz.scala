package biconcustomerstream

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 每个月月初计算一次客户的消费水平,在计算完月客户打标之后
  * 存量客户为最近3个月的消费平均值，新增客户为最近1个月的消费值，流失客户消费层次为0，
  * 计算出来的结果如计算出2018年3个月的消费水平作为19年1月的消费水平，
  * 1月份的计算结果作为2月份的消费水平使用，依次类推,
  * 存储方式为月初1号算出的结果保存到当月的分区表中
  */
object CusLevelHz {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("error")
    val day = getDate(1)//昨天的日期
    val nityDay = getDate(91)//90天的日期
    val month = getAimMonth(1)//昨天的月份
    val mon = getAimMonth(0)//今天所属月份
    val res = getLevel(spark, month,nityDay,day)
    val path2 = "hdfs://nameservice1/user/hive/warehouse/sys_customer.db/cus_consume/month=" + mon
    res.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv(path2)
  }


  /**
    * 近3个月的消费金额平均值
    *
    * @param spark
    * @param month
    * @param day
    */
  def getLevel(spark: SparkSession, month: String,nityday:String,day: String): DataFrame = {
    /*
    计算正常客户的消费层次，如果在月统计中是存量，
    拿过去3个月的平均值，直接除以3,
    注意这里的起始日期不是正好月初，要算90天前
     */
    val nordata = spark.sql(
      """
        |select f.customerid,sum(amount)/3 consumemoney,g.is_own from sys_customer.sys_customer_salefact f
        |join sys_customer.pcd_goods g on f.goodsid=g.goodsid
        |join sys_customer.dm_custom_month m on f.customerid=m.customerid
        |where m.norflag=1 and m.month=
      """.stripMargin+"\""+month+"\""+" and sdate between "+"\""+nityday+"\""+" and "+"\""+day+"\""+
        " group by f.customerid,g.is_own"
    )
    println(nordata.count()+"@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

    /*
    新增客户的计算为一个月内的，所以从上个月的月汇总中找到新增的，
    然后算一个月的
     */
    val adddata = spark.sql(
      """
        |select f.customerid,sum(amount) consumemoney,is_own from sys_customer.sys_customer_salefact f
        |join sys_customer.dm_custom_month m on f.customerid=m.customerid
        |join sys_customer.pcd_goods g on f.goodsid=g.goodsid
        |where m.addflag=1 and m.month= """
      .stripMargin+"\""+month+"\""+" and substr(sdate,1,7)= "+"\""+month+"\""
      + "group by f.customerid,g.is_own"
    )
    println(adddata.count()+"###############################")
    val tmpdata = nordata.union(adddata)

    tmpdata.createOrReplaceTempView("consume_level")
    val norDel = spark.sql(
      """
        |select customerid,
        |case
        |when consumemoney<10000 then 1
        |when 10000<consumemoney and consumemoney<100000 then 2
        |when 100000<consumemoney and consumemoney<1000000 then 3
        |else 4
        |end as consumelevel,is_own
        |from consume_level
      """.stripMargin)

    /*
    流失客户的计算，直接给个消费层次0
     */
    val deldata = spark.sql(
      """
        |select customerid,0 consumelevel,selfflag from  sys_customer.dm_custom_month
        |where delflag=1 and month=
      """.stripMargin+"\""+month+"\"")
    println(deldata.count()+"$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    val res = norDel.union(deldata)
    res
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
   获取昨天所属的月份
    */
  def getAimMonth(num:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -num)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
}