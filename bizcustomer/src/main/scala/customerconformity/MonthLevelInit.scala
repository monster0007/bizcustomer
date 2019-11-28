package customerconformity

import java.text.{ParsePosition, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, SparkSession}

object MonthLevelInit {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
//    val lastday=args(0).toString//月的最后一天
//    val curmonth=args(1).toString//这个月的月份值
//    val nityday=getnityDate(lastday,90)//最后一天的90天前日期
    val lastday="2019-05-31"
    val curmonth="201905"
    val nityday=getnityDate(lastday,90)
    val res=monthLevel(spark,nityday,lastday,curmonth)
    val path2 = "hdfs://nameservice1/user/hive/warehouse/sys_customer_conformity.db/cus_consume/month="+curmonth
    res.repartition(1).write.mode("overwrite").csv(path2)
  }

  /*
   算哪个月的消费层次就是读这个月的客户状态，
   客户为新增装态,就算1个月的，
   客户为常量客户则算，三个月的，
   流失客户直接给个0
  */
  def monthLevel(spark:SparkSession,nityday:String,lastday:String,month:String):DataFrame={

    //存量客户的计算
    val nordata = spark.sql(
      """
        |select f.customerid,sum(amount)/3 consumemoney,1 is_own from sys_customer_conformity.sys_customer_salefact f
        |join sys_customer_conformity.pcd_goods g on f.goodsid=g.goodsid
        |join sys_customer_conformity.dm_custom_month m on f.customerid=m.customerid
        |where g.is_own=1
        |and  m.norflag=1 and m.month=""".stripMargin+"\""+month+"\""+" and sdate between "+"\""+nityday+"\""+" and "+"\""+lastday+"\""+" group by f.customerid"
    )

    //新增客户的计算
    val adddata = spark.sql(
      """
        |select f.customerid,sum(amount) consumemoney,1 is_own from sys_customer_conformity.sys_customer_salefact f
        |join sys_customer_conformity.dm_custom_month m on f.customerid=m.customerid
        |join sys_customer_conformity.pcd_goods g on f.goodsid=g.goodsid
        |where m.addflag=1 and m.month=""".stripMargin +"\""+month+"\""+" and substr(sdate,1,7)="+"\""+month+"\""+"group by f.customerid"
    )

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
        |select customerid,0 consumelevel,selfflag from  sys_customer_conformity.dm_custom_month
        |where delflag=1 and  month=
      """.stripMargin+"\""+month+"\"")

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
  获取某个日期90天前的日期
   */
  def getnityDate(curdate:String,day:Int): String ={
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val par= new ParsePosition(0)
    val date:Date=dateFormat.parse(curdate,par)//转date类型
    var cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -day)
    var res = dateFormat.format(cal.getTime())
    res
  }

}
