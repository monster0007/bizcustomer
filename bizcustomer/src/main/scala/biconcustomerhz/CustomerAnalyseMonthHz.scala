package biconcustomerhz

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 按月对客户打标
  * 每个月1号跑，月的新增客户是整月所有新增聚合，
  * 月的流失是最后一天流失，
  * 月的存量就是去除新增流失，
  * 月的客户属性取最后一天，(一个客户在一个月内可能从交叉变为纯自有)
  * 月的客户自有标志取最后一天
  */
object CustomerAnalyseMonthHz {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("hz_month-sys").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
//    val day=getDate(args(0).toInt)
//    val month=getMonth(args(1).toInt)
    val day="2019-07-31"
    val month="2019-07"
    val res=getdata(spark,month,day)
    val path1="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/dm_custom_month/month="+month
    res.repartition(1).write.mode("overwrite").csv(path1)
  }

  def getDate(day:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    var date = dateFormat.format(cal.getTime())
    date
  }

  def getMonth(month:Int):String={
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -month)
    var mon = dateFormat.format(cal.getTime())
    mon
  }

  def getdata(spark:SparkSession,month:String,day:String): DataFrame ={
    spark.sql(
      """
        |msck repair table sys_customer.dm_custom
      """.stripMargin)
    //这一个月的所有新增客户
    val addData=spark.sql(
      """
        |select customerid,createtime,lasttime,leasttime,1 addflag,0 norflag,0 delflag from sys_customer.dm_custom where addflag=1 and substr(currenttime,1,7)=
      """.stripMargin+"\""+month+"\"")
    //所有流失客户,即最后一天的流失客户
    val delData=spark.sql(
      """
        |select customerid,createtime,lasttime,leasttime,0 addflag,0 norflag,1 delflag from sys_customer.dm_custom where delflag=1 and currenttime="2019-07-31"
      """.stripMargin)
//    val delData=spark.sql(
//      """
//        |select customerid,createtime,lasttime,leasttime,0 addflag,0 norflag,1 delflag from sys_customer.dm_custom where delflag=1 and curretntime=
//      """.stripMargin+"\""+day+"\"")
    val addAndDel=addData.union(delData)
    addAndDel.createOrReplaceTempView("add_and_del")
    //存量客户,最后一天的数据去除新增的和流失的就是存量
    val norData=spark.sql(
      """
        |select d.customerid,d.createtime,d.lasttime,d.leasttime,0 addflag,1 norflag,0 delflag from sys_customer.dm_custom d
        |left join add_and_del a
        |on d.customerid=a.customerid
        |where currenttime="2019-07-31"
        |and a.customerid is null
      """.stripMargin)
//    val norData=spark.sql(
//      """
//        |select d.customerid,createtime,lasttime,leasttime,d.addflag,d.norflag,d.delflag from sys_customer.dm_custom d
//        |left join add_and_del a
//        |on d.customerid=a.customerid
//        |where currenttime=
//      """.stripMargin+"\""+day+"\""+"and a.customerid is null")
    val restmp=addAndDel.union(norData)
    //关联出最后一天的客户属性及所属
   restmp.createOrReplaceTempView("restmp")
    val res=spark.sql(
      """
        |select t.*,d.cusattr,d.selfflag from restmp t
        |join sys_customer.dm_custom d
        |on t.customerid=d.customerid
        |where d.currenttime="2019-07-31"
      """.stripMargin)
    res
  }
}
