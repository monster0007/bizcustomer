package biconcustomer

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 按月对客户打标
  * 每个月1号跑，获取前一天当前月的所有新增客户id,
  * 获取前一月的所有流失客户和正常客户,
  * 从2019-03-01凌晨开始，计算结果存储在2019-02，
  * 依次类推
  */
object CustomerAnalyseMonth {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
//    val day=getDate(args(0).toInt)
//    val month=getMonth(args(1).toInt)
    val day="2019-04-30"
    val month="2019-04"
    val res=getdata(spark,month,day)
    val path1="hdfs://bd198:8020/user/hive/warehouse/sys_customer.db/dm_custom_month/month="+month
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
        |select customerid,createtime,lasttime,leasttime,1 addflag,1 norflag,0 delflag from sys_customer.dm_custom where addflag=1 and substr(currenttime,1,7)=
      """.stripMargin+"\""+month+"\"")
    //所有流失客户,即最后一天的流失客户
    val delData=spark.sql(
      """
        |select customerid,createtime,lasttime,leasttime,0 addflag,0 norflag,1 delflag from sys_customer.dm_custom where delflag=1 and currenttime="2019-04-30"
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
        |where currenttime="2019-04-30"
        |and a.customerid is null
      """.stripMargin)
//    val norData=spark.sql(
//      """
//        |select d.customerid,createtime,lasttime,leasttime,d.addflag,d.norflag,d.delflag from sys_customer.dm_custom d
//        |left join add_and_del a
//        |on d.customerid=a.customerid
//        |where currenttime=
//      """.stripMargin+"\""+day+"\""+"and a.customerid is null")
    val res=addAndDel.union(norData)
    res
  }
}
