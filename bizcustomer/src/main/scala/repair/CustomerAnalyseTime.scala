package repair

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerAnalyseTime {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    val today=getDate(0)
    val monthDay=getDate(30)
    val yesToday=getDate(1)
    val yesMonDay=getDate(31)
    val todayData=getTodayData(spark,today,monthDay)
    val yestodayData=getTodayData(spark,yesToday,yesMonDay)
    val res=cusromerWithFlag(spark,yestodayData,todayData,today)
  }
  def getDate(day:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    var date = dateFormat.format(cal.getTime())
    date
  }
  def getTodayData(spark:SparkSession,today:String,lastMonthDate:String):DataFrame={
    val todayData=spark.sql(
      """
        |select customerid,count(customerid) buytime from lyb_customer.sale_fact sf
        |join lyb_customer.self_goods sg on sf.goodsid=sg.goodsid
        |where sf.sdate BETWEEN
      """.stripMargin+"\""+lastMonthDate+"\""+"and"+"\""+today+"\"" +
        """
          |group by customerid
        """.stripMargin
    )
    todayData
  }
  def cusromerWithFlag(spark:SparkSession,baseData:DataFrame,todayData:DataFrame,today:String)={
    //流失客户
      val delCustomer=baseData.join(todayData,baseData("customerid")===todayData("customerid"),"left").filter(todayData("customerid").isNull)
    //新增客户
     val addCustomer=todayData.join(baseData,todayData("customerid")===baseData("customerid"),"left").filter(baseData("customerid").isNull)
     spark.sql(
       """
         |insert into lyb.pcd_customer as select customerid,customername,customertype,province,city,nomflag,delflag,addflag
       """.stripMargin+","+today+

       """
         |from lyb.pcd_customer
       """.stripMargin)
  }
}
