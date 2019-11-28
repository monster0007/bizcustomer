package biconcustomersy

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
计算历史每个月的新增流失
 */
object MonthInitAcc {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("MonthInitAcc").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
    val preDate=args(0).toString//上个月最后一天
    val firstdate=args(1).toString//本月1号
    val curdate=args(2).toString//这个月最后一天
    val month=args(3).toString//这个月
    val res=getPreMon(spark,preDate,firstdate,curdate)
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/dm_custom_month_sy/month="+month
    res.repartition(1).write.mode("overwrite").csv(path)
  }

  def getPreMon(spark:SparkSession,predate:String,firstDate:String,lastDate:String):DataFrame={
    val preData = spark.sql(
      """
        |select customerid,min(sdate) premintime,max(sdate) premaxtime,accountid preaccountid from sys_customer.sys_customer_salefact f
        |join sys_customer.pcd_goods g on f.goodsid=g.goodsid
        |where g.is_own=1
        |and sdate between "2016-10-01" and """
        .stripMargin + "\"" + predate + "\"" + " group by customerid,accountid")

    val curData=spark.sql(
      """
        |select customerid,min(sdate) curmintime,max(sdate) curmaxtime,accountid cusaccountid from sys_customer.sys_customer_salefact f
        |join sys_customer.pcd_goods g on f.goodsid=g.goodsid
        |where g.is_own=1
        |and sdate between
      """.stripMargin+"\""+firstDate+"\""+"and"+"\""+lastDate+"\"" + " group by customerid,accountid"
    )
    val timeDf=preData.join(curData,Seq("customerid"),"full")
    timeDf.show()
    timeDf.createOrReplaceTempView("time_info")

    /*
    然后根据时间进行打标
     */
      val flagDf=  spark.sql(
          """
            |select customerid,
            |case when premintime is null then curmintime
            |else premintime
            |end as createtime,
            |"2010-10-01" lasttime,
            | case when curmaxtime is null then premaxtime
            | else curmaxtime
            | end as leasttime,
            | case when premaxtime is null then 1
            |  when datediff(curmintime,premaxtime)>90 then 1
            | else 0
            | end as addflag,
          """.stripMargin+" case when curmaxtime is null and datediff("+"\""+lastDate+"\""+",premaxtime)<=90 then 1 when curmaxtime is not null and datediff(curmintime,premaxtime)<=90 then 1 else 0 end as norflag, case when datediff(" +"\""+lastDate+"\""+",premaxtime)>90 and curmintime is null then 1 else 0 end as delflag,1 cusattr,1 selfflag,case when preaccountid is null then cusaccountid  else preaccountid end as accountid from time_info")

    flagDf
  }

}
