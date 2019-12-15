package biconcustomerhz

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerResultHzMonth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CustomerResultHzMonth").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
//    val timeList=List("2017-01","2017-02","2017-03","2017-04","2017-05","2017-06","2017-07","2017-08",
//      "2017-09","2017-10","2017-11","2017-12","2018-01","2018-02","2018-03","2018-04","2018-05","2018-06",
//      "2018-07","2018-08","2018-09","2018-10","2018-11","2018-12"
//    )
    //val timeList=List("2019-11")
    val timeList=List(args(0).toString)
    timeList.foreach(dat=>{
      val num=dat
      val date=num.replace("-","")
      val customreDf=getResult(spark,num)
      val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/sys_cus_hz_"+date
      customreDf.repartition(1).write.mode("overwrite").csv(path)
    })
  }

  def getResult(spark: SparkSession,num:String): DataFrame = {
    val date=num.replace("-","")
    spark.sql(
      """
        |create table if not exists sys_customer.sys_cus_hz_"""
        .stripMargin+date+"""
        |(customerid bigint,
        |createtime date,
        |leasttime date,
        |addflag int,
        |norflag int,
        |delflag int,
        |cusattr int,
        |selfflag int,
        |consumlevel int,
        |customname string,
        |customtype string,
        |address string,
        |contactname string,
        |phonenum string,
        |provincename string,
        |cityname string
        |)row format delimited fields terminated by ','
      """.stripMargin
    )
    val data=spark.sql(
      """
        |select m.customerid,m.createtime,m.leasttime,m.addflag,m.norflag,m.delflag,m.cusattr,m.selfflag,
        |case when m.delflag=1 then 0
        |else c.consumelevel
        |end as consumlevel,
        |ifnull(i.customname,"其他"),ifnull(i.customtype,"其他"),ifnull(i.address,"其他"),ifnull(i.contactname,"其他"),ifnull(i.phone,"其他") phonenum,
        |ifnull(i.provincename,"其他"),ifnull(i.cityname,"其他")
        |from sys_customer.dm_custom_month m
        |join sys_customer.cus_consume c on m.customerid=c.customerid and m.selfflag=c.is_own
        |left join sys_customer.customer_info i on m.customerid=i.id
        |where c.month=""".stripMargin+"\""+num+"\""+" and m.month= " +"\""+num+"\"")
    data
      }
  }
