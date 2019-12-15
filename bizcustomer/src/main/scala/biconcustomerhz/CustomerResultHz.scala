package biconcustomerhz

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *c_hz_day_20191127 出现异常 主键重复
  * step1.修改hdfs 文件块
  *step2. cus_customer 去重后 需要重跑
  *
  */
object CustomerResultHz {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("local[*]").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    //val num=args(0).toString //"2019-11-26"
    val num="2019-11-27" //
    val date=num.replace("-","")
    val customreDf=getResult(spark,num)
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/sys_cus_hz_"+date
    customreDf.repartition(1).write.mode("overwrite").csv(path)
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
        |from sys_customer.dm_custom m
        |join sys_customer.cus_consume c on m.customerid=c.customerid and m.selfflag=c.is_own
        |left join sys_customer.customer_info i on m.customerid=i.id
        |where c.month="2019-11"
        |and m.currenttime=
      """.stripMargin+"\""+num+"\"")
    data
      }


  }
