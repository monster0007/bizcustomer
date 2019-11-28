package etl

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SjhtStreamCustomer {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext

   val data= spark.sql(
     """
       |select a1.key id,
       |a2.cfr_date sadte,
       |a1.goodsid,
       |a2.customerid,
       |a1.goodsqty productnumber,
       |a1.l_money amount,
       |(a1.l_money-ifnull(a3.mon,0)) taxmoney,
       |(a1.l_money-ifnull(a3.mon,0)) checkgrossprofit
       |from hiveonhbase.cms_sale_dtl a1
       |join hiveonhbase.cms_sale a2 on a1.saleid=a2.saleid
       |left join
       |(select sum(t1.creditmony) mon,t2.saledtlid from
       |(select sum((if(a.comefrom=4, -a.creditqty, a.creditqty) * b.unitprice)) creditmony,a.sourceid
       |from hiveonhbase.bfi_ska_io_doc a
       |left join  hiveonhbase.bs_batch b on a.batchid=b.batchid
       |where a.summary=' 销售结算'
       |group by a.sourceid) t1
       |join hiveonhbase.CMS_SALE_SETTLE_DTL t2
       |on t1.sourceid=t2.salesetdtlid
       |group by t2.saledtlid) a3
       |on a1.saledtlid=a3.saledtlid
       |where  length(a2.cfr_date)>10
     """.stripMargin)
   val finalRdd= data.rdd.map(line=>{
     val accountid="6000"
     val id=line(0)
     val sadte=line(1).toString.substring(0,10)
     val goodsid=accountid+line(2).toString
     val customerid=accountid+line(3).toString
     val productnumber=line(4)
     val amount=line(5)
     val taxmoney=line(6)
     val checkgrossprofit=line(7)
     val pardate=line(1).toString.substring(0,7).replace("-","")
     Row(id,sadte,goodsid,customerid,accountid,productnumber,amount,taxmoney,checkgrossprofit,pardate)
   })
    val finlSchema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("sdate", StringType, true),
        StructField("goodsid", StringType, true),
        StructField("customerid", StringType, true),
        StructField("accountid", StringType, true),
        StructField("productnumber", IntegerType, true),
        StructField("amount", DoubleType, true),
        StructField("taxmoney", DoubleType, true),
        StructField("checkgrossprofit", DoubleType, true),
        StructField("pardate", StringType, true)
      )
    )

    val finalDf=spark.createDataFrame(finalRdd,finlSchema)
    val mysqlDatabase="sys_customer"
    val customerRelTab="pcd_custom_relationship_dw"
    val goodsRelTab="pcd_goods_rel"
    val username="root"
    val passwd="123456"
    val customerRelDf = getFromMysql(spark,mysqlDatabase,customerRelTab,username,passwd)
    customerRelDf.createTempView("customerRel")
    val goodsRelDf=getFromMysql(spark,mysqlDatabase,goodsRelTab,username,passwd)
    goodsRelDf.createTempView("goodsRel")
    finalDf.createTempView("final_table")

    val dat=getYesterdayMonthHive()
    val res=spark.sql("select t1.id,t1.sdate,t3.ownproduct_id goodsid,IFNULL(t2.pcd_id,0) customerid,t1.accountid,t1.productnumber*t3.quantity productnumber,t1.amount,case when t1.amount/t1.productnumber*t3.quantity<1 then 1 else 0 end as is_gift from final_table t1 left join customerRel t2 on t1.customerid=t2.customerid  join goodsRel t3 on t1.goodsid=t3.bus_goodsid where t1.pardate="+dat)
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/dw_sale_fact"+"/pardate="+dat.replace("-","")
    res.repartition(1).write.mode("append").option("timestampFormat","yyyy/MM/dd HH:mm:ss").csv(path)

  }
  def getYesterdayMonth():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getYesterdayMonthHive():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getFromMysql(spark:SparkSession,databaseName:String,tableName:String,userName:String,password:String): DataFrame ={
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.1.24.209:3306/" + databaseName)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password).load()
    jdbcDF
  }
}
