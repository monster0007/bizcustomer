package etl

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Editor:         idea
  * Department:     BigData Group
  * Author:         Hao Cheng
  * Data:           2019/3/11  9:21
  * Description:
  *
  */
object BcSelfGoods {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().master("yarn").appName("bc to hive").getOrCreate()
    ss.sparkContext.setLogLevel("error")

    val saleset=readhb(ss,"BIZ_BC:WORKXSYWHZ")
    val salesetdtl=readhb(ss,"BIZ_BC:WORKXSYWMX")

    import ss.implicits._

   val salesetdf=saleset
      .map({case(_,result)=>
      val rowkey=Bytes.toString(result.getRow).toString
      val salesetid=Bytes.toString(result.getValue("cf".getBytes,"DANJBH".getBytes))
      val customerid=Bytes.toString(result.getValue("cf".getBytes,"WANGLDWID".getBytes))
        var sdate=(Bytes.toString(result.getValue("cf".getBytes,"RQ".getBytes)))
        if(sdate == null){
          sdate="0000-00"
        }else{
          sdate
        }
        val date = sdate.split(" ")(0).replaceAll("-","")
        val pardate=date.substring(0,6)

        (rowkey,salesetid,customerid,sdate,pardate,date)
    }).toDF("row","salesetid","customerid","sdate","pardate","date")

   val salesetdtldf=salesetdtl.map({case(_,result)=>
      val rowkey=Bytes.toString(result.getRow).toString
      val salesetdtlid=Bytes.toString(result.getValue("cf".getBytes,"DANJXH".getBytes))
      val salesetid=Bytes.toString(result.getValue("cf".getBytes,"DANJBH".getBytes))
      val goodsid=Bytes.toString(result.getValue("cf".getBytes,"SHANGPID".getBytes))
      val goodsqty=Bytes.toString(result.getValue("cf".getBytes,"SHUL".getBytes))
      val jobdtlstatus=Bytes.toString(result.getValue("cf".getBytes,"JOBDTLSTATUS".getBytes))
      val amount=Bytes.toString(result.getValue("cf".getBytes,"HANSJE".getBytes))
      val taxmoney=Bytes.toString(result.getValue("cf".getBytes,"MAOL".getBytes))
      val checkgrossprofit=Bytes.toString(result.getValue("cf".getBytes,"MAOL".getBytes))
      (rowkey,salesetdtlid,salesetid,goodsid,goodsqty,jobdtlstatus,amount,taxmoney,checkgrossprofit)
    }).toDF("row","salesetdtlid","salesetid","goodsid","goodsqty","jobdtlstatus","amount","taxmoney","checkgrossprofit")

    salesetdf.createOrReplaceTempView("sale_settle")
    salesetdtldf.createOrReplaceTempView("sale_settle_dtl")
    val resultdf=ss.sql("select t2.salesetid,concat(t1.date,t2.salesetdtlid,3000) id,3000 as accountid,t2.amount,t2.checkgrossprofit,concat(3000,t1.customerid) customerid,concat(3000,t2.goodsid) goodsid,t2.jobdtlstatus,t2.goodsqty productnumber,t1.sdate,t1.pardate,t2.taxmoney from sale_settle_dtl t2 left join sale_settle t1 on t1.salesetid=t2.salesetid")

    val mysqlDatabase="sys_customer"
    val customerRelTab="pcd_custom_relationship_dw"
    val goodsRelTab="pcd_goods_rel"
    val username="root"
    val passwd="123456"
    val customerRelDf = getFromMysql(ss,mysqlDatabase,customerRelTab,username,passwd)
    customerRelDf.createTempView("customerRel")
    val goodsRelDf=getFromMysql(ss,mysqlDatabase,goodsRelTab,username,passwd)
    goodsRelDf.createTempView("goodsRel")
    resultdf.createTempView("final_table")

    val dat=getYesterdayMonthHive()
    val res = ss.sql("select t1.id,t1.sdate,t3.ownproduct_id goodsid,IFNULL(t2.pcd_id,0) customerid,t1.accountid,t1.productnumber*t3.quantity productnumber,t1.amount,case when t1.amount/t1.productnumber*t3.quantity<1 then 1 else 0 end as is_gift from final_table t1 left join customerRel t2 on t1.customerid=t2.customerid  join goodsRel t3 on t1.goodsid=t3.bus_goodsid where t1.pardate ="+"\""+ dat +"\"")
    val path="hdfs://nameservice1/user/hive/warehouse/sys_customer.db/dw_sale_fact"+"/pardate="+dat
    res.repartition(1).write.mode("append").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv(path)
  }


  def getFromMysql(spark:SparkSession,databaseName:String,tableName:String,userName:String,password:String): DataFrame ={
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.1.24.209:3306/" + databaseName)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", userName)

    jdbcDF.option("password", password).load()
  }

  def getYesterdayMonthHive():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }


  private def readhb(sparkSession: SparkSession,table:String) = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf.set("hbase.zookeeper.quorum", "10.1.24.197,10.1.24.198,10.1.24.199")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")

    val stuRDD = sparkSession.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    stuRDD
  }
}
