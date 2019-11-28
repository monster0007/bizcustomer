package etl

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.hadoop.hbase.client.RegionInfo

object SjhtStreamCopy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("fromhive").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
//select a1.*,a2.cfr_date,ifnull(a3.mon,0)
    val data = spark.sql("""
                          select a1.key id,a2.cfr_date sadte,a1.goodsid,a2.customerid,a1.goodsqty productnumber,a1.l_money amount,(a1.l_money-ifnull(a3.mon,0)) taxmoney,(a1.l_money-ifnull(a3.mon,0)) checkgrossprofit
                          from hiveonhbase.cms_sale_dtl a1
                          join hiveonhbase.cms_sale a2 on a1.saleid=a2.saleid
                          left join
                          (select sum(t1.creditmony) mon,t2.saledtlid from
                          (select sum((if(a.comefrom=4, -a.creditqty, a.creditqty) * b.unitprice)) creditmony,a.sourceid
                          from hiveonhbase.bfi_ska_io_doc a
                          left join  hiveonhbase.bs_batch b on a.batchid=b.batchid
                          where a.summary=' 销售结算'
                          group by a.sourceid) t1
                          join hiveonhbase.CMS_SALE_SETTLE_DTL t2
                          on t1.sourceid=t2.salesetdtlid
                          group by t2.saledtlid) a3
                          on a1.saledtlid=a3.saledtlid
                          where substr(a2.cfr_date,1,10)='2019-04-04'
                          and length(a2.cfr_date)>10
                            """)
    data.show()

  }
}