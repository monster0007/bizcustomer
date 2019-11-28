package oushang.moneyrate

import org.apache.spark.sql.{DataFrame, SparkSession}

object RxTest {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession
                      .builder()
                      .master("local[*]")
                      .appName("RxTest")
                        .config("spark.default.parallelism",4)
                      .getOrCreate()
    val spark=sparkSession.sparkContext


  }

  /**
    * 从mysql读取数据
    * @param sparkSession
    * @param sqltxt
    * @return
    */
    def getFromMysql(sparkSession:SparkSession,sqltxt:String): DataFrame ={
      val userName="root"
      val password="bicon@123"
      val url="jdbc:mysql://10.1.24.230/biz_rx_new"
      val jdbcDF = sparkSession.read.format("jdbc")
        .option("url", url)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "("+sqltxt+") t")
        .option("user", userName)
        .option("password", password).load()
      jdbcDF
  }

    def dataDealPre(sparkSession:SparkSession): Unit ={

      //销售结算细单
      val cms_sale_settle_dtlsql=
        """
          |select d.salesetdtlid,
          |	d.goodsid,
          |	d.goodsqty,
          |  d.receiptsumqty,
          |	d.jobdtlstatus,
          |	d.saledtlid,
          |  d.salesetid
          |from cms_sale_settle_dtl d
        """.stripMargin

      //销售结算总单
      val cms_sale_settlesql=
        """
          |select cfr_date,salesetid from cms_sale_settle
        """.stripMargin

      //销售回款细单
//      val cms_sale_receipt_dtlsql=
//        """
//          |select receiptdtlid,
//          |		receiptid,
//          |		goodsqty,
//          |		jobdtlstatus,
//          |		salesetdtlid
//          |from cms_sale_receipt_dtl
//        """.stripMargin

      //销售回款总单
      /*
      val cms_sale_receiptsql=
        """
          |select receiptid,
          |		    cfr_date
          |from cms_sale_receipt
        """.stripMargin
      */

      //销售出库细单
      val cms_sale_dtlsql=
        """
          |select saledtlid,
          |goodsid,
          |lotid,
          |batchid
          |from cms_sale_dtl
        """.stripMargin

      //采购入库细单
      val cms_su_dtlsql=
        """
          |select sudtlid,
          |goodsid,
          |lotid,
          |batchid
          |from cms_su_dtl
        """.stripMargin

      //采购结算细单
      val cms_su_settle_dtlsql=
        """
          |select susetdtlid,
          |goodsqty,
          |unitprice,
          |sumpayqty,
          |sudtlid
          |from cms_su_settle_dtl
        """.stripMargin


      /*
      销售业务数据
       */
//      val cms_sale_settle_dtl=getFromMysql(sparkSession,cms_sale_settle_dtlsql)
//      val cms_sale_settle=getFromMysql(sparkSession,cms_sale_settlesql)
//      val sale_settle_info=cms_sale_settle_dtl.join(org.apache.spark.sql.functions.broadcast(cms_sale_settle),"salesetid")


      //val cms_sale_dtl=getFromMysql(sparkSession,cms_sale_dtlsql)
      // val sale_info=sale_settle_info.join(cms_sale_dtl,Seq("saledtlid"),"left")

//
//      //采购业务数据
      val cms_su_dtl=getFromMysql(sparkSession,cms_su_dtlsql)
      val cms_su_settle_stl=getFromMysql(sparkSession,cms_su_settle_dtlsql)
      val cms_su_info=cms_su_dtl.join(cms_su_settle_stl,"sudtlid")

      val savePath="hdfs://nameservice1/user/hive/warehouse/aftest.db/ods_su_info/"
      cms_su_info.repartition(1).write.mode("overwrite").orc(savePath)
    }

    def getFromHive(sparkSession: SparkSession): Unit ={
      val data_source=sparkSession.sql(
        """
          |select
          |s.salesetdtlid,
          |s.goodsid,
          |s.goodsqty sale_goodqty,
          |s.receiptsumqty sale_repqty,
          |s.cfr_date,
          |u.goodsqty su_goodqty,
          |u.unitprice,
          |u.sumpayqty su_payqty
          |from ods_sale_settle_info s
          |join ods__sale_info i on s.saledtlid=i.saledtlid
          |join ods_su_info u on i.goodsid=u.goodsid and i.lotid=u.lotid and i.batchid=u.batchid
        """.stripMargin)


    }
}
