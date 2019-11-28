package biconcustomer

import org.apache.spark.sql.SparkSession
import until.MysqlUntil.getFromMysql
object CustomerfromMyToHi {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("frommysqltohive").master("local[*]").enableHiveSupport().config("file.encoding", "gbk").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")
    spark.udf.register("replace",(column:String,beforStr:String,afterStr:String)=>{
      column.replace('''+beforStr+''','''+afterStr+''')
    })
    val sql="select * from pcd_custom_dw"
    val customer=getFromMysql(spark,"jdbc:mysql://10.1.24.209:3306/","sys_customer?useUnicode=true&characterEncoding=utf8","root","123456",sql)

    customer.createOrReplaceTempView("customer")
   val res1= spark.sql(
      """
        |select replace(id,'，',' '),
        |replace(customname,'，',' '),
        |replace(customtype,'，',' '),
        |replace(address,'，',' '),
        |replace(contactname,'，',' '),
        |replace(phone,'，',' '),
        |replace(province,'，',' '),
        |replace(city,'，',' '),
        |replace(isexcel,'，',' ') from customer
      """.stripMargin)
    res1.createOrReplaceTempView("res")
    val res=spark.sql(
      """
        |replace(id',',''),
        |replace(customname',',''),
        |replace(customtype',',''),
        |replace(address',',''),
        |replace(contactname',',''),
        |replace(phone',',''),
        |replace(province',',''),
        |replace(city',',''),
        |replace(isexcel',','') from res
      """.stripMargin)
    res.show()
   // res.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv("hdfs://nameservice1/user/hive/warehouse/sys_customer.db/customer_info")
  }
}
