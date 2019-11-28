package until

import org.apache.spark.sql.SparkSession
import until.MysqlUntil.getFromMysql

object CustomerfromMyToHi {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("frommysqltoHive").master("local[*]").enableHiveSupport().config("file.encoding", "gbk").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("error")

    val sql="select id,customname,customtype,address,contactname,phone,province,city,isexcel from pcd_custom_20190715"
    val res=getFromMysql(spark,"jdbc:mysql://10.1.24.208:3306/","test?useUnicode=true&characterEncoding=utf8","root","bicon@123",sql)

   res.repartition(1).write.mode("overwrite").option("timestampFormat", "yyyy/MM/dd HH:mm:ss").csv("hdfs://nameservice1/user/hive/warehouse/sys_customer.db/customer_info")
  }
}
