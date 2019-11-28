package repair.test

import org.apache.spark.sql.SparkSession

object Test2 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("fromhive").master("local[*]").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc=spark.sparkContext
    val today="2019-02-20"
    val data=spark.sql(
      """
        |select customerid,createtime,lasttime,leasttime,addflag,norflag,delflag,
        |case
        |when consum_money<10000 then 1
        |when 10000<consum_money and consum_money<100000 then 2
        |when 100000<consum_money and consum_money<1000000 then 3
        |else 4
        |end as consumelevel
        |from sys_customer.dm_custom_cus
        |where curretntime=
      """.stripMargin+"\""+today+"\"")
    data.show()
  }
}
