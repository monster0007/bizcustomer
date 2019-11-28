package until

import org.apache.spark.sql.SparkSession

object UdfTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UdfTest").master("local[*]").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    spark.udf.register("len",(len:String)=>len.length)

    val data=spark.sql(
      """
        |select len(customname) from lyb_customer.customer_info limit 5
      """.stripMargin)
    data.show()
  }

}
