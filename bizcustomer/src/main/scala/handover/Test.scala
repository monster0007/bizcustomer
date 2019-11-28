package handover

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().master("local[*]").appName("offsettest").enableHiveSupport().getOrCreate()
    val data=sparkSession.sql(
      """
        |select * from
      """.stripMargin)
  }
}
