package etl

import org.apache.spark.sql.SparkSession

object ReadCtity {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("fromhive").master("local[*]").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext

    val data=sc.textFile("file:///H:/city.txt")
    data.map(lines=>{
      if(lines.length<8){
        val province= lines.split("、")(0)
      }else{
        lines.split("、")
      }
    })
  }
}
