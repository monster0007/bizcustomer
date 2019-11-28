package handover

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingBroadcastTest {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("broadcasttest").master("local[*]").getOrCreate()
    sparkSession.conf.set("charset","utf-8")
    val sc=sparkSession.sparkContext
    val ssc=new StreamingContext(sc,Seconds(10))
    import sparkSession.implicits._


    val inputStream=ssc.socketTextStream("10.1.24.134",19999).map(line=>{
     val touple=line.split(",")
      (touple(0),touple(1))
    })

    inputStream.transform(rdd=>{
      rdd.map(line=>{
        val name=line._1
        val sex=line._2
        (name,sex)
      }).toDF("name","sex")


      rdd
    })
//    .foreachRDD(rdd=>{
//      val data1=sc.textFile(path)
//      val data2=data1.map(line=>{
//        val files=line.toString.split(",")
//        val name=files(0)
//        val age=files(1)
//        (name,age)
//      }).toDF("name","age")
//     val dataBroadcast=sc.broadcast(data2)
//     val data6=rdd.map(y=>{
//        val x=y.split(",")
//        val name=x(0)
//        val sex=x(1)
//        (name,sex)
//      }).toDF("name","sex")
//      data6.join(dataBroadcast.value,"name").show()
//    })
    ssc.start()
    ssc.awaitTermination()
  }

  def brocastTable(spark:SparkSession){

    val path="file:\\H:\\filetest\\a.txt"
          val data1=spark.sparkContext.textFile(path)
          val data2=data1.map(line=>{
            val files=line.toString.split(",")
            val name=files(0)
            val age=files(1)
            (name,age)
          })
         val dataBroadcast=spark.sparkContext.broadcast(data2)
  }

}

