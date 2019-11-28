package handover


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OffsetTest {
  var topics:Array[String]=null
  var parames:Map[String,Object]=null
  val groupid="offtest1"
  var sc:SparkContext=null
  var ssc:StreamingContext=null
  var kafkaStream:InputDStream[ConsumerRecord[String,String]]=null
  var reloadFlag=0
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().master("local[*]").appName("offsettest").getOrCreate()
    sc=sparkSession.sparkContext
     ssc=new StreamingContext(sc,Seconds(10))
     topics=gettopics(sc)
     parames=getKafkaParams(groupid)
    kafkaStream=KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](topics,parames))
    getTableNameAndKey(sc,ssc,kafkaStream,groupid)
    ssc.start()
    ssc.awaitTermination()

  }

  def getKafkaParams(groupid:String):Map[String,Object]  ={
    val brokers="10.1.24.230:9092"
    val kafkaParams = scala.collection.immutable.Map[String,Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

  def getTableNameAndKey(sc:SparkContext,ssc:StreamingContext,inputStream:InputDStream[ConsumerRecord[String,String]],groupid:String) {

    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    inputStream.foreachRDD(rdd => {
        rdd.foreach(line=>{
          val fields = line.value().split(",")
          val name = fields(0)
          val age = fields(1)
          val sex = fields(2)
          println(line.topic()+"--------"+"name----"+name+"age-----"+age+"sex-----"+sex)
        })
//         val offsetranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//         inputStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)
      }
      )


             val nowDate:String=getNowDate()
    println(nowDate)
    if(nowDate.equals("2019-11-21 08:56")) {
      println("#########################################")
//                    topics=gettopics(sc)
//                    for (elem <- topics) {println("haha***************************************"+elem)}
//                    parames=getKafkaParams(groupid)
//                    kafkaStream=KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](topics,parames))
//                    getTableNameAndKey(sc,ssc,kafkaStream,groupid)
    }
    }

  def gettopics(sc:SparkContext): Array[String] ={
    val topicNamePath="file:\\H:\\b.txt"
    val whiteTableNames=sc.textFile(topicNamePath).collect()
    whiteTableNames
  }

  def getNowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val date = dateFormat.format(now)
    return date
  }

}
