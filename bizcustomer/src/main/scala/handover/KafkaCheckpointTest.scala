package handover

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * spark读取kafka后offset存储的几种方式
  * 1、auto.commmit设为true,每次拉取数据一定时间后自动提交offset到kafka本身
  * 2、手动提交
  *   2.1 提交到kafka
  *   2.2 提交到driver的checkpoint,但这种只有代码出错自动重试时有用,代码重跑是不行的,不适合生产
  *   2.3 提交到mysql
  *   2.4 提交到hbase
  */
object KafkaCheckpointTest {
  def main(args: Array[String]): Unit = {
  // val streamingContext= StreamingContext.getOrCreate("file:\\H:\\checkpoint2",functionToCreateContext)
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaDirectTest")
    val sparkContext=new SparkContext(sparkConf)
    val streamingContext=new StreamingContext(sparkContext,Seconds(10))
    val inputStream=getKafkaStream(streamingContext)
    saveToKafka(inputStream)
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
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

  /**
    * offset存储到checkpoint
    * @return
    */
  def functionToCreateContext():StreamingContext={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaDirectTest")
    val sparkContext=new SparkContext(sparkConf)
    sparkContext.setLogLevel("error")
    val ssc =  new StreamingContext(sparkContext, Seconds(10))
    val stream=getKafkaStream(ssc)

    ssc.checkpoint("file:///H://checkpoint2")
    ssc
  }

  /**
    * 异步方式提交到kafka，没有同步提交
    * @param result
    */
  def saveToKafka(stream:InputDStream[ConsumerRecord[String,String]])={
    stream.foreachRDD(rdd=>{
      val rdd1=rdd.map(line=>{
        val topic=line.topic()
        val value=line.value()
        val offsets=line.offset()
        (topic,value,offsets)
      })
      rdd1.foreach(println)
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      offsetRanges.foreach(r=>{
        println(r.fromOffset+"-----"+r.untilOffset)
      })
    })
  }

  /**
    * offset提交到mysql
    */
  def saveToMysql(stream:InputDStream[ConsumerRecord[String,String]]): Unit ={

  }

  /**
    * Inputstream流获取
    * @param ssc
    * @return
    */
  def getKafkaStream(ssc:StreamingContext): InputDStream[ConsumerRecord[String,String]] ={
    val parames=getKafkaParams("test01")
    val topics=Array("offsettomysqltest")
    var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topics,parames))
    stream
  }

}
