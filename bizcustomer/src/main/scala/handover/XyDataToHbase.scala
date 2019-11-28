package handover

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

object XyDataToHbase {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().appName("xydatatohbase").master("local[*]").getOrCreate()
    val spark=sparkSession.sparkContext
    val streamContext=new StreamingContext(spark,Seconds(5))

    val topics=Array("xy.public.cms_sale_settle","xy.public.cms_sale_settle_dtl")//getTopicNames(spark)
    val parames=getKafkaParams("tmp03")
    val kafkaStream=KafkaUtils.createDirectStream(streamContext,PreferConsistent,Subscribe[String,String](topics,parames))
    getTableNameAndKey(kafkaStream)
    streamContext.start()
    streamContext.awaitTermination()
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

  def getTopicNames(sparkContext:SparkContext): Array[String] ={
    val topicNamePath="hdfs://nameservice1/user/config/analysis_kafka_hbase/topic.txt"
    val whiteTableNames=sparkContext.textFile(topicNamePath).collect()
    whiteTableNames
  }

  def getTableNameAndKey(inputStream:InputDStream[ConsumerRecord[String,String]]) ={

    val hbaseBaseData = inputStream.map(line=>{
        var hbaseRowkey=""
        val hbaseTableName = "biz_test:"+line.topic().split("\\.")(2)
        val keyObj=JSON.parseFull(line.key()).get.asInstanceOf[Map[String,Int]]
        val keyList=keyObj.keySet.toList
        keyList.map(key=>{
          hbaseRowkey=hbaseRowkey+"|"+keyObj.get(key).getOrElse("").toString()
        })
        hbaseRowkey=hbaseRowkey.substring(1,hbaseRowkey.length)
        val jsonValue=JSON.parseFull(line.value()).get.asInstanceOf[Map[String,Any]]
        val hbaseOpretaType=jsonValue.get("op").get.toString
        val hbaseLeastData=jsonValue.get("after").get.asInstanceOf[Map[String,Any]]
      (hbaseTableName,hbaseRowkey,hbaseOpretaType,hbaseLeastData)
      })

    hbaseBaseData.foreachRDD(rdd=>{
      rdd.foreach(x=>println(x._1))
      //if(!rdd.isEmpty()){
//        rdd.foreachPartition(par=>{
//          val conf = HBaseConfiguration.create()
//          conf.set("hbase.zookeeper.quorum", "10.1.24.199,10.1.24.200,10.1.24.201")
//          conf.set("hbase.zookeeper.property.clientPort", "2181")
//          conf.set("hbase.defaults.for.version.skip", "true")
//          var connection:Connection = null
//          connection = ConnectionFactory.createConnection(conf)
//          par.foreach(hbaseline=>{
//            val hbaseTableName=hbaseline._1
//            val hbaseRowkey=hbaseline._2
//            val hbaseOpretaType=hbaseline._3
//            val hbaseLeastData=hbaseline._4
//            val table:TableName=TableName.valueOf(hbaseTableName)
//            val tableCon=connection.getTable(table)
//            val put=new Put(Bytes.toBytes(hbaseRowkey))
//            if (hbaseOpretaType.matches("[urc]")){
//              hbaseLeastData.keySet.map(fieldkey=>{
//                val fieldValuse=hbaseLeastData.get(fieldkey)
//                fieldValuse match{
//                  case Some(null)=>put.addColumn("cf".getBytes("utf-8"),fieldkey.getBytes("utf-8"),"".getBytes("utf-8"))
//                  case None=>put.addColumn("cf".getBytes("utf-8"),fieldkey.getBytes("utf-8"),"".getBytes("utf-8"))
//                  case _=>put.addColumn("cf".getBytes("utf-8"),fieldkey.getBytes("utf-8"),fieldValuse.get.toString.getBytes("utf-8"))
//                }
//              })
//              tableCon.put(put)
//            }else if(hbaseOpretaType.equals("d")){
//              val delete = new Delete(Bytes.toBytes(hbaseRowkey))
//             tableCon.delete(delete)
//            }
//            tableCon.close()
//          })
//        })
     // }
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      hbaseBaseData.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      println("this is hello")
    }
    )
    }
}
