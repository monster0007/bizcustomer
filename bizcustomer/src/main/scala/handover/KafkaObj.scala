package handover

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange

class KafkaObj() {
  private  var offsetRanges:Array[OffsetRange] = _
  private  var stream:DStream[(String,String,Long)]= _

  def this(stream:DStream[(String,String,Long)]){
    this()
    this.stream=stream
  }
  def this(og:Array[OffsetRange],stream:DStream[(String,String,Long)]){
    this()
    this.offsetRanges=og
    this.stream=stream
  }
  def offrs=offsetRanges;
  def offrs_=(newoffrs:Array[OffsetRange])={
    offsetRanges=newoffrs
  }
  def mystream=stream
  def mystream_=(newStream:DStream[(String,String,Long)]): Unit ={
    stream=mystream
  }
}
object KafkaObj{
  def apply(og:Array[OffsetRange],stream:DStream[(String,String,Long)]): KafkaObj = new KafkaObj(og,stream)
}
