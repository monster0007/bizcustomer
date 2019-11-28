package org.apache.spark

import org.apache.spark.rdd.RDD

object RDDUtilsInSpark {
  def getCheckpointRDD[T](sc:SparkContext, path:String) = {
    //path要到part-000000的父目录
    val result : RDD[Any] = sc.checkpointFile(path)
    result.asInstanceOf[T]
  }

}
