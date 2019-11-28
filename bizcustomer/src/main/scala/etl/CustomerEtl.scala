package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomerEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("yarn").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext

    val data=sc.textFile("hdfs://nameservice1/user/Kingleading/hivetedst/part-m-00000")
    val cus=data.map(lines=>{
    val customerid=lines.split(",")(0)
    })
  }

  def getFromMysql(spark:SparkSession,databaseName:String,tableName:String,userName:String,password:String): DataFrame ={
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.1.24.209:3306/" + databaseName)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password).load()
    jdbcDF
  }

}
