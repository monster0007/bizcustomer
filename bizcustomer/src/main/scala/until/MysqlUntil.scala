package until

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlUntil {
  /*
    从Mysql获取数据
     */
  def getFromMysql(spark:SparkSession,url:String,databaseName:String,userName:String,password:String,sqltxt:String): DataFrame ={
    val jdbcDF = spark.read.format("jdbc")
      .option("url", url + databaseName)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "("+sqltxt+") t")
      .option("user", userName)
      .option("password", password).load()
    jdbcDF
  }

  /*
  向Mysql写数据
   */

  def WriterToMysql(spark:SparkSession,ipAdress:String,userName:String,password:String,database:String,tableName:String,resDf:DataFrame,mode:String): Unit ={
    val props=new Properties()
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    props.setProperty("user", userName)
    props.setProperty("password", password)
    resDf.write.mode(mode).jdbc("jdbc:mysql://"+ipAdress+"/"+database+"?useUnicode=true&characterEncoding=utf8",tableName,props)
  }
}
