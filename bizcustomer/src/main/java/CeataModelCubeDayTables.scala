import org.apache.spark.sql.SparkSession

/**
  * 每月构建商业公司 model 和cube 时首先执行此类
  * 用于在hive中创建 对应的商业和汇总需要用的表
  * sys_customer.sys_cus_sy_20200101~31
  * sys_customer.sys_cus_hz_20200101~31
  */

class CeataModelCubeDayTables {

}
object  CeataModelCubeDayTables{

  def main(args: Array[String]): Unit = {
    val spark = getSparksqlOnHive()
    val sys_cus_sy = "sys_cus_sy_"
    val sys_cus_hz = "sys_cus_hz_"
    val lastMonth = "202001"
    val currentMonth = "202002"
    val start =  1
    val end = 29

    //指定客户类型
    val customeFlag = sys_cus_hz
    //商业公司每日客户表
    createtable_sys_cus_day(spark,customeFlag,lastMonth,currentMonth,start,end)
    //每月客户表
    createtable_sys_cus_month(spark,customeFlag,lastMonth,currentMonth)
  }


  /**
    * 在 sys_customer 创建每日商业公司客户表  和 汇总客户 sys_cus_sy_ and sys_cus_hz_
    *
    */
  def createtable_sys_cus_day(spark : SparkSession,customeFlag : String ,lastMonth : String,currentMonth : String,start : Int, end : Int): Unit ={
    val s ="0"
    val st ="01"
    var sql:String = ""
      s"""
        |create table sys_customer.sys_cus_sy_$currentMonth like sys_customer.sys_cus_sy_$lastMonth$st
      """.stripMargin
    val i  = 0
    for(i <- start to end){
      if (i <10 ) sql  = s"create table sys_customer.$customeFlag$currentMonth$s$i like sys_customer.$customeFlag$lastMonth$st"
      else sql = s"create table sys_customer.$customeFlag$currentMonth$i like sys_customer.$customeFlag$lastMonth$st"
      println(i+sql)
      spark.sql(sql)
    }
  }

  /**
    *
    * @param spark
    * @param customeFlag
    * @param lastMonth
    * @param currentMonth
    */
  def createtable_sys_cus_month(spark : SparkSession,customeFlag : String ,lastMonth : String,currentMonth : String): Unit ={
    val sqlsy = s"create table sys_customer.$customeFlag$currentMonth like sys_customer.$customeFlag$lastMonth"
    println(sqlsy)
    spark.sql(sqlsy)
  }




    /**
    *
    * @return
    */
  def getSparksqlOnHive(): SparkSession ={
    val spark  = SparkSession.builder().master("local").appName("createtable_sys_cus_sy").enableHiveSupport().getOrCreate()
    spark
  }
}