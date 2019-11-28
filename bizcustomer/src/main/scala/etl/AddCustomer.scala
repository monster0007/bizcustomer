package etl
import java.util.NoSuchElementException

import until.MysqlUntil.{WriterToMysql, getFromMysql}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AddCustomer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("writToCusDemssion").master("local[1]").enableHiveSupport().config("file.encoding", "UTF-8").getOrCreate()
    val sc = spark.sparkContext
    val res=CustomerDeal(spark)//得出最新客户，同时把最新客户的标打进原标
    val relSql="select * from sys_customer.addcustomer"
    val relDatd=getFromMysql(spark,"jdbc:mysql://10.1.24.209:3306/","sys_customer","root","123456",relSql)
    val relRes=GetCusRel(spark,relDatd)//落最新客户的关系表
    WriterToMysql(spark,"10.1.24.209","root","123456","sys_customer","addcustomer",res,"append")
    WriterToMysql(spark,"10.1.24.209","root","123456","sys_customer","addcustomerrel",relRes,"append")
  }

  def CustomerDeal(spark:SparkSession): DataFrame ={
    val userName="root"
    val passwd="bicon@123"
    val url="jdbc:mysql://10.1.24.230:3306/"
    val markSql = "select * from addcustomer"
    val maxMark = getFromMysql(spark,url,"addcustomer",userName,passwd,markSql)//从mysql读取每个公司的当前最大客户值
    var map = scala.collection.mutable.Map[String,String]()
    val markArr = maxMark.rdd.collect()//DataFrame转成数组
    markArr.map(lines=>{
      val line=lines.toString().replace("[","").replace("]","").split(",")
      map+=(line(0)->line(1))
    })

      //润祥新增客户
         var rxSql = "SELECT 1000 accountid,a.CUSTOMERID,a.CUSTOMERNAME,IFNULL(a.CUSTOMERTYPE,'-1') CUSTOMERTYPE,a.REGISTERADDR,a.LEGALPERSON,a.CONTACT,c.PROVINCENAME,b.CITYNAME FROM  bs_customer a left join base_city b on a.cityid=b.CITYID left join base_province c on a.PROVINCEID=c.PROVINCEID where CUSTOMERID>" + map("1000")
       val rxCustomer = getFromMysql(spark, url, "biz_rx_new", userName, passwd, rxSql)
      //鑫和新增客户
      val xhSql = "SELECT 2000 accountid,customid,customname,ifnull(customtype,'-1') customtype,address,ordername,ordertel,'湖南' PROVINCENAME,'长沙' CITYNAME FROM u_custom where customid >" + map("2000")
      val xhCustomer = getFromMysql(spark, url, "biz_xh", userName, passwd, xhSql)
      //百川新增客户
      val bcSql = "select 3000 accountid,WangLDWID,WangLDWMC,ifnull(fenl,'-1') fenl,ShouHDZ,ShouHR,ShouHDH,province,city from infowldw where WangLDWID >" + "\"" + map("3000") + "\""
      val bcCustomer = getFromMysql(spark, url, "biz_bc", userName, passwd, bcSql)
      //康利新增客户
      val klSql = "select 4000 accountid,mate_id,mate_name,ifnull(cust_type,'-1') cust_type,address,deputy,phone,'江西' PROVINCENAME,'樟树' CITYNAME from tb_busimate where mate_id >" + map("4000")
      val klCustomer = getFromMysql(spark, url, "biz_kl", userName, passwd, klSql)
      //绿洲n新增客户
      val lznSql = "select 'n5000' accountid,id,名称,ifnull(企业类型,'-1') 企业类型,地址,联系人,移动电话,'青海' PROVINCENAME,'西宁' CITYNAME from 基础_客商信息 where id>" + map("n5000")
      val lznCustomer = getFromMysql(spark, url, "biz_lzn", userName, passwd, lznSql)
      //绿洲s新增客户
      var lzsSql = "select 's5000' accountid,id,名称,ifnull(企业类型,'-1') 企业类型,地址,联系人,移动电话,'青海' PROVINCENAME,'西宁' CITYNAME from 基础_客商信息 where id>" + map("s5000")
    val lzsCustomer = getFromMysql(spark, url, "lzs", userName, passwd, lzsSql)
      //四季新增客户
      val sjSql = "SELECT 6000 accountid,a.CUSTOMERID,a.CUSTOMERNAME,ifnull(a.CUSTOMERTYPE,'-1') CUSTOMERTYPE,a.REGISTERADDR,a.LEGALPERSON,CONTACT_NUMBER,'北京' PROVINCENAME,'朝阳区' CITYNAME FROM  bs_customer a where CUSTOMERID > " + map("6000")
      val sjCustomer = getFromMysql(spark, url, "biz_sjht", userName, passwd, sjSql)
      //新阳新增客户
      val xySql = "select 7000 accountid,a.BUSINESSID,a.BUSINESSNAME,ifnull(b.CLIENTTYPE,'-1') CLIENTTYPE,a.ADDRESS,a.CONTACT,a.TELEPHONE,'江苏' PROVINCENAME,'新沂' CITYNAME  FROM businessdoc a left join clientdoc b on a.BUSINESSID=b.CLIENTID where a.BUSINESSID >" + "\"" + map("7000") + "\""
      val xyCustomer = getFromMysql(spark, url, "biz_xy", userName, passwd, xySql)
      val cusCount = rxCustomer.union(bcCustomer).union(xhCustomer).union(klCustomer).union(lznCustomer).union(lzsCustomer).union(sjCustomer).union(xyCustomer)

    val cusTypeSql="select * from customertype_rel"
    val url2="jdbc:mysql://10.1.24.209:3306/"
    val customerTypeDf=getFromMysql(spark,url2,"sys_customer",userName,"123456",cusTypeSql)
    cusCount.createOrReplaceTempView("bus_customer")
    customerTypeDf.createOrReplaceTempView("cus_type")

    //得到的结果写到客户信息表
    val res=spark.sql(
      """
        |select a.accountid,a.CUSTOMERID,a.CUSTOMERNAME,t.customertype,a.REGISTERADDR,a.LEGALPERSON,a.CONTACT,a.PROVINCENAME,a.CITYNAME from bus_customer a
        |join cus_type t on a.accountid=t.accountid and a.CUSTOMERTYPE=t.source_CUSTOMERTYPE
      """.stripMargin)
    //获取每个商业公司的客户取到哪个了，然后写到标记表
    val maxRes=spark.sql(
      """
        |select accountid,max(customerid) max_customerid from bus_customer group by accountid
      """.stripMargin)
    maxRes.createOrReplaceTempView("maxRes")
    maxMark.createOrReplaceTempView("maxMark")
   val markResult= spark.sql(
      """
        |select
        |case when f2.accountid is null
        |then f1.accountid
        |else f2.accountid
        |end as accountid,
        |case when f2.max_customerid is null
        |then f1.max_customerid
        |else f2.max_customerid
        |end as max_customerid
        |from maxMark f1 left join
        |maxRes f2 on f1.accountid=f2.accountid
      """.stripMargin)
markResult.show()

    WriterToMysql(spark,"10.1.24.230","root","bicon@123","addcustomer","addcustomer",markResult,"overwrite")
    res
  }

  //把新增的客户关系表也落到一个临时表里
  def GetCusRel(spark:SparkSession,addCustomer:DataFrame): DataFrame ={
   addCustomer.createOrReplaceTempView("addcustomer")
   val relres= spark.sql(
      """
        |select concat(accountid,customerid) customerid,id pcd_id from addcustomer
      """.stripMargin)
    relres
  }


}
