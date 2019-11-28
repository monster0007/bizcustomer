import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2019/6/17 0017.
  */

object EXTCustome {

  def main(args: Array[String]) {
    val  spark= SparkSession.builder().master("local[*]").appName("read mysql to hive").getOrCreate()

    //230,本地，以及208的mysql的链接URL
    val url230="jdbc:mysql://10.1.24.230:3306/"
    //val url218="jdbc:oracle:thin:@10.1.24.218:1521:sjhterp?"
    val url208="jdbc:mysql://10.1.24.208:3306/bicon_pcd?"
    val urllocal="jdbc:mysql://10.1.24.208:3306/automation?"
    //三个库的登录用户密码
    //230的
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","bicon@123")
    //208的
    val prop1 =new Properties()
    prop1.setProperty("user","root")
    prop1.setProperty("password","bicon@123")
    //218的oracle
    /*   val prop2 =new Properties()
       prop2.setProperty("user","zserp")
       prop2.setProperty("password","zserp")*/
    //本地的
    val prop3 =new Properties()
    prop3.setProperty("user","root")
    prop3.setProperty("password","root")

    //自定义得拼接字符串
    spark.udf.register("concat",(str1:String,str2:String)=>{
      str1.concat(str2)
    })

    spark.udf.register("pronull",(str:String)=>{
      try{
        if(str.equals("")){
          null
        }else{
          str
        }
      }catch {
        case e:NullPointerException=>null
      }
    })
    spark.udf.register("updatep1",(str:String)=>{
      try{
        if(str.contains("省")){
          str.split("省")
        }else{
          null
        }
      }catch {
        case e:NullPointerException=>null
      }
    })
    //update3筛选父id为0的地级市
    /* spark.udf.register("updatec3",(str2:String,str3:String)=>{
       try{
         if(str3!=0){
           val str3=str2
           val sss=spark.sql("select id,pid from area where id="+str3)
           sss
         }else{
           str2
         }
       }catch {
         case e:NullPointerException=>null
       }
     })*/
    /* def updatec4(f:String=>String):String={
       val sss=spark.sql("select id,pid from area where id="+f)
       if(sss.select("pid")!=0){
        val b=sss.select("pid")
         b.toString()
       }else {
         val c=sss.select("id")
           c.toString()
       }
     }*/
    spark.udf.register("updatec5",(str:String)=>{
      if(str==null){
        ""
      }else{
        val sss1=spark.sql("select id,pid from area where id="+str)
        val a1=sss1.select("pid").toString().toInt
        if(a1<34){
          val c=sss1.select("id")
          c.toString()
        }else {
          val sss2=spark.sql("select id,pid from area where id="+a1)
          val a2=sss2.select("pid").toString().toInt
          if(a2<34){
            val c2=sss2.select("id")
            c2.toString()
          }else{
            val sss3=spark.sql("select id,pid from area where id="+a2)
            val a3=sss3.select("pid").toString().toInt
            if(a3<34){
              val c3=sss3.select("id")
              c3.toString()
            }else{
              val sss4=spark.sql("select id,pid from area where id="+a3)
              val a4=sss4.select("pid").toString()
              a4
            }
          }
        }}
    })
    //处理没有市字段的city
    spark.udf.register("updatec2",(str:String)=>{
      try{
        val s4=str.indexOf("市")
        val s3=str.indexOf("区")
        val s1=str.indexOf("县")
        val s2=str.indexOf("镇")
        if(s4>0){
          val tmp4 = str.substring(0,s4+1)
          tmp4
        }else if(s3>0){
          val tmp3 = str.substring(0,s3+1)
          tmp3
        }else if(s1>0){
          val tmp1 = str.substring(0,s1+1)
          tmp1
        }else if(s2>0){
          val tmp2 = str.substring(0,s2+1)
          tmp2
        }else{
          null
        }
      }catch {
        case e:NullPointerException=>null
      }
    })
    //先判断4个直辖市的区，如果有区就用区，如果没有就用市
    //再判断其他省的市，拿到市字段
    spark.udf.register("updatec1",(str:String)=>{
      try{
        if(str.matches("北京市.+?|重庆市.+?|天津市.+?|上海市.+?")){
          val p1=str.indexOf('区')
          val p3=str.indexOf('市')
          if(p1>0){
            if(p1>p3){
              val tmp1 = str.substring(0,p1+1)
              val tmp2=tmp1.substring(p3+1)
              tmp2
            }else{
              val tmp4=str.substring(0,p1+1)
              tmp4}
          }else{
            val tmp1 = str.substring(0,p3+1)
            tmp1
          }
        }else {
          val p1 = str.indexOf('市')
          if (p1 > 0) {
            val tmp = str.substring(0, p1 + 1)
            /* val tmp1=str.replaceAll(tmp,"")
           val res=tmp1.substring('市',1)
           res*/
            val p2 = tmp.indexOf('省')
            if (p2 > 0) {
              val tmp1 = tmp.substring(p2 + 1)
              tmp1
            } else {
              tmp
            }
          } else {
            null
          }
        }
      }catch {
        case e:NullPointerException=>null
      }
    })
    //修改客户类型
    spark.udf.register("updatetype",(str1: String,str2:String)=>{
      if (str1==null){
        ""
      }else {
        if (str2.matches("^1000.+?")) {
          str1.trim match {
            case "1" => "商业"
            case "10" => "商业"
            case "8" => "医疗机构"
            case "9" => "医疗机构"
            case "2" => "医疗机构"
            case "4" => "二终端"
            case "3" => "三终端"
            case "5" => "三终端"
            case "16" => "三终端"
            case "6" => "三终端"
            case "17" => "三终端"
            case "11" => "三终端"
            case "7" => "三终端"
            case "12" => "工业"
            case _=>"其他"
          }
        } else if (str2.matches("^2000.+?")) {
          str1.trim match {
            case "23" => "商业"
            case "22" => "三终端"
            case "26" => "三终端"
            case "24" => "二终端"
            case "27" => "三终端"
            case "25" => "三终端"
            case "28" => "医疗机构"
            case "20" => "其他"
            case "29" => "其他"
            case "9" => "其他"
            case _=>"其他"
          }
        }else if (str2.matches("^3000.+?")) {
          str1.trim match {
            case "生产企业" => "工业"
            case "经营企业" => "商业"
            case "医疗机构" => "医疗机构"
            case _=>"其他"
          }
        }else if (str2.matches("^4000.+?")) {
          str1.trim match {
            case "0" => "医疗机构"
            case "1" => "医疗机构"
            case "2" => "商业"
            case "3" => "三终端"
            case "4" => "医疗机构"

            case "5" => "三终端"
            case "6" => "三终端"
            case "7" => "医疗机构"
            case "8" => "医疗机构"
            case "9" => "三终端"
            case "10" => "三终端"
            case "11" => "三终端"
            case _=>"其他"
          }
        }else if (str2.matches("^n5000.+?")) {
          str1.trim match {
            case "批发企业" => "商业"
            case "乡卫生院" => "三终端"
            case "农村诊所" => "三终端"
            case "生产企业" => "工业"
            case "零售及连锁" => "三终端"
            case "医疗机构" => "医疗机构"
            case _=>"其他"
          }
        }
        else if (str2.matches("^6000.+?")) {
          str1.trim match {
            case "1" => "二终端"
            case "2" => "三终端"
            case "3" => "医疗机构"
            case "4" => "医疗机构"
            case "5" => "三终端"
            case "6" => "三终端"
            case "7" => "商业"
            case "8" => "三终端"
            case "9" => "三终端"
            case _=>"其他"
          }
        }
        else if (str2.matches("^7000.+?")) {
          str1.trim match {
            case "0" => "商业"
            case "1" => "医疗机构"
            case "2" => "医疗机构"
            case "3" => "三终端"
            case "4" => "三终端"
            case "5" => "工业"
            case _=>"其他"
          }
        }else {
          ""
        }

      }})
    //读取各自库下的表
    val customer=spark.read.jdbc(url230+"biz_rx_new","bs_customer",prop)
    val city=spark.read.jdbc(url230+"biz_rx_new","base_city",prop)
    val province=spark.read.jdbc(url230+"biz_rx_new","base_province",prop)
    val u_custom=spark.read.jdbc(url230+"biz_xh","u_custom",prop)
    val infowldw=spark.read.jdbc(url230+"biz_bc","infowldw",prop)
    val tb_busimate=spark.read.jdbc(url230+"biz_kl","tb_busimate",prop)
    val jcksxx=spark.read.jdbc(url230+"biz_lzn","基础_客商信息",prop)
    val xy1=spark.read.jdbc(url230+"biz_xy","businessdoc",prop)
    val xy2=spark.read.jdbc(url230+"biz_xy","clientdoc",prop)
    val province_city=spark.read.jdbc(urllocal,"province_city",prop)
    val area=spark.read.jdbc(urllocal,"area",prop)
    //原218的oracle四季汇通客户
    /*val SJCustomer=spark.read.format("jdbc")
       .option("url","jdbc:oracle:thin:@10.1.24.218:1521:ORCL")
       .option("dbtable","bs_customer")
       .option("user","sjhterp")
       .option("password","sjhterp").load()*/
    val SJCustomer=spark.read.jdbc(url230+"biz_sjht","bs_customer",prop)
    //  val aff=spark.read.jdbc(url218,"aff",prop2)
    //230创建各自库下的表的临时表
    customer.createOrReplaceTempView("bs_customer")
    city.createOrReplaceTempView("base_city")
    province.createOrReplaceTempView("base_province")
    u_custom.createOrReplaceTempView("u_custom")
    infowldw.createOrReplaceTempView("infowldw")
    tb_busimate.createOrReplaceTempView("tb_busimate")
    jcksxx.createOrReplaceTempView("jcksxx")
    SJCustomer.createOrReplaceTempView("SJCustomer")
    xy1.createOrReplaceTempView("businessdoc")
    xy2.createOrReplaceTempView("clientdoc")
    province_city.createOrReplaceTempView("province_city")

    area.createOrReplaceTempView("area")
    //   aff.createOrReplaceTempView("aff")
    //读取208最新的客户和关系表
    val pcd_custom_relationship=spark.read.jdbc(url208,"pcd_custom_relationship_105862_20190613",prop1)
    //208读取表的临时表
    pcd_custom_relationship.createOrReplaceTempView("pcd_custom_relationship")


    //读取润祥客户的8个字段，挑出新增客户
    val a1=spark.sql("SELECT concat(1000,a.CUSTOMERID) as id,a.CUSTOMERNAME as customname,a.CUSTOMERTYPE as customtype,a.REGISTERADDR as address,c.PROVINCENAME as province,b.cityname as city,a.LEGALPERSON as contactname,a.CONTACT as phone FROM  `bs_customer` a  left join base_city b on a.cityid=b.CITYID left join base_province c on a.PROVINCEID=c.PROVINCEID")
    a1.createOrReplaceTempView("rx_customer_source")
    val DF1=spark.sql("select * from rx_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
    //读取鑫和客户的8个字段，挑出新增客户
    val a2=spark.sql("SELECT concat(2000,customid) as id, customname,customtype,address,'' as province,'' as city,ordername as contactname,ordertel as phone from u_custom")
    a2.createOrReplaceTempView("xh_customer_source")
    val DF2=spark.sql("select * from xh_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
    //读取百川客户的8个字段，挑出新增客户
    val a3=spark.sql("SELECT concat(3000,WangLDWID) as id,WangLDWMC as customname,fenl as customtype,ShouHDZ as address,province,city,ShouHR as contactname,ShouHDH as phone from infowldw")
    a3.createOrReplaceTempView("bc_customer_source")
    val DF3=spark.sql("select * from bc_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
    //读取康力客户的8个字段，挑出新增客户
    val a4=spark.sql("SELECT concat(4000,mate_id) as id,mate_name as customname,cust_type as customtype,address,'' as province,'' as city,deputy as contactname,phone   from tb_busimate")
    a4.createOrReplaceTempView("kl_customer_source")
    val DF4=spark.sql("select * from kl_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
    //读取新绿洲客户的8个字段，挑出新增客户
    val a5=spark.sql("select concat('n5000',id) as id,`名称` as customname,`企业类型` as customtype,`地址` as address,`固定电话` as gudin,`联系人` as contactname,`移动电话` as phone from jcksxx")
    a5.createOrReplaceTempView("lzn_customer_source1")
    val a51=spark.sql("select id,customname,customtype,address,'' as province,'' as city,contactname,ifnull(phone,gudin) as phone  from lzn_customer_source1")
    a51.createOrReplaceTempView("lzn_customer_source")
    val DF5=spark.sql("select * from lzn_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
    //读取四季汇通客户的8个字段，挑出新增客户
    val a6=spark.sql("SELECT concat(6000,customerid) as id,customername as customname,customertype as customtype,registeraddr as address,'' as province,'' as city,legalperson as contactname,contact_number as phone   from SJCustomer")
    a6.createOrReplaceTempView("sj_customer_source")
    val DF6=spark.sql("select * from sj_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")
    //读取新阳客户的8个字段，挑出新增客户
    val a7=spark.sql("SELECT concat(7000,a.BUSINESSID) as id,a.BUSINESSNAME as customname,b.CLIENTTYPE as customtype,a.ADDRESS as address,'' as province,'' as city,a.CONTACT as contactname,a.TELEPHONE as phone FROM businessdoc a left join clientdoc b on a.BUSINESSID=b.CLIENTID")
    a7.createOrReplaceTempView("xy_customer_source")
    val DF7=spark.sql("select * from xy_customer_source a where not exists (select b.customid from pcd_custom_relationship b where a.id=b.customid)")

    val DFALL=DF1.union(DF2).union(DF3).union(DF4).union(DF5).union(DF6).union(DF7)
    DFALL.createOrReplaceTempView("pcd_new_customer_city_empty")
    //先把表中省和市为空字符串的改为null
    val nullcity=spark.sql("select id,customname,updatetype(customtype,id) customtype ,address,pronull(province) as province,pronull(city) as city,contactname,phone from pcd_new_customer_city_empty")
    nullcity.createOrReplaceTempView("pcd_new_customer")
    //抽出省和市为空的
    //省和市不为空
    val is_city=spark.sql("select * from pcd_new_customer where province is not null and city is not null")
    is_city.createOrReplaceTempView("have_city")
    //省和市为空
    val no_city=spark.sql("select * from pcd_new_customer where province is null or city is null")
    no_city.createOrReplaceTempView("no_province_city")


    //is_city.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_is_city",prop3)
    //val no_city_no=spark.sql("select * from no_province_city where city like '%省%' and city like '%市%'")
    //将那些地址中有省市的，拿到市字段
    val b11=spark.sql("select id,customname,customtype,address,'' as province,updatec1(address) as city,contactname,phone from no_province_city")
    b11.createOrReplaceTempView("may_have_city")

    val unoincity=spark.sql("select * from may_have_city union select * from have_city")
    unoincity.createOrReplaceTempView("unoincity")
    val have_province=spark.sql("select a.id,a.customname,a.customtype,a.address,b.province,a.city,a.contactname,a.phone from  unoincity a left join province_city b on a.city=b.city")
    // unoincity.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_unoincity",prop3)
    val null_pipei_province=have_province.select("id","customname","customtype","address","province","city","contactname","phone").where("province is null")
    val pipei_province=have_province.select("id","customname","customtype","address","province","city","contactname","phone").where("province is not null")
    null_pipei_province.createOrReplaceTempView("null_pipei_province")
    val null_province=spark.sql("select id,customname,customtype,address,province,updatec2(address) as city,contactname,phone from null_pipei_province")

    null_province.createOrReplaceTempView("null_province")
    val np1=spark.sql("select a.id,a.customname,a.customtype,a.address,a.province,updatec5(b.citycode) as cityid,a.contactname,a.phone from null_province a left join area b on a.city=b.name")
    np1.createOrReplaceTempView("np1")
    np1.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_unoincity114",prop)
    // np1.select("cityid")
    //  spark.sql("select a.id,a.customname,a.customtype,a.address,a.province,b.name,a.contactname,a.phone from np1 a left join area b on a.cityid=b.id").write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_unoincity114",prop3)








    // null_province.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_unoincity113",prop3)
    // write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_unoincity112",prop3)

    // have_province.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_unoincity1111",prop3)
    //  unoincity.where("city like '%区%'").limit(10).show()
    //处理完后，客户本身去重的时候，按照 名称、省、市三个维度
    //spark.sql("          ")












    // DFALL.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_20190620_ALL",prop3)

    /*DF3.write.mode("overwrite").jdbc(urllocal,"pcd_new_customer_20190620",prop3)
      DF1.write.mode("append").jdbc(urllocal,"pcd_new_customer_20190620",prop3)
      DF2.write.mode("append").jdbc(urllocal,"pcd_new_customer_20190620",prop3)
      DF4.write.mode("append").jdbc(urllocal,"pcd_new_customer_20190620",prop3)
      DF5.write.mode("append").jdbc(urllocal,"pcd_new_customer_20190620",prop3)*/

  }

}
