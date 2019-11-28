package repair.test

import java.util.Date
import java.text.{ParsePosition, SimpleDateFormat}
import java.util.Calendar

object Test {
  def main(args: Array[String]): Unit = {
    println(getDate(19))//60 55已跑，5-02result还没跑
    println(getDate(0))
    println(getDate(0))
    println(getDate(103))
    println(getDate(44))
    println(getDate(134))
    println(getMonth(5))

    println (getnityDate("2019-03-01",90))
   println(getYesterdayMonth()+"@@@@@@@@@@@")
println(getAimMonth(24))
    println(getAimMonth(25))

  }
  def getDate(day:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -day)
    var date = dateFormat.format(cal.getTime())
    date
  }

  def getMonth(month:Int):String={
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -month)
    var mon = dateFormat.format(cal.getTime())
    mon
  }

  /*
  获取昨天所属的月份
   */
  def getYesterdayMonth():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  /*
  获取某个日期90天前的日期
   */
  def getnityDate(curdate:String,day:Int): String ={
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val par= new ParsePosition(0)
    val date:Date=dateFormat.parse(curdate,par)//转date类型
    var cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -day)
    var res = dateFormat.format(cal.getTime())
    res
  }

  def getAimMonth(num:Int):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -num)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }


}
