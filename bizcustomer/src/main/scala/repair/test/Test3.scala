package repair.test

object Test3 {
  def main(args: Array[String]): Unit = {
    test
  }
  def test: Unit ={
   try{ val a=10/0
   val b=100/10
     println(a)
     println(b)
   }catch{
     case e:Exception=>println("分母不能为0")
   }
    val b=1/10
    println(b)
  }
}
