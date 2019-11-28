package repair.test

class ExceptionExample {
  def divide(a:Int, b:Int) = {
    try{
      a/b
    }catch{
      case e: ArithmeticException => println(e)
    }
    println("Rest of the code is executing...")
  }
}
object Demo{
  def main(args:Array[String]){
    var e = new ExceptionExample()
    e.divide(100,0)
  }
}
