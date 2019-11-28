package handover

/**
  * 这个是有两个构造器的对象，主构造器是name,辅助是name+age 伴生对象的话是主构造器生成的对象
  * @param name
  */
class Student( var name:String) {
  private var myage:Int = _
  def this(name:String,age:Int){
    this(name)
    this.myage=age
  }
  //getter
  def age={
    myage
  }
//  //setter
  def age_=(newage:Int): Unit ={
    myage=newage
  }
  override def toString = s"Student($name, $age)"
}
object Student{
  def apply(name:String): Student = new Student(name)
}

