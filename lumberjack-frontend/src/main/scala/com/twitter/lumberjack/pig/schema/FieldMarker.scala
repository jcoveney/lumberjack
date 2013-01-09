package com.twitter.lumberjack.pig.schema

/**
 * Created with IntelliJ IDEA.
 * User: jcoveney
 * Date: 1/7/13
 * Time: 9:20 PM
 * To change this template use File | Settings | File Templates.
 */
trait FieldMarker {
  def generateField:String
}
case class SimpleFieldMarker(name:String, typeName:String, number:Int) extends FieldMarker {
  override def generateField:String = "def %s:%s = _tuple.get(%d).asInstanceOf[%2$s]".format(name, typeName, number)
}
case class MapFieldMarker(name:String, typeName:String, number:Int) extends FieldMarker {
  override def generateField:String = "def %s:%s = %2$s(_tuple.get(%d).asInstanceOf[Map[String,Object]])".format(name, typeName, number)
}
case class BagFieldMarker(name:String, typeName:String, number:Int) extends FieldMarker {
  override def generateField:String = "def %s:%s = %2$s(_tuple.get(%d).asInstanceOf[DataBag])".format(name, typeName, number)
}
case class TupleFieldMarker(name:String, typeName:String, number:Int) extends FieldMarker {
  override def generateField:String = "def %s:%s = %2$s(_tuple.get(%d).asInstanceOf[Tuple])".format(name, typeName, number)
}