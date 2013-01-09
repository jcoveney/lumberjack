package com.twitter.lumberjack.pig.schema

import org.apache.pig.impl.logicalLayer.schema.Schema
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema
import org.apache.pig.data.DataType
import collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: jcoveney
 * Date: 1/7/13
 * Time: 9:04 PM
 * To change this template use File | Settings | File Templates.
 */
object SchemaGenObject {

//TODO add toString to the object so people can mess around with it and see what's up, though code completion will be nice
val imports =
"""import scala.collection.Iterator
import org.apache.pig.data.{Tuple, DataBag}
import java.util.{Iterator => jIterator}"""

  val mapTemplate =
"""object %1$s {
  def apply(m:Map[String,Object]) = new %1$s(m)
}
class %1$s private (m:Map[String,Object]) {
  def get(key:String):%2$s = %2$s(m.get(key).asInstanceOf[%3$s])
}"""

  val bagTemplate =
"""object %1$s {
  def apply(wrappedBag:DataBag) = new %1$s(wrappedBag)
}
class %1$s private (wrappedBag:DataBag) extends Iterable[%2$s] {
  override def iterator:Iterator[%2$s] = new Iterator[%2$s] {
    private val wrappedIterator:jIterator[Tuple] = wrappedBag.iterator
    override def next():%2$s = %2$s(wrappedIterator.next())
    override def hasNext:Boolean = wrappedIterator.hasNext
  }
}"""

  val tupleTemplate =
"""object %1$s {
  def apply(t:Tuple) = new %1$s(t)
}
class %1$s private (_tuple:Tuple) {
%2$s
}"""

  val baseTemplate =
"""object %1$s {
  def apply(t:Tuple) = new %1$s(t)
  lazy val getDummyWrapper = apply(null)
}
class %1$s private (_tuple:Tuple) {
%2$s
}"""

  def getTypeName(idx:Int, fs:FieldSchema, ctd:Map[String,SchemaGenObject]):(Map[String,SchemaGenObject], String) = {
    import DataType._
    fs.`type` match {
      case INTEGER => (ctd, "Int")
      case LONG => (ctd, "Long")
      case FLOAT => (ctd, "Float")
      case DOUBLE => (ctd, "Double")
      case BOOLEAN => (ctd, "Boolean")
      case CHARARRAY => (ctd, "String")
      case MAP => {
        val mapClassName = "Ljm" + idx
        val descendentClassName = mapClassName + "v"
        val ctd1 = ctd + (mapClassName -> MapGenObject(descendentClassName, "Tuple"))
        //todo need to get the key type depending on whether it is complex...for now we assume it is a tuple
        val (fields, map) = convertSchema(fs.schema.getField(0).schema)
        (ctd1 + (descendentClassName -> TupleGenObject(fields, map)), mapClassName)
      }
      case TUPLE => {
        val tupleClassName = "Ljt" + idx
        val (fields, map) = convertSchema(fs.schema)
        (ctd + (tupleClassName -> TupleGenObject(fields, map)), tupleClassName)
      }
      case BAG => {
        val bagClassName = "Ltb" + idx
        val tupleClassName = bagClassName + "t"
        val ctd1 = ctd + (bagClassName -> BagGenObject(tupleClassName))
        val (fields, map) = convertSchema(fs.schema.getField(0).schema)
        (ctd1 + (tupleClassName -> TupleGenObject(fields, map)), bagClassName)
      }
    }
  }

  def convertBaseSchema(name:String, s:Schema) = {
    val (fields, ctd) = convertSchema(s)
    BaseSchemaObject(name, fields, ctd)
  }

  def convertSchema(s:Schema) = {
    val (fieldsInRev, _, map) = {
      s.getFields.foldLeft((Nil:List[FieldMarker],0,Map():Map[String,SchemaGenObject])) {
        (next:(List[FieldMarker],Int,Map[String, SchemaGenObject]),fs:FieldSchema) =>
        val (cur, idx, ctd) = (next._1, next._2, next._3)
        val (updatedCtd, typeName) = getTypeName(idx, fs, ctd)
        import DataType._
        val fm = fs.`type` match {
          case MAP => MapFieldMarker(fs.alias, typeName, idx)
          case BAG => BagFieldMarker(fs.alias, typeName, idx)
          case TUPLE => TupleFieldMarker(fs.alias, typeName, idx)
          case _ => SimpleFieldMarker(fs.alias, typeName, idx)
        }
        (fm::cur, idx+1, updatedCtd)
      }
    }
    (fieldsInRev.reverse, map)
  }

  def generateClassCode(m:Map[String,SchemaGenObject]):List[String] = m.map(next => next._2.generateCode(next._1)).toList
  def generateFieldsCode(fields:List[FieldMarker]):List[String] = fields.map(_.generateField)
  def indent(l:List[String]):String = l.map(_.split("\\n").map("  " + _).mkString("\n")).mkString("\n")
}
//TODO since these are all referred to by their name anyway in the map, that is redundant info
abstract class SchemaGenObject(template:String) {
  def generateCode(name:String):String = generateCode(name, template)

  protected def generateCode(name:String, template:String):String
}
class MapGenObject(valueClass:String, mapValuePigClass:String) extends SchemaGenObject(SchemaGenObject.mapTemplate) {
  override def generateCode(name:String, template:String):String = template.format(name, valueClass, mapValuePigClass)
}
object BagGenObject {
  def apply(tupleClass:String) =
}
class BagGenObject(tupleClass:String) extends SchemaGenObject(SchemaGenObject.bagTemplate) {
  override def generateCode(name:String,template:String):String = template.format(name, tupleClass)
}
object TupleGenObject {
  def apply(fields:List[FieldMarker], classesToDefine:Map[String, SchemaGenObject]) = new TupleGenObject(fields, classesToDefine)
}
class TupleGenObject(fields:List[FieldMarker], classesToDefine:Map[String, SchemaGenObject]) extends SchemaGenObject(SchemaGenObject.tupleTemplate) {
  import SchemaGenObject._
  override def generateCode(name:String, template:String):String = {
    val classString = generateClassCode(classesToDefine)
    val fieldsString = generateFieldsCode(fields)
    val toIndent:List[String] = if (!classString.isEmpty && !fieldsString.isEmpty) {
      classString ::: "" :: fieldsString
    } else if (!classString.isEmpty) {
      classString
    } else if (!fieldsString.isEmpty) {
      fieldsString
    }  else {
      List("// There was no generated code or fields")
    }
    template.format(name, indent(toIndent))
  }
}

case class BaseSchemaObject(name:String, fields:List[FieldMarker], classesToDefine:Map[String,SchemaGenObject])
  extends TupleGenObject(fields, classesToDefine) {
  import SchemaGenObject._
  def generateCode:String = imports + "\n\n" + generateCode(name, SchemaGenObject.baseTemplate)
}