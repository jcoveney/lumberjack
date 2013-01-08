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
  def getTypeName(idx:Int, fs:FieldSchema, ctd:Map[String,SchemaGenObject]):(Map[String,SchemaGenObject], String) = {
    fs.`type` match {
      case DataType.INTEGER => (ctd, "Int")
      case DataType.LONG => (ctd, "Long")
      case DataType.FLOAT => (ctd, "Float")
      case DataType.DOUBLE => (ctd, "Double")
      case DataType.BOOLEAN => (ctd, "Boolean")
      case DataType.CHARARRAY => (ctd, "String")
      case DataType.MAP => {
        val mapClassName = "Ljm" + idx
        val descendentClassName = mapClassName + "v"
        val ctd1 = ctd + (mapClassName -> MapGenObject(descendentClassName))
        //todo need to get the key type depending on whether it is complex...for now we assume it is a tuple
        val (fields, map) = convertSchema(fs.schema.getField(0).schema)
        (ctd1 + (descendentClassName -> TupleGenObject(fields, map)), mapClassName)
      }
      case DataType.TUPLE => {
        val tupleClassName = "Ljt" + idx
        val (fields, map) = convertSchema(fs.schema)
        (ctd + (tupleClassName -> TupleGenObject(fields, map)), tupleClassName)
      }
      case DataType.BAG => {
        val bagClassName = "Ltb" + idx
        val tupleClassName = bagClassName + "t"
        val ctd1 = ctd + (bagClassName -> BagGenObject(tupleClassName))
        val (fields, map) = convertSchema(fs.schema.getField(0).schema)
        (ctd1 + (tupleClassName -> TupleGenObject(fields, map)), bagClassName)
      }
    }
  }

  def convertSchema(s:Schema) = {
    val (fieldsInRev, _, map) = {
      s.getFields.foldLeft((Nil:List[FieldMarker],0,Map():Map[String,SchemaGenObject])) {
        (next:(List[FieldMarker],Int,Map[String, SchemaGenObject]),fs:FieldSchema) =>
        val (cur, idx, ctd) = (next._1, next._2, next._3)
        val (updatedCtd, typeName) = getTypeName(idx, fs, ctd)
        (FieldMarker(fs.alias, typeName, idx)::cur, idx+1, updatedCtd)
      }
    }
    (fieldsInRev.reverse, map)
  }
}
//TODO since these are all referred to by their name anyway in the map, that is redundant info
trait SchemaGenObject
case class MapGenObject(valueClass:String) extends SchemaGenObject
case class TupleGenObject(fields:List[FieldMarker], classesToDefine:Map[String, SchemaGenObject]) extends SchemaGenObject
case class BagGenObject(tupleClass:String) extends SchemaGenObject