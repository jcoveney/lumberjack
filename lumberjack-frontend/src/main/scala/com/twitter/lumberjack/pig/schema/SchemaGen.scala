package com.twitter.lumberjack.pig.schema

import org.apache.pig.data._
import org.apache.pig.impl.logicalLayer.schema.Schema
import collection.JavaConversions._
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema
import util.Random

/**
 * Created with IntelliJ IDEA.
 * User: jcoveney
 * Date: 1/8/13
 * Time: 3:25 PM
 * To change this template use File | Settings | File Templates.
 */
object SchemaGen {
  val tupleFactory = TupleFactory.getInstance
  val bagFactory = BagFactory.getInstance
  val r = new Random

  def generate(s:Schema):Tuple = {
    s.getFields.foldLeft(tupleFactory.newTuple) { (t,fs) =>
      t.append(fillColumn(fs))
      t
    }
  }

  def fillColumn(fs:FieldSchema):Any = {
    import DataType._
    fs.`type` match {
      case INTEGER => r.nextInt()
      case LONG => r.nextLong()
      case FLOAT => r.nextFloat()
      case DOUBLE => r.nextDouble()
      case CHARARRAY => r.nextString(10)
      case BOOLEAN => r.nextBoolean()
      case MAP => {
        Range(0, r.nextInt(10)+2).foldLeft(Map[String,Object]()) { (map, _) =>
          map + (r.nextString(10)->fillColumn(fs.schema.getField(0)).asInstanceOf[Object])
        }
      }
      case TUPLE => generate(fs.schema)
      case BAG => {
        val db = bagFactory.newDefaultBag()
        for (_ <- 1 to r.nextInt(10)+2) db.add(fillColumn(fs.schema.getField(0)).asInstanceOf[Tuple])
        db
      }
    }
  }
}
