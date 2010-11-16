package com.bumnetworks.casbah
package mapper

import annotations.raw._
import com.mongodb.casbah.commons.Logging
import com.mongodb.casbah.Imports._
import java.beans.{Introspector, PropertyDescriptor}
import scala.collection.mutable.Buffer

class RichPropertyDescriptor(val idx: Int, val pd: PropertyDescriptor, val parent: Class[_]) extends Logging {
  import MapperUtils._

  override def toString = "Prop/%s(%s @ %d (%s <- %s))".format(parent.getSimpleName, name, idx, innerType, outerType)

  lazy val name = pd.getName
  lazy val key = {
    (if (annotated_?(pd, classOf[ID])) "_id"
     else {
       annotation(pd, classOf[Key]) match {
         case None => name
         case Some(ann) => ann.value match {
           case "" => name
           case x => x
         }
       }
     }) match {
      case "_id" if !id_? => throw new Exception("only @ID props can have key == \"_id\"")
      case s if s.contains(".") || s.contains("$") => throw new Exception("keys can't contain . or $")
      case p => p
    }
  }

  lazy val pid: Option[Any] = {
    // XXX ?!
    def pid0(t: Class[Any]): Option[Any] = t match {
      case _ if iterable_? => None // `pid` is undefined for iterables
      case _ if embedded_? => None // ditto for embedded documents
      case _ if option_? => None // N/A to Option-s
      case _ if t.isAssignableFrom(classOf[Double]) => Some(idx.toDouble)
      case _ if t.isAssignableFrom(classOf[Float]) => Some(idx.toFloat)
      case _ if t.isAssignableFrom(classOf[Long]) => Some(idx.toLong)
      case _ if t.isAssignableFrom(classOf[Int]) => Some(idx)
      case _ if t.isAssignableFrom(classOf[Short]) => Some(idx.toShort)
      case _ if t.isAssignableFrom(classOf[Byte]) => Some(idx.toByte)
      case _ if t == classOf[String] => Some("%d".format(idx))
      case _ => None
    }
    pid0(innerType)
  }

  lazy val read = pd.getReadMethod
  lazy val write = if (pd.getWriteMethod == null) None else Some(pd.getWriteMethod)

  lazy val field = try {
    val f = parent.getDeclaredField(name)
    f.setAccessible(true)
    Some(f)
  }
  catch {
    case _ => None
  }

  private def squashNulls(value: Any): Any =
    value match {
      case null if option_? => None
      case _ => value
    }

  def write(dest: AnyRef, value: Any): Unit =
    field match {
      case Some(field) => {
        // HACK HACK HACK: fire "read" method first, which should
        // "prime" lazy vals. The issue being: lazy val's field can be
        // set using Field.set(), but the wrapping method will still
        // think it's supposed to fire even after that.
        //try { read.invoke(dest) }
        //catch { case _ => {} }

        // Now, go and set the field.
        field.set(dest, squashNulls(value))
      }
      case None => write match {
        case Some(write) => write.invoke(dest, squashNulls(value).asInstanceOf[AnyRef])
        case None => // NOOP
      }
    }

  lazy val innerTypes = typeParams(read)
  lazy val innerType = {
    outerType match {
      case c if c == classOf[Option[_]] => innerTypes.head
      case c if map_? => innerTypes.last
      case c if iterable_? => innerTypes.head
      case c if set_? => innerTypes.head
      case c => c
    }
  }.asInstanceOf[Class[Any]]

  lazy val outerType = pd.getPropertyType.asInstanceOf[Class[Any]]

  lazy val option_? = outerType == classOf[Option[_]]
  lazy val id_? = annotated_?(pd, classOf[ID])
  lazy val autoId_? = id_? && annotation(pd, classOf[ID]).get.auto
  lazy val embedded_? = annotated_?(pd, classOf[Key]) && (annotated_?(pd, classOf[UseTypeHints]) || Mapper(innerType.getName).isDefined)
  lazy val ignoreOut_? = annotation[Ignore](pd, classOf[Ignore]) match {
    case Some(ann) => ann.out
    case _ => false
  }
  lazy val ignoreIn_? = annotation[Ignore](pd, classOf[Ignore]) match {
    case Some(ann) => ann.in
    case _ => false
  }

  lazy val iterable_? = !map_? && (list_? || buffer_?)
  lazy val list_? = outerType.isAssignableFrom(classOf[List[_]])
  lazy val buffer_? = outerType.isAssignableFrom(classOf[Buffer[_]])
  lazy val map_? = outerType.isAssignableFrom(classOf[Map[_,_]])
  lazy val set_? = outerType.isAssignableFrom(classOf[Set[_]])

  lazy val useTypeHints_? = annotation[UseTypeHints](pd, classOf[UseTypeHints]) match {
    case Some(ann) if ann.value => true
    case _ => false
  }

  lazy val mapKeyStrategy = annotation[KeyStrategy](pd, classOf[KeyStrategy]) match {
    case Some(ann) => Some(ann.value.newInstance.asInstanceOf[MapKeyStrategy])
    case _ => None
  }

  def mapKey(k: Any): String = {
    k match {
      case s: String if s != null && s != "" => s
      case _ => mapKeyStrategy match {
        case Some(strategy) => strategy.transform(k)
        case _ => {
          log.warning("%s: transforming non-string map key '%s' (%s) using toString",
                      this, k, (if (k == null) "NULL" else k.asInstanceOf[AnyRef].getClass))
          "%s".format(k)
        }
      }
    }
  }

  def readMapper(p: AnyRef) = {
    log.trace("readMapper: %s -> %s, %s", p, Mapper(innerType.getName).isDefined, Mapper(p.getClass.getName).isDefined)
    Mapper(innerType.getName) match {
      case Some(mapper) if !mapper.interface_? => mapper
      case _ if useTypeHints_? => Mapper(p.getClass.getName) match {
        case Some(mapper) => mapper
        case _ => throw new MissingMapper(ReadMapper, p.getClass, "in %s".format(this))
      }
      case _ => throw new MissingMapper(ReadMapper, innerType, "in %s".format(this))
    }
  }.asInstanceOf[Mapper[AnyRef]]

  def writeMapper(dbo: MongoDBObject) = {
    log.trace("writeMapper: %s -> %s, %s", dbo, Mapper(innerType.getName).isDefined,
              dbo.get(TYPE_HINT).isDefined && Mapper(dbo(TYPE_HINT).asInstanceOf[String]).isDefined)
    Mapper(innerType.getName) match {
      case Some(mapper) => mapper
      case None if useTypeHints_? => dbo.get(TYPE_HINT) match {
        case Some(typeHint: String) => Mapper(typeHint) match {
          case Some(mapper) => mapper
          case _ => throw new MissingMapper(WriteMapper, Class.forName(typeHint))
        }
        case _ => throw new MissingMapper(WriteMapper, innerType, "no @UseTypeHints on %s".format(this))
      }
      case _ => throw new MissingMapper(WriteMapper, innerType)
    }
  }.asInstanceOf[Mapper[AnyRef]]

  var enum: Option[AnyRef] = None
  lazy val enum_? = enum.isDefined
  private lazy val enumWithName = enum.get.getClass.getMethod("withName", classOf[String])
  def deserializeEnum(s: String) = enumWithName.invoke(enum.get, s)
  def serializeEnum(p: AnyRef) = read.invoke(p).toString

  override def equals(o: Any): Boolean = o match {
    case other: RichPropertyDescriptor => pd.equals(other.pd)
    case _ => false
  }

  override def hashCode(): Int = pd.hashCode()
}
