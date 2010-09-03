package com.bumnetworks.casbah
package mapper

import java.lang.reflect.Method
import java.lang.annotation.Annotation
import java.beans.{Introspector, PropertyDescriptor}

import scala.reflect.{BeanInfo, Manifest}
import scala.collection.JavaConversions._
import scala.collection.mutable.{Buffer, ArrayBuffer}
import scala.collection.immutable.{List, Map => IMap}
import scala.collection.mutable.{Map => MMap, HashMap}

import _root_.scala.math.{BigDecimal => ScalaBigDecimal}
import java.math.{BigDecimal => JavaBigDecimal, RoundingMode, MathContext}

import annotations.raw._
import commons.util.Logging
import com.novus.casbah.Imports._

object Mapper extends Logging {
  private val _m = new java.util.concurrent.ConcurrentHashMap[String, Mapper[_]]

  def apply[P <: AnyRef : Manifest](): Mapper[P] =
    apply(manifest[P].erasure.getName).get.asInstanceOf[Mapper[P]]

  def apply[P <: AnyRef](p: String): Option[Mapper[P]] =
    if (_m.containsKey(p)) Some(_m.get(p).asInstanceOf[Mapper[P]])
    else None

  def apply[P <: AnyRef : Manifest](p: Class[P]): Option[Mapper[P]] = apply(p.getName)

  def update[P <: AnyRef](p: String, m: Mapper[P]): Unit =
    if (!_m.contains(p)) _m(p) = m.asInstanceOf[Mapper[P]]

  def update[P <: AnyRef : Manifest](p: Class[P], m: Mapper[P]): Unit = update(p.getName, m)
}

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
      case s if s.startsWith("_") && !id_? => throw new Exception("keys can't start with underscores")
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
      case Some(field) => field.set(dest, squashNulls(value))
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
      case Some(mapper) => mapper
      case None if useTypeHints_? => Mapper(p.getClass.getName) match {
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

abstract class Mapper[P <: AnyRef : Manifest]() extends Logging with OJ {
  import Mapper._
  import MapperUtils._

  protected val obj_klass = manifest[P].erasure.asInstanceOf[Class[P]]
  Mapper(obj_klass) = this

  //implicit def rpd2pd(prop: RichPropertyDescriptor): PropertyDescriptor = prop.pd

  implicit protected def s2db(name: String): MongoDB = conn(name)
  implicit protected def s2coll(name: String): MongoCollection = db(name)

  class PimpedString(p: String) {
    def enum(e: AnyRef) = propNamed(p) match {
      case Some(prop) => prop.enum = Some(e)
      case _ => throw new Exception("no such prop '%s' in '%s'".format(p, obj_klass))
    }
  }

  implicit def pimpString(p: String) = new PimpedString(p)

  var conn: MongoConnection = _
  var db  : MongoDB         = _
  var coll: MongoCollection = _

  lazy val info = {
    try {
      Introspector.getBeanInfo(obj_klass)
    }
    catch {
      case t: Throwable => throw new InfoError(obj_klass, t)
    }
  }

  lazy val allProps = {
    info.getPropertyDescriptors.filter {
      prop => (annotated_?(prop, classOf[ID]) || annotated_?(prop, classOf[Key]))
    }
    .sortWith { case (a, b) => a.getName.compareTo(b.getName) < 0 }
    .zipWithIndex.map {
      case (pd: PropertyDescriptor, idx: Int) =>
        new RichPropertyDescriptor(idx, pd, obj_klass)
    }.toSet
  }

  lazy val propsByPid = Map.empty ++ (allProps.map {
    p => p.pid match {
      case Some(pid) => Some(pid -> p)
      case None => None
    }
  }).filter(_.isDefined).map(_.get)

  lazy val idProp = allProps.filter(_.id_?).headOption match {
    case Some(id) if id.autoId_? =>
      if (id.innerType != classOf[ObjectId])
        throw new Exception("only ObjectId _id fields are supported when auto = true (%s . %s)".format(obj_klass.getName, id.name))
      else Some(id)
    case Some(id) => Some(id)
    case _ => None
  }

  lazy val nonIdProps = idProp match {
    case Some(id) => allProps - id
    case None => allProps
  }

  lazy val useTypeHints_? = obj_klass.isAnnotationPresent(classOf[UseTypeHints])

  override def toString =
    "Mapper(%s -> idProp: %s, is_auto_id: %s, allProps: %s)".format(
      obj_klass.getName, idProp.map(_.name).getOrElse("N/A"), idProp.map(_.autoId_?).getOrElse(false),
      allProps.map(p =>
        "Prop(%s -> %s, is_option: %s)".format(p.name,
                                               p.innerType,
                                               p.option_?))
    )

  def propNamed(key: String) =
    nonIdProps.filter(_.name == key).toList match {
      case List(prop) => Some(prop)
      case _ => None
    }

  private def embeddedPropValue(p: P, prop: RichPropertyDescriptor, embedded: AnyRef) = {
    log.trace("EMB: %s -> %s -> %s", p, prop, embedded)

    val dbo = prop.readMapper(embedded).asDBObject(embedded match {
      case Some(vv: AnyRef) if prop.option_? => vv
      case _ => embedded
    })

    if (prop.useTypeHints_?)
      dbo(TYPE_HINT) = (embedded match {
        case Some(vv: AnyRef) if prop.option_? => vv.getClass
        case _ => embedded.getClass
      }).getName

    dbo
  }

  def ensureID(p: P): Option[ObjectId] = idProp match {
    case Some(ip) if ip.autoId_? => Some(propValue(p, ip).get.asInstanceOf[ObjectId])
    case _ => None
  }

  def propValue(p: P, prop: RichPropertyDescriptor): Option[Any] = {
    log.trace("V: %s , %s with %s", p, prop, prop.read)

    val readValue = try {
      prop.read.invoke(p)
    }
    catch {
      case t =>
        throw new Exception("failed to read: V: %s , %s with %s".format(p, prop, prop.read))
    }
    (readValue match {
      case null => {
        if (prop.id_? && prop.autoId_?) {
          val id = new ObjectId
          prop.write(p, id)
          Some(id)
        } else { None }
      }
      case _ if prop.enum_? => Some(prop.serializeEnum(p))
      case v: AnyRef if v.getClass.getName.contains("Enum") && !prop.enum_? =>
	throw new Exception("achtung! %s in %s is not an enum, but should be one!".format(prop, obj_klass))
      case l: List[AnyRef] if prop.embedded_? => Some(l.map(embeddedPropValue(p, prop, _)))
      case b: Buffer[AnyRef] if prop.embedded_? => Some(b.map(embeddedPropValue(p, prop, _)))
      case s: Set[AnyRef] if prop.set_? && prop.embedded_? => Some(s.map(embeddedPropValue(p, prop, _)))
      case m if prop.map_? => Some(m.asInstanceOf[scala.collection.Map[String, Any]].map {
        case (k, v) => {
          prop.mapKey(k) -> (if (prop.embedded_?) embeddedPropValue(p, prop, v.asInstanceOf[AnyRef])
                             else v)
        }
      }.asDBObject)
      case Some(v: Any) if prop.option_? => {
        log.trace("option: embedded_? %s <- %s -> %s", prop.embedded_?, prop, v)
        if (prop.embedded_?)
          Some(embeddedPropValue(p, prop, v.asInstanceOf[AnyRef]))
        else
          Some(v)
      }
      case v if prop.embedded_? => {
        log.trace("bare embedded: %s", v)
        Some(embeddedPropValue(p, prop, v))
      }
      case None if prop.option_? => None
      case None if !prop.option_? => throw new Exception("%s should be option but is not?".format(prop))
      case v => Some(v)
    }) match {
      case Some(bd: ScalaBigDecimal) => Some(bd(MATH_CONTEXT).toDouble)
      case Some(bd: JavaBigDecimal) => Some(bd.round(MATH_CONTEXT).doubleValue)
      case x => x
    }
  }

  def asKeyValueTuples(p: P) = {
    log.trace("AKVT: %s", p)

    val tuples = allProps.toList
    .map {
      prop =>
        log.trace("AKVT: %s -> %s", p, prop)
      propValue(p, prop) match {
        case Some(value) => Some(prop.key -> value)
        case _ => None
      }
    }.filter(_.isDefined).map(_.get)

    if (useTypeHints_?)
      tuples ::: (TYPE_HINT -> p.getClass.getName) :: Nil
    else
      tuples
  }

  def asDBObject(p: P): DBObject = {
    val result = {
      asKeyValueTuples(p)
      .foldLeft(MongoDBObject.newBuilder) {
        (builder, t) => builder += t
      }
      .result
    }

    log.trace("%s: %s -> %s", obj_klass.getName, p, result)
    result
  }

  private def writeNested(p: P, prop: RichPropertyDescriptor, nested: MongoDBObject) = {
    val e = prop.writeMapper(nested).asObject(nested)
    log.trace("write nested '%s' to '%s'.'%s' using: %s -OR- %s", nested, p, prop.key, prop.write, prop.field)
    prop.write(p, if (prop.option_?) Some(e) else e)
  }

  private def writeSeq(p: P, prop: RichPropertyDescriptor, src: MongoDBObject): Unit =
    writeSeq(p, prop, src.map { case (k, v) => v }.toList)

  private def writeSeq(p: P, prop: RichPropertyDescriptor, src: Seq[_]): Unit = {
    def init: Iterable[Any] =
      if (prop.list_?) Nil
      else if (prop.buffer_?) ArrayBuffer()
      else if (prop.set_?) Set()
      else throw new Exception("whaaa! whaa! I'm lost! %s.%s".format(p, prop.name))

    val dst = src.foldLeft(init) {
      case (list, v) =>
        init ++ (list.toList ::: (if (prop.embedded_?) {
          val nested = v.asInstanceOf[DBObject]
          prop.writeMapper(nested).asObject(nested)
        } else v) :: Nil)
    }

    log.trace("write list '%s' (%s) to '%s'.'%s' using %s -OR- %s",
              dst, dst.getClass.getName, p, prop.key, prop.write, prop.field)

    prop.write(p, dst)
  }

  private def writeMap(p: P, prop: RichPropertyDescriptor, src: MongoDBObject) = {
    def init: scala.collection.Map[String, Any] =
      if (prop.outerType.isAssignableFrom(classOf[IMap[_,_]]))
        IMap.empty[String, Any]
      else if (prop.outerType.isAssignableFrom(classOf[MMap[_,_]]))
        HashMap.empty[String, Any]
      else
        throw new Exception("%s: unable to find proper Map[String, Any] impl".format(prop))

    val dst = init ++ src.map {
      case (k, v) =>
        k -> (v match {
          case nested: DBObject if prop.embedded_? =>
            prop.writeMapper(nested).asObject(nested)
          case _ => v
        })
    }

    log.trace("write ---MAP--- '%s' (%s) to '%s'.'%s' using: %s -OR- %s",
              dst, dst.getClass.getName, p, prop.key, prop.write, prop.field)

    prop.write(p, dst)
  }

  private def write(p: P, prop: RichPropertyDescriptor, v: Any): Unit =
    v match {
      case Some(l: MongoDBObject) if prop.iterable_? || prop.set_? => writeSeq(p, prop, l)
      case Some(l: DBObject) if prop.iterable_? || prop.set_? => writeSeq(p, prop, l)
      case Some(l: List[_]) if prop.iterable_? || prop.set_? => writeSeq(p, prop, l)

      case Some(m: MongoDBObject) if prop.map_? => writeMap(p, prop, m)
      case Some(v: MongoDBObject) if prop.embedded_? => writeNested(p, prop, v)

      case Some(m: DBObject) if prop.map_? => writeMap(p, prop, m)
      case Some(v: DBObject) if prop.embedded_? => writeNested(p, prop, v)

      case Some(v) => {
        log.trace("write raw '%s' (%s) to '%s'.'%s' using: %s -OR- %s",
                  v, v.asInstanceOf[AnyRef].getClass.getName, p, prop.key, prop.write, prop.field)
        prop.write(p, (v match {
          case oid: ObjectId => oid
	  case e: String if prop.enum_? => prop.deserializeEnum(e)
          case s: String if prop.id_? && idProp.map(_.autoId_?).getOrElse(false) => new ObjectId(s)
          case d: Double if prop.innerType == classOf[JavaBigDecimal] => new JavaBigDecimal(d, MATH_CONTEXT)
          case d: Double if prop.innerType == classOf[ScalaBigDecimal] => ScalaBigDecimal(d, MATH_CONTEXT)
          case _ => v
        }) match {
          case x if x != null && prop.option_? => Some(x)
          case x if x == null && prop.option_? => None
          case x => x
        })
      }
      case None if prop.option_? => prop.write(p, None)
      case _ =>
    }

  def empty: P = try {
    obj_klass.newInstance
  } catch {
    case _ => newInstance[P](obj_klass)(manifest[P])
  }

  def asObject(dbo: MongoDBObject): P =
    dbo.get(TYPE_HINT) match {
      case Some(hint: String) if hint != obj_klass.getName => {
	Mapper(hint) match {
	  case Some(hintedMapper) => hintedMapper.asObject(dbo).asInstanceOf[P]
	  case _ => throw new MissingMapper(ReadMapper, Class.forName(hint), "while loading type-hinted DBO in %s".format(this))
	}
      }
      case _ =>
	allProps.foldLeft(empty) {
	  (p, prop) => write(p, prop, dbo.get(prop.key))
	  p
	}
    }

  def findOne(id: Any): Option[P] =
    coll.findOne("_id" -> id) match {
      case None => None
      case Some(dbo) => Some(asObject(dbo))
    }

  def example: P =
    (propsByPid.map { case (k,v) => v->k }).foldLeft(empty) {
      case (e, (prop, pid)) => {
        write(e, prop, Some(pid))
        e
      }
    }

  def upsert(p: P): P = {
    coll.insert(asDBObject(p))
    p // XXX: errors? dragons?
  }
}

object MapperUtils {
  val MATH_CONTEXT = new MathContext(16, RoundingMode.HALF_UP);
  val TYPE_HINT = "_typeHint"

  def annotation[A <: Annotation](prop: PropertyDescriptor, ak: Class[A]): Option[A] =
    (List(prop.getReadMethod, prop.getWriteMethod).filter(_ != null).filter {
      meth => meth.isAnnotationPresent(ak)
    }) match {
      case Nil => None
      case x => Some(x.head.getAnnotation(ak))
    }

  def annotated_?[A <: Annotation](prop: PropertyDescriptor, ak: Class[A]): Boolean =
    annotation(prop, ak) match { case Some(a) => true case _ => false }

  def typeParams(m: Method) =
    (m.getGenericReturnType match {
      case c: Class[_] => c :: Nil
      case t => t.asInstanceOf[java.lang.reflect.ParameterizedType].getActualTypeArguments.toList
    }).map {
      case pt: java.lang.reflect.ParameterizedType => pt.getRawType
      case c => c
    }.map(_.asInstanceOf[Class[_]])
}

trait OJ {
  import org.objenesis._

  val std = new ObjenesisStd
  val ser = new ObjenesisSerializer

  def choose(clazz: Class[_]) =
    if (classOf[java.io.Serializable].isAssignableFrom(clazz)) ser
    else std

  def newInstance[T: Manifest](clazz: Class[T]): T =
    manifest[T].erasure.cast(choose(clazz).newInstance(clazz)).asInstanceOf[T]
}

private[mapper] sealed trait MapperDirection
private[mapper] case object ReadMapper extends MapperDirection
private[mapper] case object WriteMapper extends MapperDirection
class MissingMapper(d: MapperDirection, c: Class[_], m: String = "no further info") extends Exception("%s is missing for: %s (%s)".format(d, c, m))
class InfoError(c: Class[_], t: Throwable) extends Exception("%s: unable to retrieve info".format(c), t)

trait MapKeyStrategy {
  def transform(k: Any): String
}
