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
import com.mongodb.casbah.commons.Logging
import com.mongodb.casbah.Imports._

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

  implicit def coll2mapped(mc: MongoCollection) = new {
    def mapped[P <: AnyRef : Manifest] = new MongoMappedCollection(mc.underlying)(manifest[P])
  }
}

abstract class Mapper[P <: AnyRef : Manifest]() extends Function1[P, DBObject] with Logging with OJ {
  import Mapper._
  import MapperUtils._

  protected[mapper] val obj_klass = manifest[P].erasure.asInstanceOf[Class[P]]
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
      case (pd: PropertyDescriptor, idx: Int) => {
        val pri = annotation[Key](pd, classOf[Key]) match {
          case Some(a) => a.pri
          case _ => -1
        }
        new RichPropertyDescriptor(if (pri == -1) idx else pri, pd, obj_klass)
      }
    }
    .sortWith { case (a, b) => a.idx <= b.idx }.toSet
  }

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
  lazy val interface_? = obj_klass.isInterface

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

    val dbo = {
      try {
        prop.readMapper(embedded).asDBObject(embedded match {
          case Some(vv: AnyRef) if prop.option_? => vv
          case _ => embedded
        })
      }
      catch {
        case t: Throwable => throw new Exception("OOPS! %s ---> %s".format(this, prop), t)
      }
    }

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
    (prop.read.invoke(p) match {
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
    .filter {
      prop => !prop.ignoreOut_?
    }
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

  def apply(p: P) = asDBObject(p)
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

    val dst = src.foldRight(init) {
      case (v, list) =>
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

  def write(p: P, prop: RichPropertyDescriptor, v: Any): Unit =
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
          case x if prop.innerType == classOf[ScalaBigDecimal] => ScalaBigDecimal(x.toString, MATH_CONTEXT)
          case x if prop.innerType == classOf[JavaBigDecimal] => new JavaBigDecimal(x.toString, MATH_CONTEXT)
          case _ => v
        }) match {
          case x if x != null && prop.option_? => Some(x)
          case x if x == null && prop.option_? => None
          case x => x
        })
      }
      case None if prop.option_? => prop.write(p, None)
      case _ if prop.option_? => prop.write(p, None)
      case _ =>
    }

  protected def companion(c: Class[_]) = {
    try {
      Some(Class.forName("%s$".format(c.getName)))
    }
    catch { case _ => None }
  }

  def empty: P = try {
    obj_klass.newInstance
  } catch {
    case _ => newInstance[P](obj_klass)
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
        allProps.filter(!_.ignoreIn_?).foldLeft(empty) {
          (p, prop) => write(p, prop, dbo.get(prop.key))
          p
        }
    }
}

trait DefaultArgsSetter[P <: AnyRef] {
  self: Mapper[P] =>

    override def empty: P = try {
      obj_klass.newInstance
    } catch {
      case _ => {
        companion(obj_klass) match {
          case Some(comp) => {
            val singleton = comp.getField("MODULE$").get(null)
            val const = obj_klass.getConstructors.head // XXX: what if there are more?
            val defaults = comp.getMethods.toList.filter(_.getName.startsWith("init$default$"))
            val args = Range(0, const.getParameterTypes.size).toList.map {
              d => defaults.filter(_.getName == "init$default$%d".format(d + 1)).headOption match {
                case Some(default) => default.invoke(singleton)
                case _ => {
                  const.getParameterTypes.toList.apply(d) match {
                    case t if t.isAssignableFrom(classOf[Double]) => Double.box(0)
                    case t if t.isAssignableFrom(classOf[Float]) => Float.box(0)
                    case t if t.isAssignableFrom(classOf[Long]) => Long.box(0)
                    case t if t.isAssignableFrom(classOf[Int]) => Int.box(0)
                    case t if t.isAssignableFrom(classOf[Short]) => Short.box(0)
                    case t if t.isAssignableFrom(classOf[Byte]) => Byte.box(0)
                    case t if t.isAssignableFrom(classOf[Option[_]]) => None
                    case t if t.isAssignableFrom(classOf[Set[_]]) => Set.empty
                    case t if t.isAssignableFrom(classOf[Map[_, _]]) => Map.empty
                    case t if t.isAssignableFrom(classOf[Buffer[_]]) => Buffer.empty
                    case t if t.isAssignableFrom(classOf[List[_]]) => List.empty
                    case t if t.isAssignableFrom(classOf[Seq[_]]) => Seq.empty
                    case t if t.isAssignableFrom(classOf[Boolean]) => Boolean.box(false)
                    case _ => null
                  }
                }
              }
            }
            const.newInstance(args :_*).asInstanceOf[P]
          }
          case _ => newInstance[P](obj_klass)
        }
      }
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

  def newInstance[T](clazz: Class[T]): T =
    clazz.cast(choose(clazz).newInstance(clazz)).asInstanceOf[T]
}

private[mapper] sealed trait MapperDirection
private[mapper] case object ReadMapper extends MapperDirection
private[mapper] case object WriteMapper extends MapperDirection
class MissingMapper(d: MapperDirection, c: Class[_], m: String = "no further info") extends Exception("%s is missing for: %s (%s)".format(d, c, m))
class InfoError(c: Class[_], t: Throwable) extends Exception("%s: unable to retrieve info".format(c), t)

trait MapKeyStrategy {
  def transform(k: Any): String
}
