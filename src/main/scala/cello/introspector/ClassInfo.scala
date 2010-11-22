/**
 * @author Marcello Bastea-Forte - http://marcello.cellosoft.com/
 * @date Nov 21, 2010
 * @license zlib - http://www.opensource.org/licenses/zlib-license.php
 */
package cello.introspector

import reflect.NameTransformer
import java.util.regex.Pattern
import java.lang.reflect.{Field, Method}
import java.lang.annotation.Annotation

object ClassInfo {
  def apply[T](clazz:Class[T]) = new ClassInfo(clazz)

  private val NAME_PATTERN = Pattern.compile("^\\w+$")
}

class ClassInfo[T] private[ClassInfo] (val scalaClass:Class[T]) {
  lazy val methods = Map(scalaClass.getMethods.map{m => NameTransformer.decode(m.getName) -> m}: _*)
  lazy val fields = Map(getAllFields().map{f => f.getName -> f}: _*)
  lazy val properties = methods.filter{ case (name, getter) => getter.getParameterTypes.isEmpty &&
                                                               getter.getReturnType != Void.TYPE &&
                                                               ClassInfo.NAME_PATTERN.matcher(name).matches }
                                  .map{ case (name, getter) => name -> new Property(name,getter) }

  private def getAllFields(clazz:Class[_] = scalaClass):List[Field] = {
    if (clazz == null) Nil else clazz.getDeclaredFields.toList ++ getAllFields(clazz.getSuperclass)
  }

  override def toString = "ClassInfo[%s]".format(scalaClass)
  override def equals(o: Any) = o match {
    case other:ClassInfo[T] => scalaClass == other.scalaClass
    case _ => false
  }
  override def hashCode = scalaClass.hashCode

  class Property(val name:String, val getter:Method) {
    lazy val propertyType:Class[_] = getter.getReturnType
    lazy val setter:Option[Method] = methods.get(name+"_=").filter(_.getParameterTypes.toList == List(propertyType))
    lazy val field:Option[Field] = fields.get(name)
    lazy val annotations:Set[Annotation] = getter.getAnnotations.toSet ++
                                           setter.map(_.getAnnotations).getOrElse(Array()) ++
                                           field.map(_.getAnnotations).getOrElse(Array())

    def annotation[A <: Annotation](ac:Class[A]) = annotations.find(a => ac.isAssignableFrom(a.getClass))
                                                              .asInstanceOf[Option[A]]
    def annotated_?(ac:Class[_ <: Annotation]) = annotation(ac).isDefined

    def get(target:T):AnyRef = getter.invoke(target)
    def set(target:T, value:AnyRef):Unit = setter.get.invoke(target,value)

    override def toString = "Property[%s.%s]".format(scalaClass,name)
    override def equals(o: Any) = o match {
      case other:Property => getter == other.getter
      case _ => false
    }
    override def hashCode = getter.hashCode
  }
}