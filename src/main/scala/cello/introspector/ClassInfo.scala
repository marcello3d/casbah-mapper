/**
 * @author Marcello Bastea-Forte - http://marcello.cellosoft.com/
 * @date Nov 21, 2010
 * @license zlib - http://www.opensource.org/licenses/zlib-license.php
 */
package cello.introspector

import reflect.NameTransformer
import java.lang.reflect.{Field, Method}
import java.lang.annotation.Annotation
import java.util.Locale.ENGLISH

object ClassInfo {
  def apply[T](clazz:Class[T], beanSupport:Boolean = true) = new ClassInfo(clazz, beanSupport)
}
class ClassInfo[T](val scalaClass:Class[T], val beanSupport:Boolean) {
  lazy val methods = scalaClass.getMethods.map{ m => NameTransformer.decode(m.getName) -> m }
  lazy val fields = {
    def fields(c:Class[_]):List[Field] = if (c == null) Nil else c.getDeclaredFields.toList ++ fields(c.getSuperclass)
    fields(scalaClass).map{f => f.getName -> f}.toMap
  }
  lazy val properties = {
    val Getter = """^\w+$""".r
    val BeanGetter = """^(?:get|is)([A-Z])(\w*)$""".r
    val getters = methods.filter{ case (name,getter) => getter.getParameterTypes.length == 0 &&
                                                        getter.getReturnType != Void.TYPE &&
                                                        (Getter findFirstIn name).isDefined }.toMap
    if (beanSupport) {
      def beanify(name:String) = BeanGetter findFirstMatchIn name map{m => m.group(1).toLowerCase(ENGLISH) + m.group(2)}
      // convert non-bean getters to scala properties
      getters.filter{ case (name,getter) => beanify(name).isEmpty }
                .map{ case (name,getter) => name -> new Property(name,getter,name+"_=") } ++
      // add bean getter properties that aren't already scala getters
      getters.filter{ case (name,getter) => beanify(name).map(!getters.contains(_)).getOrElse(false) }
                .map{ case (name,getter) => beanify(name).map{prop => prop -> new Property(prop,getter,
                                                                       BeanGetter replaceFirstIn(name,"set$1$2"))}.get }
    } else {
      getters.map{ case (name,getter) => name -> new Property(name,getter,name+"_=") }
    }
  }
  override def toString = "ClassInfo[%s%s]".format(scalaClass,if (beanSupport) " with bean support" else "")
  override def equals(o:Any) = o match {
    case other:ClassInfo[T] => scalaClass == other.scalaClass && beanSupport == other.beanSupport
    case _ => false
  }
  override def hashCode = scalaClass.hashCode ^ beanSupport.hashCode

  class Property(val name:String, val getter:Method, setterName:String) {
    lazy val propertyType = getter.getReturnType
    lazy val field = fields.get(name)
    lazy val setter = methods.find{ case (name,m) => name == setterName &&
                                                     m.getParameterTypes.toList == List(propertyType) }.map(_._2)
    lazy val annotations = getter.getAnnotations.toSet ++
                           setter.map(_.getAnnotations).getOrElse(Array()) ++
                           field.map(_.getAnnotations).getOrElse(Array())

    def annotation[A <: Annotation](ac:Class[A]) = annotations.find(a => ac.isAssignableFrom(a.getClass)).asInstanceOf[Option[A]]
    def annotated_?(ac:Class[_ <: Annotation]) = annotation(ac).isDefined
    def get(target:T):AnyRef = getter.invoke(target)
    def set(target:T, value:AnyRef):Unit = setter.get.invoke(target,value)

    override def toString = "Property[%s.%s]".format(scalaClass.getName,name)
    override def equals(o:Any) = o match {
      case other:Property => getter == other.getter
      case _ => false
    }
    override def hashCode = getter.hashCode
  }
}