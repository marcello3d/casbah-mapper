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
import java.util.Locale.ENGLISH

object ClassInfo {
  def apply[T](clazz:Class[T], beanSupport:Boolean = true) = new ClassInfo(clazz, beanSupport)
  private val NAME_PATTERN = Pattern.compile("^\\w+$")
  private val BEAN_GETTER_PATTERN = Pattern.compile("^(?:get|is)([A-Z])(\\w*)$")
}

class ClassInfo[T] private[ClassInfo] (val scalaClass:Class[T], beanSupport:Boolean) {
  lazy val methods = Map(scalaClass.getMethods.map{m => NameTransformer.decode(m.getName) -> m}: _*)
  lazy val fields = {
    def allFields(clazz:Class[_]):List[Field] = {
      if (clazz == null) Nil else clazz.getDeclaredFields.toList ++ allFields(clazz.getSuperclass)
    }
    Map(allFields(scalaClass).map{f => f.getName -> f}: _*)
  }
  lazy val properties = {
    val getters = methods.filter{ case (name, getter) => getter.getParameterTypes.isEmpty &&
                                                         getter.getReturnType != Void.TYPE &&
                                                         ClassInfo.NAME_PATTERN.matcher(name).matches}
    if (beanSupport) {
      def beanName(name:String) = {
        val matcher = ClassInfo.BEAN_GETTER_PATTERN.matcher(name)
        if (matcher.matches) Some(matcher.group(1).toLowerCase(ENGLISH) + matcher.group(2)) else None
      }
      // filter out properties that appear as bean-style getters and scala getters
      getters.filter{ case (name,getter) => beanName(name).isEmpty }
                .map{ case (name,getter) => name -> new ScalaProperty(name,getter) } ++
      // add bean properties that don't appear as scala getters
      getters.filter{ case (name,getter) => beanName(name).map(!getters.contains(_)).getOrElse(false) }
                .map{ case (name,getter) => beanName(name).map(name => name -> new JavaBeanProperty(name,getter)).get }
    } else {
      getters.map{ case (name,getter) => name -> new ScalaProperty(name,getter) }
    }
  }

  override def toString = "ClassInfo[%s%s]".format(scalaClass,if (beanSupport) " with bean support" else "")
  override def equals(o: Any) = o match {
    case other:ClassInfo[T] => scalaClass == other.scalaClass
    case _ => false
  }
  override def hashCode = scalaClass.hashCode

  class ScalaProperty(name:String, getter:Method) extends Property(name,getter) {
    protected override def setterName = name+"_="
  }
  class JavaBeanProperty(name:String, getter:Method) extends Property(name,getter) {
    private def isBoolean = propertyType == java.lang.Boolean.TYPE || propertyType == classOf[Boolean]
    protected override def setterName = "set" + name(0).toUpper + name.substring(1)
  }
  abstract class Property(val name:String, val getter:Method) {
    lazy val propertyType:Class[_] = getter.getReturnType
    protected def setterName:String
    lazy val setter:Option[Method] = methods.get(setterName).filter(_.getParameterTypes.toList == List(propertyType))
    lazy val field:Option[Field] = fields.get(name)
    lazy val annotations:Set[Annotation] = getter.getAnnotations.toSet ++
                                           setter.map(_.getAnnotations).getOrElse(Array()) ++
                                           field.map(_.getAnnotations).getOrElse(Array())

    def annotation[A <: Annotation](ac:Class[A]) = annotations.find(a => ac.isAssignableFrom(a.getClass))
                                                              .asInstanceOf[Option[A]]
    def annotated_?(ac:Class[_ <: Annotation]) = annotation(ac).isDefined

    def get(target:T):AnyRef = getter.invoke(target)
    def set(target:T, value:AnyRef):Unit = setter.get.invoke(target,value)

    override def toString = "%s[%s.%s]".format(getClass.getSimpleName,scalaClass,name)
    override def equals(o: Any) = o match {
      case other:Property => getter == other.getter
      case _ => false
    }
    override def hashCode = getter.hashCode
  }
}