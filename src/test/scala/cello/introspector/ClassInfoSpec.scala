package cello.introspector

import reflect.{BeanProperty, BeanInfo}
import org.specs._
import java.lang.annotation.Annotation
import specification.Examples

/**
 * @author Marcello Bastea-Forte (marcello@cellosoft.com)
 * @created 2010.11.22 
 */

class ScalaBean {
  @TestAnnotation var foo = "foo"
  @TestAnnotation var four = 4
  @TestAnnotation var maybeFive:Option[Int] = Some(5)
  val readOnly = true
}

@BeanInfo
class ScalaJavaBean {
  @TestAnnotation var foo = "foo"
  @TestAnnotation var four = 4
  @TestAnnotation var maybeFive:Option[Int] = Some(5)
  val readOnly = true
}

class ScalaHalfBean {
  @BeanProperty @TestAnnotation var foo = "foo"
  @BeanProperty @TestAnnotation var four = 4
  @TestAnnotation var maybeFive:Option[Int] = Some(5)
  val readOnly = true
}

object ClassInfoSpec extends Specification {
  shareVariables

  def haveProperty(classInfo:ClassInfo[_], name:String) = "have property "+name in {
    classInfo.properties must have the key(name)
  }
  def notHaveProperty(classInfo:ClassInfo[_], name:String) = "not have property "+name in {
    classInfo.properties must not have the key(name)
  }
  def propertyIsReadOnly(classInfo:ClassInfo[_], name:String) = {
    "property "+name+" is readonly" in {
      classInfo.properties(name).setter must beNone
    }
  }
  def propertyIsNotReadOnly(classInfo:ClassInfo[_], name:String) = {
    "property "+name+" has setter" in {
      classInfo.properties(name).setter must beSomething
    }
  }

  def propertyHasAnnotation(classInfo:ClassInfo[_], name:String, annotation:Class[_ <: Annotation]) = {
    "property "+name+" has annotation "+annotation in {
      classInfo.properties(name).annotated_?(annotation) must beTrue
    }
  }
  def propertyDoesNotHaveAnnotation(classInfo:ClassInfo[_], name:String, annotation:Class[_ <: Annotation]) = {
    "property "+name+" doesn't have annotation "+annotation in {
      classInfo.properties(name).annotated_?(annotation) must beFalse
    }
  }
  def haveMethod(classInfo:ClassInfo[_], name:String) = "have method "+name in {
    classInfo.methods must have the key(name)
  }
  def notHaveMethod(classInfo:ClassInfo[_], name:String) = "not have method "+name in {
    classInfo.methods must not have the key(name)
  }

  // Without Bean info
  val sbClassInfo = ClassInfo(classOf[ScalaBean], false)
  sbClassInfo.toString should {
    val info = sbClassInfo
    haveMethod(info, "foo")
    haveMethod(info, "four")
    haveMethod(info, "maybeFive")
    haveMethod(info, "readOnly")

    haveMethod(info, "foo_=")
    haveMethod(info, "four_=")
    haveMethod(info, "maybeFive_=")
    notHaveMethod(info, "readOnly_=")

    notHaveMethod(info, "getFoo")
    notHaveMethod(info, "getFour")
    notHaveMethod(info, "getMaybeFive")
    notHaveMethod(info, "isReadOnly")

    notHaveMethod(info, "getReadOnly")

    haveProperty(info, "foo")
    haveProperty(info, "four")
    haveProperty(info, "maybeFive")
    haveProperty(info, "readOnly")
    propertyIsNotReadOnly(info, "foo")
    propertyIsNotReadOnly(info, "four")
    propertyIsNotReadOnly(info, "maybeFive")
    propertyIsReadOnly(info, "readOnly")

    propertyHasAnnotation(info, "foo", classOf[TestAnnotation])
    propertyHasAnnotation(info, "four", classOf[TestAnnotation])
    propertyHasAnnotation(info, "maybeFive", classOf[TestAnnotation])
    propertyDoesNotHaveAnnotation(info, "readOnly", classOf[TestAnnotation])

    notHaveProperty(info, "getFoo")
    notHaveProperty(info, "getFour")
    notHaveProperty(info, "getMaybeFive")
    notHaveProperty(info, "isReadOnly")

    notHaveProperty(info, "getReadOnly")
  }
  val sjbClassInfo = ClassInfo(classOf[ScalaJavaBean], false)
  sjbClassInfo.toString should {
    val info = sjbClassInfo
    haveMethod(info, "foo")
    haveMethod(info, "four")
    haveMethod(info, "maybeFive")
    haveMethod(info, "readOnly")

    haveMethod(info, "foo_=")
    haveMethod(info, "four_=")
    haveMethod(info, "maybeFive_=")
    notHaveMethod(info, "readOnly_=")

    haveMethod(info, "getFoo")
    haveMethod(info, "getFour")
    haveMethod(info, "getMaybeFive")
    haveMethod(info, "isReadOnly")

    notHaveMethod(info, "getReadOnly")

    haveProperty(info, "foo")
    haveProperty(info, "four")
    haveProperty(info, "maybeFive")
    haveProperty(info, "readOnly")
    propertyIsNotReadOnly(info, "foo")
    propertyIsNotReadOnly(info, "four")
    propertyIsNotReadOnly(info, "maybeFive")
    propertyIsReadOnly(info, "readOnly")
    propertyHasAnnotation(info, "foo", classOf[TestAnnotation])
    propertyHasAnnotation(info, "four", classOf[TestAnnotation])
    propertyHasAnnotation(info, "maybeFive", classOf[TestAnnotation])
    propertyDoesNotHaveAnnotation(info, "readOnly", classOf[TestAnnotation])

    haveProperty(info, "getFoo")
    haveProperty(info, "getFour")
    haveProperty(info, "getMaybeFive")
    haveProperty(info, "isReadOnly")

    notHaveProperty(info, "getReadOnly")
  }

  val shbClassInfo = ClassInfo(classOf[ScalaHalfBean], false)
  shbClassInfo.toString should {
    val info = shbClassInfo
    haveMethod(info, "foo")
    haveMethod(info, "four")
    haveMethod(info, "maybeFive")
    haveMethod(info, "readOnly")

    haveMethod(info, "foo_=")
    haveMethod(info, "four_=")
    haveMethod(info, "maybeFive_=")
    notHaveMethod(info, "readOnly_=")

    haveMethod(info, "getFoo")
    haveMethod(info, "getFour")
    notHaveMethod(info, "getMaybeFive")
    notHaveMethod(info, "isReadOnly")

    notHaveMethod(info, "getReadOnly")

    haveProperty(info, "foo")
    haveProperty(info, "four")
    haveProperty(info, "maybeFive")
    haveProperty(info, "readOnly")
    propertyIsNotReadOnly(info, "foo")
    propertyIsNotReadOnly(info, "four")
    propertyIsNotReadOnly(info, "maybeFive")
    propertyIsReadOnly(info, "readOnly")
    propertyHasAnnotation(info, "foo", classOf[TestAnnotation])
    propertyHasAnnotation(info, "four", classOf[TestAnnotation])
    propertyHasAnnotation(info, "maybeFive", classOf[TestAnnotation])
    propertyDoesNotHaveAnnotation(info, "readOnly", classOf[TestAnnotation])

    haveProperty(info, "getFoo")
    haveProperty(info, "getFour")
    notHaveProperty(info, "getMaybeFive")
    notHaveProperty(info, "isReadOnly")

    notHaveProperty(info, "getReadOnly")
  }

  val jbClassInfo = ClassInfo(classOf[JavaBean], false)
  jbClassInfo.toString should {
    val info = jbClassInfo
    notHaveMethod(info, "foo")
    notHaveMethod(info, "four")
    notHaveMethod(info, "maybeFive")
    notHaveMethod(info, "readOnly")

    notHaveMethod(info, "foo_=")
    notHaveMethod(info, "four_=")
    notHaveMethod(info, "maybeFive_=")
    notHaveMethod(info, "readOnly_=")

    haveMethod(info, "getFoo")
    haveMethod(info, "getFour")
    haveMethod(info, "getMaybeFive")
    haveMethod(info, "isReadOnly")

    notHaveMethod(info, "getReadOnly")

    notHaveProperty(info, "foo")
    notHaveProperty(info, "four")
    notHaveProperty(info, "maybeFive")
    notHaveProperty(info, "readOnly")

    haveProperty(info, "getFoo")
    haveProperty(info, "getFour")
    haveProperty(info, "getMaybeFive")
    haveProperty(info, "isReadOnly")

    notHaveProperty(info, "getReadOnly")
  }

  // With Bean info (everything is the same)
  List(ClassInfo(classOf[ScalaBean], true),
       ClassInfo(classOf[ScalaJavaBean], true),
       ClassInfo(classOf[ScalaHalfBean], true),
       ClassInfo(classOf[JavaBean], true)).map {
    info => info.toString should {
      haveProperty(info, "foo")
      haveProperty(info, "four")
      haveProperty(info, "maybeFive")
      haveProperty(info, "readOnly")
      propertyIsNotReadOnly(info, "foo")
      propertyIsNotReadOnly(info, "four")
      propertyIsNotReadOnly(info, "maybeFive")
      propertyIsReadOnly(info, "readOnly")
      propertyHasAnnotation(info, "foo", classOf[TestAnnotation])
      propertyHasAnnotation(info, "four", classOf[TestAnnotation])
      propertyHasAnnotation(info, "maybeFive", classOf[TestAnnotation])
      propertyDoesNotHaveAnnotation(info, "readOnly", classOf[TestAnnotation])

      notHaveProperty(info, "getFoo")
      notHaveProperty(info, "getFour")
      notHaveProperty(info, "getMaybeFive")
      notHaveProperty(info, "isReadOnly")

      notHaveProperty(info, "getReadOnly")
    }
  }
}