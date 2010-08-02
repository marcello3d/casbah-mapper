
package com.bumnetworks.casbah
package mongodb
package test

import net.lag.configgy.Configgy

import org.specs._
import org.specs.specification.PendingUntilFixed

import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanInfo

import com.novus.casbah.Imports._
import Imports.log
import mongodb.mapper.Mapper
import mongodb.mapper.annotations._

@BeanInfo
@MappedBy(classOf[WidgetMapper])
class Widget(@ID var name: String, @Key var price: Int) {
  def this() = this(null, 0)
  override def toString() = "Widget(" + name + ", " + price + ")"
}

@BeanInfo
@MappedBy(classOf[PiggyMapper])
class Piggy {
  @ID(auto = true)
  var id: String = _

  @Key
  var giggity: String = _

  def this(g: String) = {
    this()
    giggity = g
  }
}

@BeanInfo
@MappedBy(classOf[ChairMapper])
class Chair {
  @ID(auto = true)
  var id: String = _

  @Key
  var optional_piggy: Option[Piggy] = None
}

class ChairMapper extends Mapper[String, Chair] {
  conn = MongoConnection()
  db = "mapper_test"
  coll = "chairs"
}

class WidgetMapper extends Mapper[String, Widget] {
  conn = MongoConnection()
  db = "mapper_test"
  coll = "widgets"
}

class PiggyMapper extends Mapper[String, Piggy] {
  conn = MongoConnection()
  db = "mapper_test"
  coll = "piggies"
}

class MapperSpec extends Specification with PendingUntilFixed {
  detailedDiffs()

  doBeforeSpec {
    Configgy.configure("src/test/resources/casbah.conf")
    // drop db
  }

  "a mapper" should {
    shareVariables

    val widget = new Widget("something", 7824)
    Mapper[Widget].upsert(widget)

    "discover mapper for a class" in {
      Mapper[Piggy].getClass.getSimpleName must_== classOf[PiggyMapper].getSimpleName
    }

    "compute id" in {
      Mapper[Widget].idProp must haveClass[java.beans.PropertyDescriptor]
      Mapper[Widget].idProp.getName must_== "name"
    }

    "cull non-ids" in {
      Mapper[Widget].nonIdProps must exist(pd => pd.getName == "price")
    }

    "convert object to MongoDBObject" in {
      val dbo = Mapper[Widget].asMongoDBObject(widget)
      dbo must havePair("_id", "something")
    }

    "retrieve an object" in {
      Mapper[Widget].findOne(widget.name) must beSome[Widget].which {
        loaded =>
          loaded.name must_== widget.name
        loaded.price must_== widget.price
      }
    }

    "automatically assign (& retrieve objects by) MongoDB OID-s" in {
      val piggy = new Piggy("oy vey")
      Some(Mapper[Piggy].upsert(piggy)) must beSome[Piggy].which {
        saved =>
          saved.id must notBeNull
        Mapper[Piggy].findOne(saved.id) must beSome[Piggy].which {
          retrieved =>
            retrieved.id must_== saved.id
          retrieved.giggity must_== piggy.giggity
        }
      }
    }

    "save & de-serialize nested documents" in {
      val before = new Chair
      before.optional_piggy = Some(new Piggy("foo"))

      val id = Mapper[Chair].upsert(before).id

      Mapper[Chair].findOne(id) must beSome[Chair].which {
        after =>
          after.optional_piggy must beSome[Piggy].which {
            piggy =>
              piggy.giggity == before.optional_piggy.get.giggity
          }
      }
    }
  }

  "a mapped collection" should {
    val coll = MongoConnection()("mapper_test").mapped[Piggy]
    "return objects of required class" in {
      coll.findOne must beSome[Piggy]
    }
  }
}
