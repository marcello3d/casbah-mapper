package com.bumnetworks.casbah
package mapper

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.Implicits._
import com.mongodb.{DBObject, WriteResult}

trait Mapped {
  self: AnyRef =>

    protected[mapper] lazy val mapper: Mapper[AnyRef] = {
      Mapper(getClass.getName)
      .map((m: Mapper[_]) => m.asInstanceOf[Mapper[AnyRef]])
      .getOrElse(throw new Exception("no mapper found for %s which extends trait Mapped".format(getClass)))
    }

  protected[mapper] lazy val coll: MongoMappedCollection[AnyRef] = {
    mapper.coll
    .getOrElse(throw new Exception("no collection found in mapper %s".format(mapper.getClass)))
  }

  private[mapper] implicit def this2dbo(a: AnyRef): DBObject = mapper.asDBObject _

  def insert: WriteResult = coll.insert(this)
  def insert(wc: WriteConcern): WriteResult = coll.insert(this, wc)
  def save = coll.save(this)
  def save(wc: WriteConcern) = coll.save(this, wc)
}
