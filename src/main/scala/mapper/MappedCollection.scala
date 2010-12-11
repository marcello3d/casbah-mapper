package com.bumnetworks.casbah.mapper

import com.mongodb.{DBCollection, DBCursor, WriteConcern, WriteResult}
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoConnection, MongoCursor, MongoCollectionWrapper}

import scala.reflect.Manifest
import scalaj.collection.Imports._

case class MongoMappedCursor[P <: AnyRef : Manifest](val wrapped: DBCursor, val mapper: Mapper[P]) extends Iterator[P] {
  def count = wrapped.count
  def numGetMores = wrapped.numGetMores
  def numSeen =  wrapped.numSeen
  def remove() = wrapped.remove

  def curr: P = mapper.asObject(wrapped.curr)
  def explain = wrapped.explain

  def next: P = mapper.asObject(wrapped.next)
  def hasNext: Boolean = wrapped.hasNext

  override def size = count.intValue

  def batchSize(n: Int) = copy(wrapped = wrapped.batchSize(n), mapper = mapper)
  def getSizes() = wrapped.getSizes.asScala
  def hint(indexKeys: DBObject) = copy(wrapped = wrapped.hint(indexKeys), mapper = mapper)
  def hint(indexName: String) = copy(wrapped = wrapped.hint(indexName), mapper = mapper)

  def limit(n: Int) = copy(wrapped = wrapped.limit(n), mapper = mapper)

  def skip(n: Int) = copy(wrapped = wrapped.skip(n), mapper = mapper)
  def snapshot() = copy(wrapped = wrapped.snapshot(), mapper = mapper)
  def sort(orderBy: DBObject) = copy(wrapped = wrapped.sort(orderBy), mapper = mapper)

  def toArray(): Array[P] = throw new Exception("you've got to be kidding")
  def toArray(min: Int): Array[P] = toArray()

  override def toString() =
    "MongoMappedCursor{Iterator[%s] with %d objects.}".format(manifest[P].erasure.getName, count)
}

class MongoMappedCollection[P <: AnyRef : Manifest](val underlying: com.mongodb.DBCollection) extends Iterable[P] with MongoCollectionWrapper {
  protected[mapper] val mapper: Mapper[P] = {
    Mapper[P](manifest[P].erasure.getName).map {
      case m: Mapper[_] => m.asInstanceOf[Mapper[P]]
    }.getOrElse(throw new Exception("no mapper found for %s".format(manifest[P].erasure)))
  }

  implicit val p2dbo: (P) => DBObject = mapper.asDBObject _

  def insert(ps: P*) = underlying.insert(ps.map(mapper.asDBObject(_)).toList.asJava)
  def insert(p: P, wc: WriteConcern) = underlying.insert(mapper.asDBObject(p), wc)
  def insert(ps: Array[P], wc: WriteConcern) = underlying.insert(ps.map(mapper.asDBObject(_)).toArray, wc)
  def insert(ps: List[P]) = underlying.insert(ps.map(mapper.asDBObject(_)).asJava)

  def save(p: P, wc: Option[WriteConcern] = None) = wc match {
    case Some(writeConcern) => underlying.save(mapper.asDBObject(p), writeConcern)
    case _ => underlying.save(mapper.asDBObject(p))
  }

  override def elements = find
  override def iterator = find
  def find() = new MongoMappedCursor[P](underlying.find, mapper)(manifest[P])
  def find(ref: DBObject) = new MongoMappedCursor[P](underlying.find(ref), mapper)(manifest[P])
  def find(ref: DBObject, keys: DBObject) = new MongoMappedCursor[P](underlying.find(ref, keys), mapper)(manifest[P])
  def find(ref: DBObject, fields: DBObject, numToSkip: Int, batchSize: Int) = new MongoMappedCursor[P](underlying.find(ref, fields, numToSkip, batchSize), mapper)(manifest[P])
  def findOne() = Option(underlying.findOne()).map(mapper.asObject(_))
  def findOne(o: DBObject) = Option(underlying.findOne(o)).map(mapper.asObject(_))
  def findOne(o: DBObject, fields: DBObject) = Option(underlying.findOne(o, fields)).map(mapper.asObject(_))
  def findOne(obj: Object) = Option(underlying.findOne(obj)).map(mapper.asObject(_))
  def findOne(obj: Object, fields: DBObject) = Option(underlying.findOne(obj, fields)).map(mapper.asObject(_))
  def findAndModify[A <% DBObject : Manifest, B <% DBObject : Manifest](query: A, update: B) = Option(underlying.findAndModify(query, update)).asInstanceOf[P]
  def findAndModify[A <% DBObject : Manifest, B <% DBObject : Manifest, C <% DBObject : Manifest](query: A, sort: B, update: C) = Option(underlying.findAndModify(query, sort, update)).asInstanceOf[P]
  def findAndModify[A <% DBObject : Manifest, B <% DBObject : Manifest, C <% DBObject : Manifest, D <% DBObject : Manifest](
    query: A, fields: B, sort: C, remove: Boolean, update: D, returnNew: Boolean, upsert: Boolean
  ) = Option(underlying.findAndModify(query, fields, sort, remove, update, returnNew, upsert)).asInstanceOf[P]

  def findAndRemove[A <% DBObject : Manifest](query: A) = Option(underlying.findAndRemove(query)).asInstanceOf[P]

  override def head = findOne.get
  override def headOption = findOne
  override def tail = find.skip(1).toList

  def setHintFields(lst: List[DBObject]) = underlying.setHintFields(lst.asJava)

  def setObjectClass[A <: DBObject : Manifest](c: Class[A]) =
    throw new IllegalArgumentException("operation not supported")

  override def toString() =
    "MongoMappedCollection[%s](%d objects)".format(manifest[P].erasure, find.count)

  def update(q: DBObject, o: DBObject) = underlying.update(q, o)
  def update(q: DBObject, o: DBObject, upsert: Boolean, multi: Boolean) = underlying.update(q, o, upsert, multi)
  def updateMulti(q: DBObject, o: DBObject) = underlying.updateMulti(q, o)

  override def equals(obj: Any) = obj match {
    case other: MongoCollectionWrapper => underlying.equals(other.underlying)
    case _ => false
  }
}
