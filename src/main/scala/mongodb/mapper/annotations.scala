package com.bumnetworks.casbah.mongodb.mapper

import scala.reflect.BeanInfo
import scala.annotation.target.{getter, setter}

package object annotations {
  type ID         = raw.ID       @getter
  type Key        = raw.Key      @getter
}
