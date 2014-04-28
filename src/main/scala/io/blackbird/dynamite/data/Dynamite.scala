package io.blackbird.dynamite.data

import java.util.concurrent.ConcurrentHashMap

object Dynamite {
  private val buckets = new ConcurrentHashMap[String, Bucket]

  def forceLoad(name:String) = {
    val b = Bucket.create(name)
    buckets.put(name, b)
  }

  def getBucket(name:String): Bucket = {
    if (buckets.containsKey(name)) {
      buckets.get(name)
    } else {
      val b = Bucket.create(name)
      buckets.put(name, b)
      b
    }
  }
}