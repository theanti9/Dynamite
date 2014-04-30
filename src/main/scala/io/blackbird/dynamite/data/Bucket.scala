package io.blackbird.dynamite.data

import io.blackbird.dynamite.storage.DiskStorage
import java.io.File
import scala.concurrent.{Future, Await, future}
import scala.concurrent.duration._
import io.blackbird.dynamite.util.Utils.context
import scala.collection.mutable

object Bucket {
  private val loadedBuckets: mutable.Map[String, Bucket] = mutable.Map[String, Bucket]()
  def create(name:String):Bucket = {
    if (loadedBuckets.contains(name)) return loadedBuckets.get(name).get
    if (exists(name)) {
      val b = new Bucket(name)
      loadedBuckets.put(name, b)
      return b
    }
    val storage = new File(DiskStorage.storageDir)
    val bucketDir = new File(storage, name)
    bucketDir.mkdir()
    for (i <- (1 to 3).par) {
      val f = new File(bucketDir.getAbsoluteFile, name+i.toString+".data").createNewFile()
    }
    val b = new Bucket(name)
    loadedBuckets.put(name, b)
    b
  }

  def getBucket(name:String):Bucket = {
    loadedBuckets.get(name) match {
      case Some(b) => b
      case None => create(name)
    }
  }
  
  def exists(name:String):Boolean = new File(DiskStorage.storageDir).list().exists(p =>{p == name})
}

class Bucket private(name:String)  {
  private val bucketName: String = name
  private val diskStorage: DiskStorage = DiskStorage.loadBucketStorage(bucketName)

  def set(key:String, value:String, timeout:Int = 5) {
    Await.result(setAsync(key, value), timeout seconds)
  }

  def get(key:String, timeout:Int = 5): Option[String] = {
    Await.result(getAsync(key), timeout seconds)
  }
  
  def rem(key:String, timeout:Int = 5) {
    Await.result(remAsync(key), timeout seconds)
  }

  def setAsync(key:String, value:String): Future[Boolean] = {
    val kvp = new KeyValuePair(key,value)
    future {
      diskStorage.store(kvp)
    }
  }

  def getAsync(key:String): Future[Option[String]] = {
    diskStorage.fetch(key)
  }
  
  def remAsync(key:String): Future[Boolean] = {
    diskStorage.rem(key)
  }
  
  def close(){
    diskStorage.close()
  }
}
