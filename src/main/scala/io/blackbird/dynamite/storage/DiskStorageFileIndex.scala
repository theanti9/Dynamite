package io.blackbird.dynamite.storage

import java.io.{EOFException, RandomAccessFile}
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.ConcurrentHashMap
import io.blackbird.dynamite.util.Utils.context
import java.util.concurrent.locks.ReentrantReadWriteLock

object DiskStorageFileIndex {
  def loadFromIndexFile(path:String): DiskStorageFileIndex = {
    val index = new DiskStorageFileIndex(path)
    val m = new ConcurrentHashMap[String, Long]()
    val f = new RandomAccessFile(path, "r")
    var done:Boolean = false
    while (!done) {
      try {
        val kLen = f.readInt()
        val key = (0 until kLen).map(_ => f.readChar()).mkString
        val offset = f.readLong()
        m.put(key, offset)
      } catch {
        case e:EOFException => done = true
      }
    }
    f.close()
    index.setIndexMap(m)
    index
  }
}

class DiskStorageFileIndex(path:String) {
  private var keyIndexMap = new ConcurrentHashMap[String, Long]
  private val rwl = new ReentrantReadWriteLock()
  private val fileHandler = new IndexFileHandler(path)
  private var closed = false
  
  private def setIndexMap(m:ConcurrentHashMap[String, Long]) {
    keyIndexMap = m
  }

  def close() {
    closed = true
    fileHandler.close()
  }
  
  def getIndexLocation(key:String):Future[Long] = future {
    if (closed) throw new IllegalAccessException("Already shutting down")
    if (keyIndexMap.containsKey(key)) {
      val lo = keyIndexMap.get(key)
      lo
    } else {
      -1L
    }
  }

  def keyExists(key:String):Boolean = keyIndexMap.keySet.contains(key)

  def addToIndex(key:String, location:Long): Future[Boolean] = {
    val w: Future[Boolean] = future {
      val indexCommit = fileHandler.write(key, location)
      //println(s"Setting $key at $location")
      keyIndexMap.put(key, location)
      val t = for {
        commit <- indexCommit
      } yield commit
      Await.result(t, 5 seconds)
    }
    w
  }
}
