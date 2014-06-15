package io.blackbird.dynamite.storage

import java.io.{EOFException, RandomAccessFile}
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.ConcurrentHashMap
import io.blackbird.dynamite.util.Utils.context
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.io.FileInputStream
import java.nio.charset.Charset
import java.nio.ByteBuffer
import java.nio.ByteOrder

object DiskStorageFileIndex {
  def loadFromIndexFile(path:String): DiskStorageFileIndex = {
    val index = new DiskStorageFileIndex(path)
    val m = new ConcurrentHashMap[String, Long]()
    val f = new RandomAccessFile(path, "r")
    val b = new FileInputStream(path)
    val bytes = new Array[Byte](4000)
    var done:Boolean = false
    while (b.read(bytes) > -1) {
      for (i <- 0 until 100) {
        val key = new String(bytes.slice(i*40, i*40+32), Charset.forName("ascii"))
        val sl = bytes.slice(i*40+32, i*40+40)
        
        sl.length match {
          case 8 => {
        	  val offset = ByteBuffer.wrap(sl).order(ByteOrder.BIG_ENDIAN).asLongBuffer().get()
		      offset match {
		        case -1 => if (m.containsKey(key)) m.remove(key)
		        case _ => m.put(key, offset)
		      }
          } case _ => // Ignore, hit the end of the actual buffer.
        }
        
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

  def removeKey(key:String): Future[Boolean] = {
    val w: Future[Boolean] = future {
      val indexCommit = fileHandler.write(key, -1)
      val t = for {
        commit <- indexCommit
      } yield commit
      Await.result(t, 5 seconds)
    }
    keyIndexMap.remove(key)
    w
  }
  
  def addToIndex(key:String, location:Long): Future[Boolean] = {
    val w: Future[Boolean] = future {
      val indexCommit = fileHandler.write(key, location)
      val t = for {
        commit <- indexCommit
      } yield commit
      Await.result(t, 5 seconds)
    }
    keyIndexMap.put(key, location)
    w
  }
}
