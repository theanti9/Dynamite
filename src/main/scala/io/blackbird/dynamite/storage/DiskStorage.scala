package io.blackbird.dynamite.storage

import io.blackbird.dynamite.data.{KeyValuePair, Bucket}
import scala.collection.immutable.LinearSeq
import io.blackbird.dynamite.hashing.{MD5HashFunction, ConsistentHash}
import java.io.{EOFException, File, RandomAccessFile}
import scala.concurrent._
import scala.concurrent.duration._
import io.blackbird.dynamite.util.Utils.context

object DiskStorage {
  val storageDir:String = "E:\\data"

  def loadBucketStorage(bucket:String): DiskStorage =
    new DiskStorage(bucket, new File(storageDir,bucket).listFiles().map(f => {
      val v = f.getAbsoluteFile
      v.getAbsolutePath}).toList, 0)

}

class DiskStorage(bucket:String, fileNodes:LinearSeq[String], maxNodeSize:Int) {
  private val storageBucket:String = bucket
  private val fileNodeHash = ConsistentHash(new MD5HashFunction, 10000, fileNodes.filter(s=> !s.endsWith(".index")))
  private val fileMap: Map[String, (DataFileHandler, DiskStorageFileIndex)] = fileNodes.par
                      .filter(s => !s.endsWith("index"))
                      .map(f =>
                        f -> {
                          (new DataFileHandler(f),
                            DiskStorageFileIndex.loadFromIndexFile(f+".index"))
                        }
                      ).seq.toMap[String, (DataFileHandler, DiskStorageFileIndex)]
  private val hashFunc: MD5HashFunction = new MD5HashFunction

  def close() {
    fileMap.par.foreach(k => {
      k._2._1.close
      k._2._2.close
    })
  }
  
  def rem(key:String): Future[Boolean] = {
    try{
      val hash = hashFunc.hash(key)
      val f_i = mapKey(key)
      f_i._2.removeKey(hash)
    } catch {
      case e:NoSuchFieldException => future { false }
    }
  }
  
  def store(kvp:KeyValuePair): Boolean =  {
      try {
        val hash = hashFunc.hash(kvp.key)
        val f_i = mapKey(kvp.key)
        Await.result(for {
          w <- f_i._1.write(kvp.value)
          i <- f_i._2.addToIndex(hash, w)
        } yield i, 5 seconds)
      } catch {
      	case e:NoSuchFieldException => false 
      }
    }

  def fetch(key:String): Future[Option[String]] = {
    try {
      val hash = hashFunc.hash(key)
      val f_i = mapKey(key)
      val value:Future[Option[String]] = for {
	    i:Long <- f_i._2.getIndexLocation(hash)
	    r <- f_i._1.read(i)
	  } yield r
	  value
    } catch {
      case e:NoSuchFieldException => future { None }
    }
    
  }
  
  private def mapKey(key:String):(DataFileHandler, DiskStorageFileIndex) = fileNodeHash.get(Some(key)) match {
      case Some(node) => fileMap.get(node) match {
        case Some(f_i) => f_i
        case None => throw new NoSuchFieldException()
      } 
      case None => throw new NoSuchFieldException()
    }
}
