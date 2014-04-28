package io.blackbird.dynamite.storage

import io.blackbird.dynamite.data.{KeyValuePair, Bucket}
import scala.collection.immutable.LinearSeq
import io.blackbird.dynamite.hashing.{MD5HashFunction, ConsistentHash}
import java.io.{EOFException, File, RandomAccessFile}
import scala.concurrent._
import scala.concurrent.duration._
import io.blackbird.dynamite.util.Utils.context

object DiskStorage {
  val storageDir:String = "data"

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

  def close() {
    fileMap.foreach(k => {
      k._2._1.close
      k._2._2.close
    })
  }
  
  def store(kvp:KeyValuePair): Boolean = fileNodeHash.get(Some(kvp.key)) match {
      case Some(node) => fileMap.get(node) match {
        case Some(f_i) => {
          Await.result(for {
            w <- f_i._1.write(kvp.value)
            i <- f_i._2.addToIndex(kvp.key, w)
          } yield i, 5 seconds)
        } case None => println("WHAT THE SHIT, NO NODE?"); false
      } case None => println("WHAT THE SHIT, NO HASH?"); false
    }

  def fetch(key:String): Future[Option[String]] = fileNodeHash.get(Some(key)) match {
      case Some(node) => fileMap.get(node) match {
        case Some(f_i) => {
          val value:Future[Option[String]] = for {
            i:Long <- f_i._2.getIndexLocation(key)
            r <- f_i._1.read(i)
          } yield r
          value
        } case None => println("WHAT THE SHIT, NO NODE?"); future { None }
      } case None => println("WHAT THE SHIT, NO HASH?"); future { None }
    }
}
