package io.blackbird.dynamite.storage

import java.io.RandomAccessFile
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Future, promise, Promise}
import io.blackbird.dynamite.util.Utils

/**
 * Created by ryan on 4/26/2014.
 */
class DataFileHandler(path:String) {
  private val readHandle = new RandomAccessFile(path, "r")
  private val writeHandle = new RandomAccessFile(path, "rw")
  private val writeQueue = new LinkedBlockingQueue[(String, Promise[Long])]()
  private val readQueue = new LinkedBlockingQueue[(Long, Promise[Option[String]])]()

  private val readConsumer = new ReadConsumer(readHandle,readQueue)
  private val readConsumerThread = new Thread(readConsumer)
  readConsumerThread.start()

  private val writeConsumer = new WriteConsumer(writeHandle, writeQueue)
  private val writeConsumerThread = new Thread(writeConsumer)
  writeConsumerThread.start()

  private var closed = false

  def read(index:Long): Future[Option[String]] = {
    if (closed) throw new IllegalAccessException("Already shutting down")
    val value = promise[Option[String]]()
    readQueue.put((index,value))
    value.future
  }

  def write(content:String): Future[Long] = {
    if (closed) throw new IllegalAccessException("Already shutting down")
    val value = promise[Long]()
    writeQueue.put((content,value))
    value.future
  }

  def close() {
    closed = true
    writeConsumer.close()
    readConsumer.close()
    readConsumerThread.join()
    readHandle.close()
    writeConsumerThread.join()
    writeHandle.close()
  }

  class ReadConsumer(handle:RandomAccessFile, queue:LinkedBlockingQueue[(Long, Promise[Option[String]])])
    extends FileConsumer[Long, Option[String]](handle, queue) {
    def handle(param:Long, result:Promise[Option[String]]) {
      if (param > handle.length()) {
        result failure new IllegalAccessException
      }
      handle.synchronized {
        if (param < 0 || param > handle.length()){
          result success None
        } else {
          handle.getChannel.position(param)
          val len = handle.readInt()
          result success Some(Utils.longRange(0, len).map(_=>handle.readChar).mkString)
        }
      }
    }
  }

  class WriteConsumer(handle:RandomAccessFile, queue:LinkedBlockingQueue[(String, Promise[Long])])
    extends FileConsumer[String, Long](handle, queue) {
    def handle(param:String, result:Promise[Long]) {
      var start = 0L
      handle.synchronized {
        if (handle.length() > 0) {
          val position = handle.length()
          if (handle.getChannel.position() != position) {
            handle.getChannel.position(position)
          }
          start = position
        }
        handle.writeInt(param.length)
        handle.writeChars(param)
      }
      result success start
    }
  }
}
