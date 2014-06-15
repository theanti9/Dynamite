package io.blackbird.dynamite.storage

import java.io.RandomAccessFile
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent._
import java.nio.charset.Charset

/**
 * Created by ryan on 4/26/2014.
 */
class IndexFileHandler(path:String) {
  private val US_ASCII_CHARSET = Charset.forName("US-ASCII")
  private val writeHandle = new RandomAccessFile(path, "rw")
  private val writeQueue = new LinkedBlockingQueue[((String, Long), Promise[Boolean])]()
  private val writeConsumer = new WriteConsumer(writeHandle, writeQueue)
  private val writeConsumerThread = new Thread(writeConsumer)
  writeConsumerThread.start()

  def write(key:String, location:Long): Future[Boolean] = {
    val value = promise[Boolean]()
    writeQueue.put(((key,location),value))
    value.future
  }

  def close() {
    writeConsumer.close
    writeConsumerThread.join()
    writeHandle.close()
  }
  
  class WriteConsumer(handle:RandomAccessFile, queue:LinkedBlockingQueue[((String, Long), Promise[Boolean])])
    extends FileConsumer[(String, Long), Boolean](handle, queue) {
    def handle(param:(String, Long), result:Promise[Boolean]) {
      handle.synchronized {
        if (handle.getChannel().size() > 0) {
          val position = handle.getChannel().size()
          if (handle.getChannel.position() != position) {
            handle.getChannel.position(position)
          }
        }
        handle.write(param._1.getBytes(US_ASCII_CHARSET))
        handle.writeLong(param._2)
      }
      result success true
    }
  }
}
