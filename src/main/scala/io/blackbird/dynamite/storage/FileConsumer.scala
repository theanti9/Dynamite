package io.blackbird.dynamite.storage

import java.io.RandomAccessFile
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}
import scala.concurrent.Promise

/**
 * Created by ryan on 4/26/2014.
 */
abstract class FileConsumer[T,S](handle:RandomAccessFile, queue:LinkedBlockingQueue[(T, Promise[S])]) extends Runnable {
  var stop:Boolean = false
  def run() {
    while (!stop || queue.size()>0) {
      Option(queue.poll(1, TimeUnit.SECONDS)) match {
        case Some(d) => {
          try{
            handle(d._1, d._2)
          } catch {
            case e:Exception => d._2 failure e
          }
        } case None => // Empty. Do nothing
      }
    }
  }

  def close() {
    stop = true
  }

  def handle(param:T, result:Promise[S])
}