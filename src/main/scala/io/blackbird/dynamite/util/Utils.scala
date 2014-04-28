package io.blackbird.dynamite.util

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

/**
 * Created by ryan on 4/26/2014.
 */
object Utils {

  lazy val pool = Executors.newFixedThreadPool(20)
  implicit lazy val context = ExecutionContext.fromExecutor(pool)

  def longRange(first:Long, last:Long) = new Iterator[Long] {
    private var i = first

    def hasNext = i < last

    def next() = {
      val r = i
      i += 1
      r
    }
  }
}
