package io.blackbird.dynamite

import io.blackbird.dynamite.data.{Dynamite, Bucket}
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.Future
import io.blackbird.dynamite.util.Utils.context
import java.util.concurrent.{TimeUnit, Executors, BlockingQueue, LinkedBlockingQueue}
import scala.io.Source
import java.util.Random
import io.blackbird.dynamite.util.Utils
import io.blackbird.dynamite.hashing.MD5HashFunction
import scala.collection.mutable
import io.blackbird.dynamite.network.NetListener

/**
 * Created by ryan on 4/25/2014.
 */
object Runner {

  def main(args:Array[String]) {
    //interactive()
    val listener = new NetListener(9696)
    listener.run()
  }

  def interactive() {
    var b:Bucket = Bucket.create("test")
    var running:Boolean = true
    println("Loaded!")
    while (running) {
      print("> ")
      System.out.flush()
      val input = readLine()
      input match {
        case "exit" => {
          println("Starting safe shutdown...")
          running = false 
          b.close();
          println("Killing execution pool")
          Utils.pool.shutdown()
          println("Execution pool shut down. exiting...")
        }
        case _ => {
          val cmd = input.split("\\s", 2)
          cmd.length match {
            case 2 => cmd(0) match {
              case "use" =>b.close(); b = Bucket.create(cmd(1)); println(s"Switched to bucket ${cmd(1)}")
              case "set" => {
                val params = cmd(1).split(" ", 2)
                params.length match {
                  case 2 => {
                    val start = System.currentTimeMillis()
                    b.set(params(0), params(1))
                    val end = System.currentTimeMillis()
                    println(s"Finished write in ${(end - start)/1000.0} seconds")
                  }
                  case _ => println("Invalid params for SET")
                }
              } case "get" =>{
                val start = System.currentTimeMillis()
                val out = b.get(cmd(1)).getOrElse("<empty>")
                val end = System.currentTimeMillis()
                println(s"'${out}'")
                println(s"Read finished in ${(end - start)/1000.0} seconds")
              } case "del" => {
                val start = System.currentTimeMillis()
                b.rem(cmd(1))
                val end = System.currentTimeMillis()
                println(s"Remove finished in ${(end - start)/1000.0} seconds")
              }
              case _ => println(s"Unknown command ${cmd(0)}")
            }
            case _ => println(s"Invalid command $cmd")
          }
        }
      }
      println()
    }
  }

}
