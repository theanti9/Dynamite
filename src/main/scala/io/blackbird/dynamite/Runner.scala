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

/**
 * Created by ryan on 4/25/2014.
 */
object Runner {

  def main(args:Array[String]) {
    println("Loading...")
    //dict_test()
    //volume_test()
    interactive()
  }
  
  def dict_test() {
    println("Running dictionary test...")
    val b:Bucket = Bucket.create("dictionary")
    val l = scala.io.Source.fromFile("en-US.dic").getLines.toList
    val write_time_start = System.currentTimeMillis()
    var write_millis = 0L
    var write_counter = 0.0
    l.map(word => {
      val write_start = System.currentTimeMillis()
      b.set(word, word)
      val write_end = System.currentTimeMillis()
      write_millis += write_end - write_start
      write_counter += 1.0
    })
    val write_time_end = System.currentTimeMillis()
    println(s"Wrote dictionary in ${(write_time_end - write_time_start) / 1000.0} seconds")
    println(s"Average write time: ${write_millis/write_counter/1000.0} seconds")
    b.close()
  }
  
  def volume_test() {
    println("Running volume test!")
    val b:Bucket = Bucket.create("volume")
    val write_time_start = System.currentTimeMillis()
    var write_millis = 0L
    (1 to 10000000).map(i=>{
      val write_start = System.currentTimeMillis()
      b.set(i.toString(),i.toString())
      val write_end = System.currentTimeMillis()
      write_millis += write_end - write_start
    })
    val write_time_end = System.currentTimeMillis()
    println(s"Wrote 10000000 keys in ${(write_time_end - write_time_start) / 1000.0} seconds. Average write time: ${write_millis/1000.0/1000.0} seconds")
    println("Starting 1000 random reads")
    val random = new Random()
    var total_millis = 0L
    (1 to 1000).foreach(_ =>{
      val r = random.nextInt(10000000)
      val read_start = System.currentTimeMillis()
      b.get(r.toString())
      val read_end = System.currentTimeMillis()
      total_millis += read_end - read_start
    })
    println(s"Finished random reads with average time of ${total_millis/1000.0/1000.0} seconds.")
    b.close()
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
