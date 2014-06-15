package io.blackbird.dynamite.network

import java.nio.CharBuffer
import scala.concurrent.Future
import java.nio.ByteBuffer
import io.blackbird.dynamite.data.Bucket
import java.nio.channels.SocketChannel
import scala.util.Success
import scala.concurrent.ExecutionContext
import io.blackbird.dynamite.network.NetListener.context
import java.nio.charset.Charset

object ProtocolParser {

	def handle(inputBuffer:ByteBuffer, sock:SocketChannel ) {
	  val len = inputBuffer.getInt(0)
	  println(s"Length: $len")
	  val buff = new Array[Byte](len)
	  inputBuffer.position(4)
	  inputBuffer.get(buff)
	  val input = new String(buff, Charset.forName("US-ASCII"))
	  println(s"Got input: $input")
	  val partOne = input.split(":", 2)
	  partOne.length match {
	    case 2 => {
	      val bucketName = partOne(0)
	      val partTwo = partOne(1)
	      val bucket = Bucket.getBucket(bucketName)
	      val cmd = partTwo.split("\\s", 2)
	      cmd.length match {
	        case 2 => cmd(0) match {
	          case "set" => {
	            val params = cmd(1).split(" ", 2)
	            val result = bucket.setAsync(params(0), params(1))
	            result onSuccess {
	              case b => {
	                val responseBuffer = ByteBuffer.allocate(6)
	                responseBuffer.putInt(2)
	                responseBuffer.putShort(1)
	                responseBuffer.flip()
	                sock.write(responseBuffer)
	              }
	            }
	            result onFailure {
	              case e => {
	                println(e.getMessage())
	                val responseBuffer = ByteBuffer.allocate(6)
	                responseBuffer.putInt(2)
	                responseBuffer.putShort(0)
	                responseBuffer.flip()
	                sock.write(responseBuffer)
	              }
	            }
	          }
	          case "get" => {
	            val result = bucket.getAsync(cmd(1))
	            result onSuccess {
	              case Some(value) => {
	                val responseBuffer = ByteBuffer.allocate(4+value.length())
	                responseBuffer.putInt(value.length())
	                responseBuffer.put(value.getBytes(Charset.forName("US-ASCII")))
	                responseBuffer.flip()
	                sock.write(responseBuffer)
	              }
	              case None => {
	                val responseBuffer = ByteBuffer.allocate(4)
	                responseBuffer.putInt(0)
	                responseBuffer.flip()
	                sock.write(responseBuffer)
	              }
	            }
	          } 
	          case "del" =>
	          case _ =>
	        }
	        case _ =>
	      }
	    } 
	    case _ =>
	  }
	} 
}