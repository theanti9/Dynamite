package io.blackbird.dynamite.network

import java.net.InetSocketAddress
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import java.util.concurrent.Executors._
import scala.concurrent.ExecutionContext
import scala.concurrent._
import scala.collection.mutable.{LinkedHashMap, SynchronizedMap}

import java.nio.ByteBuffer

object NetListener {
  	lazy val handlerPool = newFixedThreadPool(1, defaultThreadFactory)
	implicit lazy val context = ExecutionContext.fromExecutor(handlerPool)
	val handling = scala.collection.mutable.HashSet[SelectionKey]()
}

class NetListener(port:Int) {
	val selector: Selector = Selector.open()
	val ssc: ServerSocketChannel = ServerSocketChannel.open()
	ssc.configureBlocking(false)

	val socket = ssc.socket();
	val activeClients = new LinkedHashMap[SelectionKey, ByteBuffer]() with SynchronizedMap[SelectionKey, ByteBuffer]
	
	def run() {
	  socket.bind(new InetSocketAddress(port))
	  ssc.register(selector, SelectionKey.OP_ACCEPT)
	  while (true) {
		  val num = selector.select(100)
		  val selectedKeys = selector.selectedKeys().iterator()
		  while (selectedKeys.hasNext()) {
		    val key = selectedKeys.next().asInstanceOf[SelectionKey]
		    NetListener.handling.contains(key) match {
		      case false => {
		        NetListener.handling += key
			    val handled = future({
			      handle(key)
			    })(NetListener.context)
		      }
		      case true => // Don't do anything. Already being processed.
		    }
		    
		    selectedKeys.remove()
		  }
		  
	  }
	  
	}
	
	def handle(key:SelectionKey) {
	  if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
		val socketChannel = key.channel().asInstanceOf[ServerSocketChannel].accept()
		println(s"Client accepted: ${socketChannel.getRemoteAddress().toString()}")
		socketChannel.configureBlocking(false)
		val newKey = socketChannel.register(selector, SelectionKey.OP_READ)
	  } else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
		val socketChannel = key.channel().asInstanceOf[SocketChannel]
		val buff = ByteBuffer.allocate(4096)
		val read = socketChannel.read(buff)
		if (read > 0) {
		  println("Got data to read")
		  activeClients.get(key) match {
		    case Some(prevBuff) => prevBuff.put(buff); prevBuff.flip(); activeClients(key) = prevBuff
		    case None => activeClients(key) = buff
		  }
		  val totalBuff = activeClients.get(key)
		  totalBuff match {
		    case Some(msgBuff) => {
		      println(s"TotalBuffer of ${msgBuff.capacity() - msgBuff.remaining()}")
		      println(s"Size int = ${msgBuff.duplicate().getInt(0)}")
		      if (msgBuff.duplicate().getInt(0) == msgBuff.capacity() - msgBuff.remaining() - 4) {
		        println("Calling handle...")
		        msgBuff.flip()
		        try {
		        	ProtocolParser.handle(msgBuff, socketChannel)
		        	activeClients.remove(key)
		        	NetListener.handling -= key
		        } catch {
		          case e:Exception => println(e.getStackTraceString)
		        }
		        
		      }
		    }
		    case None => // wat
		  }
		}
	  }
	}
	
}