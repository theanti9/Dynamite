package io.blackbird.dynamite.hashing

/*
 * Kindly borrowed from https://gist.github.com/opyate/1927001
 */

import scala.collection.immutable.LinearSeq
import java.util.{TreeMap => JTreeMap}
import java.util.{SortedMap => JSortedMap}
import java.security.MessageDigest
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.math.BigInteger

/**
 * Inspired by http://www.lexemetech.com/2007/11/consistent-hashing.html
 */
object ConsistentHash {
  def apply[K <% Ordered[K], V](
                                 hashFunction: HashFunction[K],
                                 numberOfReplicas: Int,
                                 nodes: LinearSeq[V]) = {

    val ch = new ConsistentHash(hashFunction, numberOfReplicas, nodes)

    nodes.foreach{n => {
      ch.add(Some(n))
    }}

    ch
  }
}

/**
 * HashFunction returns a hash of type K, which will be Ordered for the TreeMap
 * Nodes are of type V
 */
class ConsistentHash[K <% Ordered[K], V] private(
                                                  hashFunction: HashFunction[K],
                                                  numberOfReplicas: Int,
                                                  nodes: LinearSeq[V]) {

  var numNodes = 0
  val SEPARATOR = ":"
  val circle: JSortedMap[K, V] = new JTreeMap[K, V]()

  /**
   * Add a node V to the pool.
   */
  def add(node: Option[V]) {
    node.map{n =>
      (0 to numberOfReplicas).foreach{i =>
        val hash = hashFunction.hash("%s%s%s".format(n, SEPARATOR, i))
        circle.put(hash, n)
      }
    }
    numNodes = numNodes + 1
  }

  /**
   * Remove a node V from the pool.
   */
  def remove(node: Option[V]) {
    node.map{n =>
      (0 to numberOfReplicas).foreach{i =>
        val hash = hashFunction.hash("%s%s%s".format(n, SEPARATOR, i))
        circle.remove(hash)
      }
    }
    numNodes = numNodes - 1
  }

  /**
   * @return the cache node that will contain our object, by key.
   */
  def get(key: Option[AnyRef]): Option[V] = circle.isEmpty match {
    case true => throw new RuntimeException("No nodes in ring")
    case false => key.map{k =>
        hashFunction.hash(k) match {
          case hash if !circle.containsKey(hash) => circle.tailMap(hash) match {
              case tailMap if tailMap.isEmpty => Some(circle.get(circle.firstKey))
              case tailMap => Some(circle.get(tailMap.firstKey))
            }
          case hash => Some(circle.get(hash))
        }
      }.getOrElse(None)
  }

  def ringSize(): Int = numNodes
}

trait HashFunction[H] {
  def hash(o: AnyRef): H
}

class MD5HashFunction extends HashFunction[String] {

  val md: MessageDigest = MessageDigest.getInstance("MD5")

  override def hash(o: AnyRef): String = {
    byteArrayToString(md.digest(toBinary(o)))
  }

  private[this] def byteArrayToString(data: Array[Byte]): String = {
    val bigInteger = new BigInteger(1, data)
    var hash = bigInteger.toString(16)
    while (hash.length() < 32) {
      hash = "0" + hash
    }
    hash
  }

  private[this] def toHex(b: Byte): Char = {
    require(b >= 0 && b <= 15, "Byte " + b + " was not between 0 and 15")
    if(b < 10)
      ('0'.asInstanceOf[Int] + b).asInstanceOf[Char]
    else
      ('a'.asInstanceOf[Int] + (b-10)).asInstanceOf[Char]
  }

  private[this] def toBinary(obj: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(obj)
    out.close()
    bos.toByteArray
  }
}