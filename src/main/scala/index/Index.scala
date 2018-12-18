package index

import java.util.UUID
import scala.reflect.ClassTag

class Index[T: ClassTag, K: ClassTag, V: ClassTag](val id: T,
                                                   val DATA_ORDER: Int,
                                                   val META_ORDER: Int)(implicit val ord: Ordering[K])
  extends Partition [T, K, V]{

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1

  val MIN = DATA_MAX
  val MAX = META_MAX * DATA_MAX

  var count = 0

  val meta = new Meta[T, K, V](DATA_ORDER, META_ORDER)

  override def find(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.find(k)
    }
  }

  override def left(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.left(k)
    }
  }

  override def right(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.right(k)
    }
  }

  override def near(k: K): Option[V] = {
    meta.find(k) match {
      case None => None
      case Some(p) => p.near(k)
    }
  }

  override def last: (K, V) = ???

  override def isFull(): Boolean = count == MAX

  override def isEmpty(): Boolean = count == 0

  override def hasMinimum(): Boolean = count >= MIN

  def insertNoPartition(data: Seq[(K, V)]): (Boolean, Int) = {
    val p = new Block[T, K, V](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)

    val (ok, n) = p.insert(data)

    if(!ok) return false -> 0

    meta.insert(Seq(p.last._1 -> p))

    true -> n
  }

  def insert(p: Partition[T, K, V], data: Seq[(K, V)]): (Boolean, Int) = {

    val max = p.last._1

    if(p.isFull()){

      val right = p.split()

      meta.remove(Seq(max))
      meta.insert(Seq(p.last._1 -> p, right.last._1 -> right))

      return true -> 0
    }

    val (ok, n) = p.insert(data)

    if(!ok) return false -> 0

    meta.remove(Seq(max))
    meta.insert(Seq(p.last._1 -> p))

    true -> n
  }

  override def insert(data: Seq[(K, V)]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)

    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = meta.find(k) match {
        case None => insertNoPartition(list)
        case Some(p) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, p.last._1)}
          if(idx > 0) list = list.slice(0, idx)

          insert(p, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    println(s"[insert] pos ${pos} size $size\n")

    count += size

    true -> size
  }

  def merge(left: Partition[T, K, V], lmax: K, right: Partition[T, K, V], rmax: K): Boolean = {
    left.merge(right)

    meta.remove(Seq(lmax, rmax))
    meta.insert(Seq(left.last._1 -> left))

    return true
  }

  def borrow(max: K, p: Partition[T, K, V]): Boolean = {

    val lopt = meta.left(max)
    val ropt = meta.right(max)

    if(lopt.isEmpty && ropt.isEmpty){

      if(p.isEmpty()){
        println(s"\nonly one node empty\n")
        return meta.remove(Seq(max))
      }

      println(s"\nonly one node\n")

      if(!ord.equiv(max, p.last._1)){
        meta.remove(Seq(max))
        meta.insert(Seq(p.last._1 -> p))
      }

      return true
    }

    if(lopt.isDefined && lopt.get.canBorrowTo(p)){
      val left = lopt.get
      val lmax = left.last

      left.borrowLeftTo(p)

      meta.remove(Seq(lmax._1, max))
      meta.insert(Seq(
        left.last._1 -> left,
        p.last._1 -> p
      ))

      println(s"borrow from left")

      return true
    }

    if(ropt.isDefined && ropt.get.canBorrowTo(p)){

      val right = ropt.get
      val rmax = right.last._1

      right.borrowRightTo(p)

      meta.remove(Seq(max, rmax))
      meta.insert(Seq(
        p.last._1 -> p,
        right.last._1 -> right
      ))

      println(s"borrow from right")

      return true
    }

    if(lopt.isDefined) {
      println(s"merge with left...")
      return merge(lopt.get, lopt.get.last._1, p, max)
    }

    println(s"merge with right...")

    merge(p, max, ropt.get, ropt.get.last._1)
  }

  def remove(p: Partition[T, K, V], data: Seq[K]): (Boolean, Int) = {

    val max = p.last._1
    val (ok, n) = p.remove(data)

    if(!ok) return false -> 0

    if(p.hasMinimum()){

      meta.remove(Seq(max))
      meta.insert(Seq(p.last._1 -> p))

      return true -> n
    }

    borrow(max, p) -> n
  }

  override def remove(data: Seq[K]): (Boolean, Int) = {

    val sorted = data.sorted
    val size = data.length
    var pos = 0

    while(pos < size){
      var list = sorted.slice(pos, size)
      val k = list(0)

      val (ok, n) = meta.find(k) match {
        case None => false -> 0
        case Some(p) =>

          if(p.find(k).isEmpty){
            false -> 0
          } else {
            val idx = list.indexWhere{k => ord.gt(k, p.last._1)}
            if(idx > 0) list = list.slice(0, idx)
            remove(p, list)
          }
      }

      if(!ok) return false -> 0

      pos += n
    }

    count -= size

    true -> size
  }

  override def split(): Partition[T, K, V] = ???

  override def canBorrowTo(p: Partition[T, K, V]): Boolean = ???

  override def borrowLeftTo(p: Partition[T, K, V]): Partition[T, K, V] = ???

  override def borrowRightTo(p: Partition[T, K, V]): Partition[T, K, V] = ???

  override def merge(p: Partition[T, K, V]): Partition[T, K, V] = ???

  override def inOrder(): Seq[(K, V)] = {
    meta.inOrder().foldLeft(Seq.empty[(K, V)]){ case (p, (_, n)) =>
      p ++ n.inOrder()
    }
  }
}
