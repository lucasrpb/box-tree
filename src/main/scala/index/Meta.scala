package index

import java.util.UUID
import scala.reflect.ClassTag

class Meta[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                  val META_ORDER: Int)(implicit val ord: Ordering[K]) {

  val DATA_MIN = DATA_ORDER - 1
  val DATA_MAX = DATA_ORDER*2 - 1

  val META_MIN = META_ORDER - 1
  val META_MAX = META_ORDER*2 - 1

  var partitions: Partition[T, K, Partition[T, K, V]] =
    new Block[T, K, Partition[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)

  def find(k: K): Option[Partition[T, K, V]] = {
    partitions.near(k)
  }

  def left(k: K): Option[Partition[T, K, V]] = {
    partitions.left(k)
  }

  def right(k: K): Option[Partition[T, K, V]] = {
    partitions.right(k)
  }

  def insert(data: Seq[(K, Partition[T, K, V])]): Boolean = {

    if(partitions.isFull()){

      println(s"FULL...")

      val list = partitions.inOrder()
      partitions = new Index[T, K, Partition[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T],
        DATA_ORDER, META_ORDER)
      partitions.insert(list)
    }

    val len = data.length
    val (ok, n) = partitions.insert(data)

    if(!ok) return false

    if(n < len){
      return insert(data.slice(n, len))
    }

    true
  }

  def remove(keys: Seq[K]): Boolean = {
    val (ok, _) = partitions.remove(keys)

    if(!ok) return false

    if(!partitions.hasMinimum() && partitions.isInstanceOf[Index[T, K, V]]){

      println(s"LESS THAN MINIMUM...")

      val list = partitions.inOrder()
      partitions = new Block[T, K, Partition[T, K, V]](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX)

      partitions.insert(list)
    }

    true
  }

  def isFull(): Boolean = {
    partitions.isFull()
  }

  def isEmpty(): Boolean = {
    partitions.isEmpty()
  }

  def inOrder(): Seq[(K, Partition[T, K, V])] = {
    partitions.inOrder()
  }

}
