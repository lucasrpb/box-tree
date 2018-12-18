package index

trait Partition[T, K, V] {

  def find(k: K): Option[V]
  def near(k: K): Option[V]
  def left(k: K): Option[V]
  def right(k: K): Option[V]
  def last: (K, V)
  def split(): Partition[T, K, V]
  def canBorrowTo(p: Partition[T, K, V]): Boolean
  def borrowLeftTo(p: Partition[T, K, V]): Partition[T, K, V]
  def borrowRightTo(p: Partition[T, K, V]): Partition[T, K, V]
  def merge(p: Partition[T, K, V]): Partition[T, K, V]
  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimum(): Boolean
  def insert(data: Seq[(K, V)]): (Boolean, Int)
  def remove(keys: Seq[K]): (Boolean, Int)
  def inOrder(): Seq[(K, V)]

}
