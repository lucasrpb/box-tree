package index

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =  x - y
  }

  val MAX_VALUE = 1000//Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val DATA_ORDER = rand.nextInt(2, 10)
    val META_ORDER = rand.nextInt(2, 10)

    var data = Seq.empty[Int]
    val index = new Index[String, Int, Int](UUID.randomUUID.toString, DATA_ORDER, META_ORDER)

    val n = rand.nextInt(1, 100)

    def insert(): Unit = {
      val m = rand.nextInt(1, 100)
      var list = Seq.empty[Int]

      for(j<-0 until m){
        val k = rand.nextInt(1, MAX_VALUE)

        if (!data.contains(k) && !list.contains(k)) {
          list = list :+ k
        }
      }

      if(index.insert(list.map(k => k -> k))._1){
        data = data ++ list
      }
    }

    def remove(): Unit = {
      val len = data.length

      if(len < 2) return

      var list = scala.util.Random.shuffle(data)
      val n = rand.nextInt(1, len)

      list = list.slice(0, n)

      if(index.remove(list)._1){
        data = data.filterNot{k => list.contains(k)}
      }
    }

    for(i<-0 until n) {
      rand.nextInt() match {
        case n if n % 2 == 0 => insert()
        case _ => remove()
      }
    }

    val dsorted = data.sorted
    val isorted = index.inOrder().map(_._1)

    println(s"dsorted: ${dsorted}\n")
    println(s"isroted: ${isorted}\n")

    assert(dsorted.equals(isorted))
  }

  "index data " should "be equal to test data" in {

    val n = 1000

    for(i<-0 until n){
      test()
    }

  }

}
