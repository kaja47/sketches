package atrox.sketch

trait HashFunc[@scala.specialized(Int, Long) T] extends Serializable {
  def apply(x: T): Int
}

object HashFunc {
  def random(seed: Int, randomBits: Int = 32): HashFunc[Int] = {
    val rand = new scala.util.Random(seed)
    new HashFunc[Int] {
      private[this] val M = randomBits
      private[this] val a: Long = (rand.nextLong() & ((1L << 62)-1)) * 2 + 1       // random odd positive integer (a < 2^w)
      private[this] val b: Long = math.abs(rand.nextLong() & ((1L << (64 - M))-1)) // random non-negative integer (b < 2^(w-M)
      def apply(x: Int): Int = ((a*x+b) >>> (64-M)).toInt

      override def toString = s"HashFunc: f(x) = (${a}L * x + ${b}L) >>> ${64-M}"
    }
  }
}

trait HashFuncLong[T] extends Serializable {
  def apply(x: T): Long
}
