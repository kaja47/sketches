package atrox

class Majority[T] {
  private var elem: T = _
  private var count: Int = 0

  def feed(xs: Iterable[T]): this.type = {
    for (x <- xs) {
      if (count == 0) {
        elem = x
        count = 1
      } else if (elem == x) {
        count += 1
      } else {
        count -= 1
      }
    }
    this
  }

  def get: T = elem
}

object Majority {
  def apply[T](xs: Iterable[T]): Majority[T] = {
    new Majority[T]().feed(xs)
  }
}


/** FREQUENT generalizes MAJORITY to find up to k items that occur more than
  * 1/k fraction of the time
  * http://dmac.rutgers.edu/Workshops/WGUnifyingTheory/Slides/cormode.pdf
  */
class Frequent[T](k: Int) {
  private val elems = collection.mutable.Map[T, Int]()
  private var streamLength = 0L

  def feed(xs: Iterable[T]): this.type = {
    for (x <- xs) {
      streamLength += 1
      if (elems.contains(x)) {
        elems(x) += 1
      } else if (elems.size < k) {
        elems(x) = 1
      } else {
        for (k <- elems.keySet) {
          val c = elems(k)
          if (c <= 1) {
            elems.remove(k)
          } else {
            elems(k) = c-1
          }
        }
      }
    }

    this
  }

  def get(k: T): Int =
    elems(k)

  def getAll: Seq[T] = elems.toSeq.sortBy(-_._2).map(_._1)

  def error = 1.0 / k * streamLength // ±
}

object Frequent {
  def apply[T](k: Int, xs: Iterable[T]): Frequent[T] =
    new Frequent[T](k).feed(xs)
}
