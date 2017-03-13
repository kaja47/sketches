package atrox.sketch

import scala.collection.mutable.{ ArrayBuffer, BitSet }
import java.util.concurrent._


class WorkStack(val size: Int) {
  private var arr = new Array[Int](size)
  private var top = 0
  def isEmpty = top == 0
  def isFull = top == arr.length
  def push(x: Int) = { arr(top) = x ; top += 1 }
  def pop(): Int = { top -= 1 ; arr(top) }
}


object Crawl {
  type Idxs = Array[Int]

  /** Compact strategy processing most similar items first. That way a next
    * processed element shares most of it's candidates with a previously
    * processed one. Those shared candidates are ready in a CPU cache.
    * This strategy is often 25% faster than naive compact linear strategy. */
  def iterator(itemsCount: Int, compute: Int => Array[Int]): Iterator[(Int, Idxs)] = {

    val mark = new BitSet(itemsCount)
    var waterline = 0
    val stack = new WorkStack(256)

    def progressWaterlineAndFillStackIfEmpty() = {
      if (stack.isEmpty) {
        while (waterline < itemsCount && mark(waterline)) { waterline += 1 }
        if (waterline < itemsCount) {
          stack.push(waterline)
          mark(waterline) = true
        }
      }
    }

    new Iterator[(Int, Idxs)] {

      def hasNext = {
        progressWaterlineAndFillStackIfEmpty()
        !stack.isEmpty
      }

      def next() = {
        progressWaterlineAndFillStackIfEmpty()
        val w = stack.pop()

        val sims = compute(w)
        //mark(w)

        for (s <- sims) {
          if (!stack.isFull && !mark(s)) {
            stack.push(s)
            mark(s) = true
          }
        }

        (w, sims)
      }

    }

  }

}


object ParallelCrawl {
  type Idxs = Array[Int]

  def iterator(itemsCount: Int, compute: Int => Array[Int]): Iterator[(Int, Idxs)] =
     new ParallelCrawl(itemsCount, compute).run()
}


class ParallelCrawl(
    val itemsCount: Int,
    val compute: Int => Array[Int]
) { self =>
  type Idxs = Array[Int]

  val mark = new BitSet(itemsCount)
  var waterline = 0

  val threads = Runtime.getRuntime().availableProcessors()
  val pool = Executors.newCachedThreadPool
  val queue = new ArrayBlockingQueue[Any](256)

  object Tombstone

  def run() = {
    val cl = new CountDownLatch(threads)

    for (t <- 0 until threads) {
      pool.execute { new Runnable {
          def run() = {
            cl.countDown()
            crawl()
          }
        }
      }
    }

    cl.await()
    pool.shutdown()

    iterator
  }

  def progressWaterlineAndFillStackIfEmpty(stack: WorkStack) = {
    if (stack.isEmpty) {
      while (waterline < itemsCount && mark(waterline)) { waterline += 1 }
      if (waterline < itemsCount) {
        stack.push(waterline)
        mark(waterline) = true
      }
    }
  }

  def crawl(): Unit = {
    val stack = new WorkStack(64)

    while (true) {

      if (stack.isEmpty) {
        self.synchronized {
          progressWaterlineAndFillStackIfEmpty(stack)
        }
      }

      if (stack.isEmpty) {
        queue.put(Tombstone)
        return
      }

      val res = new ArrayBuffer[(Int, Idxs)](stack.size)
      while (!stack.isEmpty) {
        val w = stack.pop()
        res += ((w, compute(w)))
      }

      res.foreach { r => queue.put(r) }

      self.synchronized {
        val neighborhood = res(0)._2
        var i = 0
        while (i < neighborhood.size && !stack.isFull) {
          val s = neighborhood(i)
          if (!mark(s)) {
            stack.push(s)
            mark(s) = true
          }
          i += 1
        }
      }

    }
  }

  def iterator = new Iterator[(Int, Idxs)] {
    private var running = threads
    var el: (Int, Idxs) = null

    def hasNext = {
      if (el != null) true 
      else if (running == 0) false
      else {
        val x = queue.take()
        if (x == Tombstone) {
          running -= 1
          hasNext
        } else if (x == null) {
          sys.error("this should not happen")
        } else {
          el = x.asInstanceOf[(Int, Idxs)]
          true
        }
      }
    }

    def next() = if (!hasNext) null else {
      val res = el
      el = null
      res
    }
  }

}


