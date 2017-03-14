package atrox.sketch

import scala.collection.mutable.{ ArrayBuffer, BitSet }
import java.util.concurrent._
import atrox.Cursor


protected class WorkStack(val size: Int) {
  private var arr = new Array[Int](size)
  private var top = 0
  def isEmpty = top == 0
  def isFull = top == arr.length
  def push(x: Int) = { arr(top) = x ; top += 1 }
  def pop(): Int = { top -= 1 ; arr(top) }
}


protected object Crawl {
  def par[R](itemsCount: Int, compute: Int => R, read: R => Cursor[Int]): Iterator[(Int, R)] =
     new ParallelCrawl(itemsCount, compute, read).run()

  /** Compact strategy processing most similar items first. That way a next
    * processed element shares most of it's candidates with a previously
    * processed one. Those shared candidates are ready in a CPU cache.
    * This strategy is often 25% faster than naive compact linear strategy. */
   def seq[R](itemsCount: Int, compute: Int => R, read: R => Cursor[Int]): Iterator[(Int, R)] = {

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

    new Iterator[(Int, R)] {

      def hasNext = {
        progressWaterlineAndFillStackIfEmpty()
        !stack.isEmpty
      }

      def next() = {
        progressWaterlineAndFillStackIfEmpty()
        val w = stack.pop()

        val sims = compute(w)
        //mark(w)

        val cur = read(sims)
        while (cur.moveNext() && !stack.isFull) {
          val s = cur.value
          if (!mark(s)) {
            stack.push(s)
            mark(s) = true
          }
        }

        (w, sims)
      }

    }

  }

}


protected class ParallelCrawl[R](
    val itemsCount: Int,
    val compute: Int => R,
    val read: R => Cursor[Int]
) { self =>

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

      val res = new ArrayBuffer[(Int, R)](stack.size)
      while (!stack.isEmpty) {
        val w = stack.pop()
        res += ((w, compute(w)))
      }

      res.foreach { r => queue.put(r) }

      self.synchronized {
        val cur = read(res(0)._2)
        while (cur.moveNext() && !stack.isFull) {
          val s = cur.value
          if (!mark(s)) {
            stack.push(s)
            mark(s) = true
          }
        }
      }

    }
  }

  def iterator = new Iterator[(Int, R)] {
    private var running = threads
    var el: (Int, R) = null

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
          el = x.asInstanceOf[(Int, R)]
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


