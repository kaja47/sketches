package atrox.sketch

import atrox.{ fastSparse, IntFreqMap, IntSet, Bits, Cursor2 }
import java.util.concurrent.{ CopyOnWriteArrayList, ThreadLocalRandom }
import java.util.concurrent.atomic.LongAdder
import java.lang.Math.{ pow, log, max, min }
import scala.util.hashing.MurmurHash3
import scala.collection.{ mutable, GenSeq }
import scala.collection.mutable.{ ArrayBuilder, ArrayBuffer }
import scala.language.postfixOps
import Sketching.{ BitSketching, IntSketching } // types


case class LSHBuildCfg(
  bands: Int,

  hashBits: Int = -1,

  /** every bucket that has more than this number of elements is discarded */
  maxBucketSize: Int = Int.MaxValue,

  /** Every bucket that has less than this number of elements is discarded. It
    * might have sense to set this to 2 for closed datasets. It also filters
    * out a lot of tiny arrays which reduce memory usage. */
  minBucketSize: Int = 1
)

case class LSHCfg(
  /** Size of the biggest bucket that will be used for selecting candidates.
    * Eliminating huge buckets will not harm recall very much, because they
    * might be result of some hashing anomaly and their items are with high
    * probability present in another bucket. */
  maxBucketSize: Int = Int.MaxValue,

  /** How many candidates to select. */
  maxCandidates: Int = Int.MaxValue,

  /** How many final results to return. */
  maxResults: Int = Int.MaxValue,

  /** If this is set to false, some operations might use faster code, that
    * needs to store all similar items in memory */
  compact: Boolean = true,

  /** Perform bulk queries in parallel? */
  parallel: Boolean = false,

  /** This option controls proportional size of intermediate results allocated
    * during parallel non-compact bulk queries producing top-k results. Setting
    * it lower than 1 saves some memory but might reduce precision. */
  parallelPartialResultSize: Double = 1.0
) {
  require(parallelPartialResultSize > 0.0 && parallelPartialResultSize <= 1.0)

  override def toString = s"""
    |LSHCfg(
    |  maxBucketSize = $maxBucketSize
    |  maxCandidates = $maxCandidates
    |  maxResults = $maxResults
    |  compact = $compact
    |  parallel = $parallel
    |  parallelPartialResultSize = $parallelPartialResultSize
    |)
  """.trim.stripMargin('|')
}


/** sim field might be similarity or distance, depending whether SimFun or DistFun was used */
case class Sim(idx: Int, sim: Double)


object LSH {

  val NoEstimate = Double.NegativeInfinity

  def pickBands(threshold: Double, hashes: Int) = {
    require(threshold > 0.0)
    val target = hashes * -1 * log(threshold)
    var bands = 1
    while (bands * log(bands) < target) {
      bands += 1
    }
    bands
  }

  /** Picks best number of sketch hashes and LSH bands for given similarity
    * threshold */
  def pickHashesAndBands(threshold: Double, maxHashes: Int) = {
    val bands = pickBands(threshold, maxHashes)
    val hashes = (maxHashes / bands) * bands
    (hashes, bands)
  }

  def _buildTables[T, SkArr](sk: Sketching[T, SkArr], params: LSHTableParams, cfg: LSHBuildCfg)(has: HashAndSlice[SkArr]): IntArrayLSHTable[SkArr] = {
    val bandSize = (1 << params.hashBits)

    def runItems(f: (Int, Int, Int) => Unit) = { // f args: band, hash, itemIdx
      var itemIdx = 0 ; while (itemIdx < sk.itemsCount) {
        val skarr = sk.getSketchFragment(itemIdx)
        var band = 0 ; while (band < params.bands) {
          val h = has.hashFun(skarr, 0, band, params)

          assert(h <= bandSize, s"$h < $bandSize")
          f(band, h, itemIdx)
          band += 1
        }
        itemIdx += 1
      }
    }

    val bandMap = new Grouping.Sorted(params.bands * bandSize, params.bands * sk.itemsCount)
    runItems { (band, h, i) => bandMap.add(band * bandSize + h, i) }

    val idxs = new Array[Array[Int]](params.bands * bandSize)

    bandMap.getAll foreach { case (h, is) =>
      require(fastSparse.isDistinctIncreasingArray(is))
      if (is.length <= cfg.maxBucketSize && is.length >= cfg.minBucketSize) {
        idxs(h) = is
      }
    }

    val _params = params
    new IntArrayLSHTable[SkArr](idxs) {
      val params = _params
      val hashFun  = has.hashFun
      val sliceFun = has.sliceFun
    }
  }

  def buildBitLshTable[T](sk: BitSketching[T], cfg: LSHBuildCfg): IntArrayLSHTable[Array[Long]] = {
    val bands = cfg.bands
    val hashBits = cfg.hashBits
    require(sk.sketchLength % 64 == 0, "BitLSH must have multiple of 64 hashes for now")
    require(bands > 0, "number of bands must be non-negative")

    val bandBits = sk.sketchLength / bands
    require(bandBits < 32)
    require(hashBits <= 0 || hashBits == bandBits) // not set or same as band length in bits

    val params = LSHTableParams(
      sketchLength = sk.sketchLength,
      bands        = bands,
      bandLength   = bandBits,
      hashBits     = bandBits, // not hashBits
      itemsCount   = sk.itemsCount
    )

    _buildTables(sk, params, cfg)(HashAndSlice.Bit)
  }

  def buildIntLshTable[T](sk: IntSketching[T], cfg: LSHBuildCfg): IntArrayLSHTable[Array[Int]] = {
    val bands = cfg.bands
    val hashBits = cfg.hashBits
    val realHashBits = if (hashBits <= 0) pickBits(sk.itemsCount) else hashBits
    require(realHashBits > 0, "number of hash bits must be non-negative")
    require(bands > 0, "number of bands must be non-negative")

    val bandBits = sk.sketchLength / bands // how many elements from a sketch is used in one band

    val params = LSHTableParams(
      sketchLength = sk.sketchLength,
      bands        = bands,
      bandLength   = bandBits,
      hashBits     = realHashBits,
      itemsCount   = sk.itemsCount
    )

    _buildTables(sk, params, cfg)(HashAndSlice.Int)
  }



  sealed trait Switch[SketchArray]
  implicit object SwitchInt extends Switch[Array[Int]]
  implicit object SwitchBit extends Switch[Array[Long]]

  def mkTlb[T, SketchArray: Switch](sk: Sketching[T, SketchArray], cfg: LSHBuildCfg): IntArrayLSHTable[SketchArray] = implicitly[Switch[SketchArray]] match {
    case SwitchInt => buildIntLshTable(sk.asInstanceOf[Sketching[T, Array[Int]]], cfg)
    case SwitchBit => buildBitLshTable(sk.asInstanceOf[Sketching[T, Array[Long]]], cfg)
  }

  def apply[T, SketchArray: Switch](items: IndexedSeq[T], sk: Sketchers[T, SketchArray], cfg: LSHBuildCfg): LSHObj[T, T, SketchArray, IntArrayLSHTable[SketchArray]] = {
    require(sk.rank.nonEmpty, "No default rank object. It must be passed explicitly.")
    apply(SketchingOf(items, sk), sk.rank.get(items), cfg)
  }

  def apply[T, S, SketchArray: Switch](items: IndexedSeq[T], sk: Sketchers[T, SketchArray], rank: Rank[T, S], cfg: LSHBuildCfg): LSHObj[T, S, SketchArray, IntArrayLSHTable[SketchArray]] =
    apply(SketchingOf(items, sk), rank, cfg)

  def apply[T, S, SketchArray: Switch](sk: Sketching[T, SketchArray], rank: Rank[T, S], cfg: LSHBuildCfg): LSHObj[T, S, SketchArray, IntArrayLSHTable[SketchArray]] =
    apply(mkTlb(sk, cfg), sk, rank)

  def apply[T, S, SketchArray: Switch, Table <: LSHTable[SketchArray]](table: Table, sk: Sketching[T, SketchArray], rank: Rank[T, S]): LSHObj[T, S, SketchArray, Table] =
    LSHObj(table, SketchingQuery(sk), rank)



  def estimating[T, SketchArray: Switch](sk: Sketch[T, SketchArray], cfg: LSHBuildCfg): LSHObj[T, (SketchArray, Int), SketchArray, IntArrayLSHTable[SketchArray]] =
    apply(sk, SketchRank(sk), cfg)

  def estimating[T, SketchArray: Switch](items: IndexedSeq[T], sk: Sketchers[T, SketchArray], cfg: LSHBuildCfg): LSHObj[T, (SketchArray, Int), SketchArray, IntArrayLSHTable[SketchArray]] =
    apply(items, sk, SketchRank(Sketch(items, sk)), cfg)



  def ripBits(sketchArray: Array[Long], bitsPerSketch: Int, i: Int, band: Int, bandBits: Int): Int =
    Bits.getBitsOverlapping(sketchArray, bitsPerSketch * i + band * bandBits, bandBits).toInt

  private def pickBits(len: Int) = math.max(32 - Integer.numberOfLeadingZeros(len), 4)


  def hashSlice(skarr: Array[Int], sketchLength: Int, i: Int, band: Int, bandLength: Int, hashBits: Int) = {
    val start = i * sketchLength + band * bandLength
    val end   = start + bandLength
    _hashSlice(skarr, start, end) & ((1 << hashBits) - 1)
  }

  /** based on scala.util.hashing.MurmurHash3.arrayHash */
  private def _hashSlice(arr: Array[Int], start: Int, end: Int): Int = {
    var h = MurmurHash3.arraySeed
    var i = start
    while (i < end) {
      h = MurmurHash3.mix(h, arr(i))
      i += 1
    }
    MurmurHash3.finalizeHash(h, end-start)
  }

  def getSlice(skarr: Array[Int], sketchLength: Int, i: Int, band: Int, bandLength: Int) = {
    val start = i * sketchLength + band * bandLength
    val end   = start + bandLength
    java.util.Arrays.copyOfRange(skarr, start, end)
  }




  def makeReverseMapping(length: Int, bands: Int, idxs: Array[Array[Int]]): Array[Array[Int]] =
    makeReverseMapping(length, bands, idxs, idxs.length)

  def makeReverseMapping(length: Int, bands: Int, idxs: Int => Array[Int], idxsLength: Int): Array[Array[Int]] = {
    val revmap = Array.fill(length) {
      val ab = new ArrayBuilder.ofInt
      ab.sizeHint(bands)
      ab
    }

    for (bidx <- 0 until idxsLength) {
      val bucket = idxs(bidx)
      if (bucket != null) {
        var i = 0; while (i < bucket.length) {
          revmap(bucket(i)) += bidx
          i += 1
        }
      }
    }

    val res = revmap.map(_.result)
    res
  }

}



/** Trait implementing querying into LSH tables. */
trait Query[-Q, SketchArray] {
  def query(q: Q): (SketchArray, Int)
  def query(idx: Int): (SketchArray, Int)
}

/** Sketching can be fully materialized Sketch table or dataset wrapped in
  * Sketching class */
case class SketchingQuery[Q, SketchArray](sk: Sketching[Q, SketchArray]) extends Query[Q, SketchArray] {
  def query(q: Q) = (sk.sketchers.getSketchFragment(q), 0)
  def query(idx: Int) = (sk.getSketchFragment(idx), 0)

  // TODO Is this specialization needed? Does it have any speed benefit? If
  // not, it might not be nesessary to produce pair (skarr, skidx) but only
  // allocated skarr value.
  //def query(idx: Int) = (sketchTable.sketchArray, idx)
}


trait Rank[-Q, S] {
  def map(q: Q): S
  def map(idx: Int): S

  /** Similarity/distance function. Result must be mapped to integer such as
    * higher number means bigger similarity or smaller distance. */
  def rank(a: S, b: S): Int

  def rank(a: S, b: Int): Int = rank(a, map(b))

  /** Index based rank. It's intended for bulk methods that operates only on
    * internal tables. In that case it should be overwritten to use indexes
    * into Sketch object without any extra allocations. */
  def rank(a: Int, b: Int): Int = rank(map(a), map(b))

  /** Recover similarity/distance encoded in integer rank value. */
  def derank(r: Int): Double
}


trait SimRank[@specialized(Long) S] extends Rank[S, S] {
  def apply(a: S, b: S): Double

  def rank(a: S, b: S): Int = Bits.floatToSortableInt(apply(a, b).toFloat)
  def derank(r: Int): Double = Bits.sortableIntToFloat(r)
}

case class SimFun[S](f: (S, S) => Double, dataset: IndexedSeq[S]) extends SimRank[S] {
  def map(q: S): S = q
  def map(idx: Int): S = dataset(idx)

  def apply(a: S, b: S) = f(a, b)
}


trait DistRank[@specialized(Long) S] extends Rank[S, S] {
  def apply(a: S, b: S): Double

  def rank(a: S, b: S): Int = ~Bits.floatToSortableInt(apply(a, b).toFloat)
  def derank(r: Int): Double = Bits.sortableIntToFloat(~r)
}

case class DistFun[@specialized(Long) S](f: (S, S) => Double, dataset: IndexedSeq[S]) extends DistRank[S] {
  def map(q: S): S = q
  def map(idx: Int): S = dataset(idx)

  def apply(a: S, b: S) = f(a, b)
}

case class SketchRank[Q, SketchArray](sk: Sketch[Q, SketchArray]) extends Rank[Q, (SketchArray, Int)] {

  type S = (SketchArray, Int)
  def es = sk.estimator

  def map(q: Q): S = (sk.sketchers.getSketchFragment(q), 0)
  def map(idx: Int): S = (sk.sketchArray, idx)

  def rank(a: S, b: S): Int = {
    val (skarra, idxa) = a
    val (skarrb, idxb) = b
    es.sameBits(skarra, idxa, skarrb, idxb)
  }
  override def rank(a: S, b: Int): Int = {
    val (skarra, idxa) = a
    es.sameBits(skarra, idxa, sk.sketchArray, b)
  }
  override def rank(a: Int, b: Int): Int = es.sameBits(sketch.sketchArray, a, sketch.sketchArray, b)

  /** Recover similarity/distance encoded in integer rank value. */
  def derank(r: Int): Double = es.estimateSimilarity(r)
}



/** LSH
  *
  * - Methods with "raw" prefix might return duplicates and are not affected by
  *   maxBucketSize, maxCandidates and maxResults config options.
  * - Methods without "raw" prefix must never return duplicates and query item itself
  *
  * Those combinations introduce non-determenism:
  * - non compact + max candidates (candidate selection is done by random sampling)
  * - parallel + non compact
  *
  * every Idxs array must be sorted
  */
abstract class LSH[Q, S] { self =>
  type SketchArray
  type Idxs = Array[Int]

  def query: Query[Q, SketchArray]
  def rank: Rank[Q, S]
  def itemsCount: Int
  def cfg: LSHCfg
  def bands: Int
  def sketchLength: Int

  val candidatesStats = new LongAdder

  def withConfig(cfg: LSHCfg): LSH[Q, S]


  def probabilityOfInclusion(sim: Double) = {
    val bandLen = sketchLength / bands
    1.0 - pow(1.0 - pow(sim, bandLen), bands)
  }

  def estimatedThreshold = {
    val bandLen = sketchLength / bands
    pow(1.0 / bands, 1.0 / bandLen)
  }



  protected def accept(cfg: LSHCfg)(idxs: Array[Int]) = idxs != null && idxs.length <= cfg.maxBucketSize

  abstract class QQ[Res] {
    def apply(q: Q): Res = apply(q, self.cfg)
    def apply(q: Q, cfg: LSHCfg): Res
    def apply(idx: Int): Res = apply(idx, self.cfg)
    def apply(idx: Int, cfg: LSHCfg): Res
  }



  // raw = without any filtering, presented as is
  def rawStreamIndexes: Iterator[Idxs]
  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs]

  protected def rawCandidateIndexes(ss: (SketchArray, Int)): Array[Idxs] = {
    val (skarr, skidx) = ss
    rawCandidateIndexes(skarr, skidx)
  }

  def rawCandidateIndexes(q: Q): Array[Idxs]     = rawCandidateIndexes(query.query(q))
  def rawCandidateIndexes(idx: Int): Array[Idxs] = rawCandidateIndexes(query.query(idx))



  def streamIndexes: Iterator[Idxs] = rawStreamIndexes filter accept(cfg)

  def candidateIndexes = new QQ[Idxs] {
    def apply(q: Q, cfg: LSHCfg)     = fastSparse.union(rawCandidateIndexes(q) filter accept(cfg))
    def apply(idx: Int, cfg: LSHCfg) = fastSparse.union(rawCandidateIndexes(idx) filter accept(cfg))
  }

  def similarIndexes = new QQ[Idxs] {
    def apply(q: Q, cfg: LSHCfg)     = _similar(rawCandidateIndexes(q), rank.map(q), cfg).result
    def apply(idx: Int, cfg: LSHCfg) = _similar(rawCandidateIndexes(idx), idx, cfg).result
  }

  def similarItems = new QQ[IndexedSeq[Sim]] {
    def apply(q: Q, cfg: LSHCfg)     = indexResultBuilderToSims(_similar(rawCandidateIndexes(q), rank.map(q), cfg))
    def apply(idx: Int, cfg: LSHCfg) = indexResultBuilderToSims(_similar(rawCandidateIndexes(idx), idx, cfg))
  }

  protected def _similar(candidateIdxs: Array[Idxs], s: S, cfg: LSHCfg): IndexResultBuilder = {
    val candidates = selectCandidates(candidateIdxs, cfg)
    val res = IndexResultBuilder.make(false, cfg.maxResults)
    candidatesStats.add(candidates.length)

    var i = 0 ; while (i < candidates.length) {
      res += (candidates(i), rank.rank(s, candidates(i)))
      i += 1
    }

    res
  }

  protected def _similar(candidateIdxs: Array[Idxs], idx: Int, cfg: LSHCfg): IndexResultBuilder = {
    val candidates = selectCandidates(candidateIdxs, cfg)
    val res = IndexResultBuilder.make(false, cfg.maxResults)
    candidatesStats.add(candidates.length)

    var i = 0 ; while (i < candidates.length) {
      if (candidates(i) != idx) {
        res += (candidates(i), rank.rank(idx, candidates(i)))
      }
      i += 1
    }

    res
  }


  /** must never return duplicates */
  protected def selectCandidates(candidateIdxs: Array[Idxs], cfg: LSHCfg): Idxs = {
    // TODO candidate selection can be done on the fly without allocations
    //      using some sort of merging cursor

    val cidxs = candidateIdxs filter accept(cfg)

    var candidateCount, i = 0
    while (i < cidxs.length) {
      candidateCount += cidxs(i).length
      i += 1
    }

    if (candidateCount <= cfg.maxCandidates) {
      fastSparse.union(cidxs, candidateCount)

    } else {
      val map = new IntFreqMap(initialSize = cfg.maxCandidates, loadFactor = 0.42, freqThreshold = bands)
      for (idxs <- cidxs) { map ++= (idxs, 1) }
      map.topK(cfg.maxCandidates)
    }
  }


  protected def indexResultBuilderToSims(irb: IndexResultBuilder): IndexedSeq[Sim] = {
    val res = new Array[Sim](irb.size)
    val cur = irb.idxScoreCursor

    var i = res.length
    while (cur.moveNext()) {
      i -= 1
      res(i) = Sim(cur.key, rank.derank(cur.value))
    }

    assert(i == 0, i)

    res
  }

}



trait LSHBulkOps[Q, S] { self: LSH[Q, S] =>

  def allSimilarIndexes: Iterator[(Int, Idxs)] = allSimilarIndexes(self.cfg)
  def allSimilarIndexes(cfg: LSHCfg): Iterator[(Int, Idxs)] =
    (cfg.compact, cfg.parallel) match {
      case (true, par) => parallelBatches(0 until itemsCount iterator, par) { idx => (idx, similarIndexes(idx, cfg)) }
      case (false, _)  => _allSimilar_notCompact(cfg) map { case (idx, res) => (idx, res.result) }
    }

  def allSimilarItems: Iterator[(Int, IndexedSeq[Sim])] = allSimilarItems(self.cfg)
  def allSimilarItems(cfg: LSHCfg): Iterator[(Int, IndexedSeq[Sim])] =
    (cfg.compact, cfg.parallel) match {
      case (true, par) => parallelBatches(0 until itemsCount iterator, par) { idx => (idx, similarItems(idx, cfg)) }
      case (false, _)  =>
        val iter = _allSimilar_notCompact(cfg)
        parallelBatches(iter, cfg.parallel, batchSize = 256) { case (idx, simIdxs) =>
          (idx, indexResultBuilderToSims(simIdxs))
        }
    }

  /** Needs to store all similar indexes in memory + some overhead, but it's
    * much faster, because it needs to do only half of iterations and accessed
    * data have better cache locality. */
  protected def _allSimilar_notCompact(cfg: LSHCfg): Iterator[(Int, IndexResultBuilder)] = {

    // maxCandidates option is simulated via sampling
    val comparisons = streamIndexes.map { idxs => (idxs.length.toDouble - 1) * idxs.length / 2 } sum
    val ratio = itemsCount.toDouble * cfg.maxCandidates / comparisons
    assert(ratio >= 0)

    if (cfg.parallel) {
      val partialResults = new CopyOnWriteArrayList[Array[IndexResultBuilder]]()
      val truncatedResultSize =
        if (cfg.maxResults < Int.MaxValue && cfg.parallelPartialResultSize < 1.0)
          math.max((cfg.maxResults * cfg.parallelPartialResultSize).toInt, 1)
        else cfg.maxResults

      val tl = new ThreadLocal[Array[IndexResultBuilder]] {
        override def initialValue = {
          val local = Array.fill(itemsCount)(IndexResultBuilder.make(distinct = true, truncatedResultSize))
          partialResults.add(local)
          local
        }
      }

      streamIndexes.grouped(1024).toVector.par.foreach { idxsgr =>
        val local = tl.get
        for (idxs <- idxsgr) {
          runTile(idxs, ratio, local)
        }
      }

      val idxsArr = new Array[IndexResultBuilder](itemsCount)
      val pr = partialResults.toArray(Array[Array[IndexResultBuilder]]())
      (0 until itemsCount).par foreach { i =>
        val target = IndexResultBuilder.make(distinct = true, cfg.maxResults)
        for (p <- pr) target ++= p(i)
        idxsArr(i) = target

        for (p <- pr) p(i) = null
      }
      Iterator.tabulate(itemsCount) { idx => (idx, idxsArr(idx)) }

    } else {
      val res = Array.fill(itemsCount)(IndexResultBuilder.make(distinct = true, cfg.maxResults))
      for (idxs <- streamIndexes) {
        runTile(idxs, ratio, res)
      }
      Iterator.tabulate(itemsCount) { idx => (idx, res(idx)) }
    }

  }

  protected def runTile(idxs: Idxs, ratio: Double, res: Array[IndexResultBuilder]): Unit = {
    var c = 0
    val stripeSize = 64
    var stripej = 0 ; while (stripej < idxs.length) {
      if (ThreadLocalRandom.current().nextDouble() < ratio) {
        val endi = min(stripej + stripeSize, idxs.length)
        val startj = stripej + 1
        var j = startj ; while (j < idxs.length) {
          val realendi = min(j, endi)
          var i = stripej ; while (i < realendi) {

            val score = rank.rank(idxs(i), idxs(j))
            res(idxs(i)) += (idxs(j), score)
            res(idxs(j)) += (idxs(i), score)
            c += 1

            i += 1
          }
          j += 1
        }
      }
      stripej += stripeSize
    }

    candidatesStats.add(c)
  }

  protected def parallelBatches[T, U](xs: Iterator[T], inParallel: Boolean, batchSize: Int = 1024)(f: T => U): Iterator[U] =
    if (inParallel) {
      xs.grouped(batchSize).flatMap { batch => batch.par.map(f) }
    } else {
      xs.map(f)
    }

}


final case class LSHObj[Q, S, SkArr, Table <: LSHTable[SkArr]](
    table: Table,
    query: Query[Q, SkArr],
    rank: Rank[Q, S],
    cfg: LSHCfg = LSHCfg()
  ) extends LSH[Q, S] with LSHBulkOps[Q, S] {

  type SketchArray = SkArr

  def withConfig(newCfg: LSHCfg): LSHObj[Q, S, SkArr, Table] = copy(cfg = newCfg)

  val itemsCount = table.params.itemsCount
  val sketchLength = table.params.sketchLength
  val bands = table.params.bands

  def rawStreamIndexes =
    table.rawStreamIndexes

  def rawCandidateIndexes(skarr: SkArr, skidx: Int): Array[Idxs] =
    table.rawCandidateIndexes(skarr, skidx)
}



case class LSHTableParams(
  sketchLength: Int,
  bands: Int,
  bandLength: Int,
  hashBits: Int,
  itemsCount: Int
)


// for bit LSH must hold:
// - sketchLength, bandLength and hashBits are in bits
// - bandLength == hashBits
trait LSHTable[SketchArray] {
  type Idxs = Array[Int]

  def params: LSHTableParams
  def hashFun: (SketchArray, Int, Int, LSHTableParams) => Int
  def sliceFun: (SketchArray, Int, Int, LSHTableParams) => SketchArray

  def rawStreamIndexes: Iterator[Idxs]
  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(params.bands) { band => lookup(skarr, skidx, band) }

  protected def lookup(skarr: SketchArray, skidx: Int, band: Int): Idxs
}


abstract class IntArrayLSHTable[SketchArray](val idxs: Array[Array[Int]]) extends LSHTable[SketchArray] {

  def rawStreamIndexes: Iterator[Idxs] =
    idxs.iterator filter (_ != null)

  protected def lookup(skarr: SketchArray, skidx: Int, band: Int): Idxs =
    idxs(band * (1 << params.hashBits) + hashFun(skarr, skidx, band, params))

}


case class HashAndSlice[SketchArray](
  hashFun: (SketchArray, Int, Int, LSHTableParams) => Int,
  sliceFun: (SketchArray, Int, Int, LSHTableParams) => SketchArray
)

object HashAndSlice {
  implicit val Int = HashAndSlice[Array[Int]](
    (skarr, skidx, band, p) => LSH.hashSlice(skarr, p.sketchLength, skidx, band, p.bandLength, p.hashBits),
    (skarr, skidx, band, p) => ???
  )
  implicit def Bit = HashAndSlice[Array[Long]](
    (skarr, skidx, band, p) => LSH.ripBits(skarr, p.sketchLength, skidx, band, p.bandLength),
    (skarr, skidx, band, p) => ???
  )
}
