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
  parallelPartialResultSize: Double = 1.0,

  threshold: Double = Double.NegativeInfinity
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

  def _runItems[T, SkArr](sk: Sketching[T, SkArr], params: LSHTableParams, has: HashAndSlice[SkArr])(f: (Int, Int, Int, SkArr) => Unit) = { // f args: band, hash, itemIdx, sketch array slice
    val bandSize = (1 << params.hashBits)
    var itemIdx = 0 ; while (itemIdx < sk.itemsCount) {
      val skarr = sk.getSketchFragment(itemIdx)
      var band = 0 ; while (band < params.bands) {
        val h = has.hashFun(skarr, 0, band, params)
        val s = has.sliceFun(skarr, 0, band, params)

        assert(h <= bandSize, s"$h < $bandSize")
        f(band, h, itemIdx, s)
        band += 1
      }
      itemIdx += 1
    }
  }

  protected def validBucketSize(arr: Array[Int], cfg: LSHBuildCfg) =
    arr.length <= cfg.maxBucketSize && arr.length >= cfg.minBucketSize

  def _buildTables[T, SkArr](sk: Sketching[T, SkArr], params: LSHTableParams, cfg: LSHBuildCfg)(has: HashAndSlice[SkArr]): IntArrayLSHTable[SkArr] = {
    val bandSize = (1 << params.hashBits)
    val bandMap = new Grouping.Sorted(params.bands * bandSize, params.bands * sk.itemsCount)
    _runItems(sk, params, has) { (band, h, i, s) => bandMap.add(band * bandSize + h, i) }

    val idxs = new Array[Array[Int]](params.bands * bandSize)

    bandMap.getAll foreach { case (h, is) =>
      require(fastSparse.isDistinctIncreasingArray(is))
      if (validBucketSize(is, cfg)) { idxs(h) = is }
    }

    val _params = params
    new IntArrayLSHTable[SkArr](idxs) {
      val params = _params
      val hashAndSlice = has
    }
  }


  def _buildHashTables[T](sk: Sketching[T, Array[Int]], params: LSHTableParams, cfg: LSHBuildCfg)(has: HashAndSlice[Array[Int]]): HashLSHTable = {
    val map = collection.mutable.Map[Seq[Int], ArrayBuilder.ofInt]()
    _runItems(sk, params, has) { (band, h, i, s) =>
      map.getOrElseUpdate(s, new ArrayBuilder.ofInt) += i
    }

    val res = map.iterator.map { case (k, vs) =>
      val arr = vs.result
      require(fastSparse.isDistinctIncreasingArray(arr))
      if (validBucketSize(arr, cfg)) { (k, arr) } else null
    }.filter(_ != null).toMap

    val _params = params
    new HashLSHTable(res) {
      val params = _params
      val hashAndSlice = has
    }
  }

  protected def mkParams[T, SkArr](sk: Sketching[T, SkArr], cfg: LSHBuildCfg, bandBits: Int, hashBits: Int) =
    LSHTableParams(
      sketchLength = sk.sketchLength,
      bands        = cfg.bands,
      bandLength   = bandBits,
      hashBits     = hashBits,
      itemsCount   = sk.itemsCount
    )


  def buildBitLshTable[T](sk: BitSketching[T], cfg: LSHBuildCfg): IntArrayLSHTable[Array[Long]] = {
    require(sk.sketchLength % 64 == 0, "BitLSH must have multiple of 64 hashes for now")
    require(cfg.bands > 0, "number of bands must be non-negative")

    val bandBits = sk.sketchLength / cfg.bands
    require(bandBits < 32)
    require(cfg.hashBits <= 0 || cfg.hashBits == bandBits) // not set or same as band length in bits

    val params = mkParams(sk, cfg, bandBits = bandBits, hashBits = bandBits /* intentionally not hashBits */)
    _buildTables(sk, params, cfg)(HashAndSlice.Bit)
  }

  def buildIntLshTable[T](sk: IntSketching[T], cfg: LSHBuildCfg): IntArrayLSHTable[Array[Int]] = {
    val realHashBits = if (cfg.hashBits <= 0) pickBits(sk.itemsCount) else cfg.hashBits
    require(realHashBits > 0, "number of hash bits must be non-negative")
    require(cfg.bands > 0, "number of bands must be non-negative")

    val bandBits = sk.sketchLength / cfg.bands // how many elements from a sketch is used in one band
    val params = mkParams(sk, cfg, bandBits = bandBits, hashBits = realHashBits)
    _buildTables(sk, params, cfg)(HashAndSlice.Int)
  }

  def buildIntLshHashTable[T](sk: IntSketching[T], cfg: LSHBuildCfg): HashLSHTable = {
    val realHashBits = if (cfg.hashBits <= 0) pickBits(sk.itemsCount) else cfg.hashBits
    require(realHashBits > 0, "number of hash bits must be non-negative")
    require(cfg.bands > 0, "number of bands must be non-negative")

    val bandBits = sk.sketchLength / cfg.bands // how many elements from a sketch is used in one band
    val params = mkParams(sk, cfg, bandBits = bandBits, hashBits = realHashBits)
    _buildHashTables(sk, params, cfg)(HashAndSlice.Int)
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

  def rawCandidateIndexes(q: Q): Array[Idxs]     = rawCandidateIndexes(query.query(q), 0)
  def rawCandidateIndexes(idx: Int): Array[Idxs] = rawCandidateIndexes(query.query(idx), 0)



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

  protected def threshold(cfg: LSHCfg): Int =
    if (cfg.threshold == Double.NegativeInfinity) Int.MinValue else rank.rank(cfg.threshold)

  protected def _similar(candidateIdxs: Array[Idxs], s: S, cfg: LSHCfg): IndexResultBuilder = {
    val candidates = selectCandidates(candidateIdxs, cfg)
    val res = IndexResultBuilder.make(false, cfg.maxResults)
    candidatesStats.add(candidates.length)

    val t = threshold(cfg)
    var i = 0 ; while (i < candidates.length) {
      val r = rank.rank(s, candidates(i))
      if (r >= t) { res += (candidates(i), r) }
      i += 1
    }

    res
  }

  protected def _similar(candidateIdxs: Array[Idxs], idx: Int, cfg: LSHCfg): IndexResultBuilder = {
    val candidates = selectCandidates(candidateIdxs, cfg)
    val res = IndexResultBuilder.make(false, cfg.maxResults)
    candidatesStats.add(candidates.length)

    val t = threshold(cfg)
    var i = 0 ; while (i < candidates.length) {
      if (candidates(i) != idx) {
        val r = rank.rank(idx, candidates(i))
        if (r >= t) { res += (candidates(i), r) }
      }
      i += 1
    }

    res
  }


  /** must never return duplicates */
  protected def selectCandidates(candidateIdxs: Array[Idxs], cfg: LSHCfg): Idxs = {
    // TODO candidate selection can be done on the fly without allocations
    //      using some sort of merging cursor

    val candidateCount = filterCandidatesAndCountThem(candidateIdxs, cfg)

    if (candidateCount <= cfg.maxCandidates) {
      fastSparse.union(candidateIdxs, candidateCount)

    } else {
      val map = new IntFreqMap(initialSize = cfg.maxCandidates, loadFactor = 0.42, freqThreshold = bands)
      for (idxs <- candidateIdxs) { map ++= (idxs, 1) }
      map.topK(cfg.maxCandidates)
    }
  }

  def filterCandidatesAndCountThem(candidateIdxs: Array[Idxs], cfg: LSHCfg): Int = {
    var candidateCount, i = 0
    while (i < candidateIdxs.length) {
      if (accept(cfg)(candidateIdxs(i))) {
        candidateCount += candidateIdxs(i).length
      } else {
        candidateIdxs(i) = null
      }
      i += 1
    }
    candidateCount
  }


  protected def indexResultBuilderToSims(irb: IndexResultBuilder): IndexedSeq[Sim] = {
    val res = new Array[Sim](irb.size)
    val cur = irb.idxScoreCursor

    var i = res.length
    while (cur.moveNext()) {
      i -= 1
      res(i) = Sim(cur.key, rank.derank(cur.value))
    }

    //assert(i == 0)

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
          runTile(idxs, ratio, local, cfg)
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
        runTile(idxs, ratio, res, cfg)
      }
      Iterator.tabulate(itemsCount) { idx => (idx, res(idx)) }
    }

  }

  protected def runTile(idxs: Idxs, ratio: Double, res: Array[IndexResultBuilder], cfg: LSHCfg): Unit = {
    val t = threshold(cfg)
    var c = 0
    val stripeSize = 64
    var stripej = 0 ; while (stripej < idxs.length) {
      if (ThreadLocalRandom.current().nextDouble() < ratio) {
        val endi = min(stripej + stripeSize, idxs.length)
        val startj = stripej + 1
        var j = startj ; while (j < idxs.length) {
          val realendi = min(j, endi)
          var i = stripej ; while (i < realendi) {

            val r = rank.rank(idxs(i), idxs(j))
            if (r >= t) {
              res(idxs(i)) += (idxs(j), r)
              res(idxs(j)) += (idxs(i), r)
            }

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
  def hashAndSlice: HashAndSlice[SketchArray]

  def rawStreamIndexes: Iterator[Idxs]
  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(params.bands) { band => lookup(skarr, skidx, band) }

  protected def lookup(skarr: SketchArray, skidx: Int, band: Int): Idxs
}


abstract class IntArrayLSHTable[SketchArray](val idxs: Array[Array[Int]]) extends LSHTable[SketchArray] {

  def rawStreamIndexes: Iterator[Idxs] =
    idxs.iterator filter (_ != null)

  protected def lookup(skarr: SketchArray, skidx: Int, band: Int): Idxs =
    idxs(band * (1 << params.hashBits) + hashAndSlice.hashFun(skarr, skidx, band, params))

}


abstract class HashLSHTable(val idxs: Map[Seq[Int], Array[Int]]) extends LSHTable[Array[Int]] {

  def rawStreamIndexes: Iterator[Idxs] =
    idxs.valuesIterator filter (_ != null)

  protected def lookup(skarr: Array[Int], skidx: Int, band: Int): Idxs =
    idxs.getOrElse(hashAndSlice.sliceFun(skarr, skidx, band, params), null)

}


case class HashAndSlice[SketchArray](
  hashFun: (SketchArray, Int, Int, LSHTableParams) => Int,
  sliceFun: (SketchArray, Int, Int, LSHTableParams) => SketchArray
)

object HashAndSlice {
  implicit val Int = HashAndSlice[Array[Int]](
    (skarr, skidx, band, p) => LSH.hashSlice(skarr, p.sketchLength, skidx, band, p.bandLength, p.hashBits),
    (skarr, skidx, band, p) => LSH.getSlice(skarr, p.sketchLength, skidx, band, p.bandLength)
  )
  implicit def Bit = HashAndSlice[Array[Long]](
    (skarr, skidx, band, p) => LSH.ripBits(skarr, p.sketchLength, skidx, band, p.bandLength),
    (skarr, skidx, band, p) => Array(LSH.ripBits(skarr, p.sketchLength, skidx, band, p.bandLength))
  )
}
