package atrox.sketch

import atrox.{ fastSparse, IntFreqMap, IntSet, Bits, Cursor2 }
import java.util.concurrent.{ CopyOnWriteArrayList, ThreadLocalRandom }
import java.lang.Math.{ pow, log, max, min }
import scala.util.hashing.MurmurHash3
import scala.collection.{ mutable, GenSeq }
import scala.collection.mutable.{ ArrayBuilder, ArrayBuffer }
import scala.language.postfixOps
import Sketching.{ BitSketching, IntSketching } // types


case class LSHBuildCfg(
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

  /** Forces LSH object to use only estimate to select top-k results.
    * This option has effect only for allSimilarItems methods called with
    * option maxResults set. */
  orderByEstimate: Boolean = false,

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

  def accept(idxs: Array[Int]) = idxs != null && idxs.length <= maxBucketSize

  override def toString = s"""
    |LSHCfg(
    |  maxBucketSize = $maxBucketSize
    |  maxCandidates = $maxCandidates
    |  maxResults = $maxResults
    |  orderByEstimate = $orderByEstimate
    |  compact = $compact
    |  parallel = $parallel
    |  parallelPartialResultSize = $parallelPartialResultSize
    |)
  """.trim.stripMargin('|')
}


case class Sim(idx: Int, estimatedSimilarity: Double, similarity: Double) {
  def this(idx: Int, estimatedSimilatity: Double) = this(idx, estimatedSimilatity, estimatedSimilatity)
}


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

  def buildTables[SkArr](sk: Sketching[SkArr], params: LSHTableParams, cfg: LSHBuildCfg)(has: HashAndSlice[SkArr]): IntArrayLSHTable[SkArr] = {
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



  trait Switch[SketchArray] { def run: (Sketching[SketchArray], Int, Int, LSHBuildCfg) => IntArrayLSHTable[SketchArray] }
  implicit object SwitchInt extends Switch[Array[Int]]  { val run = applyInt _ }
  implicit object SwitchBit extends Switch[Array[Long]] { val run = applyBit _ }

  def apply[SketchArray](sk: Sketching[SketchArray], bands: Int, hashBits: Int = -1, cfg: LSHBuildCfg = LSHBuildCfg())(implicit sw: Switch[SketchArray]) = {
    val table = sw.run(sk, bands, hashBits, cfg)
    LSHObj[Int, SketchArray, IntArrayLSHTable[SketchArray]](sk, sk.estimator, LSHCfg(), table, Querying.sketching(sk))
  }


  def applyBit(sk: BitSketching, bands: Int, hashBits: Int, cfg: LSHBuildCfg) = {
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

    buildTables(sk, params, cfg)(HashAndSlice.Bit)
//      (sk, itemIdx, band, params) =>
//        sk.getSketchFragment(itemIdx, band*params.bandLength, (band+1)*params.bandLength)(0).toInt
  }

  def applyInt(sk: IntSketching, bands: Int, hashBits: Int, cfg: LSHBuildCfg) = {
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

    buildTables(sk, params, cfg)(HashAndSlice.Int)
//      (sk, itemIdx, band, params) =>
//        val arr = sk.getSketchFragment(itemIdx, band*params.bandLength, (band+1)*params.bandLength)
//        hashSlice(arr, params.bandLength, 0, 0, params.bandLength, params.hashBits)
  }


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

  type SimFun = (Int, Int) => Double

}


object Querying {
  type Fun[T, SketchArray] = T => (SketchArray, Int)

  def sketch[SketchArray](sk: Sketch[SketchArray])                    = (itemIdx: Int) => (sk.sketchArray, itemIdx)
  def sketching[SketchArray](sk: atrox.sketch.Sketching[SketchArray]) = (itemIdx: Int) => (sk.getSketchFragment(itemIdx), 0)
  def sketcher[T, SketchArray](sk: Sketchers[T, SketchArray])         = (item: T)      => (sk.getSketchFragment(item), 0)
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
abstract class LSH[T] { self =>
  type SketchArray
  type Sketching = atrox.sketch.Sketching[SketchArray]
  type Idxs = Array[Int]
  type SimFun = LSH.SimFun

  def sketch: Sketching // might be null
  def querying: Querying.Fun[T, SketchArray]
  def itemsCount: Int
  def estimator: Estimator[SketchArray]
  def cfg: LSHCfg
  def bands: Int

  protected def requireSketchArray(): SketchArray = sketch match {
    case sk: atrox.sketch.Sketch[_] => sk.sketchArray.asInstanceOf[SketchArray]
    case _ => sys.error("Sketch instance required")
  }

  def withConfig(cfg: LSHCfg): LSH[T]

  def probabilityOfInclusion(sim: Double) = {
    val bandLen = estimator.sketchLength / bands
    1.0 - pow(1.0 - pow(sim, bandLen), bands)
  }

  def estimatedThreshold = {
    val bandLen = estimator.sketchLength / bands
    pow(1.0 / bands, 1.0 / bandLen)
  }

  trait Query[Res] {
    def apply(candidateIdxs: Array[Idxs], idx: Int, minEst: Double, minSim: Double, f: SimFun): Res

    def apply(q: T, minEst: Double, minSim: Double, f: SimFun): Res = {
      val (skarr, skidx) = querying(q)
      apply(rawCandidateIndexes(skarr, skidx), -1, minEst, minSim, f)
    }

    /** skidx is an index into the provided sketch array instance
     *  idx is the global index of the current item that will be used as a field in Sim object and passed into SimFun */
    def apply(skarr: SketchArray, skidx: Int, idx: Int, minEst: Double, minSim: Double, f: SimFun): Res =
      apply(rawCandidateIndexes(skarr, skidx), idx, minEst, minSim, f)
    def apply(skarr: SketchArray, skidx: Int, idx: Int, minEst: Double): Res =
      apply(skarr, skidx, idx, minEst, 0.0, null)

    def apply(idx: Int, minEst: Double, minSim: Double, f: SimFun): Res = {
      require(idx >= 0 && idx < itemsCount, s"index $idx is out of range (0 until $itemsCount)")
      apply(sketch.getSketchFragment(idx), 0, idx, minEst, minSim, f)
    }
    def apply(idx: Int, minEst: Double): Res = apply(idx, minEst, 0.0, null)
    def apply(idx: Int): Res = apply(idx, 0.0, 0.0, null)
    def apply(idx: Int, f: SimFun): Res = apply(idx, LSH.NoEstimate, 0.0, f)
    def apply(idx: Int, minSim: Double, f: SimFun): Res = apply(idx, LSH.NoEstimate, minSim, f)
  }

  trait BulkQuery[Res] {
    def apply(minEst: Double, minSim: Double, f: SimFun): Res
    def apply(minEst: Double): Res = apply(minEst, 0.0, null)
    def apply(): Res = apply(0.0, 0.0, null)
    def apply(f: SimFun): Res = apply(LSH.NoEstimate, 0.0, f)
    def apply(minSim: Double, f: SimFun): Res = apply(LSH.NoEstimate, minSim, f)

  }

  // =====

  def rawStreamIndexes: Iterator[Idxs]
  /** same as rawStreamIndexes, but arrays are limited by maxBucketSize */
  def streamIndexes: Iterator[Idxs] = rawStreamIndexes filter cfg.accept

  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs]
  def rawCandidateIndexes(sketch: Sketching, skidx: Int): Array[Idxs] = rawCandidateIndexes(sketch.getSketchFragment(skidx), 0)
  def rawCandidateIndexes(idx: Int): Array[Idxs] = rawCandidateIndexes(sketch.getSketchFragment(idx), 0)

  def candidateIndexes(skarr: SketchArray, idx: Int): Idxs = fastSparse.union(rawCandidateIndexes(skarr, idx))
  def candidateIndexes(sketch: Sketching, idx: Int): Idxs = fastSparse.union(rawCandidateIndexes(sketch, idx))
  def candidateIndexes(idx: Int): Idxs = fastSparse.union(rawCandidateIndexes(idx))

  // If minEst is set to Double.NegativeInfinity, no estimates are computed.
  // Instead candidates are directly filtered through similarity function.
  val rawSimilarIndexes = new Query[Idxs] {
    def apply(candidateIdxs: Array[Idxs], idx: Int, minEst: Double, minSim: Double, f: SimFun): Idxs = {
      val res = new ArrayBuilder.ofInt

      if (minEst == LSH.NoEstimate) {
        requireSimFun(f)
        for (idxs <- candidateIdxs) {
          var i = 0 ; while (i < idxs.length) {
            if (f(idxs(i), idx) >= minSim) { res += idxs(i) }
            i += 1
          }
        }

      } else {
        val minBits = estimator.minSameBits(minEst)
        val skarr = requireSketchArray()
        for (idxs <- candidateIdxs) {
          var i = 0 ; while (i < idxs.length) {
            val bits = estimator.sameBits(skarr, idxs(i), skarr, idx)
            if (bits >= minBits && (f == null || f(idxs(i), idx) >= minSim)) { res += idxs(i) }
            i += 1
          }
        }

      }

      res.result
    }
  }

  protected def _similar(candidateIdxs: Array[Idxs], idx: Int, minEst: Double, minSim: Double, f: SimFun): IndexResultBuilder = {
    val candidates = selectCandidates(candidateIdxs)
    val res = newIndexResultBuilder()
    runLoop(idx, candidates, minEst, minSim, f, res)
    res
  }

  // similarIndexes().toSet == (rawSimilarIndexes().distinct.toSet - idx)
  val similarIndexes = new Query[Idxs] {
    def apply(candidateIdxs: Array[Idxs], idx: Int, minEst: Double, minSim: Double, f: SimFun): Idxs =
      _similar(candidateIdxs, idx, minEst, minSim, f).result
  }

  val similarItems = new Query[Iterator[Sim]] {
    def apply(candidateIdxs: Array[Idxs], idx: Int, minEst: Double, minSim: Double, f: SimFun): Iterator[Sim] = {
      if (cfg.orderByEstimate) require(minEst != LSH.NoEstimate, "For orderByEstimate to work estimation must not be disabled.")
      val ff = if (cfg.orderByEstimate) null else f
      val simIdxs = _similar(candidateIdxs, idx, minEst, minSim, ff)
      indexResultBuilderToSims(idx, simIdxs, f, minEst == LSH.NoEstimate)
    }
  }

  val allSimilarIndexes = new BulkQuery[Iterator[(Int, Idxs)]] {
    def apply(minEst: Double, minSim: Double, f: SimFun) =
      (cfg.compact, cfg.parallel) match {
        case (true, par) => parallelBatches(0 until itemsCount iterator, par) { idx => (idx, similarIndexes(idx, minEst, minSim, f)) }
        case (false, _)  => _allSimilar_notCompact(minEst, minSim, f) map { case (idx, res) => (idx, res.result) }
      }
  }

  val allSimilarItems = new BulkQuery[Iterator[(Int, Iterator[Sim])]] {
    def apply(minEst: Double, minSim: Double, f: SimFun) = {
      (cfg.compact, cfg.parallel) match {
        case (true, par) => parallelBatches(0 until itemsCount iterator, par) { idx => (idx, similarItems(idx, minEst, minSim, f)) }
        case (false, _)  =>
          if (cfg.orderByEstimate) require(minEst != LSH.NoEstimate, "For orderByEstimate to work estimation must not be disabled.")
          val ff = if (cfg.orderByEstimate) null else f
          val iter = _allSimilar_notCompact(minEst, minSim, ff)
          parallelBatches(iter, cfg.parallel, batchSize = 256) { case (idx, simIdxs) =>
            (idx, indexResultBuilderToSims(idx, simIdxs, f, minEst == LSH.NoEstimate))
          }
      }
    }
  }

  val rawSimilarItemsStream = new BulkQuery[Iterator[(Int, Iterator[Sim])]] {
    def apply(minEst: Double, minSim: Double, f: SimFun) = {
      if (minEst == LSH.NoEstimate) {
        requireSimFun(f)
        rawStreamIndexes flatMap { idxs =>
          val res = ArrayBuffer[(Int, Iterator[Sim])]()
          var i = 0 ; while (i < idxs.length) {
            val buff = ArrayBuffer[Sim]()
            var j = i+1 ; while (j < idxs.length) {
              var sim: Double = 0.0
              if ({ sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
                buff += Sim(idxs(j), 0.0, sim)
              }
              j += 1
            }
            res += ((idxs(i), buff.iterator))
            i += 1
          }
          res
        }

      } else {
        val minBits = estimator.minSameBits(minEst)
        val skarr = requireSketchArray()
        rawStreamIndexes flatMap { idxs =>
          val res = ArrayBuffer[(Int, Iterator[Sim])]()
          var i = 0 ; while (i < idxs.length) {
            val buff = ArrayBuffer[Sim]()
            var j = i+1 ; while (j < idxs.length) {
              val bits = estimator.sameBits(skarr, idxs(i), skarr, idxs(j))
              if (bits >= minBits) {
                var sim: Double = 0.0
                if (f == null || { sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
                  buff += Sim(idxs(j), estimator.estimateSimilarity(bits), sim)
                }
              }
              j += 1
            }
            res += ((idxs(i), buff.iterator))
            i += 1
          }
          res
        }
      }
    }
  }


  /** must never return duplicates */
  private def selectCandidates(candidateIdxs: Array[Idxs]): Idxs = {
    // TODO candidate selection can be done on the fly without allocations
    //      using some sort of merging cursor

    val candidateCount = sumLength(candidateIdxs)

    if (candidateCount <= cfg.maxCandidates) {
      fastSparse.union(candidateIdxs, candidateCount)

    } else {
      val map = new IntFreqMap(initialSize = cfg.maxCandidates, loadFactor = 0.42, freqThreshold = bands)
      for (idxs <- candidateIdxs) { map ++= (idxs, 1) }
      map.topK(cfg.maxCandidates)
    }
  }

  private def sumLength(xs: Array[Idxs]) = {
    var sum, i = 0
    while (i < xs.length) {
      sum += xs(i).length
      i += 1
    }
    sum
  }

  private def runLoop(idx: Int, candidates: Idxs, minEst: Double, minSim: Double, f: SimFun, res: IndexResultBuilder) =
    if (minEst == LSH.NoEstimate) {
      requireSimFun(f)
      runLoopNoEstimate(idx, candidates, minSim, f, res)
    } else {
      val skarr = requireSketchArray()
      runLoopYesEstimate(idx, candidates, skarr, minEst, minSim, f, res)
    }

  private def runLoopNoEstimate(idx: Int, candidates: Idxs, minSim: Double, f: SimFun, res: IndexResultBuilder) = {
    var i = 0 ; while (i < candidates.length) {
      var sim = 0.0
      if (idx != candidates(i) && { sim = f(candidates(i), idx) ; sim >= minSim }) {
        res += (candidates(i), sim)
      }
      i += 1
    }
  }

  private def runLoopYesEstimate(idx: Int, candidates: Idxs, skarr: SketchArray, minEst: Double, minSim: Double, f: SimFun, res: IndexResultBuilder) = {
    val minBits = estimator.minSameBits(minEst)
    var i = 0 ; while (i < candidates.length) {
      val bits = estimator.sameBits(skarr, candidates(i), skarr, idx)
      var sim = 0.0
      if (bits >= minBits && idx != candidates(i) && (f == null || { sim = f(candidates(i), idx) ; sim >= minSim })) {
        res += (candidates(i), if (f == null) estimator.estimateSimilarity(bits) else sim)
      }
      i += 1
    }
  }

  /** Needs to store all similar indexes in memory + some overhead, but it's
    * much faster, because it needs to do only half of iterations and accessed
    * data have better cache locality. */
  protected def _allSimilar_notCompact(minEst: Double, minSim: Double, f: SimFun): Iterator[(Int, IndexResultBuilder)] = {

    // maxCandidates option is simulated via sampling
    val comparisons = streamIndexes.map { idxs => (idxs.length.toDouble - 1) * idxs.length / 2 } sum
    val ratio = itemsCount.toDouble * cfg.maxCandidates / comparisons
    assert(ratio >= 0)

    if (minSim == LSH.NoEstimate) requireSimFun(f)

    if (cfg.parallel) {
      val partialResults = new CopyOnWriteArrayList[Array[IndexResultBuilder]]()
      val truncatedResultSize =
        if (cfg.maxResults < Int.MaxValue && cfg.parallelPartialResultSize < 1.0)
          (cfg.maxResults * cfg.parallelPartialResultSize).toInt
        else cfg.maxResults

      val tl = new ThreadLocal[Array[IndexResultBuilder]] {
        override def initialValue = {
          val local = Array.fill(itemsCount)(newIndexResultBuilder(distinct = true, truncatedResultSize))
          partialResults.add(local)
          local
        }
      }

      streamIndexes.grouped(1024).toVector.par.foreach { idxsgr =>
        val local = tl.get
        for (idxs <- idxsgr) {
          runTile(idxs, ratio, minEst, minSim, f, local)
        }
      }

      val idxsArr = new Array[IndexResultBuilder](itemsCount)
      val pr = partialResults.toArray(Array[Array[IndexResultBuilder]]())
      (0 until itemsCount).par foreach { i =>
        val target = newIndexResultBuilder(distinct = true)
        for (p <- pr) target ++= p(i)
        idxsArr(i) = target

        for (p <- pr) p(i) = null
      }
      Iterator.tabulate(itemsCount) { idx => (idx, idxsArr(idx)) }

    } else {
      val res = Array.fill(itemsCount)(newIndexResultBuilder(distinct = true))
      for (idxs <- streamIndexes) {
        runTile(idxs, ratio, minEst, minSim, f, res)
      }
      Iterator.tabulate(itemsCount) { idx => (idx, res(idx)) }
    }

  }

  protected def runTile(idxs: Idxs, ratio: Double, minEst: Double, minSim: Double, f: SimFun, res: Array[IndexResultBuilder]): Unit =
    if (minEst == LSH.NoEstimate) {
      runTileNoEstimate(idxs, ratio, minSim, f, res)
    } else {
      runTileYesEstimate(idxs, ratio, minEst, minSim, f, res)
    }

  protected def runTileNoEstimate(idxs: Idxs, ratio: Double, minSim: Double, f: SimFun, res: Array[IndexResultBuilder]): Unit = {
    val stripeSize = 64
    var stripej = 0 ; while (stripej < idxs.length) {
      if (ThreadLocalRandom.current().nextDouble() < ratio) {
        val endi = min(stripej + stripeSize, idxs.length)
        val startj = stripej + 1
        var j = startj ; while (j < idxs.length) {
          val realendi = min(j, endi)
          var i = stripej ; while (i < realendi) {

            var sim = 0.0
            if ({ sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
              res(idxs(i)) += (idxs(j), sim)
              res(idxs(j)) += (idxs(i), sim)
            }

            i += 1
          }
          j += 1
        }
      }
      stripej += stripeSize
    }
  }

  protected def runTileYesEstimate(idxs: Idxs, ratio: Double, minEst: Double, minSim: Double, f: SimFun, res: Array[IndexResultBuilder]): Unit = {
    val minBits = estimator.minSameBits(minEst)
    val skarr = requireSketchArray()

    val stripeSize = 64
    var stripej = 0 ; while (stripej < idxs.length) {
      if (ThreadLocalRandom.current().nextDouble() < ratio) {
        val endi = min(stripej + stripeSize, idxs.length)
        val startj = stripej + 1
        var j = startj ; while (j < idxs.length) {
          val realendi = min(j, endi)
          var i = stripej ; while (i < realendi) {

            val bits = estimator.sameBits(skarr, idxs(i), skarr, idxs(j))
            var sim = 0.0
            if (bits >= minBits && (f == null || { sim = f(idxs(i), idxs(j)) ; sim >= minSim })) {
              sim = if (f == null) estimator.estimateSimilarity(bits) else sim
              res(idxs(i)) += (idxs(j), sim)
              res(idxs(j)) += (idxs(i), sim)
            }

            i += 1
          }
          j += 1
        }
      }
      stripej += stripeSize
    }
  }

  protected def newIndexResultBuilder(distinct: Boolean = false, maxResults: Int = cfg.maxResults): IndexResultBuilder =
    IndexResultBuilder.make(distinct, maxResults)

  /** This method converts the provided IndexResultBuilder to an iterator of Sim
    * objects while using similarity measure that was used internally in the
    * IRB for sorting and ordering. That way it's possible to avoid recomputing
    * estimate/similarity in certain cases. These cases are when we don't need
    * both similarity and it's estimate at the same time. Drawback is the fact
    * that the IRB is internally using float instead of double and therefore
    * the result might have slightly lower accuracy. */
  protected def indexResultBuilderToSims(idx: Int, irb: IndexResultBuilder, f: SimFun, noEstimates: Boolean): Iterator[Sim] = {
    val cur = irb.idxSimCursor
    val res = new ArrayBuffer[Sim](initialSize = irb.size)

    if (noEstimates && f != null) {
      while (cur.moveNext()) {
        res += Sim(cur.key, 0.0, cur.value)
      }

    } else if (!noEstimates && f == null) {
      while (cur.moveNext()) {
        res += Sim(cur.key, cur.value, cur.value)
      }

    } else if (!noEstimates && f != null && cfg.orderByEstimate) {
      while (cur.moveNext()) {
        res += Sim(cur.key, cur.value, f(idx, cur.key))
      }

    } else if (!noEstimates && f != null) {
      val skarr = requireSketchArray()
      while (cur.moveNext()) {
        val est = estimator.estimateSimilarity(skarr, idx, skarr, cur.key)
        res += Sim(cur.key, est, cur.value)
      }
    } else {
      sys.error("this should not happen")
    }
    res.iterator
  }

  protected def parallelBatches[T, U](xs: Iterator[T], inParallel: Boolean, batchSize: Int = 1024)(f: T => U): Iterator[U] =
    if (inParallel) {
      xs.grouped(batchSize).flatMap { batch => batch.par.map(f) }
    } else {
      xs.map(f)
    }

  protected def requireSimFun(f: SimFun) = require(f != null, "similarity function is required")

}





/*
/** bandLengh - how many elements is in one band
  * hashBits  - how many bits of a hash is used (2^hashBits should be roughly equal to number of items)
  */
final case class IntLSH(
    sketch: IntSketching, estimator: Estimator[Array[Int]], cfg: LSHCfg,
    idxs: Array[Array[Int]],        // mapping from a bucket to item idxs that hash into it
    itemsCount: Int,
    sketchLength: Int, bands: Int, bandLength: Int, hashBits: Int
  ) extends LSH with Serializable {

  type SketchArray = Array[Int]
  override type Sketching = IntSketching

  def withConfig(newCfg: LSHCfg): IntLSH = copy(cfg = newCfg)

  def rawStreamIndexes: Iterator[Idxs] = idxs.iterator filter (_ != null)

  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.hashSlice(skarr, sketchLength, skidx, b, bandLength, hashBits)
      val bucket = b * (1 << hashBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }
}




final case class BitLSH(
    sketch: BitSketching, estimator: Estimator[Array[Long]], cfg: LSHCfg,
    idxs: Array[Array[Int]],        // mapping from a bucket to item idxs that hash into it
    itemsCount: Int,
    bitsPerSketch: Int, bands: Int, bandBits: Int
  ) extends LSH with Serializable {

  type SketchArray = Array[Long]
  override type Sketching = BitSketching

  def withConfig(newCfg: LSHCfg): BitLSH = copy(cfg = newCfg)


  def rawStreamIndexes: Iterator[Idxs] = idxs.iterator filter (_ != null)

  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.ripBits(skarr, bitsPerSketch, skidx, b, bandBits)
      val bucket = b * (1 << bandBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }
}
  */




final case class LSHObj[T, SkArr, Table <: LSHTable[SkArr]](
    sketch: atrox.sketch.Sketching[SkArr],
    estimator: Estimator[SkArr],
    cfg: LSHCfg,
    table: Table,
    querying: Querying.Fun[T, SkArr]
  ) extends LSH[T] {

  type SketchArray = SkArr

  def withConfig(newCfg: LSHCfg): LSHObj[T, SkArr, Table] = copy(cfg = newCfg)

  val itemsCount = table.params.itemsCount
  val sketchLength = table.params.sketchLength
  val bands = table.params.bands

  def rawStreamIndexes =
    table.rawStreamIndexes

  def rawCandidateIndexes(skarr: SkArr, skidx: Int): Array[Idxs] =
    table.rawCandidateIndexes(skarr, skidx) filter { cfg.accept }
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
