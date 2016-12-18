package atrox.sketch

import atrox.{ fastSparse, IntFreqMap, IntSet, Bits, Cursor2 }
import java.util.concurrent.{ CopyOnWriteArrayList, ThreadLocalRandom }
import java.lang.Math.{ pow, log, max, min }
import scala.util.hashing.MurmurHash3
import scala.collection.{ mutable, GenSeq }
import scala.collection.mutable.{ ArrayBuilder, ArrayBuffer }
import scala.language.postfixOps


case class LSHBuildCfg(
  /** every bucket that has more than this number of elements is discarded */
  maxBucketSize: Int = Int.MaxValue,

  /** Every bucket that has less than this number of elements is discarded. It
    * might have sense to set this to 2 for closed datasets. It also filters
    * out a lot of tiny arrays which reduce memory usage. */
  minBucketSize: Int = 1,

  /** Determine how many bands are computed during one pass over data.
    * Multiple band groups can be execute in parallel.
    *
    * Increasing this number may lower available parallelisms. On the other
    * hand it increases locality or reference which may lead to better
    * performance but need to instantiate all sketchers and those might be
    * rather big (in case of RandomHyperplanes and RandomProjections)
    *
    * 1         - bands are computed one after other in multiple passes,
    *             if computeBandsInParallel is set, all of them are compute in parallel.
    * lsh.bands - all bands are computed in one pass over data
    *             This setting needs to instantiate all sketchers at once, but
    *             it's necessary when data source behave like an iterator.
    */
  bandsInOnePass: Int = 1,

  /** If this is set to true, program does one prior pass to count number of
    * values associated with every hash. It needs to do more work but allocates
    * only necessary amount of memory. */
  collectCounts: Boolean = false,

  computeBandsInParallel: Boolean = false,

  reverseMapping: Boolean = false
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


case class Sim(a: Int, b: Int, estimatedSimilatity: Double, similarity: Double) {
  def this(a: Int, b: Int, estimatedSimilatity: Double) = this(a, b, estimatedSimilatity, estimatedSimilatity)
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


  // === BitLSH =================

  def apply(sk: BitSketching, bands: Int): BitLSH =
    apply(sk, bands, LSHBuildCfg())

  def apply(sk: BitSketching, bands: Int, cfg: LSHBuildCfg): BitLSH = {
    require(sk.sketchLength % 64 == 0, "BitLSH must have multiple of 64 hashes for now")
    require(bands > 0, "number of bands must be non-negative")

    val bandBits = sk.sketchLength / bands
    val bandMask = (1L << bandBits) - 1
    val bandSize = (1 << bandBits)
    val longsLen = sk.sketchLength / 64

    require(bandBits < 32)

    val idxs = new Array[Array[Int]](bands * bandSize)

    for ((bs, bandGroup) <- bandGrouping(bands, cfg)) {

      val skslices = bs map { b => sk.slice(b * bandBits, (b+1) * bandBits) } toArray

      def runItems(f: (Int, Int) => Unit) = {
        val base = bs(0)
        val end = bs.last
        var i = 0 ; while (i < sk.length) {
          var b = base ; while (b <= end) {
            val h = skslices(b-base).getSketchFragmentAsLong(i, 0, bandBits).toInt
            assert(h <= bandSize)
            f((b - base) * bandSize + h, i)
            b += 1
          }
          i += 1
        }
      }

      val bandMap = if (cfg.collectCounts) {
        val counts = new Array[Int](cfg.bandsInOnePass * bandSize)
        runItems((h, i) => counts(h) += 1)
        Grouping(cfg.bandsInOnePass * bandSize, Int.MaxValue, counts) // Grouping.Counted
      } else {
        Grouping(cfg.bandsInOnePass * bandSize, cfg.bandsInOnePass * sk.length) // Grouping.Sorted
      }

      runItems((h, i) => bandMap.add(h, i))

      bandMap.getAll foreach { case (h, is) =>
        require(fastSparse.isDistinctIncreasingArray(is))
        if (is.length <= cfg.maxBucketSize && is.length >= cfg.minBucketSize) {
          idxs(bandGroup * bandSize * cfg.bandsInOnePass + h) = is
        }
      }
    }

    val revmap = if (cfg.reverseMapping) makeReverseMapping(sk.length, bands, idxs) else null

    new BitLSH(sk, sk.estimator, LSHCfg(), idxs, revmap, sk.length, sk.sketchLength, bands, bandBits)
  }


  def ripBits(sketchArray: Array[Long], bitsPerSketch: Int, i: Int, band: Int, bandBits: Int): Int =
    Bits.getBitsOverlapping(sketchArray, bitsPerSketch * i + band * bandBits, bandBits).toInt


  // === IntLSH =================

  def apply(sk: IntSketching, bands: Int): IntLSH =
    apply(sk, bands, pickBits(sk.length), LSHBuildCfg())

  def apply(sk: IntSketching, bands: Int, cfg: LSHBuildCfg): IntLSH =
    apply(sk, bands, pickBits(sk.length), cfg)

  private def pickBits(len: Int) = 32 - Integer.numberOfLeadingZeros(len)

  def apply(sk: IntSketching, bands: Int, hashBits: Int): IntLSH =
    apply(sk, bands, hashBits, LSHBuildCfg())

  def apply(sk: IntSketching, bands: Int, hashBits: Int, cfg: LSHBuildCfg): IntLSH = {
    require(hashBits > 0, "number of hash buts must be non-negative")
    require(bands > 0, "number of bands must be non-negative")

    val bandElements = sk.sketchLength / bands // how many elements from sketch is used in one band
    val hashMask = (1 << hashBits) - 1
    val bandSize = (1 << hashBits) // how many hashes are possible in one band

    val idxs = new Array[Array[Int]](bands * bandSize)

    for ((bs: Seq[Int], bandGroup: Int) <- bandGrouping(bands, cfg)) {

      val scratchpads = Array.ofDim[Int](cfg.bandsInOnePass, bandElements)
      val skslices    = bs map { b => sk.slice(b * bandElements, (b+1) * bandElements) } toArray

      def runItems(f: (Int, Int) => Unit, g: (Int, Array[Int], Int) => Unit = null) = {
        val base = bs(0)
        val end = bs.last
        var i = 0 ; while (i < sk.length) { // iterate over all items
          var b = base ; while (b <= end) { // iterate over bands of an item
            val h = if (sk.isInstanceOf[IntSketch]) {
              // if sk is IntSketch instance directly access it's internal sketch array, this saves some copying
              val _sk = sk.asInstanceOf[IntSketch]
              hashSlice(_sk.sketchArray, _sk.sketchLength, i, b, bandElements, hashBits)
            } else {
              skslices(b-base).writeSketchFragment(i, scratchpads(b-base), 0)
              hashSlice(scratchpads(b-base), bandElements, 0, 0, bandElements, hashBits)
            }

            if (f != null) {
              f((b - base) * bandSize + h, i)
            }
            if (g != null) {
              val sk = skslices(b-base).getSketchFragment(i)
              g(b, sk, i)
            }
            b += 1
          }
          i += 1
        }
      }

      val bandMap = if (cfg.collectCounts) {
        val counts = new Array[Int](cfg.bandsInOnePass * bandSize)
        runItems((h, i) => counts(h) += 1)
        Grouping(cfg.bandsInOnePass * bandSize, Int.MaxValue, counts) // Grouping.Counted
      } else {
        Grouping(cfg.bandsInOnePass * bandSize, cfg.bandsInOnePass * sk.length) // Grouping.Sorted
      }

      runItems((h, i) => bandMap.add(h, i))

      bandMap.getAll foreach { case (h, is) =>
        require(fastSparse.isDistinctIncreasingArray(is))
        if (is.length <= cfg.maxBucketSize && is.length >= cfg.minBucketSize) {
          idxs(bandGroup * bandSize * cfg.bandsInOnePass + h) = is
        }
      }
    }

    val revmap = if (cfg.reverseMapping) makeReverseMapping(sk.length, bands, idxs) else null

    new IntLSH(sk, sk.estimator, LSHCfg(), idxs, revmap, sk.length, sk.sketchLength, bands, bandElements, hashBits)
  }

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

  // band groups -> (band indexes, band group index)
  def bandGrouping(bands: Int, cfg: LSHBuildCfg): GenSeq[(Seq[Int], Int)] = {
    val bss = (0 until bands grouped cfg.bandsInOnePass).zipWithIndex.toSeq ;
    if (!cfg.computeBandsInParallel) bss else bss.par
  }


  def makeReverseMapping(length: Int, bands: Int, idxs: Array[Array[Int]]) = {
    val revmap = Array.fill(length) {
      val ab = new ArrayBuilder.ofInt
      ab.sizeHint(bands)
      ab
    }

    for ((bucket, bidx) <- idxs.zipWithIndex) {
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


/** LSH configuration
  *
  * LSH, full Sketch
  * LSH, empty Sketch
  *  - can only provide list of candidates, cannot estimate similarities
  *
  * - Methods with "raw" prefix might return duplicates and are not affected by
  *   maxBucketSize, maxCandidates and maxResults config options.
  * - Methods without "raw" prefix must never return duplicates and queried item itself
  *
  * Those combinations introduce non-determenism:
  * - non compact + max candidates (candidate selection is done by random sampling)
  * - parallel + non compact
  *
  * idxs must be sorted
  */
abstract class LSH { self =>
  type SketchArray
  type Sketching <: atrox.sketch.Sketching[T] forSome { type T <: SketchArray}
  type Idxs = Array[Int]
  type SimFun = (Int, Int) => Double

  def sketch: Sketching // might be null
  def itemsCount: Int
  def estimator: Estimator[SketchArray]
  def cfg: LSHCfg
  def bands: Int
  protected def hasReverseMapping: Boolean

  protected def requireSketchArray(): SketchArray = sketch match {
    case sk: atrox.sketch.Sketch[_] => sk.sketchArray.asInstanceOf[SketchArray]
    case _ => sys.error("Sketch instance required")
  }

  def withConfig(cfg: LSHCfg): LSH

  def probabilityOfInclusion(sim: Double) = {
    val bandLen = estimator.sketchLength / bands
    1.0 - pow(1.0 - pow(sim, bandLen), bands)
  }

  def estimatedThreshold = {
    val bandLen = estimator.sketchLength / bands
    pow(1.0 / bands, 1.0 / bandLen)
  }

  def needsReverseMapping = if (!hasReverseMapping) withReverseMapping else this
  def withReverseMapping: LSH

  trait Query[Res] {
    def apply(candidateIdxs: Array[Idxs], idx: Int, minEst: Double, minSim: Double, f: SimFun): Res

    /** skidx is an index into the provided sketch array instance
     *  idx is the global index of the current item that will be used as a field in Sim object and passed into SimFun */
    def apply(skarr: SketchArray, skidx: Int, idx: Int, minEst: Double, minSim: Double, f: SimFun): Res =
      apply(rawCandidateIndexes(skarr, skidx), idx, minEst, minSim, f)
    def apply(skarr: SketchArray, skidx: Int, idx: Int, minEst: Double): Res =
      apply(skarr, skidx, idx, minEst, 0.0, null)
    def apply(sketch: Sketching, idx: Int, minEst: Double): Res =
      apply(sketch.getSketchFragment(idx), 0, idx, minEst, 0.0, null)
    def apply(sketch: Sketching, idx: Int, minEst: Double, minSim: Double, f: SimFun): Res =
      apply(sketch.getSketchFragment(idx), 0, idx, minEst, minSim, f)

    def apply(idx: Int, minEst: Double, minSim: Double, f: SimFun): Res = {
      require(idx >= 0 && idx < itemsCount, s"index $idx is out of range (0 until $itemsCount)")

      if (hasReverseMapping) {
        apply(rawCandidateIndexes(idx), idx, minEst, minSim, f)
      } else {
        apply(sketch.getSketchFragment(idx), 0, idx, minEst, minSim, f)
      }
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

  val rawSimilarItemsStream = new BulkQuery[Iterator[Sim]] {
    def apply(minEst: Double, minSim: Double, f: SimFun) = {
      if (minEst == LSH.NoEstimate) {
        requireSimFun(f)
        rawStreamIndexes flatMap { idxs =>
          val res = ArrayBuffer[Sim]()
          var i = 0 ; while (i < idxs.length) {
            var j = i+1 ; while (j < idxs.length) {
              var sim: Double = 0.0
              if ({ sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
                res += Sim(idxs(i), idxs(j), 0.0, sim)
              }
              j += 1
            }
            i += 1
          }
          res
        }

      } else {
        val minBits = estimator.minSameBits(minEst)
        val skarr = requireSketchArray()
        rawStreamIndexes flatMap { idxs =>
          val res = ArrayBuffer[Sim]()
          var i = 0 ; while (i < idxs.length) {
            var j = i+1 ; while (j < idxs.length) {
              val bits = estimator.sameBits(skarr, idxs(i), skarr, idxs(j))
              if (bits >= minBits) {
                var sim: Double = 0.0
                if (f == null || { sim = f(idxs(i), idxs(j)) ; sim >= minSim }) {
                  res += Sim(idxs(i), idxs(j), estimator.estimateSimilarity(bits), sim)
                }
              }
              j += 1
            }
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
        res += Sim(idx, cur.key, 0.0, cur.value)
      }

    } else if (!noEstimates && f == null) {
      while (cur.moveNext()) {
        res += Sim(idx, cur.key, cur.value, cur.value)
      }

    } else if (!noEstimates && f != null && cfg.orderByEstimate) {
      while (cur.moveNext()) {
        res += Sim(idx, cur.key, cur.value, f(idx, cur.key))
      }

    } else if (!noEstimates && f != null) {
      val skarr = requireSketchArray()
      while (cur.moveNext()) {
        val est = estimator.estimateSimilarity(skarr, idx, skarr, cur.key)
        res += Sim(idx, cur.key, est, cur.value)
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




/** bandLengh - how many elements in one band
  * hashBits  - how many bits of a hash is used (2^hashBits should be roughly equal to number of items)
  */
final case class IntLSH(
    sketch: IntSketching, estimator: IntEstimator, cfg: LSHCfg,
    idxs: Array[Array[Int]],        // mapping from a bucket to item idxs that hash into it
    reverseIdxs: Array[Array[Int]], // mapping from a item idx to buckets in which it's located
    itemsCount: Int,
    sketchLength: Int, bands: Int, bandLength: Int, hashBits: Int
  ) extends LSH with Serializable {

  type SketchArray = Array[Int]
  type Sketching = IntSketching

  def withConfig(newCfg: LSHCfg): IntLSH = copy(cfg = newCfg)
  def withReverseMapping = copy(reverseIdxs = LSH.makeReverseMapping(itemsCount, bands, idxs))
  def hasReverseMapping = reverseIdxs != null

  def bandHashes(sketchArray: SketchArray, idx: Int): Iterator[Int] =
    Iterator.tabulate(bands) { b => bandHash(sketchArray, idx, b) }

  def bandHash(sketchArray: SketchArray, idx: Int, band: Int): Int =
    LSH.hashSlice(sketchArray, sketch.sketchLength, idx, band, bandLength, hashBits)

  def rawStreamIndexes: Iterator[Idxs] = idxs.iterator filter (_ != null)


  override def rawCandidateIndexes(idx: Int): Array[Idxs] =
    reverseIdxs(idx) map { bucket => idxs(bucket) }

  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.hashSlice(skarr, sketchLength, skidx, b, bandLength, hashBits)
      val bucket = b * (1 << hashBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }
}




final case class BitLSH(
    sketch: BitSketching, estimator: BitEstimator, cfg: LSHCfg,
    idxs: Array[Array[Int]],        // mapping from a bucket to item idxs that hash into it
    reverseIdxs: Array[Array[Int]], // mapping from a item idx to buckets in which it's located
    itemsCount: Int,
    bitsPerSketch: Int, bands: Int, bandBits: Int
  ) extends LSH with Serializable {

  type SketchArray = Array[Long]
  type Sketching = BitSketching

  def withConfig(newCfg: LSHCfg): BitLSH = copy(cfg = newCfg)
  def withReverseMapping = copy(reverseIdxs = LSH.makeReverseMapping(itemsCount, bands, idxs))
  def hasReverseMapping = reverseIdxs != null

  def bandHashes(sketchArray: Array[Long], idx: Int): Iterator[Int] =
    Iterator.tabulate(bands) { b => LSH.ripBits(sketchArray, bitsPerSketch, idx, b, bandBits) }

  def rawStreamIndexes: Iterator[Idxs] = idxs.iterator filter (_ != null)


  override def rawCandidateIndexes(idx: Int): Array[Idxs] =
    reverseIdxs(idx) map { bucket => idxs(bucket) }

  def rawCandidateIndexes(skarr: SketchArray, skidx: Int): Array[Idxs] =
    Array.tabulate(bands) { b =>
      val h = LSH.ripBits(skarr, bitsPerSketch, skidx, b, bandBits)
      val bucket = b * (1 << bandBits) + h
      idxs(bucket)
    }.filter { idxs => cfg.accept(idxs) }
}
