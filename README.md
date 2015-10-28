## Sketches

*Sketches* is a library for sketching, locality sensitive hashing and
approximate similarity search.

### Usage

Basic use case is search for all similar items in a dataset.

```scala
import atrox.sketch._

val sets: Seq[Set[Int]] = loadMyData()

val (bands, hashes) = LSH.pickHashesAndBands(threshold = 0.5, maxHashes = 64)
val minhash = MinHash(sets, hashes)
val lsh     = LSH(minhash, bands)

val cfg = LSHCfg(maxResults = 50)

for (Sim(idx1, idx2, estimate, similarity) <- lsh.withConfig(cfg).allSimilarItems(minEst = 0.5)) {
  println(s"similarity between item $idx1 and $idx2 is estimated to $estimate")
}
```

There's more configuration options available.

```scala
lsh.withConfig(LSHCfg(
  // Return only the 100 most relevant results.
  // It's strongly recommended to use this option.
  maxResults = 100,

  // Perform similarity search in parallel.
  parallel = true,

  // Use as much memory as needed. This leads to faster bulk queries but
  // might need to store the complete result set in memory.
  compact = false,

  // Skip anomalously large buckets. This speeds things up quite a bit.
  maxBucketSize = sets.size / 10
))
```

And more query methods.

```scala
lsh.similarItems(idx)
lsh.similarIndexes(idx)
lsh.allSimilarItems()
lsh.allSimilarIndexes()
```

And more sketching methods.

- MinHash and SingleBitMinHash for estimating Jaccard index
- WeightedMinHash for estimating weighted Jaccard index
- RandomHyperplanes for estimation cosine similarity
- RandomProjections for LSH based on euclidean distance
- HammingDistance
- SimHash
