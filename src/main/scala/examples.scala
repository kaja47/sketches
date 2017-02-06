package atrox.example

import java.io.{ File, FileInputStream }
import atrox._
import atrox.sketch._

object PHashExample extends App {

  if (args.length < 1) sys.exit()

  val directory = new File(args(0))

  if (!directory.exists) sys.exit()

  val files = directory
    .listFiles
    .filter(f => f.getName.toLowerCase.matches(".*\\.(jpg|png)$"))

  println("PHashing")

  val phash = new ImagePHash(32, 8) // PHash produces 64 bit hash (8x8)

  val hashArray = files.par
    .map(f => phash(new FileInputStream(f)))
    .toArray

  println("LSH construction")

  val sketch = HammingDistance(hashArray, 64)
  val (_, bands) = LSH.pickHashesAndBands(0.85, 64)
  val lsh = LSH.estimating(sketch, LSHBuildCfg(bands = bands))

  println("LSH query")

  for ((idx, sims) <- lsh.allSimilarItems(LSHCfg(maxResults = 1, threshold = 0.85))) {
    for (sim <- sims) {
      println(files(idx)+" "+files(sim.idx)+" "+sim.sim)
    }
  }

}
