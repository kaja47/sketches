package atrox.sketch

import java.lang.Math._
import java.io.InputStream
import java.awt.image.BufferedImage
import javax.imageio.ImageIO


/**
 * pHash-like image hash.
 * Based On: http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html
 */
class ImagePHash(size: Int = 32, smallerSize: Int = 8) {

  assert((smallerSize * smallerSize) % 64 == 0)

  private[this] val cosines = Array.tabulate[Double](size, size) { (i, j) => cos((2*i+1) / (2.0*size) * j * PI) }
  private[this] val coeff   = Array.tabulate[Double](size) { i => if (i == 0) 1 / sqrt(2.0) else 1.0 }


  def apply(is: InputStream): Array[Long] =
    apply(ImageIO.read(is))


  def apply(img: BufferedImage): Array[Long] = {
    /* 1. Reduce size.
     * Like Average Hash, pHash starts with a small image.
     * However, the image is larger than 8x8 32x32 is a good size.
     * This is really done to simplify the DCT computation and not
     * because it is needed to reduce the high frequencies.
     * 2. Reduce color.
     * The image is reduced to a grayscale just to further simplify
     * the number of computations.
     */
    val resized = resizeAndGrayscale(img, size, size)

    val vals = Array.ofDim[Double](size, size)

    var x = 0
    while (x < resized.getWidth) {
      var y = 0
      while (y < resized.getHeight) {
        vals(x)(y) = getBlue(resized, x, y)
        y += 1
      }
      x += 1
    }

    /* 3. Compute the DCT.
     * The DCT separates the image into a collection of frequencies
     * and scalars. While JPEG uses an 8x8 DCT, this algorithm uses
     * a 32x32 DCT.
     */
    val dctVals = applyDCT(vals)

    /* 4. Reduce the DCT.
     * This is the magic step. While the DCT is 32x32, just keep the
     * top-left 8x8. Those represent the lowest frequencies in the
     * picture.
     * 5. Compute the average value.
     * Like the Average Hash, compute the mean DCT value (using only
     * the 8x8 DCT low-frequency values and excluding the first term
     * since the DC coefficient can be significantly different from
     * the other values and will throw off the average).
     */
    var total = 0.0

    {
      var x = 0
      while (x < smallerSize) {
        var y = 0
        while (y < smallerSize) {
          total += dctVals(x)(y)
          y += 1
        }
        x += 1
      }
      total -= dctVals(0)(0)
    }

    val avg = total / ((smallerSize * smallerSize) - 1.0)


    /* 6. Further reduce the DCT.
     * This is the magic step. Set the 64 hash bits to 0 or 1
     * depending on whether each of the 64 DCT values is above or
     * below the average value. The result doesn't tell us the
     * actual low frequencies it just tells us the very-rough
     * relative scale of the frequencies to the mean. The result
     * will not vary as long as the overall structure of the image
     * remains the same this can survive gamma and color histogram
     * adjustments without a problem.
     */
    val hash = new Array[Long]((smallerSize * smallerSize) / 64)

    {
      var x = 0
      while (x < smallerSize) {
        var y = 0
        while (y < smallerSize) {
          if (!(x == 0 && y == 0)) {
            val idx = x * smallerSize + y
            if (dctVals(x)(y) > avg) {
              hash(idx / 64) |= (1L << (idx % 64))
            }
          }
          y += 1
        }
        x += 1
      }
    }

    hash
  }

  private def resizeAndGrayscale(image: BufferedImage, width: Int, height: Int): BufferedImage = {
    val resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g = resizedImage.createGraphics()
    g.drawImage(image, 0, 0, width, height, null)
    g.dispose()
    resizedImage
  }

  private def getBlue(img: BufferedImage, x: Int, y: Int): Int =
    img.getRGB(x, y) & 0xff

  // DCT function stolen from http://stackoverflow.com/questions/4240490/problems-with-dct-and-idct-algorithm-in-java
  def applyDCT(f: Array[Array[Double]]): Array[Array[Double]] = {
    val result = Array.ofDim[Double](size, size)
    var u = 0
    while (u < size) {
      var v = 0
      while (v < size) {
        var sum = 0.0
        var i = 0
        while (i < size) {
          var j = 0
          while (j < size) {
            sum += cosines(i)(u) * cosines(j)(v) * f(i)(j)
            j += 1
          }
          i += 1
        }
        sum *= (coeff(u) * coeff(v)) / 4.0
        result(u)(v) = sum
        v += 1
      }
      u += 1
    }
    result
  }

}
