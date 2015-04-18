package atrox.sketch;

import java.awt.Graphics2D;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.InputStream;

import javax.imageio.ImageIO;
/*
 * pHash-like image hash. 
 * Author: Elliot Shepherd (elliot@jarofworms.com
 * Based On: http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html
 */
public class ImagePHash {

	private final int size;
	private final int smallerSize;
	private final double[][] cosines;
	private final double[] coeff;
	
	public ImagePHash() {
		this(32, 8);
	}
	
	public ImagePHash(int size, int smallerSize) {
		assert (smallerSize * smallerSize) % 64 == 0;

		this.size = size;
		this.smallerSize = smallerSize;

		cosines = new double[size][size];
		for (int i = 0; i < size; i++) {
			cosines[i] = new double[size];
			for (int j = 0; j < size; j++) {
				cosines[i][j] = Math.cos(((2*i+1)/(2.0*size)) * j * Math.PI);
			}
		}

		coeff = new double[size];
		coeff[0] = 1 / Math.sqrt(2.0);
		for (int i = 1; i < size; i++) {
			coeff[i] = 1;
		}
	}
	
	public long[] apply(InputStream is) throws Exception {
		BufferedImage img = ImageIO.read(is);
		
		/* 1. Reduce size. 
		 * Like Average Hash, pHash starts with a small image. 
		 * However, the image is larger than 8x8; 32x32 is a good size. 
		 * This is really done to simplify the DCT computation and not 
		 * because it is needed to reduce the high frequencies.
		 */
		/* 2. Reduce color. 
		 * The image is reduced to a grayscale just to further simplify 
		 * the number of computations.
		 */
		img = resizeAndGrayscale(img, size, size);

		double[][] vals = new double[size][size];


		for (int x = 0; x < img.getWidth(); x++) {
			for (int y = 0; y < img.getHeight(); y++) {
				vals[x][y] = getBlue(img, x, y);
			}
		}

		/* 3. Compute the DCT. 
		 * The DCT separates the image into a collection of frequencies 
		 * and scalars. While JPEG uses an 8x8 DCT, this algorithm uses 
		 * a 32x32 DCT.
		 */
		double[][] dctVals = applyDCT(vals);
		
		/* 4. Reduce the DCT. 
		 * This is the magic step. While the DCT is 32x32, just keep the 
		 * top-left 8x8. Those represent the lowest frequencies in the 
		 * picture.
		 */
		/* 5. Compute the average value. 
		 * Like the Average Hash, compute the mean DCT value (using only 
		 * the 8x8 DCT low-frequency values and excluding the first term 
		 * since the DC coefficient can be significantly different from 
		 * the other values and will throw off the average).
		 */
		double total = 0;
		
		for (int x = 0; x < smallerSize; x++) {
			for (int y = 0; y < smallerSize; y++) {
				total += dctVals[x][y];
			}
		}
		total -= dctVals[0][0];
		
		double avg = total / (double) ((smallerSize * smallerSize) - 1);

	
		/* 6. Further reduce the DCT. 
		 * This is the magic step. Set the 64 hash bits to 0 or 1 
		 * depending on whether each of the 64 DCT values is above or 
		 * below the average value. The result doesn't tell us the 
		 * actual low frequencies; it just tells us the very-rough 
		 * relative scale of the frequencies to the mean. The result 
		 * will not vary as long as the overall structure of the image 
		 * remains the same; this can survive gamma and color histogram 
		 * adjustments without a problem.
		 */
		long[] hash = new long[(smallerSize * smallerSize) / 64];
		
		for (int x = 0; x < smallerSize; x++) {
			for (int y = 0; y < smallerSize; y++) {
				if (!(x == 0 && y == 0)) {
					int idx = x * smallerSize + y;
					if (dctVals[x][y] > avg) {
						hash[idx / 64] |= (1L << (idx % 64));
					}
				}
			}
		}
		
		return hash;
	}
	
	private BufferedImage resizeAndGrayscale(BufferedImage image, int width,	int height) {
		BufferedImage resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
		Graphics2D g = resizedImage.createGraphics();
		g.drawImage(image, 0, 0, width, height, null);
		g.dispose();
		return resizedImage;
	}
	
	private static int getBlue(BufferedImage img, int x, int y) {
		return (img.getRGB(x, y)) & 0xff;
	}
	
	// DCT function stolen from http://stackoverflow.com/questions/4240490/problems-with-dct-and-idct-algorithm-in-java
	double[][] applyDCT(double[][] f) {
		double[][] result = new double[size][size];
		for (int u = 0; u < size; u++) {
			for (int v = 0; v < size; v++) {
				double sum = 0.0;
				for (int i = 0; i < size; i++) {
					for (int j = 0; j < size; j++) {
						sum += cosines[i][u] * cosines[j][v] * f[i][j];
					}
				}
				sum *= (coeff[u] * coeff[v]) / 4.0;
				result[u][v] = sum;
			}
		}
		return result;
	}

}
