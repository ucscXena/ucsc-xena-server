package cavm;

import cavm.Huffman;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;
import java.lang.Math;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;

public class HTFC implements Iterable<String> {

	public static class Buffer {
		public int length;
		public byte[] b;
		public Buffer() {}
	}

	public static Buffer getBuffer() {
		return new Buffer();
	}

	public static class Inner {
		int offset8;
		ByteBuffer buff8;
		public byte[] current;

		public int vbyteDecode() {
			int x = 0;
			while (true) {
				byte b = buff8.get();
				if ((b & 0x80) == 0) {
					x = (x | b) << 7;
				} else {
					return x | (b & 0x7f);
				}
			}
		}

		public boolean hasNext() {
			return buff8.remaining() > 0;
		}

		public void next(Buffer buff) {
			int i = vbyteDecode();
			byte b;
			// XXX check length against 'current' size
			b = buff8.get();
			while (b != 0) {
				current[i++] = b;
				b = buff8.get();
			}
			buff.b = current;
			buff.length = i;
		}

		public Inner(ByteBuffer b, byte[] header) {
			buff8 = b;
			// XXX This is probably wrong. The backing
			// array may not be the start of the buffer, and
			// may be longer.
			offset8 = 0;
			current = new byte[500]; // XXX Should handle resizing if necessary!
			System.arraycopy(header, 0, current, 0, header.length);
		}
	}

	ByteBuffer buff8;
	IntBuffer buff32;
	int length;
	int binSize;
	int binDictOffset;
	int binOffsets;
	int binCount; // drop this?
	int firstBin;
	Huffman binHuff;
	Huffman headerHuff;

	// using (x + n - 1) / n as integer ceil
	public int htDictLen(int offset32) {
		int len = buff32.get(offset32);
		return 1 + 2 * len + (len + 3) / 4;
	}

	public int huffDictLen(int offset32) {
		int bits = buff32.get(offset32);
		int sum = 0;

		for (int i = 0; i < bits; ++i) {
			sum += buff32.get(offset32 + 1 + i);
		}
		return 1 + bits + (sum + 3) / 4;
	}


	// rewrite this using ByteBuffer position? Might be more readable.
	public HTFC(byte[] buff) {
		buff8 = ByteBuffer.wrap(buff);
		buff8.order(ByteOrder.LITTLE_ENDIAN);
		buff32 = buff8.asIntBuffer();
		length = buff32.get(0);
		binSize = buff32.get(1);
		binDictOffset = 2 + htDictLen(2); // 32
		int binCountOffset = binDictOffset + huffDictLen(binDictOffset); // 32
		binCount = buff32.get(binCountOffset);
		firstBin = binCountOffset + binCount + 1; // 32
		binHuff = new Huffman();
		binHuff.tree(buff32, buff8, binDictOffset);
		headerHuff = new Huffman();
		headerHuff.htTree(buff32, buff8, 8);
		binOffsets = binCountOffset + 1;
	}

	public class BIterator {
	    int index = 0;
	    int binIndex = 0;
	    Inner inner;

	    public void next(Buffer b) {
		    if (index >= length) {
			    b.length = -1;
			    return;
		    }
		    if (index % binSize == 0) {
			    ByteArrayOutputStream out = new ByteArrayOutputStream(100);
			    int bin = 4 * firstBin + buff32.get(binOffsets + binIndex);
			    int headerP = headerHuff.decodeTo(buff8, bin, out);
			    byte[] header = out.toByteArray();
			    int rem = length % binSize;
			    int count = (rem == 0) ? binSize :
				    (binIndex == binCount - 1) ? rem :
				    binSize;
			    int upper = (binIndex == binCount - 1) ? buff8.capacity() // XXX capacity?
				    : 4 * firstBin + buff32.get(binOffsets + 1 + binIndex);
			    ByteArrayOutputStream out2 = new ByteArrayOutputStream(4000);

			    binHuff.decodeRange(buff8, headerP, upper, out2);
			    inner = new Inner(ByteBuffer.wrap(out2.toByteArray()), header);
			    binIndex += 1;
			    index += 1;
			    b.b = header;
			    b.length = header.length - 1;
		    } else {
			    index += 1;
			    inner.next(b);
		    }
	    }

	    public boolean hasNext() {
		return index < length;
	    }
	}

	public class HTFCIterator implements Iterator<String> {
	    BIterator bIterator = new BIterator();
	    Buffer b = new Buffer();
	    public String next() {
		bIterator.next(b);
		byte[] bytes = new byte[b.length];
		System.arraycopy(b.b, 0, bytes, 0, b.length);
		return new String(bytes);
	    }
	    public boolean hasNext() {
		return bIterator.hasNext();
	    }
	}

	public Iterator<String> iterator() {
	    return new HTFCIterator();
	}

	public BIterator biterator() {
	    return new BIterator();
	}

	public static int cmp(byte[] a, int lena, byte[] b, int lenb) {
		int i;
		for (i = 0; i < lena; ++i) {
			if (lenb <= i) {
				return 1;
			}
			if (a[i] == b[i]) {
				continue;
			}
			return (a[i] < b[i]) ? -1 : 1;
		}
		return (lenb == i) ? 0 : -1;
	}

	public static ArrayList<Integer> join(HTFC ha, HTFC hb) {
		ArrayList<Integer> out = new ArrayList<>(10000);
		Buffer ba = new Buffer();
		Buffer bb = new Buffer();
		int i = 0;
		BIterator ia = ha.biterator();
		BIterator ib = hb.biterator();

		ia.next(ba);
		ib.next(bb);
		while (true) {
			if (ba.length < 0) {
				break;
			}
			if (bb.length < 0) {
				out.add(null);
				ia.next(ba);
				continue;
			}
			int d = cmp(ba.b, ba.length, bb.b, bb.length);
			if (d == 0) {
				out.add(i);
				ia.next(ba);
				ib.next(bb);
				i += 1;
			} else if (d < 0) {
				out.add(null);
				ia.next(ba);
			} else {
				ib.next(bb);
				i += 1;
			}
		}
		return out;
	}
}
