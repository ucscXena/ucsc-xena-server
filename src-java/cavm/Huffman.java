package cavm;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.io.ByteArrayOutputStream;

public class Huffman {
	class Node {
	}
	class Inner extends Node {
		Node left;
		Node right;
	}
	class Leaf extends Node {
		byte symbol;
		Leaf(byte s) {
			symbol = s;
		}
	}
	public Node root;
	public Huffman() {
		root = new Inner();
	}
	public void insert(long code, int len, byte sym) {
		Inner n = (Inner)root;
		for (int i = len - 1; i > 0; i--) {
			if ((code & (1 << i)) == 0) {
				if (n.right == null) {
					n.right = new Inner();
				}
				n = (Inner)n.right;
			} else {
				if (n.left == null) {
					n.left = new Inner();
				}
				n = (Inner)n.left;
			}
		}
		if ((code & 1) == 0) {
			n.right = new Leaf(sym);
		} else {
			n.left = new Leaf(sym);
		}
	}
	public Node tree(IntBuffer buff32, ByteBuffer buff8, int offset32) {
		int len = buff32.get(offset32);
		long code = 0;
		int symbol8 = 4 * (offset32 + 1 + len);
		for (int i = 1; i <= len; ++i) {
			int N = buff32.get(offset32 + i);
			long icode = code;
			for (int j = 0; j < N; ++j, ++icode) {
				insert(icode, i, buff8.get(symbol8 + j));
			}
			code = (code + N) << 1;
			symbol8 += N;
		}
		return root;
	}

	public Node htTree(IntBuffer buff32, ByteBuffer buff8, int offset8) {
	    int offset32 = offset8 / 4;
	    int len = buff32.get(offset32);
	    int codes = 1; // wtf is this?
	    int symbols = 4 * (1 + 2 * len);
	    for (int j = 0; j < len; ++j) {
			insert(buff32.get(offset32 + codes + 2 * j),
				buff32.get(offset32 + codes + 2 * j + 1), buff8.get(offset8 + symbols + j));
	    }
	    return root;
	}

	public int decodeTo(ByteBuffer buff8, int start,
			ByteArrayOutputStream out) {
		Inner n = (Inner)root;
		for (int i = start; true; ++i) {
			byte b = buff8.get(i);
			for (int j = 0x80; j > 0; j >>= 1) {
				Node m = (((b & j) == 0) ? n.right : n.left);
				if (m instanceof Leaf) {
					byte s = ((Leaf)m).symbol;
					out.write(s);
					if (s == 0) {
					    return i + 1;
					}
					n = (Inner)root;
				} else {
					n = (Inner)m;
				}
			}
		}
	}

	public void decodeRange(ByteBuffer buff8, int start, int end,
			ByteArrayOutputStream out) {
		Inner n = (Inner)root;
		for (int i = start; i < end; ++i) {
			byte b = buff8.get(i);

			Node m;

			// unrolled for speed
			m = (((b & 0x80) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x40) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x20) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x10) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x08) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x04) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x02) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
			m = (((b & 0x01) == 0) ? n.right : n.left);
			if (m instanceof Leaf) {
				out.write(((Leaf)m).symbol);
				n = (Inner)root;
			} else {
				n = (Inner)m;
			}
		}
	}
}
