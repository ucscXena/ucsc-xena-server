package cavm;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class H2 {
	public static class Pair {
		public Object a;
		public Object b;
		Pair(Object x, Object y) {
			a = x;
			b = y;
		}
	}

	static int max(int a[]) {
		int m = 0;
		for (int i = 0; i < a.length; ++i) {
			if (a[i] > m) {
				m = a[i];
			}
		}
		return m;
	}

	// Invert column.
	// We assume here that column is a shuffle of integers 0..(column.length - 1).
	static int[] getIndices(int[] column) {
		int[] indices = new int[column.length];
		for (int i = 0; i < column.length; ++i) {
			indices[column[i]] = i;
		}
		return indices;
	}

	// Invert column and walk over the values array to build mappings of bin -> row -> output-row
	public static HashMap<Integer, ArrayList<Pair>> binMappings(int binSize, int[] values, int[] column) {
		int[] indices = getIndices(column);
		HashMap<Integer, ArrayList<Pair>> out = new HashMap<Integer, ArrayList<Pair>>();

		for (int j = 0; j < values.length; ++j) {
			int v = values[j];
			if (v != -1) {
				int i = indices[v];
				int binI = i / binSize;
				int offset = i % binSize;
				ArrayList<Pair> bin = out.get(binI);
				if (bin == null) {
					bin = new ArrayList<Pair>();
					out.put(binI, bin);
				}
				bin.add(new Pair(offset, j));
			}
		}

		return out;
	}
}
