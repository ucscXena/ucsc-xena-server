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

	static int[] getIndicies(int[] values) {
		int m = max(values);
		int[] indicies = new int[m + 1];
		Arrays.fill(indicies, -1);
		for (int i = 0; i < values.length; ++i) {
			indicies[values[i]] = i;
		}
		return indicies;
	}

	public static HashMap<Integer, ArrayList<Pair>> binMappings(int binSize, int[] values, int[] row) {
		int[] indicies = getIndicies(values);
		HashMap<Integer, ArrayList<Pair>> out = new HashMap<Integer, ArrayList<Pair>>();

		for (int i = 0; i < row.length; ++i) {
			int binI = i / binSize;
			int offset = i % binSize;
			int outRow = indicies[row[i]];
			ArrayList<Pair> bin = out.get(binI);
			if (outRow != -1) {
				if (bin == null) {
					bin = new ArrayList<Pair>();
					out.put(binI, bin);
				}
				bin.add(new Pair(offset, outRow));
			}
		}

		return out;
	}
}
