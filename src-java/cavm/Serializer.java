package cavm;

import cavm.HTFC;
import cavm.HFC;
import cavm.GetBytes;
import org.h2.api.JavaObjectSerializer;

public class Serializer implements JavaObjectSerializer {
	static final int codeLen = 4;
	static final byte[] htfc = {104, 116, 102, 99};
	static final byte[] hfc = {104, 102, 99, 0};
	private boolean match(byte[] in, byte[] pat) {
		if (in.length < codeLen) {
			return false;
		}
		for (int i = 0; i < codeLen; ++i) {
			if (in[i] != pat[i]) {
				return false;
			}
		}
		return true;
	}
	public Object deserialize(byte[] bytes) {
		if (match(bytes, htfc)) {
			return new HTFC(bytes);
		}
		// maybe throw here if hfc doesn't match.
		return new HFC(bytes);
	}
	public byte[] serialize(Object obj) {
		GetBytes gb = (GetBytes) obj;
		return gb.getBytes();
	}
}
