package cavm;

import cavm.HTFC;
import org.h2.api.JavaObjectSerializer;

public class Serializer implements JavaObjectSerializer {
	public Object deserialize(byte[] bytes) {
		return new HTFC(bytes);
	}
	public byte[] serialize(Object obj) {
		HTFC htfc = (HTFC) obj;
		return htfc.getBytes();
	}
}
