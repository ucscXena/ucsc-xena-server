package cavm;

import java.io.File;
import cavm.CohortCallback;

public interface XenaServer {
	boolean load(File file);
	void retrieveCohorts(CohortCallback cb);
	String publicUrl();
	String localUrl();
}
