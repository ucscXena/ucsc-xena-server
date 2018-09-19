package cavm;

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.lang.InterruptedException;
import javax.imageio.ImageIO;
import javax.swing.JDialog;
import java.awt.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class Splash {
    static final Logger LOG = LoggerFactory.getLogger(Splash.class);
    private static JDialog dialog;

    public static void close() {
	if (dialog != null) {
	    dialog.dispose();
	}
    }

    public static String getHTML(String urlToRead) throws Exception {
	StringBuilder result = new StringBuilder();
	URL url = new URL(urlToRead);
	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	conn.setRequestMethod("GET");
	conn.setConnectTimeout(100);
	conn.setReadTimeout(300);
	BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	String line;
	while ((line = rd.readLine()) != null) {
	    result.append(line);
	}
	rd.close();
	return result.toString();
   }

    static void splash() {
	try {
	    final BufferedImage img = ImageIO.read(Splash.class.getResourceAsStream("/battle_slug_splash.png"));
	    dialog = new JDialog() {

		@Override
		public void paint(Graphics g) {
		    g.drawImage(img, 0, 0, null);
		}
	    };
	    // use the same size as your image
	    dialog.setPreferredSize(new Dimension(500, 548)); 
	    dialog.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
	    dialog.setUndecorated(true);
	    dialog.pack();
	    dialog.setLocationRelativeTo(null);
	    dialog.setVisible(true);
	    dialog.repaint();
	} catch (Exception ex) {
	    LOG.error("Unable to open splash screen.", ex);
	}
    }

    static void runMain(String[] args) throws Exception {
	// Invoke loader method
	try {
	    Class<?> clazz = Class.forName("cavm.core");
	    Class<?>[] argClass = new Class<?>[1];
	    argClass[0] = Class.forName("[Ljava.lang.String;");
	    Object[] argObj = new Object[] {args};

	    clazz.getDeclaredMethod("main", argClass).invoke(null, argObj);
	} catch (Exception ex) {
	    LOG.error("Unable to launch main application.", ex);
	    throw ex;
	}
    }

    public static void show() {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				splash();
			}
		});
	}

    public static String getPort(String[] args) {
	String p = "7222";
	for (int i = 0; i < args.length; ++i) {
	    if ((args[i].equals("-p") || args[i].equals("--port")) && i < args.length - 1) {
		p = args[i + 1];
		break;
	    }
	}
	return p;
    }

    public static void main(final String[] args) throws Exception {
	try {
	    String raise = getHTML("http://localhost:" + getPort(args) + "/raise/");
	    if (raise == "ok") {
		LOG.info("Port already bound. Notifying other process.");
		System.exit(0);
	    }
	} catch (Exception ex) {
//	    show();
	    runMain(args);
	}
    }
}
