package cavm;

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.lang.InterruptedException;
import javax.imageio.ImageIO;
import javax.swing.JDialog;
import java.awt.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splash {
    static final Logger LOG = LoggerFactory.getLogger(Splash.class);
    private static JDialog dialog;

    public static void close() {
	if (dialog != null) {
	    dialog.dispose();
	}
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
	    Class<?> uim = Class.forName("javax.swing.UIManager"); // XXX do we need this?
	    uim.getDeclaredMethod("setLookAndFeel", String.class).invoke(null, (String) uim.getDeclaredMethod("getSystemLookAndFeelClassName").invoke(null));
	} catch (Exception ex) {
	    LOG.error("Unable to open splash screen.", ex);
	}
    }

    static void runMain(String[] args) {
	// Invoke loader method
	try {
	    Class<?> clazz = Class.forName("cavm.core");
	    Class<?>[] argClass = new Class<?>[1];
	    argClass[0] = Class.forName("[Ljava.lang.String;");
	    Object[] argObj = new Object[] {args};

	    clazz.getDeclaredMethod("main", argClass).invoke(null, argObj);
	} catch (Exception ex) {
	    LOG.error("Unable to launch main application.", ex);
	}
    }

    public static void main(final String[] args) throws InterruptedException {
	EventQueue.invokeLater(new Runnable() {
	    public void run() {
		splash();
	    }
	});
	runMain(args);
    }
}
