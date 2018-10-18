package cavm;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.*;
import java.awt.EventQueue;
import java.awt.Desktop;
import java.awt.FileDialog;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.nio.channels.FileChannel;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Date.*;
import java.util.Map.*;
import java.util.TimerTask;
import java.text.*;
import javax.swing.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class XenaImport implements ActionListener {
	
	static final Logger LOG = LoggerFactory.getLogger(XenaImport.class);
	JFrame jfrm;
	JTextArea introduction;
	JButton openXena;
	String xenaTarget = "https://xenabrowser.net/datapages/?host=https%3A%2F%2Flocal.xena.ucsc.edu%3A7223";

	private void onExit() {
		System.exit(0);
	}

	private void displayGUI() {
		jfrm = new JFrame("Local Xena Data Hub");
		JMenuBar menuBar = new JMenuBar();
		JMenu fileMenu = new JMenu("File");
		fileMenu.setMnemonic(KeyEvent.VK_F);

		JMenuItem menuItem =new JMenuItem("Quit");
		menuItem.setAccelerator(KeyStroke.getKeyStroke( KeyEvent.VK_W, ActionEvent.META_MASK));
		menuItem.addActionListener(this);
		fileMenu.add(menuItem);

		menuBar.add(fileMenu);
		jfrm.setJMenuBar(menuBar);

		// Panel that literally holds everything
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));
		//panel.setAlignmentY(Component.LEFT_ALIGNMENT);
		//panel.setLayout(new FlowLayout(FlowLayout.LEFT));

		//select area
		introduction = new JTextArea(0, 30);
		introduction.setBackground(panel.getBackground());
		introduction.setEditable(false);
		introduction.setLineWrap(true);
		introduction.setWrapStyleWord(true);
		introduction.setText("This is your local Xena hub.");
		introduction.setAlignmentX(Component.LEFT_ALIGNMENT);
		panel.add(introduction);
		panel.add(Box.createRigidArea(new Dimension(0,15)));

		openXena = new JButton("Load and view data");
		openXena.setActionCommand("openXena");
		openXena.addActionListener(this);
		openXena.setVisible(true);
		openXena.setAlignmentX(Component.LEFT_ALIGNMENT);
		panel.add(openXena);

		// sets the size of the application
		jfrm.setSize(520, 170);

		// close Application
		jfrm.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
//		jfrm.addWindowListener(new WindowAdapter () {
//			public void windowClosing(WindowEvent evt) {
//		  		onExit();
//		  	}
//		});

		// sets the layout of the application
		jfrm.setLayout(new FlowLayout(FlowLayout.CENTER));
		// centers the application on the screen
		jfrm.setLocationRelativeTo(null);

		jfrm.add(panel);
		jfrm.setExtendedState(JFrame.ICONIFIED);
		//jfrm.setState(Frame.ICONIFIED);
		// makes the application visible
		jfrm.setVisible(true);
	}

	public void actionPerformed (ActionEvent e) {
		if ("Quit".equals(e.getActionCommand())) {
			onExit();
		}

		else if ("openXena".equals(e.getActionCommand())) {
			try {
			    Desktop.getDesktop().browse(new URI(xenaTarget));
			} catch (IOException ex) { /* TODO: error handling */ }
			catch (URISyntaxException ex) { /* TODO: error handling */ }
		}
	}

	public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
	    for (Entry<T, E> entry : map.entrySet()) {
	        if (value.equals(entry.getValue())) {
	            return entry.getKey();
	        }
	    }
	    return null;
	}

	public XenaImport() {
	}

	/**
	 * @param args
	 */
	public static XenaImport start() throws IOException {
		final XenaImport obj = new XenaImport();
		EventQueue.invokeLater(new Runnable() {
		    public void run() {
			obj.displayGUI();
		    }
		});
		return obj;
	}
}
