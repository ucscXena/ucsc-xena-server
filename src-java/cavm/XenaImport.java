//version 0.1.0
/*


The program tries to do these
1. select file to import to local xena
2. generate .json at the same time, give user some help to pick and choose meta-data.
3. detecting if there is "xena-0.4.0-SNAPSHOT" running..., hard coded only check xena-0.4.0-SNAPSHOT
4. if xena is not started by this program, data import will not be allowed.
5. when xena is not running,  automatically start xena-0.4.0-SNAPSHOT,
6. provide a button to stop or start xena-0.4.0-SNAPSHOT
7. automatically build $HOME/files if it is not on user's computer
*/

/*
*notes:
when i try to start xena from your tool it tells me i need the java developer tools.
*/

// javac -g  -cp .:./external/gson-2.3.jar:./external/tools.jar XenaImport.java -d .
// test inside dir:  java -cp .:./external/gson-2.3.jar:./external/tools.jar XenaImport

// jar cfmv ../XenaServer/XenaImport.jar m.txt *.class

// http://see.stanford.edu/materials/icspmcs106a/44-packaging-jar-files.pdf

// xena.jar:  https://drive.google.com/uc?id=0BxeGFxkAhivXYjRSSlBpYXc4WkU&export=download

//TODO:
// xls file http://stackoverflow.com/questions/18077264/convert-csv-to-xls-xlsx-using-apache-poi
// cohort list from server
// probemap from server
// JWrapper

// http://stackoverflow.com/questions/19039752/removing-java-8-jdk-from-mac
//

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
import cavm.XenaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cavm.CohortCallback;


public class XenaImport implements ActionListener, CohortCallback {
	private class DatafileFilter implements FilenameFilter {
	    public boolean accept(File dir, String name) {
		return (!name.endsWith(".json") &&
			!name.endsWith("~") &&
			!name.endsWith(".bam") &&
			!name.endsWith(".xlsx") &&
			!name.endsWith(".xls") &&
			!name.endsWith(".xltx") &&
			!name.endsWith(".xlt") &&
			!name.endsWith(".xlsb") &&
			!name.endsWith(".xlsm") &&
			!name.endsWith(".xltm") &&
			!name.endsWith(".xml") &&
			!name.endsWith(".xlam") &&
			!name.endsWith(".xla") &&
			!name.endsWith(".mht") &&
			!name.endsWith(".prn") &&
			!name.endsWith(".dif") &&
			!name.endsWith(".csv") &&
			!name.endsWith(".htm") &&
			!name.endsWith(".html") &&
			!name.endsWith(".rtf") &&
			!name.endsWith(".pdf") &&
			!name.endsWith(".PDF") &&
			!name.endsWith(".svg") &&
			!name.endsWith(".doc") &&
			!name.endsWith(".DOC") &&
			!name.endsWith(".docx") &&
			!name.endsWith(".ppt") &&
			!name.endsWith(".pptx") &&
			!name.endsWith(".key") &&
			!name.endsWith(".JPG") &&
			!name.endsWith(".jpg") &&
			!name.endsWith(".tiff") &&
			!name.endsWith(".tif") &&
			!name.endsWith(".png") &&
			!name.endsWith(".zip") &&
			!name.endsWith(".gz") &&
			!name.endsWith(".tgz") &&
			!name.endsWith(".dmg") &&
			!name.endsWith(".pkg") &&
			!name.endsWith(".tar") &&
			!name.endsWith(".jar")
			);
	    }
	}

	static final Logger LOG = LoggerFactory.getLogger(XenaImport.class);

	XenaServer server;

	// Destination directory
	String dest = System.getProperty("user.home") + "/xena/files/";

	// meta data
	Map<String, Object> metadata;

	String mutationFormat = "Mutation by Position";

	HashMap<String, String> formats = new HashMap<String, String>() {
		{
			put("ROWs (identifiers)  x  COLUMNs (samples) -- often genomic data matrix","genomicMatrix");
			put("ROWs (samples)  x  COLUMNs (identifiers) -- often phenotype data","clinicalMatrix");
			put(mutationFormat,"mutationVector");
		}
	};

	String[] formatStrings =formats.keySet().toArray(new String[formats.size()]);

	String hintString = "Select or Enter your own";

	String[] dataTypeStrings = {
		hintString,
		"filter",
		"phenotype",
		"copy number",
		"DNA methylation",
		"exon expression",
		"gene expression",
		"gene expression RNAseq",
		"gene expression Array",
		"miRNA expression",
		"somatic mutation (SNP and small INDELs)",
		"somatic mutation (structural variant)",
		"somatic mutation (gene-level)",
		"protein expression RPPA",
		"PARADIGM pathway activity"
	};

	String [] noProbeMapList = {
		"phenotype","filter",
		"somatic mutation (SNP and small INDELs)",
		"somatic mutation (structural variant)"
	};

	String [] colNormList ={
		"exon expression",
		"gene expression",
		"gene expression RNAseq",
		"gene expression Array",
		"miRNA expression",
		"protein expression RPPA"
	};

	String [] assemblyStrings = {"Select","hg38","hg19","hg18"};

	File sourceFile;
	File probeMapFile = null;

	JFrame jfrm;

	JButton buttonSelect;
	JTextArea notifications;
	JButton openXena;
	String xenaTarget;

	//format/type area
	JPanel typePanel;
	JComboBox<String> formatList;

	//data type/dataSubType
	JPanel dataTypePanel;
	JComboBox<String> dataTypeList;

	//assembly area
	JPanel assemblyPanel;
	JComboBox<String> assemblyList;

	//cohort
	JPanel cohortPanel;
	JComboBox<String> cohortList;

	// display label
	JPanel displayLabelPanel;
	JTextField displayLabel;

	//description
	JPanel descPanel;
	JTextArea description;
	JScrollPane scrollDescription;

	//probeMap area
	JPanel mappingPanel;
	JButton mappingButton;
	JTextArea mappingNotifications;

	// data snippet
	JPanel dataSnippetPanel;
	JTextArea dataSnippet;
	//JTextField minColor;

	//submit
	JPanel submitPanel;
	JButton submitButton;
	JButton cancelButton;

	public void raise() {
	    java.awt.EventQueue.invokeLater(new Runnable() {
			@Override
			public void run() {
				jfrm.setAlwaysOnTop(true);
				jfrm.toFront();
//				jfrm.repaint();
				jfrm.setAlwaysOnTop(false);
				jfrm.requestFocus();
			}
		});
	}

	private void setUp() {
		// $HOME/files directory set up
		File f = new File(dest);
		if (!f.exists()){
			f.mkdirs();
		}
		if (!f.isDirectory()) {
			JOptionPane.showMessageDialog(jfrm,"We expect "+dest+" to be a directory. Program will quit.");
			onExit();
		}
	}

	private void onExit() {
		System.exit(0);
	}

	private void displayGUI() {
		jfrm = new JFrame("Local Xena Data Import");
		JMenuBar menuBar = new JMenuBar();
		JMenu fileMenu = new JMenu("File");
		fileMenu.setMnemonic(KeyEvent.VK_F);

		JMenuItem menuItem =new JMenuItem("Close");
		menuItem.setAccelerator(KeyStroke.getKeyStroke( KeyEvent.VK_W, ActionEvent.META_MASK));
		menuItem.addActionListener(this);
		fileMenu.add(menuItem);

		menuBar.add(fileMenu);
		jfrm.setJMenuBar(menuBar);

		// Panel that literally holds everything
		JPanel panel = new JPanel();
		panel.setLayout(new FlowLayout(FlowLayout.LEFT));

		//select area
		JPanel selectPanel = new JPanel();
		selectPanel.setLayout(new BoxLayout(selectPanel, BoxLayout.PAGE_AXIS));
		// area to show the file being selected
		notifications = new JTextArea("");
		notifications.setBackground(panel.getBackground());
		notifications.setEditable(false);
		selectPanel.add(notifications);

		openXena = new JButton("View dataset");
		openXena.setActionCommand(openXena.getText());
		openXena.addActionListener(this);
		openXena.setVisible(false);
		selectPanel.add(openXena);

		// creates the button for uploading a file and aligns it
		buttonSelect = new JButton("Import Data");
		buttonSelect.setActionCommand(buttonSelect.getText());
		buttonSelect.addActionListener(this);
		selectPanel.add(buttonSelect);

		// format / type
		typePanel = new JPanel();
		typePanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		JTextArea label = new JTextArea("File format",1,11);
		label.setBackground(panel.getBackground());
		label.setEditable(false);
		typePanel.add(label);
		//format drop down list
		formatList = new JComboBox<String>(formatStrings);
		formatList.setBackground(Color.WHITE);
		formatList.addActionListener(this);

		typePanel.add(formatList);
		typePanel.setVisible(false);

		// assembly
		assemblyPanel = new JPanel();
		assemblyPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		label = new JTextArea("Assembly",1,11);
		label.setBackground(panel.getBackground());
		label.setEditable(false);
		assemblyPanel.add(label);

		//format drop down list
		assemblyList = new JComboBox<String>(assemblyStrings);
		assemblyList.setBackground(Color.WHITE);
		assemblyList.addActionListener(this);

		assemblyPanel.add(assemblyList);
		assemblyPanel.setVisible(false);

		//dataSubtype
		dataTypePanel = new JPanel();
		dataTypePanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		label = new JTextArea("Type of data",1,11);
		label.setBackground(panel.getBackground());
		label.setEditable(false);
		dataTypePanel.add(label);
		//data type drop down list

		dataTypeList = new JComboBox<String>(dataTypeStrings);
		dataTypeList.setMaximumRowCount (dataTypeStrings.length);
		dataTypeList.addActionListener(this);
		dataTypeList.setEditable(true);
		dataTypePanel.add(dataTypeList);
		dataTypePanel.setVisible(false);

		// cohort
		cohortPanel = new JPanel();
		cohortPanel.setLayout(new FlowLayout(FlowLayout.LEFT));

		label = new JTextArea("Cohort",1,11);
		label.setBackground(panel.getBackground());
		label.setEditable(false);
		cohortPanel.add(label);

		String[] cohortStrings = {hintString};
		cohortList = new JComboBox<String>(cohortStrings);
		cohortList.setMaximumRowCount (20);
		cohortList.setPrototypeDisplayValue("A cohort name should be about like this");
		cohortList.addActionListener(this);
		cohortList.setEditable(true);
		cohortPanel.add(cohortList);
		cohortPanel.setVisible(false);

		// data snippet
		dataSnippetPanel = new JPanel();
		dataSnippetPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		//label = new JTextArea("min");
		//label.setBackground(panel.getBackground());
		//label.setEditable(false);
		dataSnippet = new JTextArea ("",10,50);
		//colorPanel.add(label);
		dataSnippetPanel.add(dataSnippet);
		dataSnippetPanel.setVisible(false);


		// display label
		displayLabelPanel = new JPanel();
		displayLabelPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		label = new JTextArea("Display Name (optional)",1,11);
		label.setBackground(panel.getBackground());
		label.setLineWrap(true);
		label.setWrapStyleWord(true);
		label.setEditable(false);
		displayLabel = new JTextField ("",25);
		displayLabel.setEditable(true);
		displayLabelPanel.add(label);
		displayLabelPanel.add(displayLabel);
		displayLabelPanel.setVisible(false);

		// description
		descPanel = new JPanel();
		descPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		label = new JTextArea("Description (optional)",1,11);
		label.setBackground(panel.getBackground());
		label.setLineWrap(true);
		label.setWrapStyleWord(true);
		label.setEditable(false);
		description = new JTextArea ("", 3,35);
		description.setLineWrap(true);
		description.setWrapStyleWord(true);
		description.setEditable(true);
		scrollDescription= new JScrollPane(description);
		descPanel.add(label);
		descPanel.add(scrollDescription);
		descPanel.setVisible(false);

		// probeMap data
		mappingPanel = new JPanel();
		mappingPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		label = new JTextArea("ID/Gene mapping (optional)",1,11);
		label.setLineWrap(true);
		label.setWrapStyleWord(true);
		label.setBackground(panel.getBackground());
		label.setEditable(false);
		mappingPanel.add(label);
		// area for notification
		mappingNotifications = new JTextArea("");
		mappingNotifications.setBackground(panel.getBackground());
		mappingNotifications.setEditable(false);
		mappingPanel.add(mappingNotifications);
		// creates the button for uploading a file and aligns it
		mappingButton = new JButton("Find mapping file");
		mappingButton.setActionCommand(mappingButton.getText());
		mappingButton.addActionListener(this);
		mappingPanel.add(mappingButton);
		mappingPanel.setVisible(false);

		//submit button
		submitPanel = new JPanel();
		submitPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		submitButton = new JButton("Submit");
		submitButton.setActionCommand(submitButton.getText());
		submitButton.addActionListener(this);
		submitPanel.add(submitButton);
		//cancel button
		cancelButton = new JButton("Cancel");
		cancelButton.setActionCommand(cancelButton.getText());
		cancelButton.addActionListener(this);
		submitPanel.add(cancelButton);
		submitPanel.setPreferredSize(new Dimension(650,50));
		submitPanel.setVisible(false);

		panel.add(selectPanel);
		panel.add(typePanel);
		panel.add(dataTypePanel);
		panel.add(cohortPanel);
		panel.add(displayLabelPanel);
		panel.add(descPanel);
		panel.add(assemblyPanel);
		panel.add(mappingPanel);
		panel.add(dataSnippetPanel);
		panel.add(submitPanel);

		JTabbedPane tabbedPane = new javax.swing.JTabbedPane();

		tabbedPane.addTab("Data Import", panel);
		panel.setPreferredSize(new Dimension(670, 600));

		// sets the size of the application
		jfrm.setSize(720, 700);

		// close Application
		jfrm.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		jfrm.addWindowListener(new WindowAdapter () {
			public void windowClosing(WindowEvent evt) {
		  		onExit();
		  	}
		});

		// sets the layout of the application
		jfrm.setLayout(new FlowLayout(FlowLayout.CENTER));
		// centers the application on the screen
		jfrm.setLocationRelativeTo(null);

		jfrm.add(tabbedPane);
		// makes the application visible
		jfrm.setVisible(true);
	}


	private boolean detectXena() {
        return true;
	}

	private String getXenaPID() {
		return null;
	}

	private File selectFile()  {
		// opens the operating system's default file explorer
		FileDialog fileChooser = new FileDialog((java.awt.Frame) null,
				"Upload a File to the Cancer Browser", FileDialog.LOAD);
		fileChooser.setFilenameFilter(new DatafileFilter());
		fileChooser.setVisible(true);

		String name = fileChooser.getFile();
		String dir = fileChooser.getDirectory();

		File sourceFile = new File(dir+name);

		if (name==null) return null;
		return sourceFile;
	}

	private void createJSONdataset(String dest, File sourceFile, File probeMapFile) {
		// the cohort, type, and data subtype of the file
		String nameOfCohort = cohortList.getSelectedItem().toString();
		String nameOfLabel = displayLabel.getText();
		String nameOfDescription = description.getText();
		String nameOfFormat = formatList.getSelectedItem().toString();
		String nameOfDataSubType = dataTypeList.getSelectedItem().toString();
		String nameOfAssembly = assemblyList.getSelectedItem().toString();


		// rewrite with current date
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Date today = Calendar.getInstance().getTime();
		String reportDate = df.format(today);
		metadata.put("version", reportDate);


		nameOfDescription = nameOfDescription.replace(System.getProperty("line.separator"), " ");

		if (nameOfDataSubType == hintString) {
			nameOfDataSubType="";
		}

		if (nameOfCohort == hintString) {
			nameOfCohort = "";
		}

		if (!nameOfCohort.equals("")) metadata.put("cohort", nameOfCohort);
		if (!nameOfLabel.equals("")) metadata.put("label", nameOfLabel);
		if (!nameOfDescription.equals("")) metadata.put("description", nameOfDescription);
		if (!nameOfDataSubType.equals("")) metadata.put("dataSubType", nameOfDataSubType);
		if (!nameOfFormat.equals("")) {
			nameOfFormat = formats.get(nameOfFormat);
			metadata.put("type", nameOfFormat);
		}
		if ( nameOfFormat.equals("mutationVector") && (!nameOfAssembly.equals("Select"))) {
			metadata.put("assembly", nameOfAssembly);
		}

		if (!metadata.containsKey("colNormalization") &&
			Arrays.asList(colNormList).contains(nameOfDataSubType)) {
			metadata.put("colNormalization",true);
		}

		if (probeMapFile !=null) {
			metadata.put(":probeMap", probeMapFile.getName());
			try {
				JsonWriter writer = new JsonWriter(new FileWriter(dest + probeMapFile.getName() + ".json"));
				writer.beginObject();
				writer.name("type").value("probeMap");
				writer.endObject();
				writer.close();
			} catch (Exception e) {
				notifications.setText("An error occurred 1.");
				LOG.error("Error writing json", e);
			}
		}

		// metedata file in docroot
		if (writeJsonTofile(dest + sourceFile.getName() + ".json", metadata)!=0) {
			notifications.setText("An error occurred 2.");
		}
		// metedata file in original location
		if (writeJsonTofile(sourceFile.getPath() + ".json", metadata)!=0) {
			notifications.setText("An error occurred 2.");
		}
	}

	private int writeJsonTofile (String filepath, Map jsonData) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String stringVersion = gson.toJson(jsonData);

		try {
			FileWriter file = new FileWriter(filepath);
			file.write(stringVersion);
			file.flush();
			file.close();
			return 0;
		} catch (Exception e) {
			LOG.error("Error writing json", e);
			return 1;
		}
	}

	private void copyDataToXena (File sourceFile, File destFile) throws IOException{
		if (!sourceFile.getPath().equals(destFile.getPath())){
			InputStream input = null;
			OutputStream output = null;
			try {
				input = new FileInputStream(sourceFile);
				output = new FileOutputStream(destFile);
				byte[] buf = new byte[1024];
				int bytesRead;
				while ((bytesRead = input.read(buf)) > 0) {
					output.write(buf, 0, bytesRead);
				}
			} finally {
				input.close();
				output.close();
			}
		}
	}

	private void resetImport() {
		textArea(notifications,"",0);
		buttonSelect.setText("Import Data");
		disableImportSubPanels();
		return;
	}

	private void disableImportSubPanels(){
		importSubPanels(false);
	}

	private void importSubPanels(boolean status){
		typePanel.setVisible(status);
		assemblyPanel.setVisible(status);
		dataTypePanel.setVisible(status);
		cohortPanel.setVisible(status);
		displayLabelPanel.setVisible(status);
		descPanel.setVisible(status);
		mappingPanel.setVisible(status);
		dataSnippetPanel.setVisible(status);
		submitPanel.setVisible(status);
		return;
	}

	private void optionalPanels(){
		String nameOfFormat = formatList.getSelectedItem().toString();
		String nameOfDataSubType = dataTypeList.getSelectedItem().toString();
		//assembly
		if (nameOfFormat.equals(mutationFormat)) {
			assemblyPanel.setVisible(true);
		}
		else {
			assemblyPanel.setVisible(false);
		}

		//id gene mapping
		if (nameOfFormat.equals(mutationFormat)){ // mutationVector
			mappingPanel.setVisible(false);
		}
		else if (Arrays.asList(noProbeMapList).contains(nameOfDataSubType)){
			mappingPanel.setVisible(false);
		}
		else{
			mappingPanel.setVisible(true);
		}
	}

	private void textArea(JTextArea area, String text, int col){
		if (!text.equals("")){
			area.setRows(2);
			area.setColumns(col);
			area.setLineWrap(true);
			area.setWrapStyleWord(true);
		}
		else {
			area.setRows(0);
			area.setColumns(0);
			area.setLineWrap(false);
			area.setWrapStyleWord(false);
		}
		area.setText(text);
	}

	private void loadData (File destFile)  throws Exception{
		server.load(destFile);
	}

	private void submit(){
		createJSONdataset( dest, sourceFile, probeMapFile);

		File destFile = new File(dest+sourceFile.getName());

		try {
			copyDataToXena (sourceFile,destFile ) ;

			//hack force load -- should have checked the .json
			loadData(destFile);

			if (probeMapFile!=null) {
				destFile = new File(dest+probeMapFile.getName());
				copyDataToXena(probeMapFile,destFile);
				// force load
				loadData(destFile);
			}

			String url = server.publicUrl() + "proj/site/xena/datapages/?dataset="+sourceFile.getName() +
				"&host="+ server.localUrl();

			String text = "View data at: " + url + "\n";

			textArea(notifications, text,40);
			buttonSelect.setText("Import More Data");
			if (Desktop.isDesktopSupported()) {
			    xenaTarget = url;
			    openXena.setVisible(true);
			}
		}
		catch(Exception e){
			notifications.setText("An error occurred 3." + e.toString());
			LOG.error("Error loading file", e);
		}
	}

	public void deleteFile(File file) {
		if (file.exists()) {
			file.delete();
		}
	}

	public void callback(String[] cohorts) {
	    // Combine returned list + currently selected value,
	    // make unique & sort.
	    String selected = cohortList.getSelectedItem().toString();
	    ArrayList<String> list = new ArrayList<String>(Arrays.asList(cohorts));
	    if (selected != hintString) {
		list.add(selected);
	    }
	    ArrayList<String> unique = new ArrayList<String>(new HashSet<String>(list));
	    Collections.sort(unique, String.CASE_INSENSITIVE_ORDER);


	    cohortList.removeAllItems();
	    cohortList.addItem(hintString);
	    for (int i = 0; i < unique.size(); ++i) {
		cohortList.addItem(unique.get(i));
	    }
	    cohortList.setSelectedItem(selected);
	}

	public void actionPerformed (ActionEvent e) {
		if ("Close".equals(e.getActionCommand())) {
			onExit();
		}

		else if ("Cancel".equals(e.getActionCommand())) {
			resetImport();
		}

		else if ("View dataset".equals(e.getActionCommand())) {
			try {
			    Desktop.getDesktop().browse(new URI(xenaTarget));
			} catch (IOException ex) { /* TODO: error handling */ }
			catch (URISyntaxException ex) { /* TODO: error handling */ }
		}

		else if ("Import Data".equals(e.getActionCommand())) {
			openXena.setVisible(false);
			sourceFile= selectFile();
			if (sourceFile== null) {
				resetImport();
			}
			else {
				textArea(notifications,sourceFile.getPath(),40);
				buttonSelect.setText("Change");

				metadata = new HashMap<String, Object>();
				Type type = new TypeToken<Map<String, Object>>(){}.getType();
				formatList.setSelectedIndex(0);
				dataTypeList.setSelectedIndex(0);
				displayLabel.setText(sourceFile.getName());
				description.setText("");
				mappingNotifications.setText("");
				textArea(mappingNotifications,"",0);
				probeMapFile = null;

				// if there is existing .json use information in it as much as possible
				String jsonFile = sourceFile.getPath()+".json";
				File f = new File(jsonFile);

				if (f.isFile() && f.canRead()) {
					String name;
					String value;
					String key;

					try {
						FileInputStream fis = new FileInputStream(f);
	   		 			byte[] data = new byte[(int)f.length()];
						fis.read(data);
						fis.close();
						String content = new String(data, "UTF-8");

						Gson gson = new Gson();

						metadata = gson.fromJson(content, type);

						if (metadata.containsKey("type")){
							value = metadata.get("type").toString();
							if (formats.containsValue(value)) {
								key = getKeyByValue(formats, value);
								if (!key.equals(null)) {
									formatList.setSelectedItem(key);
								}
							}
							else {
								formatList.setSelectedIndex(0);
							}
							metadata.remove("type");
					  	}
					  	if (metadata.containsKey("dataSubType")){
					  		value = metadata.get("dataSubType").toString();
							if (Arrays.asList(dataTypeStrings).contains(value)) {
								dataTypeList.setSelectedItem(value);
							}
							else{
								dataTypeList.setSelectedIndex(0);
							}
							metadata.remove("dataSubType");
						}
		 				if (metadata.containsKey("cohort")){
							cohortList.setSelectedItem(metadata.get("cohort").toString());
							metadata.remove("cohort");
						}
						if (metadata.containsKey("label")){
							displayLabel.setText(metadata.get("label").toString());
							metadata.remove("label");
						}
						if (metadata.containsKey("description")){
							description.setText(metadata.get("description").toString());
							metadata.remove("description");
						}
						if (metadata.containsKey("assembly")){
							value = metadata.get("assembly").toString();
							if (Arrays.asList(assemblyStrings).contains(value)) {
								assemblyList.setSelectedItem(value);
							}
							else {
								assemblyList.setSelectedIndex(0);
							}
							metadata.remove("assembly");
						}
						if (metadata.containsKey(":probeMap")){
							value = metadata.get(":probeMap").toString();
							if (value.substring(0,1).equals("/")) {
								probeMapFile = new File(value);
							}
							else {
								probeMapFile = new File(f.getParent(),value);
							}

							if (probeMapFile.isFile() && probeMapFile.canRead()) {
								textArea(mappingNotifications,probeMapFile.getPath(),30);
								mappingButton.setText("Change");
						  	}
						  	else{
						  		probeMapFile=null;
						  	}
						  	metadata.remove(":probeMap");
						}
	 				}
	 				catch (Exception exp) {
	 				}
				}

				//data snippet
				try {
					FileInputStream fstream = new FileInputStream(sourceFile.getPath());
					BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
					StringBuilder sb = new StringBuilder();
    			String line = br.readLine();
    			int counter=0;

    			while ( (line != null) && (counter < 10)) {
        		counter = counter + 1;
        		sb.append(line);
        		sb.append(System.lineSeparator());
        		line = br.readLine();
    			}
    			if (counter == 10){
    				sb.append("...");
    			}
    			String everything = sb.toString();
    			dataSnippet.setText(everything);
    			br.close();
    		} catch (Exception exp){
    		}

				importSubPanels(true);
				optionalPanels();
				server.retrieveCohorts(this);
			}
		}
		else if ("Find mapping file".equals(e.getActionCommand())) {
			probeMapFile= selectFile();
			if (probeMapFile != null) {
				textArea(mappingNotifications,probeMapFile.getPath(),30);
				mappingButton.setText("Change");
			}
			else {
				textArea(mappingNotifications,"",30);
				mappingButton.setText("Find mapping file");
			}
		}
		else if ("comboBoxChanged".equals(e.getActionCommand()) || "comboBoxEdited".equals(e.getActionCommand())){
			optionalPanels();
		}
		else if ("Submit".equals(e.getActionCommand())) {
			resetImport();
			submit();
		}
		else{
			notifications.setText(e.getActionCommand());
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

	public XenaImport(XenaServer server_in) {
		server = server_in;
	}

	/**
	 * @param args
	 */
	public static XenaImport start(final XenaServer server) throws IOException {
		final XenaImport obj = new XenaImport(server);
		EventQueue.invokeLater(new Runnable() {
		    public void run() {
			obj.displayGUI();
			obj.setUp();
		    }
		});
		return obj;
	}
}
