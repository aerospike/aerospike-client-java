/*
 *  Citrusleaf client examples -- gui control
 *
 *  Copyright 2011 by Citrusleaf, Inc.  All rights reserved.
 *  
 *  Availability of this source code to partners and customers includes
 *  redistribution rights covered by individual contract. Please check
 *  your contract for exact rights and responsibilities.
 */

package com.aerospike.examples;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDesktopPane;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;

public class GuiDisplay extends JPanel {

	private static final long serialVersionUID = 1L;
	private AerospikeClient client = null;
	private Parameters params;
	private Console console;

	static String sourcePath = "src/com/aerospike/examples/";

	// content frames
	static JFrame frame;
	static JInternalFrame userselect_frame, source_frame;
	static JTextArea jtaSourcefile;
	static JScrollPane scrollPane;

	// check box gui items
	private HashMap<String, JCheckBox> selections;
	JButton runButton, exitButton;
	private CheckBoxListener myCbListener = null;
	private ButtonListener myBtListener = null;
	private static final String RUNTEST_LBL = "Run";
	private static final String EXIT_LBL = "Quit";

	private GuiDisplay(String[] initExampleChoices, Parameters params, Console console) throws AerospikeException {
		this.client = new AerospikeClient(params.host, params.port);
		this.params = params;
		this.console = console;

		//
		// set up checkboxs for all examples
		//
		JPanel jplCheckBox = new JPanel();
		jplCheckBox.setLayout(new GridLayout(0, 1));	//0 rows, 1 Column

		myCbListener = new CheckBoxListener();
		selections = new HashMap<String, JCheckBox>();
		for (String example : Main.getAllExampleNames()) {
			JCheckBox jcb = new JCheckBox(example);     // register listeners for CBs
			jcb.addItemListener(myCbListener);
			selections.put(example, jcb);
			jplCheckBox.add(jcb);
		}
		
		// check the boxes based on user's initial choices
		for (String example : initExampleChoices) {
			JCheckBox jcb = selections.get(example);
			if (null != jcb) {
				jcb.setSelected(true);
			}
		}

		setLayout(new BorderLayout());
		add(jplCheckBox, BorderLayout.WEST);

		//
		// set up buttons
		//
		myBtListener = new ButtonListener();
		runButton = new JButton(RUNTEST_LBL);
		exitButton = new JButton(EXIT_LBL);

		// Register listeners for buttons
		runButton.addActionListener(myBtListener);
		exitButton.addActionListener(myBtListener);

		// Put buttons as a tool bar on the bottom
		JPanel toolbar = new JPanel();
		toolbar.setLayout(new FlowLayout(FlowLayout.LEFT));
		toolbar.add(runButton);
		toolbar.add(exitButton);

		add(toolbar, BorderLayout.SOUTH);
		setBorder(BorderFactory.createEmptyBorder(20,20,20,20));
	}

	/**
	 * SourcePath Dialog to prompt user for alternate source code path
	 */
	private class SourcePathDialog extends JDialog {
		private static final long serialVersionUID = 1L;
		private JLabel lbSourcePath;
		private JTextField tfSourcePath;
		private JButton btnOK;

		public  SourcePathDialog (Frame parent) {
			super(parent, "Enter alternate source path", true);

			JPanel panel = new JPanel(new GridLayout(0, 1));
			lbSourcePath = new JLabel("Enter path of source code of the examples, or nothing to skip source code display: ");
			panel.add(lbSourcePath);
			tfSourcePath = new JTextField();
			panel.add(tfSourcePath);
			// panel.setBorder(new LineBorder(Color.GRAY));

			
			btnOK = new JButton("OK");

			btnOK.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					// retrieve the new source path from user's input
					GuiDisplay.sourcePath = tfSourcePath.getText().trim();
					dispose();
				}
			});
			JPanel bp = new JPanel();
			bp.add(btnOK);

			getContentPane().add(panel, BorderLayout.CENTER);
			getContentPane().add(bp, BorderLayout.PAGE_END);

			pack();
			setResizable(false);
			setLocationRelativeTo(parent);
		}
	}

	/**
	 * Checkbox listener is needed to update the source code box
	 */
	private class CheckBoxListener implements ItemListener {
		public void itemStateChanged(ItemEvent e) {
			if (0 == sourcePath.length()) {
				// user indicated to skip source browsing
				return;
			}
				
			if (e.getStateChange() == ItemEvent.SELECTED) {
				Object cbSource = e.getItemSelectable();
				for (String example : Main.getAllExampleNames()) {
					// find the checked item and push out the source code
					if (cbSource == selections.get(example)) {
						String sourceText = readfile(sourcePath + example + ".java");
						if (0 == sourceText.length()) {
							// did not get source code content, ask the user for location and give it one more try
							SourcePathDialog spDialog = new SourcePathDialog(frame);
							spDialog.setVisible(true);
							if (0 < sourcePath.length()) {
								if (sourcePath.charAt(sourcePath.length()-1) != '/') 
									sourcePath += "/";
								sourceText =  readfile(sourcePath + example + ".java");
								if (0 == sourceText.length()) {
									sourceText = "Failed to read source file: " + sourcePath + example + ".java";
								}
							}
						}
						if (0 == sourceText.length()) {
							// user no longer want to see skip source window from this point on?
							if (0 == sourcePath.length()) {
								source_frame.dispose();
							}
						}
						else {
							source_frame.setTitle(example + " example source");
							jtaSourcefile.setText(sourceText);
						}
						
						JScrollBar verticalScrollBar = scrollPane.getVerticalScrollBar();
						JScrollBar horizontalScrollBar = scrollPane.getHorizontalScrollBar();
						verticalScrollBar.setValue(verticalScrollBar.getMinimum());
						horizontalScrollBar.setValue(horizontalScrollBar.getMinimum());

						break;
					}
				}
			}
		}
	}

	/**
	 * Listener to listen to button events
	 */
	private class ButtonListener implements ActionListener {
		public void actionPerformed(ActionEvent ae) {
			if (RUNTEST_LBL.equals(ae.getActionCommand())) {
				run_selected_examples();
			}
			else if  (EXIT_LBL.equals(ae.getActionCommand())) {
				console.write("Goodbye!");
				Container Frame = exitButton.getParent();
				do {
					Frame = Frame.getParent();
				} while (!(Frame instanceof JFrame));
				((JFrame) Frame).dispose();
				if (null != client) {
					client.close();
				}
			}
		}
	}

	/**
	 * Run the user selected examples
	 */
	private void run_selected_examples() {
		for (String name : Main.getAllExampleNames()) {
			if (this.selections.get(name).isSelected()) {
				try {
					Main.runExample(name, this.client, this.params, this.console);
					console.write("-------------" + name + " example ended -------------");
				}
				catch (Exception ex) {
					console.write("Exception (" + ex.toString() + ") encountered when running " + name);
				}
			}
		}
	}

	/**
	 * Present a GUI with check boxes
	 */
	public static void startGui(String[] examples, Parameters params, Console console) throws AerospikeException {
		JDesktopPane desk;

		frame =  new JFrame("Citrusleaf JAVA client examples");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		desk = new JDesktopPane();

		source_frame = new JInternalFrame("Source file area", true, false, false, true);
		source_frame.setBounds(200, 0, 950, 400);
		source_frame.setVisible(true);

		jtaSourcefile = new JTextArea(200,200);
		jtaSourcefile.setEditable(false);
		scrollPane = new JScrollPane(jtaSourcefile, 
					     JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
					     JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
		source_frame.setContentPane(scrollPane);

		// source_frame and jtaSourcefile must be created before the GuiDisplay object is created.
		// If the users has inital selections it will cause the checkbox 
		// listener to fire and tries to set the source_frame content
		userselect_frame = new JInternalFrame("Select examples", false, false, false, false);
		userselect_frame.setBounds(0, 0, 200, 400);
		userselect_frame.setVisible(true);
		userselect_frame.setContentPane(new GuiDisplay(examples, params, console));
		userselect_frame.pack();

		desk.add(userselect_frame);
		desk.add(source_frame);
		frame.add(desk);

		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				return;
			}
		});

		frame.setSize(1200,500);
		frame.setVisible(true);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}

	/**
	 * Utility to read in a source file	
	 */
	private static String readfile(String fn) {
		File aFile;

		try {
			aFile = new File(fn);
		}
		catch (NullPointerException e) {
			return("null file name");
		}

		StringBuilder contents = new StringBuilder();
		try {
			BufferedReader input =  new BufferedReader(new FileReader(aFile));
			try {
				String line = null; 
				while (( line = input.readLine()) != null){
					contents.append(line);
					contents.append(System.getProperty("line.separator"));
				}
			}
			finally {
				input.close();
			}
		}
		catch (java.io.FileNotFoundException fnfe) {
			return("");
		}
		catch (IOException ex){
			return("File " + fn + " cannot be read. \nReason = " + ex.toString());
		}
		return contents.toString();
	}
}
