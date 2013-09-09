/*
 * Aerospike client examples -- gui control
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.examples;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;

import com.aerospike.client.AerospikeException;

public class GuiDisplay {
	private Parameters params;
	private Console console;

	static String sourcePath = "src/com/aerospike/examples/";
	
	// check box gui items
	private HashMap<String, JCheckBox> selections;
	JButton runButton, exitButton;
	private CheckBoxListener myCbListener = null;

	private JFrame frmAerospikeExamples;
	private JTextArea sourceTextPane;
	private JPanel exampleSelection;
	private String[] initExampleChoices;
	private JScrollPane scrollPane;

	/**
	 * Present a GUI with check boxes
	 */
	public static void startGui(final String[] examples, final Parameters params, final Console console) throws AerospikeException {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GuiDisplay window = new GuiDisplay(examples, params, console);


					window.frmAerospikeExamples.addWindowListener(new WindowAdapter() {
						public void windowClosing(WindowEvent e) {
							return;
						}
					});

					window.frmAerospikeExamples.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public GuiDisplay(String[] initExampleChoices, Parameters params, Console console) {
		this.params = params;
		this.console = console;
		this.initExampleChoices = initExampleChoices;
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmAerospikeExamples = new JFrame();
		frmAerospikeExamples.setTitle("Aerospike Examples");
		frmAerospikeExamples.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frmAerospikeExamples.getContentPane().setLayout(new BorderLayout(0, 0));
		
		exampleSelection = new JPanel();
		exampleSelection.setLayout(new BoxLayout(exampleSelection, BoxLayout.Y_AXIS));
		frmAerospikeExamples.getContentPane().add(exampleSelection, BorderLayout.WEST);
		
		JPanel buttonPanel = new JPanel();
		frmAerospikeExamples.getContentPane().add(buttonPanel, BorderLayout.SOUTH);
		buttonPanel.setLayout(new FlowLayout(FlowLayout.LEFT));
		
		runButton = new JButton("Run");
		runButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent ev) {
				run_selected_examples();
			}
		});
		buttonPanel.add(runButton);
		
		exitButton = new JButton("Quit");
		exitButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent arg0) {
				console.write("Goodbye!");
				Container Frame = exitButton.getParent();
				do {
					Frame = Frame.getParent();
				} while (!(Frame instanceof JFrame));
				((JFrame) Frame).dispose();
			}
		});
		buttonPanel.add(exitButton);
		
		sourceTextPane = new JTextArea();
		sourceTextPane.setTabSize(5);
		sourceTextPane.setEditable(false);

		scrollPane = new JScrollPane(sourceTextPane);
		scrollPane.setViewportBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));
		scrollPane.setPreferredSize(new Dimension(600,100));
		frmAerospikeExamples.getContentPane().add(scrollPane, BorderLayout.CENTER);
		
		// set up checkboxs for all examples
		//

		myCbListener = new CheckBoxListener();
		selections = new HashMap<String, JCheckBox>();
		JCheckBox jcb = null;
		for (String example : Main.getAllExampleNames()) {
			jcb = new JCheckBox(example);     // register listeners for CBs
			jcb.addItemListener(myCbListener);
			selections.put(example, jcb);
			exampleSelection.add(jcb);
		}
		
		
		// check the boxes based on user's initial choices
		for (String example : initExampleChoices) {
			jcb = selections.get(example);
			if (null != jcb) {
				jcb.setSelected(true);
			}
		}
		exampleSelection.setSize(exampleSelection.getPreferredSize());
		frmAerospikeExamples.pack();
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
				@Override
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
							SourcePathDialog spDialog = new SourcePathDialog(frmAerospikeExamples);
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
								sourceTextPane.setText("");
							}
						}
						else {
							sourceTextPane.setText(sourceText);
							sourceTextPane.setSize(sourceTextPane.getPreferredSize());
							sourceTextPane.setCaretPosition(0);
							sourceTextPane.revalidate();
						}
						

						break;
					}
				}
			}
		}
	}

	/**
	 * Run the user selected examples
	 */
	private void run_selected_examples() {
		ArrayList<String> list = new ArrayList<String>(32);
		
		for (String name : Main.getAllExampleNames()) {
			if (this.selections.get(name).isSelected()) {
				list.add(name);
			}
		}
		String[] examples = list.toArray(new String[list.size()]);
		
		try {
			Main.runExamples(this.console, this.params, examples);
		}
		catch (Exception ex) {
			console.write("Exception (" + ex.toString() + ") encountered.");
		}
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
