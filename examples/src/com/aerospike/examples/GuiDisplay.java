/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.ButtonModel;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.util.Environment;

public class GuiDisplay implements ActionListener {
	private static String sourcePath = "src/com/aerospike/examples/";
	
	private Parameters params;
	private Console console;
	private ButtonGroup buttonGroup;
	private JButton runButton, exitButton;
	private JFrame frmAerospikeExamples;
	private JTextArea sourceTextPane;
	private JScrollPane scrollPane;
	private JPanel connectionPanel;
	private JLabel lblServerHost;
	private JTextField seedHostTextField;
	private JLabel lblPort;
	private JTextField portTextField;
	private JLabel lblusername;
	private JTextField usernameTextField;
	private JLabel lblpassword;
	private JPasswordField passwordTextField;
	private JLabel lblnameSpace;
	private JTextField namespaceTextField;
	private JLabel lblSet;
	private JTextField txtSetTextfield;
	private JSplitPane splitPane;
	private JScrollPane exampleScrollPane;
	private JPanel examplePanel;
	private JPanel mainPanel;
	private JScrollPane consoleScrollPane;
	private JTextArea consoleTextArea;

	/**
	 * Present a GUI
	 */
	public static void startGui(final Parameters params) throws AerospikeException {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GuiDisplay window = new GuiDisplay(params);
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
	public GuiDisplay(Parameters params) {
		this.params = params;
		this.console = new GuiConsole();
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmAerospikeExamples = new JFrame();
		frmAerospikeExamples.setTitle("Aerospike Java Client Examples");
		frmAerospikeExamples.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		frmAerospikeExamples.pack();
		frmAerospikeExamples.getContentPane().setLayout(new BorderLayout(0, 0));

		splitPane = new JSplitPane();
		splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
		frmAerospikeExamples.getContentPane().add(splitPane, BorderLayout.CENTER);

		mainPanel = new JPanel();
		splitPane.setLeftComponent(mainPanel);
		mainPanel.setLayout(new BorderLayout(0, 0));
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new FlowLayout(FlowLayout.LEFT));

		runButton = new JButton("Run");
		runButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent ev) {
				consoleTextArea.setText("");
				run_selected_examples();
			}
		});
		buttonPanel.add(runButton);
		mainPanel.add(buttonPanel, BorderLayout.SOUTH);

		exitButton = new JButton("Quit");
		exitButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent arg0) {
				Container Frame = exitButton.getParent();
				do {
					Frame = Frame.getParent();
				} while (!(Frame instanceof JFrame));
				((JFrame) Frame).dispose();
			}
		});
		buttonPanel.add(exitButton);
		sourceTextPane = new JTextArea();
		sourceTextPane.setTabSize(2);
		sourceTextPane.setEditable(false);

		scrollPane = new JScrollPane(sourceTextPane);
		scrollPane.setViewportBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));
		scrollPane.setPreferredSize(new Dimension(600,100));
		mainPanel.add(scrollPane, BorderLayout.CENTER);

		connectionPanel = new JPanel();
		connectionPanel.setLayout(new FlowLayout(FlowLayout.LEFT));

		lblServerHost = new JLabel("Server Host");
		connectionPanel.add(lblServerHost);


		seedHostTextField = new JTextField();
		seedHostTextField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(KeyEvent e) {
				params.host = seedHostTextField.getText();
			}
		});
		connectionPanel.add(seedHostTextField);
		seedHostTextField.setColumns(10);

		lblPort = new JLabel("Port");
		connectionPanel.add(lblPort);

		portTextField = new JTextField();
		portTextField.addFocusListener(new FocusAdapter() {
			@Override
			public void focusLost(FocusEvent arg0) {
				String newValue = namespaceTextField.getText();
				if (newValue != null && newValue != ""){
					try{
						params.port = Integer.parseInt(newValue);
					} catch (NumberFormatException ne) {
						//ne.printStackTrace();
					}
				}
			}
		});
		connectionPanel.add(portTextField);
		portTextField.setColumns(4);

		lblusername = new JLabel("User");
		connectionPanel.add(lblusername);

		usernameTextField = new JTextField();
		usernameTextField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(KeyEvent e) {
				params.user = usernameTextField.getText();
			}
		});
		connectionPanel.add(usernameTextField);
		usernameTextField.setColumns(8);

		lblpassword = new JLabel("Password");
		connectionPanel.add(lblpassword);

		passwordTextField = new JPasswordField();
		passwordTextField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(KeyEvent e) {
				params.user = new String(passwordTextField.getPassword());
			}
		});
		connectionPanel.add(passwordTextField);
		passwordTextField.setColumns(8);

		lblnameSpace = new JLabel("Namespace");
		connectionPanel.add(lblnameSpace);

		namespaceTextField = new JTextField();
		namespaceTextField.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(KeyEvent e) {
				params.namespace = namespaceTextField.getText();
			}
		});
		connectionPanel.add(namespaceTextField);
		namespaceTextField.setColumns(8);

		lblSet = new JLabel("Set");
		connectionPanel.add(lblSet);

		txtSetTextfield = new JTextField();
		txtSetTextfield.addKeyListener(new KeyAdapter() {
			@Override
			public void keyTyped(KeyEvent e) {
				params.set = txtSetTextfield.getText();
			}
		});
		connectionPanel.add(txtSetTextfield);
		txtSetTextfield.setColumns(8);
		mainPanel.add(connectionPanel, BorderLayout.NORTH);

		examplePanel = new JPanel();
		examplePanel.setLayout(new BoxLayout(examplePanel, BoxLayout.Y_AXIS));

		exampleScrollPane = new JScrollPane(examplePanel);
		mainPanel.add(exampleScrollPane, BorderLayout.WEST);

		// init values
		seedHostTextField.setText(params.host);
		portTextField.setText(Integer.toString(params.port));
		namespaceTextField.setText(params.namespace);
		txtSetTextfield.setText(params.set);

		//int width = 785;
		int width = 1000;
		int height = 220;
		consoleTextArea = new JTextArea();
		consoleTextArea.setSize(new Dimension(width, height));
		consoleTextArea.setEditable(false);
		consoleScrollPane = new JScrollPane(consoleTextArea);
		consoleScrollPane.setPreferredSize(new Dimension(width, height));
		consoleScrollPane.setSize(new Dimension(width, height));
		splitPane.setRightComponent(consoleScrollPane);

		buttonGroup = new ButtonGroup();
		JRadioButton jrb;		
		
		for (String example : Main.getAllExampleNames()) {
			jrb = new JRadioButton(example);
			jrb.setActionCommand(example);
			jrb.addActionListener(this);	
			buttonGroup.add(jrb);
			examplePanel.add(jrb);
		}
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

	public void actionPerformed(ActionEvent e) {
		String example = e.getActionCommand();
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
	}	

	/**
	 * Run the user selected examples
	 */
	private void run_selected_examples() {
		ButtonModel selected = buttonGroup.getSelection();
		
		if (selected == null) {
			console.error("Please select an example and then press Run");
			return;
		}
		
		try {
			String example = selected.getActionCommand();		
			final String[] examples = new String[1];
			examples[0] = example;

			params.host = seedHostTextField.getText().trim();
			params.port = Integer.parseInt(portTextField.getText().trim());
			params.user = usernameTextField.getText().trim();
			params.password = new String(passwordTextField.getPassword()).trim();
			params.namespace = namespaceTextField.getText().trim();
			params.set = txtSetTextfield.getText().trim();
			
			new Thread() {
				public void run() {
					try {
						Main.runExamples(console, params, examples);
					} catch (Exception ex) {
						console.error(ex.toString());
					}
				}
			}.start();
		}
		catch (Exception ex) {
			console.error(ex.toString());
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
	
	private class GuiConsole extends Console {
		@Override
		public void write(final String message) {
			consoleTextArea.append(message + Environment.Newline);
		}
	}
}
