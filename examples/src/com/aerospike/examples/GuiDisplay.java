/*
 * Aerospike client examples -- gui control
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
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
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
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
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;

import com.aerospike.client.AerospikeException;

public class GuiDisplay implements Runnable{
	private Parameters params;
	private Console console;

	static String sourcePath = "src/com/aerospike/examples/";
	// check box gui items
	private HashMap<String, JCheckBox> selections;
	JButton runButton, exitButton;
	private CheckBoxListener myCbListener = null;

	private JFrame frmAerospikeExamples;
	private JTextArea sourceTextPane;
	private String[] initExampleChoices;
	private JScrollPane scrollPane;
	private JPanel connectionPanel;
	private JLabel lblServerHost;
	private JTextField seedHostTextField;
	private JLabel lblPort;
	private JTextField portTextField;
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


	private static Thread reader;
	private static Thread reader2;
	private static boolean quit;

	private static PipedInputStream pin=new PipedInputStream();
	private static PipedInputStream pin2=new PipedInputStream();

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
						public synchronized void windowClosed(WindowEvent evt)
						{
							quit=true;
							this.notifyAll(); // stop all threads
							try { reader.join(1000);pin.close();   } catch (Exception e){}
							try { reader2.join(1000);pin2.close(); } catch (Exception e){}
							System.exit(0);
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
		namespaceTextField.setColumns(10);

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
		txtSetTextfield.setColumns(10);
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


		// set up checkboxs for all examples
		//

		myCbListener = new CheckBoxListener();
		selections = new HashMap<String, JCheckBox>();
		JCheckBox jcb = null;
		for (String example : Main.getAllExampleNames()) {
			jcb = new JCheckBox(example);     // register listeners for CBs
			jcb.addItemListener(myCbListener);
			selections.put(example, jcb);
			examplePanel.add(jcb);
		}


		// check the boxes based on user's initial choices
		for (String example : initExampleChoices) {
			jcb = selections.get(example);
			if (null != jcb) {
				jcb.setSelected(true);
			}
		}

		frmAerospikeExamples.pack();


		try
		{
			PipedOutputStream pout=new PipedOutputStream(pin);
			System.setOut(new PrintStream(pout,true));
		}
		catch (java.io.IOException io)
		{
			consoleTextArea.append("Couldn't redirect STDOUT to this console\n"+io.getMessage());
		}
		catch (SecurityException se)
		{
			consoleTextArea.append("Couldn't redirect STDOUT to this console\n"+se.getMessage());
		}

		try
		{
			PipedOutputStream pout2=new PipedOutputStream(pin2);
			System.setErr(new PrintStream(pout2,true));
		}
		catch (java.io.IOException io)
		{
			consoleTextArea.append("Couldn't redirect STDERR to this console\n"+io.getMessage());
		}
		catch (SecurityException se)
		{
			consoleTextArea.append("Couldn't redirect STDERR to this console\n"+se.getMessage());
		}

		quit=false; // signals the Threads that they should exit

		// Starting two separate threads to read from the PipedInputStreams
		//
		reader=new Thread(this);
		reader.setDaemon(true);
		reader.start();
		//
		reader2=new Thread(this);
		reader2.setDaemon(true);
		reader2.start();

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
			params.host = seedHostTextField.getText().trim();
			params.port = Integer.parseInt(portTextField.getText().trim());
			params.namespace = namespaceTextField.getText().trim();
			params.set = txtSetTextfield.getText().trim();
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

	public synchronized void run()
	{
		try
		{
			while (Thread.currentThread()==reader)
			{
				try { this.wait(100);}catch(InterruptedException ie) {}
				if (pin.available()!=0)
				{
					String input=this.readLine(pin);
					consoleTextArea.append(input);
				}
				if (quit) return;
			}

			while (Thread.currentThread()==reader2)
			{
				try { this.wait(100);}catch(InterruptedException ie) {}
				if (pin2.available()!=0)
				{
					String input=this.readLine(pin2);
					consoleTextArea.append(input);
				}
				if (quit) return;
			}
		} catch (Exception e)
		{
			consoleTextArea.append("\nConsole reports an Internal error.");
			consoleTextArea.append("The error is: "+e);
		}


	}

	public synchronized String readLine(PipedInputStream in) throws IOException
	{
		String input="";
		do
		{
			int available=in.available();
			if (available==0) break;
			byte b[]=new byte[available];
			in.read(b);
			input=input+new String(b,0,b.length);
		}while( !input.endsWith("\n") &&  !input.endsWith("\r\n") && !quit);
		return input;
	}
}
