/*
 *  Citrusleaf client examples
 *
 *  Copyright 2011 by Citrusleaf, Inc.  All rights reserved.
 *  
 *  Availability of this source code to partners and customers includes
 *  redistribution rights covered by individual contract. Please check
 *  your contract for exact rights and responsibilities.
 */

/*
 * Use notes. This stand-alone application provides a number of functional
 * end user tests. Simply compile this file with the Citrusleaf library
 * and enjoy the fine example-ish goodness. 
 * 
 * It does rely on the Apache Commons CLI (Command Line Interpreter)
 * library available from http://commons.apache.org/cli/ . This library has been stable
 * in its 1.2 version since 2002.
 */

package com.aerospike.examples;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;

import javax.swing.JPanel;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;

public class Main extends JPanel {

	private static final long serialVersionUID = 1L;
	private static final String[] ExampleNames = new String[] {
		"ServerInfo",
		"PutGet",
		"Add",
		"Append",
		"Prepend",
		"Batch",
		"Generation",
		"Serialize",
		"Expire",
		"Touch",
		"DeleteBin",
		"ScanParallel",
		"ScanSeries",
		"UserDefinedFunction"
	};
	public static String[] getAllExampleNames() { return ExampleNames; }

	/**
	 * Main entry point.
	 */
	public static void main(String[] args) {

		Console console = new Console();

		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
			options.addOption("g", "gui", false, "Invoke GUI to selectively run tests.");
			options.addOption("d", "debug", false, "Run in debug mode.");
			options.addOption("u", "usage", false, "Print usage.");

			CommandLineParser clp = new GnuParser();
			CommandLine cl = clp.parse(options, args, false);

			if (args.length == 0 || cl.hasOption("u")) {
				logUsage(console, options);
				return;
			}
			Parameters params = parseParameters(cl);
			String[] exampleNames = cl.getArgs();
			
			if ((exampleNames.length == 0) && (!cl.hasOption("g"))) {
				logUsage(console, options);
				return;			
			}
			
			// Check for all.
			for (String exampleName : exampleNames) {
				if (exampleName.equalsIgnoreCase("all")) {
					exampleNames = ExampleNames;
					break;
				}
			}
			
			if (cl.hasOption("d")) {				
				Log.setLevel(Level.DEBUG);
			}

			if (cl.hasOption("g")) {
				GuiDisplay.startGui(exampleNames, params, console);
			}
			else {
				runExamplesCommandLine(console, params, exampleNames);				
			}
		}
		catch (Exception ex) {
			console.error(ex.getMessage());
			ex.printStackTrace();
		}
	}

	/**
	 * Write usage to console.
	 */
	private static void logUsage(Console console, Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = Main.class.getName() + " [<options>] all|(<example1> <example2> ...)";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		console.write(sw.toString());
		console.write("examples:");

		for (String name : ExampleNames) {
			console.write(name.toString());			
		}
		console.write("");
		console.write("All examples will be run if 'all' is specified as an example.");
	}

	/**
	 * Parse command line parameters.
	 */
	private static Parameters parseParameters(CommandLine cl) throws Exception {
		String host = cl.getOptionValue("h", "127.0.0.1");
		String portString = cl.getOptionValue("p", "3000");
		int port = Integer.parseInt(portString);
		String namespace = cl.getOptionValue("n","test");
		String set = cl.getOptionValue("s", "demoset");

		if (set.equals("empty")) {
			set = "";
		}
		Parameters params = new Parameters(host, port, namespace, set);
		params.setServerSpecific();
		return params;
	}
	
	/**
	 * Connect and run one or more client examples.
	 */
	private static void runExamplesCommandLine(Console console, Parameters params, String[] examples) throws Exception {
		AerospikeClient client = new AerospikeClient(params.host, params.port);

		try {
			for (String exampleName : examples) {
				runExample(exampleName, client, params, console);
			}
		}
		finally {
			client.close();
		}
	}

	/**
	 * Run client example.
	 */
	public static void runExample(String exampleName, AerospikeClient client, Parameters params, Console console) throws Exception {
		String fullName = "com.aerospike.examples." + exampleName;
		Class<?> cls = Class.forName(fullName);

		if (Example.class.isAssignableFrom(cls)) {
			Constructor<?> ctor = cls.getDeclaredConstructor(Console.class);
			Example example = (Example)ctor.newInstance(console);
			example.run(client, params);
		}
		else {
			console.error("Invalid example: " + exampleName);
		}
	}
}
