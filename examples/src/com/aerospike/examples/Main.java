/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.examples;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import javax.swing.JPanel;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;

public class Main extends JPanel {

	private static final long serialVersionUID = 1L;
	private static final String[] ExampleNames = new String[] {
		"ServerInfo",
		"PutGet",
		"Replace",
		"Add",
		"Append",
		"Prepend",
		"Batch",
		"Generation",
		"Serialize",
		"Expire",
		"Touch",
		"Operate",
		"DeleteBin",
		"ScanParallel",
		"ScanSeries",
		"AsyncPutGet",
		"AsyncBatch",
		"AsyncScan",
		"ListMap",
		"UserDefinedFunction",
		"LargeSet",
		"LargeStack",
		"QueryInteger",
		"QueryString",
		"QueryFilter",
		"QuerySum",
		"QueryAverage",
		"QueryExecute"
	};
	public static String[] getAllExampleNames() { return ExampleNames; }

	/**
	 * Main entry point.
	 */
	public static void main(String[] args) {

		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
			options.addOption("g", "gui", false, "Invoke GUI to selectively run tests.");
			options.addOption("d", "debug", false, "Run in debug mode.");
			options.addOption("u", "usage", false, "Print usage.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);

			if (args.length == 0 || cl.hasOption("u")) {
				logUsage(options);
				return;
			}
			Parameters params = parseParameters(cl);
			String[] exampleNames = cl.getArgs();
			
			if ((exampleNames.length == 0) && (!cl.hasOption("g"))) {
				logUsage(options);
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
				GuiDisplay.startGui(params);
			}
			else {
				Console console = new Console();
				runExamples(console, params, exampleNames);				
			}
		}
		catch (Exception ex) {
			System.out.println(ex.getMessage());
			ex.printStackTrace();
		}
	}

	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = Main.class.getName() + " [<options>] all|(<example1> <example2> ...)";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
		System.out.println("examples:");

		for (String name : ExampleNames) {
			System.out.println(name.toString());			
		}
		System.out.println();
		System.out.println("All examples will be run if 'all' is specified as an example.");
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
		return new Parameters(host, port, namespace, set);
	}
	
	/**
	 * Connect and run one or more client examples.
	 */
	public static void runExamples(Console console, Parameters params, String[] examples) throws Exception {
		ArrayList<String> syncExamples = new ArrayList<String>();
		ArrayList<String> asyncExamples = new ArrayList<String>();
		
		for (String example : examples) {
			if (example.startsWith("Async")) {
				asyncExamples.add(example);
			}
			else {
				syncExamples.add(example);
			}
		}
		
		params.setServerSpecific();

		if (syncExamples.size() > 0) {
			Example.runExamples(console, params, syncExamples);
		}
		
		if (asyncExamples.size() > 0) {
			AsyncExample.runExamples(console, params, asyncExamples);
		}
	}
}
