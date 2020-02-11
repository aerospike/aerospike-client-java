/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;

import javax.swing.JPanel;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

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
		"StoreKey",
		"DeleteBin",
		"ListMap",
		"Operate",
		"OperateList",
		"ScanParallel",
		"ScanSeries",
		"UserDefinedFunction",
		"QueryInteger",
		"QueryString",
		"QueryFilter",
		"QuerySum",
		"QueryAverage",
		"QueryCollection",
		"QueryPredExp",
		"QueryRegion",
		"QueryRegionFilter",
		"QueryGeoCollection",
		"QueryExecute",
		"AsyncPutGet",
		"AsyncBatch",
		"AsyncQuery",
		"AsyncScan",
		"AsyncUserDefinedFunction"
	};
	public static String[] getAllExampleNames() { return ExampleNames; }

	/**
	 * Main entry point.
	 */
	public static void main(String[] args) {

		try {
			Options options = new Options();
			options.addOption("h", "host", true,
					"List of seed hosts in format:\n" +
					"hostname1[:tlsname][:port1],...\n" +
					"The tlsname is only used when connecting with a secure TLS enabled server. " +
					"If the port is not specified, the default port is used.\n" +
					"IPv6 addresses must be enclosed in square brackets.\n" +
					"Default: localhost\n" +
					"Examples:\n" +
					"host1\n" +
					"host1:3000,host2:3000\n" +
					"192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n"
					);
			options.addOption("p", "port", true, "Server default port (default: 3000)");
			options.addOption("U", "user", true, "User name");
			options.addOption("P", "password", true, "Password");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
			options.addOption("tls", "tlsEnable", false, "Use TLS/SSL sockets");
			options.addOption("tp", "tlsProtocols", true,
					"Allow TLS protocols\n" +
					"Values:  TLSv1,TLSv1.1,TLSv1.2 separated by comma\n" +
					"Default: TLSv1.2"
					);
			options.addOption("tlsCiphers", "tlsCipherSuite", true,
					"Allow TLS cipher suites\n" +
					"Values:  cipher names defined by JVM separated by comma\n" +
					"Default: null (default cipher list provided by JVM)"
					);
			options.addOption("tr", "tlsRevoke", true,
					"Revoke certificates identified by their serial number\n" +
					"Values:  serial numbers separated by comma\n" +
					"Default: null (Do not revoke certificates)"
					);
			options.addOption("tlsLoginOnly", false, "Use TLS/SSL sockets on node login only");
			options.addOption("auth", true, "Authentication mode. Values: " + Arrays.toString(AuthMode.values()));
			options.addOption("netty", false, "Use Netty NIO event loops for async examples");
			options.addOption("nettyEpoll", false, "Use Netty epoll event loops for async examples (Linux only)");
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

			if (cl.hasOption("netty")) {
				params.eventLoopType = EventLoopType.NETTY_NIO;
			}

			if (cl.hasOption("nettyEpoll")) {
				params.eventLoopType = EventLoopType.NETTY_EPOLL;
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

		String user = cl.getOptionValue("U");
		String password = cl.getOptionValue("P");

		if (user != null && password == null) {
			java.io.Console console = System.console();

			if (console != null) {
				char[] pass = console.readPassword("Enter password:");

				if (pass != null) {
					password = new String(pass);
				}
			}
		}

		TlsPolicy tlsPolicy = null;

		if (cl.hasOption("tls")) {
			tlsPolicy = new TlsPolicy();

			if (cl.hasOption("tp")) {
				String s = cl.getOptionValue("tp", "");
				tlsPolicy.protocols = s.split(",");
			}

			if (cl.hasOption("tlsCiphers")) {
				String s = cl.getOptionValue("tlsCiphers", "");
				tlsPolicy.ciphers = s.split(",");
			}

			if (cl.hasOption("tr")) {
				String s = cl.getOptionValue("tr", "");
				tlsPolicy.revokeCertificates = Util.toBigIntegerArray(s);
			}

			if (cl.hasOption("tlsLoginOnly")) {
				tlsPolicy.forLoginOnly = true;
			}
		}

		AuthMode authMode = AuthMode.INTERNAL;

		if (cl.hasOption("auth")) {
			authMode = AuthMode.valueOf(cl.getOptionValue("auth", "").toUpperCase());
		}

		return new Parameters(tlsPolicy, host, port, user, password, authMode, namespace, set);
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

		if (syncExamples.size() > 0) {
			Example.runExamples(console, params, syncExamples);
		}

		if (asyncExamples.size() > 0) {
			AsyncExample.runExamples(console, params, asyncExamples);
		}
	}
}
