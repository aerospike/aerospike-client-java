/*
 * Copyright 2012-2016 Aerospike, Inc.
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
package com.aerospike.test.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.cluster.Node;

public class Args {
	public static Args Instance = new Args();
	
	public String host;
	public int port;
	public String user;
	public String password;
	public String namespace;
	public String set;
	public boolean hasUdf;
	public boolean hasMap;
	public boolean singleBin;
	public boolean hasLargeDataTypes;
	
	public Args() {
		host = "127.0.0.1";
		port = 3000;
		namespace = "test";
		set = "test";

		String argString = System.getProperty("args");
		
		if (argString == null) {
			return;
		}
	
		try {
			String[] args = argString.split(" ");
			
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: localhost)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("U", "user", true, "User name");
			options.addOption("P", "password", true, "Password");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
			options.addOption("d", "debug", false, "Run in debug mode.");
			options.addOption("u", "usage", false, "Print usage.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);

			if (cl.hasOption("u")) {
				logUsage(options);
				throw new AerospikeException("Terminate after displaying usage");
			}
			
			host = cl.getOptionValue("h", host);
			String portString = cl.getOptionValue("p", "3000");
			port = Integer.parseInt(portString);
			namespace = cl.getOptionValue("n", namespace);
			set = cl.getOptionValue("s", "test");

			if (set.equals("empty")) {
				set = "";
			}
			
			user = cl.getOptionValue("U");
			password = cl.getOptionValue("P");
			
			if (user != null && password == null) {
				java.io.Console console = System.console();
				
				if (console != null) {
					char[] pass = console.readPassword("Enter password:");
					
					if (pass != null) {
						password = new String(pass);
					}
				}
			}
						
			if (cl.hasOption("d")) {				
				Log.setLevel(Level.DEBUG);
			}
		}
		catch (Exception ex) {
			throw new AerospikeException("Failed to parse args: " + argString);
		}
	}
	
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = "mvn test [-Dargs='<options>']";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}
	
	/**
	 * Some database calls need to know how the server is configured.
	 */
	public void setServerSpecific(AerospikeClient client) {
		Node node = client.getNodes()[0];
		String featuresFilter = "features";
		String namespaceFilter = "namespace/" + namespace;
		Map<String,String> tokens = Info.request(null, node, featuresFilter, namespaceFilter);

		String features = tokens.get(featuresFilter);
		hasUdf = false;
		hasMap = false;
		
		if (features != null) {
			String[] list = features.split(";");
			
			for (String s : list) {
				if (s.equals("udf")) {
					hasUdf = true;
					break;
				}
				else if (s.equals("cdt-map")) {
					hasMap = true;
					break;
				}
			}
		}
		
		String namespaceTokens = tokens.get(namespaceFilter);
		
		if (namespaceTokens == null) {
			throw new AerospikeException(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				host, port, namespace));
		}

		singleBin = parseBoolean(namespaceTokens, "single-bin");
		hasLargeDataTypes = parseBoolean(namespaceTokens, "ldt-enabled");
	}
	
	private static boolean parseBoolean(String namespaceTokens, String name) {
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			return false;
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}

		String value = namespaceTokens.substring(begin, end);
		return Boolean.parseBoolean(value);
	}
	
	public String getBinName(String name) {
		// Single bin servers don't need a bin name.
		return singleBin ? "" : name;
	}

	public boolean validateLDT() {
		if (! hasLargeDataTypes) {
			System.out.println("Skip test because LDT not enabled on server");
			return false;
		}
		return true;
	}
	
	public boolean validateMap() {
		if (! hasMap) {
			System.out.println("Skip test because cdt-map not enabled on server");
			return false;
		}
		return true;
	}
}
