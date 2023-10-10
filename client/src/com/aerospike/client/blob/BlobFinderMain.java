/*
 * Copyright 2012-2023 Aerospike, Inc.
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
package com.aerospike.client.blob;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.policy.AuthMode;

/**
 * Command line interface for {@link BlobFinder}.
 */
public final class BlobFinderMain {
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

			options.addOption("u", "usage", false, "Print usage.");

			options.addOption("o", "outputFile", true,
					"Optional output file path. If specified, write all bins that contain language\n" +
					"specific blobs to this file."
					);

			options.addOption("dar", "displayAfterRecs", true,
					"Display running blob totals after records returned counter reaches this value.\n" +
					"Minimum: 100000\n" +
					"Default: 100000"
					);

			options.addOption("rps", "recordsPerSecond", true,
					"Records per second limit when running the BlobFinder scan.\n" +
					"Default: 0 (no limit)"
					);

			options.addOption("st", "socketTimeout", true,
					"Socket timeout when running the BlobFinder scan.\n" +
					"Default: 30000"
					);

			options.addOption("n", "namespace", true,
					"Run on a specific namespace. If not specified, all namespaces will be examined."
					);

			CommandLineParser parser = new DefaultParser();
			CommandLine cl = parser.parse(options, args, false);

			if (args.length == 0 || cl.hasOption("u")) {
				logUsage(options);
				return;
			}

			BlobFinderPolicy bfp = new BlobFinderPolicy(cl);
			BlobFinder.run(bfp);
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
	}

	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = BlobFinderMain.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		System.out.println(sw.toString());
	}
}
