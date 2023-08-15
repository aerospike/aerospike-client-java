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

import org.apache.commons.cli.CommandLine;

import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

/**
 * Configuration options for {@link BlobFinder}.
 */
public final class BlobFinderPolicy {
	/**
	 * Host string. Format: hostname1[:tlsname1][:port1],...
	 * <p>
	 * Hostname may also be an IP address in the following formats.
	 * <ul>
	 * <li>IPv4: xxx.xxx.xxx.xxx</li>
	 * <li>IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]</li>
	 * <li>IPv6: [xxxx::xxxx]</li>
	 * </ul>
	 * IPv6 addresses must be enclosed by brackets.
	 * tlsname and port are optional.
	 */
	public String host;

	/**
	 * Default port.
	 * <p>
	 * Default: 3000
	 */
	public int port = 3000;

	/**
	 * Username.
	 * <p>
	 * Default: null
	 */
	public String user;

	/**
	 * Password.
	 * <p>
	 * Default: null
	 */
	public String password;

	/**
	 * Authentication mode.
	 * <p>
	 * Default: AuthMode.INTERNAL
	 */
	public AuthMode authMode = AuthMode.INTERNAL;

	/**
	 * Policy for connections that require TLS.
	 * <p>
	 * Default: null (Use normal sockets)
	 */
	public TlsPolicy tlsPolicy;

	/**
	 * Optional file path. If not null, write all bins that contain language specific blobs.
	 * <p>
	 * Default: null
	 */
	public String outputFile;

	/**
	 * Display running blob totals after records returned counter reaches this
	 * value. Minimum value is 100000 (display blob totals after each group of
	 * 100000 records). Final blob totals are always displayed on completion.
	 * <p>
	 * Default: 100000
	 */
	public long displayAfterRecs = 100000;

	/**
	 * Records per second limit when running the BlobFinder scan.
	 * <p>
	 * Default: 0 (no limit)
	 */
	public int recordsPerSecond;

	/**
	 * Socket timeout in milliseconds when running the BlobFinder scan.
	 * <p>
	 * Default: 30000
	 */
	public int socketTimeout = 30000;

	/**
	 * Initialize policy with command-line arguments.
	 */
	public BlobFinderPolicy(CommandLine cl) {
		host = cl.getOptionValue("h", "127.0.0.1");

		String portString = cl.getOptionValue("p", "3000");
		port = Integer.parseInt(portString);

		user = cl.getOptionValue("U");
		String pass = cl.getOptionValue("P");

		if (user != null && pass == null) {
			java.io.Console console = System.console();

			if (console != null) {
				char[] passChars = console.readPassword("Enter password:");

				if (passChars != null) {
					pass = new String(passChars);
				}
			}
		}
		password = pass;

		if (cl.hasOption("auth")) {
			authMode = AuthMode.valueOf(cl.getOptionValue("auth", "").toUpperCase());
		}

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

		if (cl.hasOption("outputFile")) {
			outputFile = cl.getOptionValue("outputFile", "blobs.dat");
		}

		String str;

		if (cl.hasOption("displayAfterRecs")) {
			str = cl.getOptionValue("displayAfterRecs", "100000");
			long recs = Long.parseLong(str);
			displayAfterRecs = (recs < 100000) ? 100000 : recs;
		}

		if (cl.hasOption("recordsPerSecond")) {
			str = cl.getOptionValue("recordsPerSecond", "0");
			recordsPerSecond = Integer.parseInt(str);
		}

		if (cl.hasOption("socketTimeout")) {
			str = cl.getOptionValue("socketTimeout", "30000");
			socketTimeout = Integer.parseInt(str);
		}
	}

	/**
	 * Default constructor.
	 */
	public BlobFinderPolicy() {
	}
}
