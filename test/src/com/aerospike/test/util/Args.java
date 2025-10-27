/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.Log.Level;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;
import com.aerospike.client.util.Version;

public class Args {
	public static Args Instance = new Args();

	public String host;
	public int port;
	public AuthMode authMode = AuthMode.INTERNAL;
	public String user;
	public String password;
	public String namespace;
	public String set;
	public final Policy indexPolicy;
	public TlsPolicy tlsPolicy;
	public EventLoopType eventLoopType = EventLoopType.DIRECT_NIO;
	public int socketTimeout = 30000;
	public int totalTimeout = 1000;
	public boolean enterprise;
	public boolean hasTtl;
	public boolean scMode;
	public Version serverVersion;

	public Args() {
		host = "127.0.0.1";
		port = 3000;
		namespace = "test";
		set = "test";

		indexPolicy = new Policy();
		indexPolicy.setTimeout(10000);

		String argString = System.getProperty("args");

		if (argString == null || argString.trim().length() == 0) {
			return;
		}

		try {
			String[] args = argString.split(" ");

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

			options.addOption("netty", false, "Use Netty NIO event loops for async tests");
			options.addOption("nettyEpoll", false, "Use Netty epoll event loops for async tests (Linux only)");
			options.addOption("elt", "eventLoopType", true,
					"Use specified event loop type for async examples\n" +
					"Value: DIRECT_NIO | NETTY_NIO | NETTY_EPOLL | NETTY_KQUEUE | NETTY_IOURING"
					);

			options.addOption("st", "socketTimeout", true,
					"Set read and write socketTimeout in milliseconds\n" +
					"for single record and batch commands."
					);
			options.addOption("tt", "totalTimeout", true,
					"Set read and write totalTimeout in milliseconds\n" +
					"for single record and batch commands."
					);

			options.addOption("d", "debug", false, "Run in debug mode.");
			options.addOption("u", "usage", false, "Print usage.");

			CommandLineParser parser = new DefaultParser();
			CommandLine cl = parser.parse(options, args, false);

			if (cl.hasOption("u")) {
				logUsage(options);
				throw new AerospikeException("Terminate after displaying usage");
			}

			host = cl.getOptionValue("h", host);
			String portString = cl.getOptionValue("p", "3000");
			port = Integer.parseInt(portString);
			namespace = cl.getOptionValue("n", namespace);
			set = cl.getOptionValue("s", set);

			if (set.equals("empty")) {
				set = "";
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

			if (cl.hasOption("auth")) {
				authMode = AuthMode.valueOf(cl.getOptionValue("auth", "").toUpperCase());
			}

			if (cl.hasOption("netty")) {
				eventLoopType = EventLoopType.NETTY_NIO;
			}

			if (cl.hasOption("nettyEpoll")) {
				eventLoopType = EventLoopType.NETTY_EPOLL;
			}

			if (cl.hasOption("eventLoopType")) {
				eventLoopType = EventLoopType.valueOf(cl.getOptionValue("eventLoopType", "").toUpperCase());
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

			if (cl.hasOption("socketTimeout")) {
				socketTimeout = Integer.parseInt(cl.getOptionValue("socketTimeout"));;
			}

			if (cl.hasOption("totalTimeout")) {
				totalTimeout = Integer.parseInt(cl.getOptionValue("totalTimeout"));;
			}

			if (cl.hasOption("d")) {
				Log.setLevel(Level.DEBUG);
			}
		}
		catch (Exception ex) {
			throw new AerospikeException("Failed to parse args: " + argString);
		}
	}

	public void setClientPolicy(ClientPolicy p) {
		p.user = user;
		p.password = password;
		p.authMode = authMode;
		p.tlsPolicy = tlsPolicy;
		p.readPolicyDefault.socketTimeout = socketTimeout;
		p.readPolicyDefault.totalTimeout = totalTimeout;
		p.writePolicyDefault.socketTimeout = socketTimeout;
		p.writePolicyDefault.totalTimeout = totalTimeout;
		p.batchPolicyDefault.socketTimeout = socketTimeout;
		p.batchPolicyDefault.totalTimeout = totalTimeout;
		p.batchParentPolicyWriteDefault.socketTimeout = socketTimeout;
		p.batchParentPolicyWriteDefault.totalTimeout = totalTimeout;
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
	@SuppressWarnings("resource")
	public void setServerSpecific(IAerospikeClient client) {
		Partitions partitions = client.getCluster().partitionMap.get(namespace);

		if (partitions != null) {
			scMode = partitions.scMode;
		}

		Node node = client.getNodes()[0];
		serverVersion = node.getServerVersion();
		String editionFilter = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "release" : "edition";
		String namespaceFilter = "namespace/" + namespace;
		Map<String,String> map = Info.request(null, node, editionFilter, namespaceFilter);

		String editionToken = map.get(editionFilter);

		if (editionToken == null) {
			throw new AerospikeException(String.format(
				"Failed to get edition: host=%s port=%d", host, port));
		}

		if (editionToken.equals("Aerospike Enterprise Edition")) {
			enterprise = true;
		}

		String namespaceTokens = map.get(namespaceFilter);

		if (namespaceTokens == null) {
			throw new AerospikeException(String.format(
				"Failed to get namespace info: host=%s port=%d namespace=%s",
				host, port, namespace));
		}

		int nsup = parseInt(namespaceTokens, "nsup-period");

		if (nsup == 0) {
			hasTtl = parseBoolean(namespaceTokens, "allow-ttl-without-nsup");
		}
		else {
			hasTtl = true;
		}
	}

	private static int parseInt(String namespaceTokens, String name) {
		String s = parseString(namespaceTokens, name);
		return Integer.parseInt(s);
	}

	private static boolean parseBoolean(String namespaceTokens, String name) {
		String s = parseString(namespaceTokens, name);
		return Boolean.parseBoolean(s);
	}

	private static String parseString(String namespaceTokens, String name) {
		String search = name + '=';
		int begin = namespaceTokens.indexOf(search);

		if (begin < 0) {
			throw new RuntimeException("Failed to find server config: " + name);
		}

		begin += search.length();
		int end = namespaceTokens.indexOf(';', begin);

		if (end < 0) {
			end = namespaceTokens.length();
		}

		return namespaceTokens.substring(begin, end);
	}
}
