/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Node;

/**
 * The L5 driver's TLS flags against stub nodes listening with TLS on 127.0.0.1, whose certificate,
 * signed by a CA made for the test, names only {@link #TLS_NAME}. Needs no server.
 */
public class TestL5DriverTls {
	private static final String TLS_NAME = "tls.l5.test";
	private static final String PASS = "changeit";

	private static Path dir;
	private static SSLContext server;

	@BeforeClass
	public static void certificates() throws Exception {
		dir = Files.createTempDirectory("l5-driver-tls");
		dir.toFile().deleteOnExit();
		keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-dname", "CN=L5 Driver Test CA", "-ext", "bc:c",
			"-validity", "2", "-keystore", "ca.p12");
		keytool("-exportcert", "-rfc", "-alias", "ca", "-keystore", "ca.p12", "-file", "ca.pem");
		keytool("-genkeypair", "-alias", "node", "-keyalg", "EC", "-dname", "CN=L5 Driver Test Node", "-validity", "2",
			"-keystore", "node.p12");
		keytool("-certreq", "-alias", "node", "-keystore", "node.p12", "-file", "node.csr");
		keytool("-gencert", "-rfc", "-alias", "ca", "-keystore", "ca.p12", "-infile", "node.csr", "-outfile", "node.pem",
			"-ext", "SAN=dns:" + TLS_NAME, "-validity", "2");
		keytool("-importcert", "-noprompt", "-alias", "ca", "-keystore", "node.p12", "-file", "ca.pem");
		keytool("-importcert", "-alias", "node", "-keystore", "node.p12", "-file", "node.pem");

		KeyStore store = KeyStore.getInstance("PKCS12");

		try (InputStream in = new FileInputStream(dir.resolve("node.p12").toFile())) {
			store.load(in, PASS.toCharArray());
		}
		KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keys.init(store, PASS.toCharArray());
		server = SSLContext.getInstance("TLS");
		server.init(keys.getKeyManagers(), null, null);
	}

	@Test
	public void withoutTlsNameConnectsInTheClear() {
		L5Driver.Options options = options("pass-through", "127.0.0.1:3000");

		assertNull(options.policy().tlsPolicy);
		assertNull(options.seed.tlsName);
	}

	@Test
	public void tlsNameWithoutCaFileTrustsTheDefaultStore() {
		L5Driver.Options options = options("pass-through", "127.0.0.1:3000", "--tls-name", TLS_NAME);

		assertNotNull(options.policy().tlsPolicy);
		assertNull(options.policy().tlsPolicy.context);
		assertEquals(new Host("127.0.0.1", TLS_NAME, 3000), options.seed);
		assertEquals(TLS_NAME, options.seed.tlsName);
	}

	@Test
	public void caFileWithoutTlsNameIsABadFlag() throws Exception {
		try {
			options("pass-through", "127.0.0.1:3000", "--tls-ca-file", ca());
			fail("--tls-ca-file without --tls-name accepted");
		}
		catch (IllegalArgumentException e) {
			assertEquals("--tls-ca-file only with --tls-name", e.getMessage());
		}
		assertEquals(2, driver("", "--seed", "127.0.0.1:3000", "--shape", "pass-through", "--tend-interval", "250",
			"--timeout", "1000", "--namespace", "test", "--set", "l5", "--tls-ca-file", ca()).status);
	}

	@Test
	public void passThroughUsesTheTlsAlternateList() throws Exception {
		try (StubInfoNode a = new StubInfoNode("BB9A", server); StubInfoNode b = new StubInfoNode("BB9B", server)) {
			a.values.put("service-tls-alt", "127.0.0.1:" + a.port());
			a.values.put("peers-tls-alt", "1,," + "[[BB9B," + TLS_NAME + ",[127.0.0.1:" + b.port() + "]]]");
			a.values.put("peers-tls-std", "1,,[[BB9B," + TLS_NAME + ",[10.255.0.2:" + b.port() + "]]]");
			b.values.put("service-tls-alt", "127.0.0.1:" + b.port());
			L5Driver.Options options = options("pass-through", "127.0.0.1:" + a.port(), "--tls-name", TLS_NAME,
				"--tls-ca-file", ca());

			try (AerospikeClient client = new AerospikeClient(options.policy(), options.seed)) {
				Node peer = TestL5DriverPortMap.await(client, "BB9B");
				assertEquals(new Host("127.0.0.1", TLS_NAME, b.port()), peer.getHost());
			}
			List<String> commands = a.commands();
			assertTrue(commands.toString(), commands.contains("service-tls-alt"));
			assertTrue(commands.toString(), commands.contains("peers-tls-alt"));

			for (String command : commands) {
				assertFalse(commands.toString(), command.startsWith("peers-clear") || command.startsWith("service-clear"));
			}
			assertTrue(b.commands().contains("node"));
		}
	}

	@Test
	public void certificateNotMatchingTlsNameExits3() throws Exception {
		try (StubInfoNode a = new StubInfoNode("BB9A", server)) {
			a.values.put("service-tls-alt", "127.0.0.1:" + a.port());
			a.values.put("peers-tls-alt", "1,,[]");
			String seed = "127.0.0.1:" + a.port();

			Result matching = driver("quit\n", "--seed", seed, "--shape", "pass-through", "--tend-interval", "250",
				"--timeout", "1000", "--namespace", "test", "--set", "l5", "--tls-name", TLS_NAME, "--tls-ca-file", ca());
			assertEquals(matching.stderr, 0, matching.status);

			Result other = driver("quit\n", "--seed", seed, "--shape", "pass-through", "--tend-interval", "250",
				"--timeout", "1000", "--namespace", "test", "--set", "l5", "--tls-name", "other.l5.test", "--tls-ca-file", ca());
			assertEquals(other.stderr, 3, other.status);
			assertTrue(other.stdout, other.stdout.matches("\\{\"type\":\"error\",\"t\":\\d+,\"message\":\"client not created: " +
				".*Invalid TLS name: other\\.l5\\.test.*\"\\}\n"));
		}
	}

	private static L5Driver.Options options(String shape, String seed, String... flags) {
		List<String> args = new ArrayList<>(Arrays.asList("--seed", seed, "--shape", shape, "--tend-interval", "250",
			"--timeout", "1000", "--namespace", "test", "--set", "l5"));
		args.addAll(Arrays.asList(flags));
		return new L5Driver.Options(args.toArray(new String[0]), null);
	}

	private static String ca() {
		return dir.resolve("ca.pem").toString();
	}

	private static void keytool(String... args) throws Exception {
		List<String> command = new ArrayList<>();
		command.add(new File(System.getProperty("java.home"), "bin/keytool").getPath());
		command.addAll(Arrays.asList(args));
		command.addAll(Arrays.asList("-storetype", "PKCS12", "-storepass", PASS));
		Process process = new ProcessBuilder(command).directory(dir.toFile()).redirectErrorStream(true).start();
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

		if (process.waitFor() != 0) {
			throw new IOException("keytool " + String.join(" ", args) + ": " + output);
		}
	}

	private static final class Result {
		int status;
		String stdout;
		String stderr;
	}

	/**
	 * Run the driver in its own JVM with {@code stdin} as its input, and return its exit status and
	 * output.
	 */
	private static Result driver(String stdin, String... args) throws Exception {
		List<String> command = new ArrayList<>(Arrays.asList(new File(System.getProperty("java.home"), "bin/java").getPath(),
			"-cp", System.getProperty("java.class.path"), L5Driver.class.getName()));
		command.addAll(Arrays.asList(args));
		Process process = new ProcessBuilder(command).start();
		ByteArrayOutputStream err = new ByteArrayOutputStream();
		Thread errReader = new Thread(() -> {
			try {
				process.getErrorStream().transferTo(err);
			}
			catch (IOException e) {
			}
		});
		errReader.start();

		try (OutputStream in = process.getOutputStream()) {
			in.write(stdin.getBytes(StandardCharsets.UTF_8));
		}
		String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

		if (! process.waitFor(60, TimeUnit.SECONDS)) {
			process.destroyForcibly();
			fail("driver did not exit: " + command);
		}
		errReader.join();
		Result result = new Result();
		result.status = process.exitValue();
		result.stdout = stdout;
		result.stderr = err.toString(StandardCharsets.UTF_8);
		return result;
	}
}
