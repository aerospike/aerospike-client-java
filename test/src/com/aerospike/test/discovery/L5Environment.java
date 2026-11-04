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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

/**
 * L5 environment the aerospike-client-conformance l5-aws harness gives a row
 * (l5-aws/README.md there): an AerospikeCluster on EKS reached only through a PrivateLink
 * endpoint name, and {@code l5ctl}, which drives the disruptions. Every {@code l5ctl} call is
 * bounded, and the {@code L5-*} lines it prints are parsed into {@link Line}s.
 */
final class L5Environment {
	static final String NAMESPACE = "test";
	static final String USER = "admin";
	static final int CLUSTER_SIZE = 3;
	static final int TEND_INTERVAL = 250;
	static final int TIMEOUT = 1000;
	static final long CTL_TIMEOUT_MS = 300000;
	static final int READY_TIMEOUT_S = 1200;
	static final int HEALTHY_TIMEOUT_S = 600;
	static final long CTL_MARGIN_MS = 120000;
	static final List<String> REQUIRED = List.of("L5_CONTROL", "L5_ENDPOINT_NAME", "L5_SEED_PORT", "L5_CTL",
		"DISCOVERY_KUBECONFIG", "DISCOVERY_ENDPOINT_HOSTNAME", "AEROSPIKE_ADMIN_PASSWORD");

	final String control;
	final String endpointName;
	final int seedPort;
	final String ctl;
	final String runnerAz;
	private final String password;

	/**
	 * One {@code L5-*} line: its tag and its {@code key=value} fields.
	 */
	static final class Line {
		final String tag;
		final Map<String,String> fields;
		final String text;

		Line(String text) {
			String[] tokens = text.trim().split("\\s+");
			Map<String,String> map = new LinkedHashMap<>();

			for (int i = 1; i < tokens.length; i++) {
				int index = tokens[i].indexOf('=');

				if (index > 0) {
					map.put(tokens[i].substring(0, index), tokens[i].substring(index + 1));
				}
			}
			this.tag = tokens[0];
			this.fields = Collections.unmodifiableMap(map);
			this.text = text.trim();
		}

		String get(String key) {
			String value = fields.get(key);
			assertNotNull("No " + key + " in " + text, value);
			return value;
		}

		@Override
		public String toString() {
			return text;
		}
	}

	/**
	 * Outcome of one {@code l5ctl} call.
	 */
	static final class Result {
		final String command;
		final int exit;
		final long elapsedMs;
		final List<Line> lines;
		final String stderr;

		Result(String command, int exit, long elapsedMs, List<Line> lines, String stderr) {
			this.command = command;
			this.exit = exit;
			this.elapsedMs = elapsedMs;
			this.lines = lines;
			this.stderr = stderr;
		}

		List<Line> tagged(String tag) {
			List<Line> list = new ArrayList<>();

			for (Line line : lines) {
				if (line.tag.equals(tag)) {
					list.add(line);
				}
			}
			return list;
		}

		Line first(String tag) {
			List<Line> list = tagged(tag);
			assertTrue("No " + tag + " line from " + command + ": " + lines, ! list.isEmpty());
			return list.get(0);
		}

		Result assertSucceeded() {
			assertEquals(command + " exited " + exit + ": " + stderr, 0, exit);
			return this;
		}
	}

	private L5Environment() {
		control = System.getenv("L5_CONTROL");
		endpointName = System.getenv("L5_ENDPOINT_NAME");
		seedPort = Integer.parseInt(System.getenv("L5_SEED_PORT"));
		ctl = System.getenv("L5_CTL");
		runnerAz = optional("L5_RUNNER_AZ");
		password = System.getenv("AEROSPIKE_ADMIN_PASSWORD");
	}

	/**
	 * Return the environment, failing with the name of every variable the harness should have set
	 * and did not: {@link #REQUIRED} and {@code extra}.
	 */
	static L5Environment require(String... extra) {
		List<String> names = new ArrayList<>(REQUIRED);
		names.addAll(Arrays.asList(extra));
		List<String> missing = new ArrayList<>();

		for (String name : names) {
			if (optional(name).isBlank()) {
				missing.add(name);
			}
		}

		if (! missing.isEmpty()) {
			System.out.println("L5-ENVIRONMENT missing=" + String.join(",", missing));
			fail("L5 environment missing " + String.join(", ", missing) + ": run the row through " +
				"aerospike-client-conformance l5-aws/run.sh with test/discovery/l5-aws.manifest, which sets them");
		}

		String control = System.getenv("L5_CONTROL");
		assertTrue("L5_CONTROL must be none or resolvable, got " + control,
			"none".equals(control) || "resolvable".equals(control));
		assertTrue("L5_SEED_PORT is not a port: " + System.getenv("L5_SEED_PORT"),
			System.getenv("L5_SEED_PORT").matches("[1-9][0-9]{0,4}"));
		assertTrue("L5_CTL " + System.getenv("L5_CTL") + " is not executable", new File(System.getenv("L5_CTL")).canExecute());
		assertEquals("DISCOVERY_ENDPOINT_HOSTNAME", System.getenv("L5_ENDPOINT_NAME"),
			System.getenv("DISCOVERY_ENDPOINT_HOSTNAME"));

		L5Environment env = new L5Environment();
		System.out.println("L5-ENVIRONMENT control=" + env.control + " endpoint=" + env.endpointName + " seedPort=" +
			env.seedPort + " ctl=" + env.ctl + " runnerAz=" + env.runnerAz);
		return env;
	}

	/**
	 * Client policy of the L2 discovery tests, with the cluster's admin credentials.
	 */
	ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.user = USER;
		policy.password = password;
		policy.tendInterval = TEND_INTERVAL;
		policy.timeout = TIMEOUT;
		return policy;
	}

	Host seed() {
		return new Host(endpointName, seedPort);
	}

	String advertised(int nodePort) {
		return endpointName + ':' + nodePort;
	}

	void waitReady() {
		ctl(READY_TIMEOUT_S * 1000L + CTL_MARGIN_MS, "wait-ready", "--timeout", String.valueOf(READY_TIMEOUT_S))
			.assertSucceeded();
	}

	void waitHealthy() {
		ctl(HEALTHY_TIMEOUT_S * 1000L + CTL_MARGIN_MS, "wait-healthy", "--timeout", String.valueOf(HEALTHY_TIMEOUT_S))
			.assertSucceeded();
	}

	/**
	 * Set the AerospikeCluster back to {@code size} and wait for it when it is not at that size.
	 */
	void restoreSize(K8sCluster cluster, int size) {
		if (cluster.size() != size || cluster.pods().size() != size) {
			ctl(CTL_TIMEOUT_MS, "scale", String.valueOf(size)).assertSucceeded();
			waitReady();
		}
	}

	/**
	 * Run {@code $L5_CTL args}, print its output and return its exit code and {@code L5-*} lines.
	 * Fails the test when it does not finish within {@code boundMs}; the caller decides what a
	 * non-zero exit means.
	 */
	Result ctl(long boundMs, String... args) {
		List<String> command = new ArrayList<>();
		command.add(ctl);
		Collections.addAll(command, args);
		String text = "l5ctl " + String.join(" ", args);

		File out = null;
		File err = null;
		Process process = null;

		try {
			out = File.createTempFile("l5ctl", ".out");
			err = File.createTempFile("l5ctl", ".err");
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")));
			pb.redirectOutput(out);
			pb.redirectError(err);
			long start = System.nanoTime();
			process = pb.start();
			boolean finished = process.waitFor(boundMs, TimeUnit.MILLISECONDS);
			long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
			String stdout = Files.readString(out.toPath(), StandardCharsets.UTF_8);
			String stderr = Files.readString(err.toPath(), StandardCharsets.UTF_8).trim();
			List<Line> lines = new ArrayList<>();

			for (String line : stdout.split("\n")) {
				if (line.startsWith("L5-")) {
					System.out.println(line);
					lines.add(new Line(line));
				}
			}

			if (! finished) {
				System.out.println("L5-CTL command=\"" + text + "\" exit=timeout elapsedMs=" + elapsedMs + " boundMs=" + boundMs);
				fail(text + " did not finish within " + boundMs + "ms: " + stderr);
			}
			System.out.println("L5-CTL command=\"" + text + "\" exit=" + process.exitValue() + " elapsedMs=" + elapsedMs +
				" boundMs=" + boundMs);

			if (process.exitValue() != 0 && ! stderr.isEmpty()) {
				System.out.println("L5-CTL-STDERR " + stderr.replace('\n', ' '));
			}
			return new Result(text, process.exitValue(), elapsedMs, lines, stderr);
		}
		catch (IOException e) {
			throw new IllegalStateException(text + " failed to run", e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(text + " interrupted", e);
		}
		finally {
			if (process != null && process.isAlive()) {
				process.descendants().forEach(ProcessHandle::destroyForcibly);
				process.destroyForcibly();
			}
			if (out != null) {
				out.delete();
			}
			if (err != null) {
				err.delete();
			}
		}
	}

	private static String optional(String name) {
		String value = System.getenv(name);
		return value == null ? "" : value;
	}
}
