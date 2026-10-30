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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kind cluster running the Aerospike Kubernetes Operator, built by
 * .github/workflows/discovery-k8s.yaml or test/discovery/run_l3.sh. The cluster arrives as
 * environment variables:
 * <ul>
 * <li>DISCOVERY_KUBECONFIG: kubeconfig of the kind cluster; without it every test is skipped</li>
 * <li>DISCOVERY_ENDPOINT_HOSTNAME: hostname the client and the server both resolve</li>
 * <li>DISCOVERY_INGRESS_PORTS: ingress-nginx tcp-services host ports</li>
 * <li>DISCOVERY_SERVICE_NODE_PORTS: NodePorts of the per-pod Services at setup, in pod order</li>
 * <li>DISCOVERY_TLS_CA_FILE: CA file of the server certificate, set once
 * .github/discovery/enable-l3-tls.sh serves TLS on the AerospikeCluster</li>
 * <li>DISCOVERY_TLS_NAME: tls-name that script configures</li>
 * </ul>
 * The AerospikeCluster, its pods, their Services and the Kubernetes nodes are read from the API
 * server at runtime through kubectl, every call bounded by {@link #KUBECTL_TIMEOUT_MS}.
 */
final class K8sCluster {
	static final String ALTERNATE_ACCESS_LABEL = "aerospike.com/configured-alternate-access-address";
	static final String SKIP = "needs a kind cluster running the Aerospike Kubernetes Operator: run " +
		"test/discovery/run_l3.sh or the discovery-k8s workflow (DISCOVERY_KUBECONFIG is not set)";
	static final long KUBECTL_TIMEOUT_MS = 120000;
	static final long SCALE_TIMEOUT_MS = 900000;

	static final String kubeconfig = optional("DISCOVERY_KUBECONFIG");
	static final String endpointHostname = optional("DISCOVERY_ENDPOINT_HOSTNAME");
	static final List<Integer> ingressPorts = ports(optional("DISCOVERY_INGRESS_PORTS"));
	static final List<Integer> serviceNodePorts = ports(optional("DISCOVERY_SERVICE_NODE_PORTS"));
	static final String tlsCaFile = optional("DISCOVERY_TLS_CA_FILE");
	static final String tlsName = optional("DISCOVERY_TLS_NAME");

	final String namespace;
	final String name;

	/**
	 * Aerospike pod as the API server reports it.
	 */
	static final class Pod {
		final String name;
		final String node;
		final String ip;
		final String phase;

		Pod(String name, String node, String ip, String phase) {
			this.name = name;
			this.node = node;
			this.ip = ip;
			this.phase = phase;
		}

		@Override
		public String toString() {
			return name + " node=" + node + " ip=" + ip + " phase=" + phase;
		}
	}

	private K8sCluster(String namespace, String name) {
		this.namespace = namespace;
		this.name = name;
	}

	/**
	 * Return the one AerospikeCluster in the kind cluster.
	 */
	static K8sCluster find() {
		List<String> lines = lines(kubectl("get", "aerospikecluster", "--all-namespaces", "-o",
			"jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{\"\\n\"}{end}"));
		assertEquals("AerospikeClusters " + lines, 1, lines.size());
		String[] parts = lines.get(0).split(" ");
		return new K8sCluster(parts[0], parts[1]);
	}

	String get(String jsonpath) {
		return kubectl("-n", namespace, "get", "aerospikecluster", name, "-o", "jsonpath={" + jsonpath + "}").trim();
	}

	int size() {
		return Integer.parseInt(get(".spec.size"));
	}

	boolean multiPodPerHost() {
		return Boolean.parseBoolean(get(".spec.podSpec.multiPodPerHost"));
	}

	int servicePort() {
		return Integer.parseInt(get(".spec.aerospikeConfig.network.service.port"));
	}

	List<Pod> pods() {
		List<Pod> pods = new ArrayList<>();

		for (String line : lines(kubectl("-n", namespace, "get", "pods", "-l", "aerospike.com/cr=" + name, "-o",
			"jsonpath={range .items[*]}{.metadata.name} {.spec.nodeName} {.status.podIP} {.status.phase}{\"\\n\"}{end}"))) {
			String[] parts = line.split(" ", -1);
			pods.add(new Pod(parts[0], parts[1], parts[2], parts[3]));
		}
		pods.sort((a, b) -> a.name.compareTo(b.name));
		return pods;
	}

	/**
	 * Return the NodePort of the pod's own Service for {@code port}, or 0 when it has none.
	 */
	int nodePort(String pod, int port) {
		String value = kubectl("-n", namespace, "get", "service", pod, "--ignore-not-found", "-o",
			"jsonpath={.spec.ports[?(@.port==" + port + ")].nodePort}").trim();
		return value.isEmpty() ? 0 : Integer.parseInt(value);
	}

	/**
	 * Return the hostPort the pod's containers publish {@code port} on, or 0 when none does.
	 */
	int hostPort(String pod, int port) {
		String value = kubectl("-n", namespace, "get", "pod", pod, "-o",
			"jsonpath={.spec.containers[*].ports[?(@.containerPort==" + port + ")].hostPort}").trim();
		return value.isEmpty() ? 0 : Integer.parseInt(value.split(" ")[0]);
	}

	/**
	 * Return the UID of the object, or an empty string when it does not exist.
	 */
	String uid(String kind, String object) {
		return kubectl("-n", namespace, "get", kind, object, "--ignore-not-found", "-o",
			"jsonpath={.metadata.uid}").trim();
	}

	String podReadyAt(String pod) {
		return kubectl("-n", namespace, "get", "pod", pod, "-o",
			"jsonpath={.status.conditions[?(@.type==\"Ready\")].lastTransitionTime}").trim();
	}

	String readinessProbe(String pod) {
		return kubectl("-n", namespace, "get", "pod", pod, "-o",
			"jsonpath={.spec.containers[?(@.name==\"aerospike-server\")].readinessProbe}").trim();
	}

	String publishNotReadyAddresses(String service) {
		return kubectl("-n", namespace, "get", "service", service, "-o",
			"jsonpath={.spec.publishNotReadyAddresses}").trim();
	}

	/**
	 * Return the number of ready endpoints in the Service's EndpointSlices.
	 */
	int readyEndpoints(String service) {
		String ready = kubectl("-n", namespace, "get", "endpointslices", "-l", "kubernetes.io/service-name=" + service,
			"-o", "jsonpath={range .items[*].endpoints[*]}{.conditions.ready} {end}");
		int count = 0;

		for (String value : ready.trim().split(" ")) {
			if ("true".equals(value)) {
				count++;
			}
		}
		return count;
	}

	void deletePod(String pod) {
		kubectl("-n", namespace, "delete", "pod", pod, "--wait=true");
	}

	/**
	 * Create a NodePort Service that sends {@code port} on {@code nodePort} to {@code pod} alone.
	 */
	void createPodNodePortService(String service, String pod, int port, int nodePort) {
		kubectl("-n", namespace, "create", "service", "nodeport", service, "--tcp=" + port + ':' + port,
			"--node-port=" + nodePort);
		kubectl("-n", namespace, "set", "selector", "service", service, "statefulset.kubernetes.io/pod-name=" + pod);
	}

	void deleteService(String service) {
		kubectl("-n", namespace, "delete", "service", service, "--ignore-not-found", "--wait=true");
	}

	/**
	 * Return the first and last port of the API server's NodePort range, read from the error the
	 * API server returns when asked to create a Service whose NodePort is outside it.
	 */
	int[] nodePortRange() {
		String probe = name + "-node-port-range";
		assertTrue("Service " + probe + " exists", uid("service", probe).isEmpty());

		try {
			String error = kubectlError("-n", namespace, "create", "service", "nodeport", probe, "--tcp=1:1",
				"--node-port=1");
			Matcher m = Pattern.compile("range of valid ports is (\\d+)-(\\d+)").matcher(error);
			assertTrue("No NodePort range in: " + error, m.find());
			return new int[] {Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))};
		}
		finally {
			deleteService(probe);
		}
	}

	/**
	 * Return the NodePorts every Service in the kind cluster holds.
	 */
	static List<Integer> allocatedNodePorts() {
		return ports(kubectl("get", "service", "--all-namespaces", "-o",
			"jsonpath={range .items[*].spec.ports[*]}{.nodePort},{end}"));
	}

	/**
	 * Return the StatefulSet ordinal at the end of the pod's name.
	 */
	static int ordinal(String pod) {
		return Integer.parseInt(pod.substring(pod.lastIndexOf('-') + 1));
	}

	/**
	 * Return the decoded value of {@code key} in the secret {@code secret}.
	 */
	byte[] secretData(String secret, String key) {
		return Base64.getDecoder().decode(kubectl("-n", namespace, "get", "secret", secret, "-o",
			"jsonpath={.data." + key.replace(".", "\\.") + "}").trim());
	}

	static String nodeLabel(String node) {
		return kubectl("get", "node", node, "-o",
			"jsonpath={.metadata.labels." + ALTERNATE_ACCESS_LABEL.replace(".", "\\.") + "}").trim();
	}

	static String nodeAddress(String node) {
		return kubectl("get", "node", node, "-o", "jsonpath={.status.addresses[?(@.type==\"InternalIP\")].address}").trim();
	}

	/**
	 * Set the AerospikeCluster size without waiting for the operator.
	 */
	void resize(int size) {
		kubectl("-n", namespace, "patch", "aerospikecluster", name, "--type", "merge",
			"-p", "{\"spec\":{\"size\":" + size + "}}");
	}

	/**
	 * Set the AerospikeCluster size and wait until the operator reports it Completed at that size
	 * with that many running pods.
	 */
	void scale(int size) throws InterruptedException {
		kubectl("-n", namespace, "patch", "aerospikecluster", name, "--type", "merge",
			"-p", "{\"spec\":{\"size\":" + size + "}}");

		String[] state = new String[1];
		boolean done = waitFor(() -> {
			state[0] = get(".status.size") + ' ' + get(".status.phase") + ' ' + pods();
			return isScaled(size);
		}, SCALE_TIMEOUT_MS, 2000);
		System.out.println("L3-SCALED cluster=" + namespace + '/' + name + " size=" + size + " state=" + state[0]);
		assertTrue("AerospikeCluster " + name + " not Completed at size " + size + " within " + SCALE_TIMEOUT_MS +
			"ms: " + state[0], done);
	}

	private boolean isScaled(int size) {
		if (! String.valueOf(size).equals(get(".status.size")) || ! "Completed".equals(get(".status.phase"))) {
			return false;
		}

		List<Pod> pods = pods();

		if (pods.size() != size) {
			return false;
		}

		for (Pod pod : pods) {
			if (! "Running".equals(pod.phase)) {
				return false;
			}
		}
		return true;
	}

	static boolean waitFor(BooleanSupplier condition, long timeoutMs, int pollMs) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMs;

		while (! condition.getAsBoolean()) {
			if (System.currentTimeMillis() >= deadline) {
				return false;
			}
			Thread.sleep(pollMs);
		}
		return true;
	}

	/**
	 * Run kubectl against DISCOVERY_KUBECONFIG and return its standard output. Fails the test when
	 * kubectl exits non-zero or does not finish within {@link #KUBECTL_TIMEOUT_MS}.
	 */
	static String kubectl(String... args) {
		List<String> command = new ArrayList<>();
		command.add("kubectl");
		command.add("--request-timeout=30s");
		Collections.addAll(command, args);

		File out = null;
		File err = null;
		Process process = null;

		try {
			out = File.createTempFile("kubectl", ".out");
			err = File.createTempFile("kubectl", ".err");
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.environment().put("KUBECONFIG", kubeconfig);
			pb.redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")));
			pb.redirectOutput(out);
			pb.redirectError(err);
			process = pb.start();

			if (! process.waitFor(KUBECTL_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
				fail(String.join(" ", command) + " did not finish within " + KUBECTL_TIMEOUT_MS + "ms");
			}

			String stdout = Files.readString(out.toPath(), StandardCharsets.UTF_8);

			if (process.exitValue() != 0) {
				fail(String.join(" ", command) + " exited " + process.exitValue() + ": " +
					Files.readString(err.toPath(), StandardCharsets.UTF_8).trim());
			}
			return stdout;
		}
		catch (IOException e) {
			throw new IllegalStateException(String.join(" ", command) + " failed to run", e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(String.join(" ", command) + " interrupted", e);
		}
		finally {
			if (process != null && process.isAlive()) {
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

	/**
	 * Run kubectl against DISCOVERY_KUBECONFIG, expecting it to fail, and return its standard
	 * error. Fails the test when kubectl exits zero or does not finish within
	 * {@link #KUBECTL_TIMEOUT_MS}.
	 */
	static String kubectlError(String... args) {
		List<String> command = new ArrayList<>();
		command.add("kubectl");
		command.add("--request-timeout=30s");
		Collections.addAll(command, args);

		File err = null;
		Process process = null;

		try {
			err = File.createTempFile("kubectl", ".err");
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.environment().put("KUBECONFIG", kubeconfig);
			pb.redirectInput(ProcessBuilder.Redirect.from(new File("/dev/null")));
			pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
			pb.redirectError(err);
			process = pb.start();

			if (! process.waitFor(KUBECTL_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
				fail(String.join(" ", command) + " did not finish within " + KUBECTL_TIMEOUT_MS + "ms");
			}
			assertFalse(String.join(" ", command) + " succeeded", process.exitValue() == 0);
			return Files.readString(err.toPath(), StandardCharsets.UTF_8).trim();
		}
		catch (IOException e) {
			throw new IllegalStateException(String.join(" ", command) + " failed to run", e);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(String.join(" ", command) + " interrupted", e);
		}
		finally {
			if (process != null && process.isAlive()) {
				process.destroyForcibly();
			}
			if (err != null) {
				err.delete();
			}
		}
	}

	private static List<String> lines(String output) {
		List<String> list = new ArrayList<>();

		for (String line : output.split("\n")) {
			if (! line.isBlank()) {
				list.add(line.trim());
			}
		}
		return list;
	}

	private static List<Integer> ports(String csv) {
		List<Integer> list = new ArrayList<>();

		for (String port : csv.split(",")) {
			if (! port.isEmpty()) {
				list.add(Integer.parseInt(port.trim()));
			}
		}
		return Collections.unmodifiableList(list);
	}

	private static String optional(String name) {
		String value = System.getenv(name);
		return value == null ? "" : value;
	}
}
