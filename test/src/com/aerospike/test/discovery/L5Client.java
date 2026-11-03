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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.K8sCluster.Pod;

/**
 * Client-side checks the L5 rows share: which pod each node is, where the client reaches it, what
 * each pod advertises, partition ownership, and a steady read and write load.
 */
final class L5Client {
	static final long ROUTING_WAIT_MS = 60000;
	static final int QUIET_TEND_INTERVALS = 5;
	static final int REACH_TIMEOUT_MS = 1000;
	static final int KEY_SEARCH_LIMIT = 1000000;
	static final int LOAD_THREADS = 4;
	static final int LOAD_KEYS = 1000;
	static final int LOAD_MAX_RETRIES = 2;
	static final long LOAD_STOP_MS = 30000;

	private static final Pattern PEER = Pattern.compile("\\[([0-9A-Fa-f]+),([^,\\[\\]]*),\\[((?:[^\\[\\]]|\\[[^\\]]*\\])*)\\]\\]");

	/**
	 * Aerospike pod with its node name and its own Service NodePort.
	 */
	static final class PodEndpoint {
		final Pod pod;
		final String nodeName;
		final int nodePort;

		PodEndpoint(Pod pod, String nodeName, int nodePort) {
			this.pod = pod;
			this.nodeName = nodeName;
			this.nodePort = nodePort;
		}
	}

	/**
	 * Return every pod of the cluster by node name, each with its Service NodePort, and assert
	 * that every pod has both and no two share a NodePort. The node name is the node ID the
	 * operator reports, as the server formats it: upper-case hexadecimal without leading zeros.
	 */
	static Map<String,PodEndpoint> podEndpoints(K8sCluster cluster) {
		int port = cluster.servicePort();
		Map<String,PodEndpoint> map = new TreeMap<>();
		Set<Integer> nodePorts = new HashSet<>();

		for (Pod pod : cluster.pods()) {
			String reported = cluster.nodeId(pod.name);
			assertTrue(pod.name + " has no hexadecimal node ID in the AerospikeCluster status: " + reported,
				reported.matches("[0-9A-Fa-f]{1,16}"));
			String name = Long.toHexString(Long.parseUnsignedLong(reported, 16)).toUpperCase(Locale.ROOT);
			int nodePort = cluster.nodePort(pod.name, port);
			System.out.println("L5-POD pod=" + pod.name + " worker=" + pod.node + " phase=" + pod.phase + " nodeId=" +
				reported + " node=" + name + " nodePort=" + nodePort);
			assertTrue(pod.name + " has no Service NodePort for " + port, nodePort > 0);
			assertTrue("NodePort " + nodePort + " of " + pod.name + " is not unique", nodePorts.add(nodePort));
			map.put(name, new PodEndpoint(pod, name, nodePort));
		}
		return map;
	}

	static void captureLog() {
		captureLog(message -> {});
	}

	/**
	 * Print the client's warnings and pass each to {@code observer}, which runs on the tend thread.
	 */
	static void captureLog(Consumer<String> observer) {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			System.out.println("CLIENT-LOG " + level + ' ' + message);
			observer.accept(message);
		});
	}

	/**
	 * Return whether the node answers an info request within {@link #REACH_TIMEOUT_MS}.
	 */
	static boolean reachable(Node node) {
		InfoPolicy policy = new InfoPolicy();
		policy.timeout = REACH_TIMEOUT_MS;

		try {
			return node.getName().equals(Info.request(policy, node, "node"));
		}
		catch (AerospikeException e) {
			return false;
		}
	}

	/**
	 * Fill {@code masters} with the master of each partition of the namespace and return the
	 * number of partitions whose master is missing, inactive, not the node the client holds under
	 * that name, or, when {@code reachable} is true, does not answer.
	 */
	static int unowned(AerospikeClient client, Node[] masters, boolean reachable) {
		Partitions partitions = client.getCluster().partitionMap.get(L5Environment.NAMESPACE);
		Map<Node,Boolean> answers = new LinkedHashMap<>();
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = partitions == null ? null : partitions.replicas[0].get(i);

			if (node == null || ! node.isActive() || node(client, node.getName()) != node ||
				(reachable && ! answers.computeIfAbsent(node, L5Client::reachable))) {
				masters[i] = null;
				unowned++;
			}
			else {
				masters[i] = node;
			}
		}
		return unowned;
	}

	/**
	 * Return the node the client holds under {@code name}, or null.
	 */
	static Node node(AerospikeClient client, String name) {
		for (Node node : client.getNodes()) {
			if (node.getName().equals(name)) {
				return node;
			}
		}
		return null;
	}

	/**
	 * Return the client's nodes that are inactive or do not answer.
	 */
	static List<String> unreachable(AerospikeClient client) {
		List<String> list = new ArrayList<>();

		for (Node node : client.getNodes()) {
			if (! node.isActive() || ! reachable(node)) {
				list.add(node.getName());
			}
		}
		return list;
	}

	/**
	 * Wait until the client holds {@code size} nodes, each active and answering, and every
	 * partition's master is one of them, then log the owners. Return the wait in milliseconds.
	 */
	static long awaitRouting(String step, AerospikeClient client, int size, long boundMs) throws InterruptedException {
		Node[] masters = new Node[Node.PARTITIONS];
		long start = System.nanoTime();
		boolean routed = K8sCluster.waitFor(() -> client.getNodes().length == size && unreachable(client).isEmpty() &&
			unowned(client, masters, true) == 0 && owners(masters).size() == size, boundMs, 100);
		long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		System.out.println("L5-ROUTING step=" + step + " nodes=" + names(client) + " unreachable=" + unreachable(client) +
			" unowned=" + unowned(client, masters, true) + " owners=" + owners(masters) + " waitedMs=" + elapsedMs +
			" boundMs=" + boundMs);
		assertTrue(step + ": client did not hold " + size + " answering nodes owning every partition within " +
			boundMs + "ms: nodes=" + names(client) + " unreachable=" + unreachable(client), routed);
		return elapsedMs;
	}

	/**
	 * Assert the client holds one node per pod, each at an address the endpoint name resolves to
	 * and at that pod's own NodePort.
	 */
	static void assertNodesAtEndpoint(String step, L5Environment env, AerospikeClient client,
		Map<String,PodEndpoint> endpoints) throws UnknownHostException {
		List<String> resolved = addresses(env.endpointName);
		Set<String> matched = new HashSet<>();

		for (Node node : client.getNodes()) {
			PodEndpoint endpoint = endpoints.get(node.getName());
			Host h = node.getHost();
			System.out.println("L5-NODE step=" + step + " node=" + node.getName() + " pod=" +
				(endpoint == null ? null : endpoint.pod.name) + " clientHost=" + h.name + ':' + h.port + " expected=" +
				env.endpointName + ':' + (endpoint == null ? null : endpoint.nodePort) + " endpointResolvesTo=" + resolved);
			assertNotNull(step + ": node " + node.getName() + " is no pod in " + endpoints.keySet(), endpoint);
			assertTrue(endpoint.pod.name + " matched twice", matched.add(endpoint.pod.name));
			assertEquals(step + ": " + node.getName() + " (" + endpoint.pod.name + ") port", endpoint.nodePort, h.port);
			assertTrue(step + ": " + node.getName() + " host " + h.name + " is not " + env.endpointName + resolved,
				h.name.equals(env.endpointName) || resolved.contains(h.name));
		}
		assertEquals(step + ": pods without a node", endpoints.size(), matched.size());
	}

	/**
	 * Assert each pod advertises the endpoint name, verbatim, with its own NodePort as its
	 * alternate service address, and that its peers-clear-alt reply lists every other pod at the
	 * endpoint name with that pod's own NodePort.
	 */
	static void assertAdvertisement(String step, L5Environment env, AerospikeClient client,
		Map<String,PodEndpoint> endpoints) {
		for (Node node : client.getNodes()) {
			PodEndpoint self = endpoints.get(node.getName());
			assertNotNull(step + ": node " + node.getName() + " is no pod in " + endpoints.keySet(), self);
			Map<String,String> info = Info.request(null, node, "service-clear-alt", "peers-clear-alt");
			String service = info.get("service-clear-alt");
			String reply = info.get("peers-clear-alt");
			Map<String,List<String>> peers = peers(reply);
			System.out.println("L5-ADVERTISED step=" + step + " node=" + node.getName() + " pod=" + self.pod.name +
				" service-clear-alt=" + service + " peers-clear-alt=" + reply);
			assertEquals(step + ": " + self.pod.name + " service-clear-alt", env.advertised(self.nodePort), service);

			for (PodEndpoint peer : endpoints.values()) {
				if (peer == self) {
					continue;
				}
				assertEquals(step + ": " + self.pod.name + " peers-clear-alt entry for " + peer.pod.name + " (" +
					peer.nodeName + ") in " + reply, List.of(env.advertised(peer.nodePort)), peers.get(peer.nodeName));
			}
			assertEquals(step + ": " + self.pod.name + " peers-clear-alt peers " + peers.keySet(), endpoints.size() - 1,
				peers.size());
		}
	}

	/**
	 * Parse a peers reply into each peer's node name and the addresses it lists, each as
	 * {@code host:port} with the reply's default port filled in.
	 */
	static Map<String,List<String>> peers(String reply) {
		assertNotNull("No peers reply", reply);
		String[] head = reply.split(",", 3);
		assertEquals("Peers reply " + reply, 3, head.length);
		String defaultPort = head[1];
		Map<String,List<String>> peers = new TreeMap<>();
		Matcher m = PEER.matcher(head[2]);

		while (m.find()) {
			List<String> addresses = new ArrayList<>();

			for (String address : m.group(3).split(",")) {
				if (address.isEmpty()) {
					continue;
				}
				boolean hasPort = address.lastIndexOf(':') > address.lastIndexOf(']');
				addresses.add(hasPort ? address : address + ':' + defaultPort);
			}
			peers.put(m.group(1).toUpperCase(Locale.ROOT), addresses);
		}
		return peers;
	}

	/**
	 * Write and read one key in every partition of the namespace, and assert each read returns
	 * the value written.
	 */
	static void assertEveryPartitionServed(String step, AerospikeClient client, String set) {
		Node[] masters = new Node[Node.PARTITIONS];
		assertEquals(step + ": unowned partitions", 0, unowned(client, masters, false));
		Key[] keys = new Key[Node.PARTITIONS];
		int found = 0;

		for (int i = 0; i < KEY_SEARCH_LIMIT && found < Node.PARTITIONS; i++) {
			Key candidate = new Key(L5Environment.NAMESPACE, set, step + '-' + i);
			int partition = Partition.getPartitionId(candidate.digest);

			if (keys[partition] == null) {
				keys[partition] = candidate;
				found++;
			}
		}
		assertEquals(step + ": keys found for partitions", Node.PARTITIONS, found);

		long start = System.nanoTime();
		WritePolicy wp = client.copyWritePolicyDefault();
		Policy rp = client.copyReadPolicyDefault();

		for (int partition = 0; partition < Node.PARTITIONS; partition++) {
			String value = step + '-' + partition;
			client.put(wp, keys[partition], new Bin("v", value));
			Record record = client.get(rp, keys[partition]);
			assertNotNull(step + ": partition " + partition + " read nothing", record);
			assertEquals(step + ": partition " + partition, value, record.getString("v"));
		}
		System.out.println("L5-SERVED step=" + step + " partitions=" + Node.PARTITIONS + " writes=" + Node.PARTITIONS +
			" reads=" + Node.PARTITIONS + " owners=" + owners(masters) + " elapsedMs=" +
			TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
	}

	/**
	 * Wait, at most {@code boundMs}, for {@link #QUIET_TEND_INTERVALS} tend intervals in which the
	 * client's error, timeout and retry counts and the load's failed count do not grow and commands
	 * succeed.
	 */
	static void awaitQuiet(String step, AerospikeClient client, Load load, long boundMs) throws InterruptedException {
		long windowMs = (long)QUIET_TEND_INTERVALS * L5Environment.TEND_INTERVAL;
		long[] from = counters(client, load);
		long start = System.nanoTime();
		boolean quiet = K8sCluster.waitFor(() -> {
			long[] now = counters(client, load);

			if (now[0] != from[0] || now[1] != from[1] || now[2] != from[2]) {
				System.arraycopy(now, 0, from, 0, now.length);
				return false;
			}
			return TimeUnit.NANOSECONDS.toMillis(now[4] - from[4]) >= windowMs && now[3] > from[3];
		}, boundMs, 50);
		long[] now = counters(client, load);
		System.out.println("L5-QUIET step=" + step + " windowMs=" + windowMs + " boundMs=" + boundMs + " waitedMs=" +
			TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " errors=" + now[0] + " retries=" + now[1] +
			" failed=" + now[2] + " succeeded=" + now[3] + " quiet=" + quiet + " lastError=" + load.lastError.get());
		assertTrue(step + ": errors, retries or failed commands still grew, or no command succeeded, in every " +
			windowMs + "ms window for " + boundMs + "ms", quiet);
	}

	private static long[] counters(AerospikeClient client, Load load) {
		return new long[] {errors(client), client.getCluster().getRetryCount(), load.failed.get(), load.succeeded.get(),
			System.nanoTime()};
	}

	static long errors(AerospikeClient client) {
		long errors = 0;

		for (Node node : client.getNodes()) {
			errors += node.getErrorCount() + node.getTimeoutCount();
		}
		return errors;
	}

	static List<String> names(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	static Map<String,Integer> owners(Node[] masters) {
		Map<String,Integer> owners = new TreeMap<>();

		for (Node node : masters) {
			if (node != null) {
				owners.merge(node.getName(), 1, Integer::sum);
			}
		}
		return owners;
	}

	static List<String> addresses(String name) throws UnknownHostException {
		List<String> addresses = new ArrayList<>();

		for (InetAddress address : InetAddress.getAllByName(name)) {
			addresses.add(address.getHostAddress());
		}
		return addresses;
	}

	/**
	 * Writes and reads on {@link #LOAD_THREADS} threads until closed. A command that fails is
	 * counted and issued again until it succeeds, so the longest time from a command's first
	 * attempt to its success measures how long the client could not serve it.
	 */
	static final class Load implements AutoCloseable {
		final AtomicLong succeeded = new AtomicLong();
		final AtomicLong failed = new AtomicLong();
		final AtomicReference<String> lastError = new AtomicReference<>();
		private final AtomicLong maxLatencyNanos = new AtomicLong();
		private final AtomicLong resetAtNanos = new AtomicLong(System.nanoTime());
		private final AtomicBoolean stop = new AtomicBoolean();
		private final ExecutorService executor = Executors.newFixedThreadPool(LOAD_THREADS);
		private final List<Future<?>> futures = new ArrayList<>();

		Load(AerospikeClient client, String set) throws InterruptedException {
			WritePolicy wp = client.copyWritePolicyDefault();
			wp.maxRetries = LOAD_MAX_RETRIES;
			wp.sleepBetweenRetries = L5Environment.TEND_INTERVAL;
			Policy rp = client.copyReadPolicyDefault();
			rp.maxRetries = LOAD_MAX_RETRIES;
			rp.sleepBetweenRetries = L5Environment.TEND_INTERVAL;

			for (int t = 0; t < LOAD_THREADS; t++) {
				int thread = t;
				futures.add(executor.submit(() -> {
					for (int i = 0; ! stop.get(); i++) {
						Key key = new Key(L5Environment.NAMESPACE, set, "load-" + ((thread + i * LOAD_THREADS) % LOAD_KEYS));
						long first = System.nanoTime();
						boolean done = false;

						while (! done && ! stop.get()) {
							try {
								if (i % 2 == 0) {
									client.put(wp, key, new Bin("v", i));
								}
								else {
									client.get(rp, key);
								}
								succeeded.incrementAndGet();
								if (first - resetAtNanos.get() >= 0) {
									maxLatencyNanos.accumulateAndGet(System.nanoTime() - first, Math::max);
								}
								done = true;
							}
							catch (RuntimeException e) {
								failed.incrementAndGet();
								lastError.set(e.getClass().getSimpleName() + ": " + e.getMessage());
							}
						}
					}
				}));
			}
			assertTrue("Load did not start within " + ROUTING_WAIT_MS + "ms: " + lastError.get(),
				K8sCluster.waitFor(() -> succeeded.get() > 0, ROUTING_WAIT_MS, 10));
		}

		/**
		 * Return the longest first-attempt-to-success time of the commands first attempted since
		 * the last reset, and reset it.
		 */
		long resetMaxLatencyMs() {
			resetAtNanos.set(System.nanoTime());
			return TimeUnit.NANOSECONDS.toMillis(maxLatencyNanos.getAndSet(0));
		}

		long maxLatencyMs() {
			return TimeUnit.NANOSECONDS.toMillis(maxLatencyNanos.get());
		}

		@Override
		public void close() throws Exception {
			stop.set(true);
			executor.shutdown();

			for (Future<?> future : futures) {
				future.get(LOAD_STOP_MS, TimeUnit.MILLISECONDS);
			}
		}
	}
}
