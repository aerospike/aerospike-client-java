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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.K8sCluster.Pod;
import com.aerospike.test.discovery.L5Client.Load;
import com.aerospike.test.discovery.L5Environment.Line;
import com.aerospike.test.discovery.L5Environment.Result;
import com.aerospike.test.util.TestBase;

/**
 * A pod added while its NodePort has no listener: first Pending for want of an Aerospike node,
 * then running and advertised by the server at a NodePort the endpoint does not serve yet, then
 * served once the listener is added. One pass-through client under a steady load keeps serving
 * through all three and uses the new node within 2 x tend interval of its listener serving.
 */
public class TestL5ScaleOutBeforeListener extends TestBase {
	private static final String SET = "l5-scale-out";
	private static final long PENDING_WAIT_MS = 300000;
	private static final long ADD_NODE_WAIT_MS = 35 * 60000L;
	private static final long SET_NODES_WAIT_MS = 35 * 60000L;
	private static final long ADVERTISED_WAIT_MS = 15 * 60000L;
	private static final long LISTENER_WAIT_MS = 20 * 60000L;
	private static final long SYNC_LISTENERS_WAIT_MS = 20 * 60000L;
	private static final long QUIET_WAIT_MS = 60000;
	private static final int PROBE_TIMEOUT_MS = 50;

	private L5Environment env;
	private K8sCluster cluster;
	private int foundSize;
	private boolean scaled;
	private String nodeGroupZone;
	private int nodeGroupSize = -1;

	@Before
	public void requireEnvironment() {
		env = L5Environment.require("L5_NODEGROUPS");
		env.waitReady();
		env.waitHealthy();
		cluster = K8sCluster.find();
		foundSize = cluster.size();
		assertEquals("AerospikeCluster size", L5Environment.CLUSTER_SIZE, foundSize);
	}

	/**
	 * Scale back, set the node group back to its starting size and remove the added listener,
	 * whether or not the test passed.
	 */
	@After
	public void restore() {
		Log.setCallback(null);

		if (cluster == null) {
			return;
		}

		try {
			if (scaled) {
				env.ctl(L5Environment.CTL_TIMEOUT_MS, "scale", String.valueOf(foundSize)).assertSucceeded();
				env.waitReady();
			}
		}
		finally {
			try {
				if (nodeGroupZone != null && nodeGroupSize >= 0) {
					env.ctl(SET_NODES_WAIT_MS, "set-nodes", nodeGroupZone, String.valueOf(nodeGroupSize), "--wait")
						.assertSucceeded();
					env.waitReady();
				}
			}
			finally {
				if (scaled) {
					env.ctl(SYNC_LISTENERS_WAIT_MS, "sync-listeners", "--prune").assertSucceeded();
				}
				env.restoreSize(cluster, foundSize);
			}
		}
	}

	@Test
	public void podAdvertisedBeforeItsListenerJoinsOnceServed() throws Exception {
		ClientPolicy policy = env.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		AtomicReference<String> watched = new AtomicReference<>("");
		AtomicInteger warnings = new AtomicInteger();
		AtomicReference<String> lastWarning = new AtomicReference<>();
		L5Client.captureLog(message -> {
			String token = watched.get();

			if (! token.isEmpty() && message.contains(token)) {
				warnings.incrementAndGet();
				lastWarning.set(message);
			}
		});
		int port = cluster.servicePort();
		List<String> existingPods = new ArrayList<>();

		for (Pod pod : cluster.pods()) {
			existingPods.add(pod.name);
		}

		try (AerospikeClient client = new AerospikeClient(policy, env.seed())) {
			L5Client.awaitRouting("start", client, foundSize, L5Client.ROUTING_WAIT_MS);
			List<String> existing = L5Client.names(client);

			try (Load load = new Load(client, SET)) {
				long failed = load.failed.get();
				scaled = true;
				env.ctl(L5Environment.CTL_TIMEOUT_MS, "scale", String.valueOf(foundSize + 1)).assertSucceeded();
				Line[] pending = {null};
				boolean seen = K8sCluster.waitFor(() -> {
					List<Line> lines = env.ctl(L5Environment.CTL_TIMEOUT_MS, "pending").assertSucceeded().tagged("L5-PENDING");
					pending[0] = lines.isEmpty() ? null : lines.get(0);
					return pending[0] != null;
				}, PENDING_WAIT_MS, 5000);
				assertTrue("No Pending pod within " + PENDING_WAIT_MS + "ms of scaling to " + (foundSize + 1), seen);
				String pod = pending[0].get("pod");
				String zone = pending[0].get("zone");
				assertFalse(pod + " is an existing pod", existingPods.contains(pod));
				assertNotEquals(pod + " rack has no zone", "unknown", zone);
				assertEquals(pod + " phase", "Pending", phase(pod));
				L5Client.awaitQuiet("pending", client, load, QUIET_WAIT_MS);
				System.out.println("L5-PENDING-TRAFFIC pod=" + pod + " zone=" + zone + " nodes=" + L5Client.names(client) +
					" failedSinceScale=" + (load.failed.get() - failed) + " lastError=" + load.lastError.get());
				assertEquals("Commands failed while " + pod + " was Pending", failed, load.failed.get());
				assertEquals("Client nodes while " + pod + " was Pending", existing, L5Client.names(client));

				Result added = env.ctl(ADD_NODE_WAIT_MS, "add-node", zone, "--wait");
				List<Line> nodes = added.tagged("L5-NODES");

				if (! nodes.isEmpty()) {
					nodeGroupZone = zone;
					nodeGroupSize = Integer.parseInt(nodes.get(0).get("desired")) - 1;
				}
				added.assertSucceeded();
				added.first("L5-NODES");

				int[] nodePort = {0};
				String[] name = {""};
				boolean advertised = K8sCluster.waitFor(() -> {
					if (! "Running".equals(phase(pod))) {
						return false;
					}
					nodePort[0] = cluster.nodePort(pod, port);
					String id = cluster.nodeId(pod);

					if (nodePort[0] == 0 || ! id.matches("[0-9A-Fa-f]{1,16}")) {
						return false;
					}
					name[0] = Long.toHexString(Long.parseUnsignedLong(id, 16)).toUpperCase(Locale.ROOT);
					return advertisedByEveryNode(client, name[0], nodePort[0]);
				}, ADVERTISED_WAIT_MS, 2000);
				assertTrue(pod + " not running and advertised at " + env.endpointName + " with its NodePort by every node " +
					"within " + ADVERTISED_WAIT_MS + "ms", advertised);
				watched.set(":" + nodePort[0]);
				boolean early = serving(nodePort[0]);
				System.out.println("L5-ADVERTISED-BEFORE-LISTENER pod=" + pod + " node=" + name[0] + " advertised=" +
					env.advertised(nodePort[0]) + " serving=" + early);
				assertFalse(env.advertised(nodePort[0]) + " already serves before its listener was added", early);

				long windowMs = (long)L5Client.QUIET_TEND_INTERVALS * L5Environment.TEND_INTERVAL;
				long windowFailed = load.failed.get();
				long windowSucceeded = load.succeeded.get();
				long windowStart = System.nanoTime();
				List<String> stuck = new ArrayList<>();
				boolean[] held = {false};
				K8sCluster.waitFor(() -> {
					for (String node : L5Client.unreachable(client)) {
						if (! stuck.contains(node)) {
							stuck.add(node);
						}
					}
					held[0] |= L5Client.node(client, name[0]) != null;
					return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - windowStart) >= windowMs;
				}, windowMs + L5Client.ROUTING_WAIT_MS, 50);
				long observedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - windowStart);
				System.out.println("L5-UNREACHABLE-PEER pod=" + pod + " node=" + name[0] + " advertised=" +
					env.advertised(nodePort[0]) + " observedMs=" + observedMs + " windowMs=" + windowMs + " clientNodes=" +
					L5Client.names(client) + " heldNewNode=" + held[0] + " stuckNodes=" + stuck + " warnings=" + warnings.get() +
					" lastWarning=" + lastWarning.get() + " failed=" + windowFailed + "->" + load.failed.get() + " succeeded=" +
					windowSucceeded + "->" + load.succeeded.get() + " lastError=" + load.lastError.get());
				assertTrue("Window observed " + observedMs + "ms, under " + windowMs + "ms", observedMs >= windowMs);
				assertTrue("Client holds nodes it cannot reach: " + stuck, stuck.isEmpty());
				assertEquals("Commands failed while " + pod + " had no listener", windowFailed, load.failed.get());
				assertTrue("No command succeeded while " + pod + " had no listener", load.succeeded.get() > windowSucceeded);

				ExecutorService executor = Executors.newSingleThreadExecutor();

				try {
					Future<long[]> listener = executor.submit(() -> {
						env.ctl(SYNC_LISTENERS_WAIT_MS, "sync-listeners").assertSucceeded();
						long synced = System.nanoTime();
						env.waitHealthy();
						return new long[] {synced, System.nanoTime()};
					});
					long[] servingAt = {0};
					long[] lastClosedAt = {0};
					long[] usableAt = {0};
					Node[] masters = new Node[Node.PARTITIONS];
					K8sCluster.waitFor(() -> {
						long probeAt = System.nanoTime();

						if (servingAt[0] == 0) {
							if (serving(nodePort[0])) {
								servingAt[0] = probeAt;
							}
							else {
								lastClosedAt[0] = probeAt;
							}
						}
						if (servingAt[0] != 0 && usableAt[0] == 0) {
							Node node = L5Client.node(client, name[0]);

							if (node != null && node.isActive() && client.getNodes().length == foundSize + 1 &&
								L5Client.unowned(client, masters, false) == 0) {
								usableAt[0] = System.nanoTime();
							}
						}
						return usableAt[0] != 0 || listener.state() == Future.State.FAILED ||
							(servingAt[0] != 0 && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - servingAt[0]) > L5Client.ROUTING_WAIT_MS);
					}, LISTENER_WAIT_MS, 10);
					long[] listenerTimes = listener.get(LISTENER_WAIT_MS, TimeUnit.MILLISECONDS);
					long budget = 2L * L5Environment.TEND_INTERVAL;
					long usableMs = usableAt[0] == 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(usableAt[0] - servingAt[0]);
					System.out.println("L5-NODE-USABLE pod=" + pod + " node=" + name[0] + " advertised=" +
						env.advertised(nodePort[0]) + " listenerAddedAt=" + instant(listenerTimes[0]) + " servingAt=" +
						(servingAt[0] == 0 ? null : instant(servingAt[0])) + " healthyReturnedAt=" + instant(listenerTimes[1]) +
						" lastClosedProbeAt=" + (lastClosedAt[0] == 0 ? null : instant(lastClosedAt[0])) + " probeTimeoutMs=" +
						PROBE_TIMEOUT_MS + " usableAt=" + (usableAt[0] == 0 ? null : instant(usableAt[0])) + " servingToUsableMs=" + usableMs +
						" tendInterval=" + L5Environment.TEND_INTERVAL + " budgetMs=" + budget + " nodes=" +
						L5Client.names(client) + " owners=" + L5Client.owners(masters));
					assertTrue("Listener for " + env.advertised(nodePort[0]) + " never served", servingAt[0] != 0);
					assertTrue(name[0] + " not active with every partition owned once its listener served", usableAt[0] != 0);
					assertTrue(name[0] + " usable " + usableMs + "ms after its listener served, budget " + budget + "ms",
						usableMs <= budget);
				}
				finally {
					executor.shutdownNow();
				}
				L5Client.awaitRouting("served", client, foundSize + 1, L5Client.ROUTING_WAIT_MS);
				L5Client.assertEveryPartitionServed("served", client, SET);
			}
		}
	}

	private String phase(String pod) {
		for (Pod p : cluster.pods()) {
			if (p.name.equals(pod)) {
				return p.phase;
			}
		}
		return "";
	}

	/**
	 * Return whether every node the client holds lists {@code name} in its peers-clear-alt reply
	 * at the endpoint name and {@code nodePort} alone.
	 */
	private boolean advertisedByEveryNode(AerospikeClient client, String name, int nodePort) {
		for (Node node : client.getNodes()) {
			List<String> addresses;

			try {
				addresses = L5Client.peers(Info.request(null, node, "peers-clear-alt")).get(name);
			}
			catch (AerospikeException e) {
				return false;
			}

			if (! List.of(env.advertised(nodePort)).equals(addresses)) {
				return false;
			}
		}
		return client.getNodes().length > 0;
	}

	/**
	 * Return whether a TCP connection to the endpoint name at {@code nodePort} opens within
	 * {@link #PROBE_TIMEOUT_MS}. The listener is taken to serve from the start of the first probe
	 * that connects.
	 */
	private boolean serving(int nodePort) {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(env.endpointName, nodePort), PROBE_TIMEOUT_MS);
			return true;
		}
		catch (IOException e) {
			return false;
		}
	}

	private static Instant instant(long nanos) {
		return Instant.now().minusNanos(System.nanoTime() - nanos);
	}
}
