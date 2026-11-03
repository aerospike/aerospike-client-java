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
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.L5Client.Load;
import com.aerospike.test.discovery.L5Environment.Result;
import com.aerospike.test.util.TestBase;

/**
 * Five minutes without any traffic in or out of the zone of the endpoint address the client holds
 * its nodes on, which also takes out that zone's rack, with one pass-through client under a steady
 * load. During the outage no partition goes without an
 * answering owner for longer than {@link #OWNERSHIP_GAP_MS} and every command succeeds within
 * {@link #COMMAND_BOUND_MS} of its first attempt; after it the cluster and the client are back to
 * every node with every partition owned.
 */
public class TestL5AzOutage extends TestBase {
	private static final String SET = "l5-az-outage";
	private static final String DURATION = "PT5M";
	private static final long DURATION_MS = 5 * 60000L;
	private static final long OWNERSHIP_GAP_MS = 30000;
	private static final long COMMAND_BOUND_MS = OWNERSHIP_GAP_MS + 3L * L5Environment.TIMEOUT;
	private static final long RUNNING_WAIT_MS = 120000;
	private static final long OUTAGE_WAIT_MS = DURATION_MS + 10 * 60000L;
	private static final long EXPERIMENT_WAIT_MS = 25 * 60000L;
	private static final long STATUS_POLL_MS = 15000;
	private static final long RECOVERY_MS = 120000;

	private L5Environment env;
	private K8sCluster cluster;
	private int foundSize;
	private String experiment;
	private boolean ended;

	@Before
	public void requireEnvironment() {
		env = L5Environment.require("L5_RUNNER_AZ");
		env.waitReady();
		env.waitHealthy();
		cluster = K8sCluster.find();
		foundSize = cluster.size();
		assertEquals("AerospikeCluster size", L5Environment.CLUSTER_SIZE, foundSize);
	}

	/**
	 * Wait for the outage to end and the cluster to be ready, whether or not the test passed.
	 */
	@After
	public void restore() {
		Log.setCallback(null);

		if (cluster == null) {
			return;
		}

		if (experiment != null && ! ended) {
			env.ctl(EXPERIMENT_WAIT_MS, "experiment", experiment, "--wait");
			env.waitReady();
		}
		env.restoreSize(cluster, foundSize);
	}

	@Test
	public void zoneOutageKeepsEveryPartitionOnReachableNodes() throws Exception {
		ClientPolicy policy = env.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		L5Client.captureLog();

		try (AerospikeClient client = new AerospikeClient(policy, env.seed())) {
			L5Client.awaitRouting("start", client, foundSize, L5Client.ROUTING_WAIT_MS);
			System.out.println("L5-RESOLVED when=before name=" + env.endpointName + " addresses=" +
				resolved(env.endpointName) + " clientHosts=" + hosts(client));

			String zone = outageZone(client);
			System.out.println("L5-OUTAGE-ZONE zone=" + zone + " runnerAz=" + env.runnerAz + " rackZones=" + cluster.rackZones());
			assertTrue("Endpoint zone " + zone + " has no rack: " + cluster.rackZones(), cluster.rackZones().contains(zone));

			try (Load load = new Load(client, SET)) {
				long failedBefore = load.failed.get();
				Result started = env.ctl(L5Environment.CTL_TIMEOUT_MS, "az-outage", zone, "--duration", DURATION)
					.assertSucceeded();
				experiment = started.first("L5-FIS").get("experiment");

				String[] status = {""};
				boolean running = K8sCluster.waitFor(() -> {
					status[0] = status();
					return "running".equals(status[0]) || "completed".equals(status[0]);
				}, RUNNING_WAIT_MS, 5000);
				assertTrue("Experiment " + experiment + " not running within " + RUNNING_WAIT_MS + "ms: " + status[0], running);
				assertEquals("Experiment " + experiment + " ended before it was observed running", "running", status[0]);
				long runningAt = System.nanoTime();
				long failedAtRunning = load.failed.get();
				load.resetMaxLatencyMs();
				long[] unownedSince = {0};
				long[] longestUnownedMs = {0};
				long[] lastStatusAt = {runningAt};
				int[] unownedStretches = {0};
				boolean[] resolvedLogged = {false};
				Node[] masters = new Node[Node.PARTITIONS];
				boolean over = K8sCluster.waitFor(() -> {
					long now = System.nanoTime();
					int unowned = L5Client.unowned(client, masters, true);

					if (unowned != 0) {
						if (unownedSince[0] == 0) {
							unownedSince[0] = now;
							unownedStretches[0]++;
							System.out.println("L5-OUTAGE-UNOWNED zone=" + zone + " afterRunningMs=" +
								TimeUnit.NANOSECONDS.toMillis(now - runningAt) + " unowned=" + unowned + " nodes=" +
								L5Client.names(client) + " unreachable=" + L5Client.unreachable(client));
						}
						longestUnownedMs[0] = Math.max(longestUnownedMs[0], TimeUnit.NANOSECONDS.toMillis(now - unownedSince[0]));
					}
					else if (unownedSince[0] != 0) {
						System.out.println("L5-OUTAGE-OWNED zone=" + zone + " afterRunningMs=" +
							TimeUnit.NANOSECONDS.toMillis(now - runningAt) + " unownedMs=" +
							TimeUnit.NANOSECONDS.toMillis(now - unownedSince[0]) + " boundMs=" + OWNERSHIP_GAP_MS + " nodes=" +
							L5Client.names(client) + " unreachable=" + L5Client.unreachable(client) + " owners=" +
							L5Client.owners(masters) + " failedCommands=" + (load.failed.get() - failedAtRunning));
						unownedSince[0] = 0;
					}

					if (! resolvedLogged[0] && TimeUnit.NANOSECONDS.toMillis(now - runningAt) >= DURATION_MS / 2) {
						resolvedLogged[0] = true;
						System.out.println("L5-RESOLVED when=during name=" + env.endpointName + " addresses=" +
							resolved(env.endpointName) + " clientHosts=" + hosts(client));
					}

					if (TimeUnit.NANOSECONDS.toMillis(now - lastStatusAt[0]) >= STATUS_POLL_MS) {
						lastStatusAt[0] = now;
						status[0] = status();
						return ! "running".equals(status[0]);
					}
					return false;
				}, OUTAGE_WAIT_MS, 250);
				long outageMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - runningAt);
				long maxCommandMs = load.maxLatencyMs();
				System.out.println("L5-OUTAGE zone=" + zone + " experiment=" + experiment + " status=" + status[0] +
					" observedMs=" + outageMs + " durationMs=" + DURATION_MS + " unownedStretches=" + unownedStretches[0] +
					" longestUnownedMs=" + longestUnownedMs[0] + " ownershipGapBoundMs=" + OWNERSHIP_GAP_MS +
					" maxCommandMs=" + maxCommandMs + " commandBoundMs=" + COMMAND_BOUND_MS + " failedCommands=" +
					(load.failed.get() - failedAtRunning) + " failedCommandsSinceLoadStart=" + (load.failed.get() - failedBefore) +
					" succeededCommands=" + load.succeeded.get() + " retries=" + client.getCluster().getRetryCount() +
					" lastError=" + load.lastError.get());
				assertTrue("Experiment " + experiment + " still running " + OUTAGE_WAIT_MS + "ms after it started", over);
				assertTrue("Partitions not owned by an answering node for " + longestUnownedMs[0] + "ms during the outage, " +
					"bound " + OWNERSHIP_GAP_MS + "ms", longestUnownedMs[0] <= OWNERSHIP_GAP_MS);
				assertTrue("A command took " + maxCommandMs + "ms to succeed during the outage, bound " + COMMAND_BOUND_MS +
					"ms", maxCommandMs <= COMMAND_BOUND_MS);

				env.ctl(EXPERIMENT_WAIT_MS, "experiment", experiment, "--wait").assertSucceeded();
				ended = true;
				env.waitReady();
				env.waitHealthy();
				L5Client.awaitRouting("after", client, foundSize, RECOVERY_MS);
				System.out.println("L5-RESOLVED when=after name=" + env.endpointName + " addresses=" +
					resolved(env.endpointName) + " clientHosts=" + hosts(client));
				L5Client.assertEveryPartitionServed("after", client, SET);
			}
		}
	}

	/**
	 * Return the zone of the endpoint address that most of the client's nodes are on.
	 */
	private String outageZone(AerospikeClient client) {
		Map<String,Integer> nodesByZone = new TreeMap<>();

		for (Node node : client.getNodes()) {
			String address = node.getAddress().getAddress().getHostAddress();
			String zone = env.ctl(L5Environment.CTL_TIMEOUT_MS, "endpoint-zone", address).assertSucceeded()
				.first("L5-ENDPOINT-ZONE").get("zone");
			System.out.println("L5-CLIENT-ENDPOINT node=" + node.getName() + " address=" + address + " zone=" + zone);
			nodesByZone.merge(zone, 1, Integer::sum);
		}
		assertTrue("Client has no nodes", ! nodesByZone.isEmpty());
		return Collections.max(nodesByZone.entrySet(), Map.Entry.comparingByValue()).getKey();
	}

	private String status() {
		return env.ctl(L5Environment.CTL_TIMEOUT_MS, "experiment", experiment).assertSucceeded()
			.first("L5-FIS").get("status");
	}

	private static String resolved(String name) {
		try {
			return String.valueOf(L5Client.addresses(name));
		}
		catch (UnknownHostException e) {
			return "unresolved";
		}
	}

	private static List<String> hosts(AerospikeClient client) {
		List<String> hosts = new ArrayList<>();

		for (Node node : client.getNodes()) {
			hosts.add(node.getName() + '=' + node.getHost().name + ':' + node.getHost().port);
		}
		return hosts;
	}
}
