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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.K8sCluster.Pod;
import com.aerospike.test.discovery.L5Client.Load;
import com.aerospike.test.discovery.L5Client.PodEndpoint;
import com.aerospike.test.util.TestBase;

/**
 * One pass-through client under a steady read and write load through a real Spot interruption of
 * the instance under one pod, then a rollout that replaces every Aerospike node. After each, the
 * cluster recovers, every replaced pod advertises the endpoint name and its own NodePort, and the
 * client holds every node, owns every partition and stops accumulating errors and retries.
 */
public class TestL5SpotInterruptAndRollout extends TestBase {
	private static final String SET = "l5-replace";
	private static final long SPOT_WAIT_MS = 25 * 60000L;
	private static final long ROLLOUT_WAIT_MS = 95 * 60000L;
	private static final long POD_REPLACED_MS = 20 * 60000L;
	private static final long CLIENT_RECOVERY_MS = 60000;
	private static final long QUIET_WAIT_MS = 60000;

	private L5Environment env;
	private K8sCluster cluster;
	private int foundSize;

	@Before
	public void requireEnvironment() {
		env = L5Environment.require();
		env.waitReady();
		env.waitHealthy();
		cluster = K8sCluster.find();
		foundSize = cluster.size();
		assertEquals("AerospikeCluster size", L5Environment.CLUSTER_SIZE, foundSize);
	}

	@After
	public void restore() {
		Log.setCallback(null);

		if (cluster != null) {
			env.restoreSize(cluster, foundSize);
		}
	}

	@Test
	public void spotInterruptionAndRolloutRecoverUnderLoad() throws Exception {
		ClientPolicy policy = env.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		L5Client.captureLog();

		try (AerospikeClient client = new AerospikeClient(policy, env.seed())) {
			L5Client.awaitRouting("start", client, foundSize, L5Client.ROUTING_WAIT_MS);
			L5Client.assertAdvertisement("start", env, client, L5Client.podEndpoints(cluster));

			try (Load load = new Load(client, SET)) {
				Pod victim = cluster.pods().get(0);
				Map<String,String[]> before = placements();
				long failed = load.failed.get();
				long start = System.nanoTime();
				env.ctl(SPOT_WAIT_MS, "spot-interrupt", victim.name, "--wait").assertSucceeded();
				String[] old = before.get(victim.name);
				boolean replaced = K8sCluster.waitFor(() -> {
					String[] now = cluster.podPlacement(victim.name);
					return ! now[0].isEmpty() && ! now[0].equals(old[0]) && ! now[1].isEmpty() && ! now[1].equals(old[1]);
				}, POD_REPLACED_MS, 5000);
				assertTrue(victim.name + " not rescheduled off " + old[1] + " within " + POD_REPLACED_MS + "ms", replaced);
				recover("spot-interrupt", client, load, before, List.of(victim.name), start, failed);

				before = placements();
				failed = load.failed.get();
				start = System.nanoTime();
				env.ctl(ROLLOUT_WAIT_MS, "rollout", "all", "--wait").assertSucceeded();
				recover("rollout", client, load, before, List.copyOf(before.keySet()), start, failed);
			}
		}
	}

	/**
	 * Wait for the cluster to be ready with every listener healthy, log each pod's old and new
	 * instance, and assert that each pod in {@code replaced} runs on a new instance, every pod
	 * advertises the endpoint name with its own NodePort, the client holds every node with every
	 * partition owned, and its error and retry counts stop growing.
	 */
	private void recover(String step, AerospikeClient client, Load load, Map<String,String[]> before,
		List<String> replaced, long start, long failedBefore) throws Exception {
		env.waitReady();
		env.waitHealthy();
		long readyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		Map<String,String[]> after = placements();

		for (Map.Entry<String,String[]> entry : before.entrySet()) {
			String[] now = after.get(entry.getKey());
			System.out.println("L5-REPLACED step=" + step + " pod=" + entry.getKey() + " oldNode=" + entry.getValue()[1] +
				" oldInstance=" + entry.getValue()[2] + " newNode=" + (now == null ? null : now[1]) + " newInstance=" +
				(now == null ? null : now[2]));
		}
		assertEquals(step + ": pods", before.keySet(), after.keySet());

		for (String pod : replaced) {
			assertNotEquals(step + ": " + pod + " instance", before.get(pod)[2], after.get(pod)[2]);
		}

		L5Client.awaitRouting(step, client, foundSize, CLIENT_RECOVERY_MS);
		Map<String,PodEndpoint> endpoints = L5Client.podEndpoints(cluster);
		L5Client.assertAdvertisement(step, env, client, endpoints);
		L5Client.assertNodesAtEndpoint(step, env, client, endpoints);
		long routedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		L5Client.awaitQuiet(step, client, load, QUIET_WAIT_MS);
		System.out.println("L5-RECOVERY step=" + step + " clusterReadyMs=" + readyMs + " clientRoutedMs=" + routedMs +
			" quietMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " failedCommands=" +
			(load.failed.get() - failedBefore) + " succeededCommands=" + load.succeeded.get() + " retries=" +
			client.getCluster().getRetryCount() + " clientRecoveryBoundMs=" + CLIENT_RECOVERY_MS + " quietBoundMs=" +
			QUIET_WAIT_MS + " lastError=" + load.lastError.get());
	}

	/**
	 * Return each pod's UID, node and instance.
	 */
	private Map<String,String[]> placements() {
		Map<String,String[]> map = new TreeMap<>();

		for (Pod pod : cluster.pods()) {
			map.put(pod.name, new String[] {cluster.uid("pod", pod.name), pod.node, K8sCluster.instance(pod.node)});
		}
		return map;
	}
}
