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
package com.aerospike.test.translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.util.TestEnv;

/**
 * Two node cluster of the aerospike-client-conformance port-map environment. Both nodes advertise
 * only their container address on port 3000, which the host cannot reach, and each is published on
 * its own host port. Node B's container is created but not started. The cluster arrives as the
 * environment variables of its values file:
 * <ul>
 * <li>ENV_HOST: host both nodes are published on</li>
 * <li>NODE_A_NAME, NODE_A_ADVERTISED_HOST:NODE_A_ADVERTISED_PORT, NODE_A_PORT: node A name, advertised
 * address, published port</li>
 * <li>NODE_B_NAME, NODE_B_ADVERTISED_HOST:NODE_B_ADVERTISED_PORT, NODE_B_PORT: node B name, advertised
 * address, published port</li>
 * <li>ENV_NAMESPACE: namespace, replication factor 1</li>
 * </ul>
 * Node B is started with the start-node-b verb.
 */
final class PortMapCluster {
	static final int TEND_INTERVAL = 1000;
	static final int JOIN_WAIT_MS = 30000;
	static final int START_WAIT_MS = 120000;

	static final String host = TestEnv.get("ENV_HOST");
	static final String nodeA = TestEnv.get("NODE_A_NAME");
	static final String addressA = TestEnv.get("NODE_A_ADVERTISED_HOST") + ':' + TestEnv.get("NODE_A_ADVERTISED_PORT");
	static final int portA = TestEnv.getInt("NODE_A_PORT");
	static final String nodeB = TestEnv.get("NODE_B_NAME");
	static final String addressB = TestEnv.get("NODE_B_ADVERTISED_HOST") + ':' + TestEnv.get("NODE_B_ADVERTISED_PORT");
	static final int portB = TestEnv.getInt("NODE_B_PORT");
	static final String namespace = TestEnv.get("ENV_NAMESPACE");

	static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.tendInterval = TEND_INTERVAL;
		policy.timeout = 1000;
		return policy;
	}

	static AerospikeClient client(ClientPolicy policy) {
		return new AerospikeClient(policy, new Host(host, portA));
	}

	static List<String> nodeNames(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	static void waitNodes(AerospikeClient client, int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;

		while (client.getNodes().length < count && System.currentTimeMillis() < deadline) {
			Thread.sleep(100);
		}
	}

	/**
	 * Wait until every partition of the namespace has a master in the client's partition map and
	 * return the number of partitions each node owns.
	 */
	static Map<String,Integer> assertAllPartitionsOwned(String name, AerospikeClient client) throws InterruptedException {
		long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;
		Map<String,Integer> owners;
		int unowned;

		do {
			owners = new TreeMap<>();
			unowned = 0;
			Partitions partitions = client.getCluster().partitionMap.get(namespace);

			if (partitions == null) {
				unowned = Node.PARTITIONS;
			}
			else {
				AtomicReferenceArray<Node> masters = partitions.replicas[0];

				for (int i = 0; i < Node.PARTITIONS; i++) {
					Node node = masters.get(i);

					if (node == null || ! node.isActive()) {
						unowned++;
					}
					else {
						owners.merge(node.getName(), 1, Integer::sum);
					}
				}
			}

			if (unowned == 0) {
				break;
			}
			Thread.sleep(100);
		} while (System.currentTimeMillis() < deadline);

		System.out.println("PORT-MAP-PARTITIONS " + name + " partitions=" + Node.PARTITIONS + " unowned=" + unowned +
			" owners=" + owners);
		assertEquals(0, unowned);
		return owners;
	}

	static void assertUnreachable(String address) {
		int index = address.lastIndexOf(':');
		String h = address.substring(0, index);
		int port = Integer.parseInt(address.substring(index + 1));

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(h, port), 2000);
			fail("Advertised address " + address + " is reachable from the host");
		}
		catch (IOException expected) {
			System.out.println("PORT-MAP-UNREACHABLE " + address + ' ' + expected);
		}
	}

	static boolean nodeBRunning() throws Exception {
		return TestEnv.control("node-b-state").equals("running");
	}

	/**
	 * Start node B in the background. The start-node-b verb returns once the two node cluster is
	 * stable, so a caller can watch node B join while it runs.
	 */
	static Future<String> startNodeB() {
		FutureTask<String> task = new FutureTask<>(() -> TestEnv.control("start-node-b"));
		new Thread(task, "start-node-b").start();
		return task;
	}
}
