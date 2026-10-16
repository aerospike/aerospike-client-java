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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Two node cluster started by test/translation/run_port_map.sh. Both nodes advertise only their
 * container address on port 3000, which the host cannot reach, and each is published on its own
 * host port. Node B's container is created but not started. The cluster arrives as system
 * properties:
 * <ul>
 * <li>portmap.host: host both nodes are published on</li>
 * <li>portmap.nodeA, portmap.addressA, portmap.portA: node A name, advertised address, published port</li>
 * <li>portmap.nodeB, portmap.addressB, portmap.portB: node B name, advertised address, published port</li>
 * <li>portmap.containerB: node B's container, started with the docker CLI</li>
 * <li>portmap.namespace: namespace, replication factor 1</li>
 * </ul>
 */
final class PortMapCluster {
	static final int TEND_INTERVAL = 1000;
	static final int JOIN_WAIT_MS = 30000;
	static final int START_WAIT_MS = 120000;

	static final String host = required("portmap.host");
	static final String nodeA = required("portmap.nodeA");
	static final String addressA = required("portmap.addressA");
	static final int portA = Integer.parseInt(required("portmap.portA"));
	static final String nodeB = required("portmap.nodeB");
	static final String addressB = required("portmap.addressB");
	static final int portB = Integer.parseInt(required("portmap.portB"));
	static final String containerB = required("portmap.containerB");
	static final String namespace = required("portmap.namespace");

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

	static boolean containerRunning() throws Exception {
		return docker("inspect", "-f", "{{.State.Running}}", containerB).trim().equals("true");
	}

	static void startContainerB() throws Exception {
		docker("start", containerB);
	}

	private static String docker(String... args) throws Exception {
		List<String> command = new ArrayList<>();
		command.add("docker");
		command.addAll(List.of(args));

		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

		assertTrue(String.join(" ", command) + " timed out", process.waitFor(START_WAIT_MS, TimeUnit.MILLISECONDS));
		assertEquals(String.join(" ", command) + ": " + output, 0, process.exitValue());
		return output;
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/translation/run_port_map.sh");
		}
		return value;
	}
}
