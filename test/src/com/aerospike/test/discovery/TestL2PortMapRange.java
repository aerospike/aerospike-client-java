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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.L2Topology.Joined;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3 row N-19 against the port-map-range profile of
 * test/discovery/run_l2.sh. The nodes sit on a fixed subnet and DISCOVERY_PORT_MAP_RANGE names a
 * contiguous range of node addresses, including addresses no node has yet. Toxiproxy has a
 * listener for every address in the range on DISCOVERY_PORT_MAP_PORT_BASE plus the address's last
 * octet, upstream by IP. One port-map built from that rule before the cluster scales out covers
 * every node started inside the range. Without the profile every case is skipped.
 */
public class TestL2PortMapRange extends TestBase {
	private static final String BLOCKER = " needs DISCOVERY_PORT_MAP_RANGE, DISCOVERY_PORT_MAP_PORT_BASE and " +
		"DISCOVERY_CONTROL_DIR, nodes on a fixed subnet with a listener per address of a range " +
		"(run_l2.sh L2_PROFILE=port-map-range)";
	private static final int MISS_RETRIES = 3;

	private final List<String> addedNodes = new ArrayList<>();

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		for (String node : addedNodes) {
			L2Topology.control("stop " + node);
		}

		if (! addedNodes.isEmpty()) {
			addedNodes.clear();
			L2Topology.waitClusterStable(L2Topology.nodes().size());
		}
	}

	/**
	 * N-19: a port-map pre-filled from a fixed rule (node address to listener port base plus the
	 * address's last octet) for every address of the range joins the cluster with no warning. A
	 * node started on a free address of the range is an active {@link Node} within 2 x
	 * tend_interval of the peers-generation bump that announces it, with no client configuration
	 * change, and the partition map is then complete with the new node among the owners.
	 */
	@Test
	public void n19PrefilledPortMapJoinsNodeInRange() throws Exception {
		requireRange();

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		List<String> names = L2Topology.nodeNames();
		Map<String,String> portMap = rangePortMap(nodes);
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(portMap);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, names);
			L2Topology.assertAllPartitionsOwned("N-19-before", client);
			assertEquals(List.of(), TestL2Topology.warnings);

			String address = freeAddress(nodes);
			long generation = L2Topology.peersGeneration(first);
			Map<String,String> ack = start(address);
			assertEquals(address, ack.get("address"));

			Joined joined = L2Topology.waitJoined(client, first, generation, names);
			Node node = joined.node;
			long budget = 2L * policy.tendInterval;
			int port = listenerPort(address);
			Map<String,String> info = Info.request(L2Topology.host, port, "node", "service-clear-std");
			System.out.println("L2-N19-TIMING node=" + node.getName() + " host=" + node.getHost() + " container=" +
				ack.get("container") + " address=" + address + " advertised=" + info.get("service-clear-std") +
				" mapped=" + portMap.get(address + ':' + servicePort(nodes)) + " generation=" + generation + "->" +
				joined.generation + " bumpToActiveMs=" + joined.bumpToActiveMs + " tendInterval=" + policy.tendInterval +
				" budgetMs=" + budget);
			assertEquals(info.get("node"), node.getName());
			assertEquals(port, node.getHost().port);
			assertTrue(node.getName() + " active " + joined.bumpToActiveMs + "ms after the bump, budget " + budget + "ms",
				joined.bumpToActiveMs <= budget);

			boolean owns = L2Topology.waitFor(() -> L2Topology.owners(client).containsKey(node.getName()));
			System.out.println("L2-N19-OWNERS " + L2Topology.owners(client));
			assertTrue(node.getName() + " owns no partitions", owns);
			assertEquals(names.size() + 1, L2Topology.owners(L2Topology.assertAllPartitionsOwned("N-19-after", client)).size());
		}
	}

	/**
	 * N-19 outside the range: a node started on an address the pre-filled port-map does not cover
	 * is decided by on_miss. With strict the UX-2 error names the node, its advertised endpoint and
	 * the translator on every tend and no {@link Node} is created. With the default the unchanged
	 * address, which the client cannot reach, is tried on every tend and no {@link Node} is created.
	 */
	@Test
	public void n19NodeOutsideRangeDecidedByOnMiss() throws Exception {
		requireRange();

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		List<String> names = L2Topology.nodeNames();
		Map<String,String> portMap = rangePortMap(nodes);
		String[] range = range();
		String address = prefix(range[1]) + (octet(range[1]) + 1);
		String advertised = address + ':' + servicePort(nodes);

		assertEquals(address, start(address).get("address"));
		L2Topology.waitClusterStable(nodes.size() + 1);

		String name = L2Topology.peerName(first, "peers-clear-std", advertised);
		System.out.println("L2-N19-OUTSIDE node=" + name + " advertised=" + advertised + " range=" + L2Topology.portMapRange);
		assertNotNull("No peer advertises " + advertised, name);
		assertFalse(portMap.containsKey(advertised));

		ClientPolicy strict = L2Topology.policy();
		strict.addressTranslator = new PortMapAddressTranslator(portMap);
		strict.translationOnMiss = TranslationMissMode.STRICT;
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(strict, L2Topology.seed())) {
			L2Topology.waitFor(() -> {
				assertEquals(names, L2Topology.nodeNames(client));
				return misses(name).size() >= MISS_RETRIES;
			});

			List<String> misses = misses(name);
			System.out.println("L2-N19-STRICT node=" + name + " nodes=" + L2Topology.nodeNames(client) + " missWarnings=" +
				misses.size());
			assertTrue("Missed node " + name + " not retried every tend: " + TestL2Topology.warnings, misses.size() >= MISS_RETRIES);
			assertEquals(names, L2Topology.nodeNames(client));

			String message = misses.get(0);
			System.out.println("L2-N19-STRICT-LOG " + message);
			Endpoint endpoint = new Endpoint(name, null, address, servicePort(nodes), Endpoint.SourceList.STANDARD);
			assertEquals(new AerospikeException.TranslationMiss(endpoint, "port-map").getMessage(), message);
		}

		ClientPolicy passThrough = L2Topology.policy();
		passThrough.addressTranslator = new PortMapAddressTranslator(portMap);
		assertEquals(TranslationMissMode.PASS_THROUGH, passThrough.translationOnMiss);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(passThrough, L2Topology.seed())) {
			String tried = "Add node " + address + ' ' + servicePort(nodes) + " failed";
			L2Topology.waitFor(() -> {
				assertEquals(names, L2Topology.nodeNames(client));
				return attempts(tried).size() >= MISS_RETRIES;
			});

			List<String> attempts = attempts(tried);
			System.out.println("L2-N19-PASS-THROUGH node=" + name + " nodes=" + L2Topology.nodeNames(client) +
				" attempts=" + attempts.size() + " missWarnings=" + misses(name).size());
			assertTrue("Unchanged address " + advertised + " not tried every tend: " + TestL2Topology.warnings,
				attempts.size() >= MISS_RETRIES);
			System.out.println("L2-N19-PASS-THROUGH-LOG " + attempts.get(0));
			assertEquals(names, L2Topology.nodeNames(client));
			assertEquals(List.of(), misses(name));
		}
	}

	private Map<String,String> start(String address) throws Exception {
		Map<String,String> ack = L2Topology.control("start address=" + address);
		assertNotNull(ack.get("container"));
		addedNodes.add(ack.get("container"));
		return ack;
	}

	private static void requireRange() {
		TestL2Topology.skipUnless(! L2Topology.portMapRange.isEmpty() && ! L2Topology.portMapPortBase.isEmpty() &&
			! L2Topology.controlDir.isEmpty(), "N-19" + BLOCKER);
	}

	/**
	 * Build the port-map from the rule, for every address of the range, and check that the
	 * cluster's nodes are inside the range and every listener of the range exists.
	 */
	private static Map<String,String> rangePortMap(List<L2Node> nodes) throws Exception {
		String[] range = range();
		String prefix = prefix(range[0]);
		int port = servicePort(nodes);
		Set<Integer> listeners = L2Topology.listenerPorts();
		Map<String,String> map = new LinkedHashMap<>();

		for (int octet = octet(range[0]); octet <= octet(range[1]); octet++) {
			int listener = listenerPort(prefix + octet);
			assertTrue("No listener on " + listener + " for " + prefix + octet + ": " + listeners, listeners.contains(listener));
			map.put(prefix + octet + ':' + port, L2Topology.listener(listener));
		}

		for (L2Node node : nodes) {
			System.out.println("L2-N19-NODE " + node.name + " advertised=" + node.advertised + " mapped=" +
				map.get(node.advertised.get(0)));
			assertTrue(node.advertised + " outside " + L2Topology.portMapRange, map.containsKey(node.advertised.get(0)));
		}
		System.out.println("L2-N19-PORT-MAP range=" + L2Topology.portMapRange + " portBase=" + L2Topology.portMapPortBase +
			" entries=" + map.size() + ' ' + map);
		return map;
	}

	private static String freeAddress(List<L2Node> nodes) {
		String[] range = range();
		String prefix = prefix(range[0]);
		int port = servicePort(nodes);
		List<String> used = new ArrayList<>();

		for (L2Node node : nodes) {
			used.addAll(node.advertised);
		}

		for (int octet = octet(range[0]); octet <= octet(range[1]); octet++) {
			if (! used.contains(prefix + octet + ':' + port)) {
				return prefix + octet;
			}
		}
		throw new IllegalStateException("No free address in " + L2Topology.portMapRange + ": " + used);
	}

	private static List<String> misses(String nodeName) {
		List<String> list = new ArrayList<>();

		for (String warning : TestL2Topology.warnings) {
			if (warning.contains("has no mapping for node " + nodeName)) {
				list.add(warning);
			}
		}
		return list;
	}

	private static List<String> attempts(String tried) {
		List<String> list = new ArrayList<>();

		for (String warning : TestL2Topology.warnings) {
			if (warning.startsWith(tried)) {
				list.add(warning);
			}
		}
		return list;
	}

	private static int listenerPort(String address) {
		return Integer.parseInt(L2Topology.portMapPortBase) + octet(address);
	}

	private static int servicePort(List<L2Node> nodes) {
		String advertised = nodes.get(0).advertised.get(0);
		return Integer.parseInt(advertised.substring(advertised.lastIndexOf(':') + 1));
	}

	private static String[] range() {
		String[] range = L2Topology.portMapRange.split("-");
		assertEquals(L2Topology.portMapRange, 2, range.length);
		assertEquals(L2Topology.portMapRange, prefix(range[0]), prefix(range[1]));
		return range;
	}

	private static String prefix(String address) {
		return address.substring(0, address.lastIndexOf('.') + 1);
	}

	private static int octet(String address) {
		return Integer.parseInt(address.substring(address.lastIndexOf('.') + 1));
	}
}
