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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.discovery.TestL2Topology.EmptyTranslator;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3 row N-10 and the alternate list cases against the reachable profile
 * of test/discovery/run_l2.sh. The node named by DISCOVERY_REACHABLE_NODE advertises a hostname and
 * its own listener port as its access address; the hostname resolves to the proxy in the client
 * and server containers. Every other node advertises its private address in the standard list and
 * the same hostname with its own listener port in the alternate list. Without the profile every
 * case is skipped.
 */
public class TestL2ReachableNode extends TestBase {
	private static final String BLOCKER = " needs DISCOVERY_REACHABLE_NODE, a node advertising an address the " +
		"client can reach (run_l2.sh L2_PROFILE=reachable, or the discovery-l2 workflow's reachable profile)";
	private static final int MISS_RETRIES = 3;

	@After
	public void restore() {
		Log.setCallback(null);
	}

	/**
	 * N-10: on_miss unset (pass_through). port-map covers every node but the reachable one, whose
	 * advertised address is used unchanged for its {@link Node}. All nodes join, no translation
	 * miss is logged and the partition map is complete.
	 */
	@Test
	public void n10UnmappedPeerPassesThroughUnchanged() throws Exception {
		TestL2Topology.skipUnless(! L2Topology.reachableNode.isEmpty(), "N-10" + BLOCKER);

		List<L2Node> nodes = L2Topology.nodes();
		L2Node reachable = reachable(nodes);
		List<L2Node> mapped = new ArrayList<>(nodes);
		mapped.remove(reachable);
		String advertised = reachable.advertised.get(0);
		Map<String,String> portMap = L2Topology.portMapByEndpoint(mapped);

		Endpoint endpoint = new Endpoint(reachable.name, null, hostOf(advertised),
			Integer.parseInt(advertised.substring(advertised.lastIndexOf(':') + 1)), Endpoint.SourceList.STANDARD);
		assertFalse(advertised + " is mapped", new PortMapAddressTranslator(portMap).translate(endpoint).isPresent());
		assertEquals(TranslationMissMode.PASS_THROUGH, new ClientPolicy().translationOnMiss);
		System.out.println("L2-N10-ADVERTISED node=" + reachable.name + " container=" + reachable.proxy +
			" advertised=" + reachable.advertised + " portMap=" + portMap);

		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(portMap);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.host, mapped.get(0).port))) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());

			Node node = client.getCluster().getNode(reachable.name);
			Host h = node.getHost();
			System.out.println("L2-N10-NODE " + node.getName() + " host=" + h.name + ':' + h.port + " active=" + node.isActive() +
				" advertised=" + advertised + " nodes=" + L2Topology.nodeNames(client));
			assertTrue(node.isActive());
			assertAddress(advertised, h);

			Map<String,Integer> owners = L2Topology.owners(L2Topology.assertAllPartitionsOwned("N-10", client));
			assertEquals(L2Topology.nodeNames(), new ArrayList<>(owners.keySet()));
			assertEquals(List.of(), translationMisses());
		}
	}

	/**
	 * useServicesAlternate with the default translator: the nodes with an alternate address join
	 * on it with no translation miss, and the reachable node, which has none, is not added and is
	 * reported on every tend.
	 */
	@Test
	public void alternateDefaultJoinsNodesWithAlternate() throws Exception {
		requireAlternates();
		assertAlternateJoins("alternate-default", alternatePolicy());
	}

	/**
	 * useServicesAlternate, on_miss strict and an ipMap that maps the alternate hostname to itself:
	 * every alternate address is mapped, so the nodes with one join.
	 */
	@Test
	public void alternateIdentityIpMapStrictJoins() throws Exception {
		requireAlternates();

		String hostname = hostOf(alternates(L2Topology.nodes().get(0)).get(0));
		ClientPolicy policy = alternatePolicy();
		policy.ipMap = Map.of(hostname, hostname);
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertAlternateJoins("alternate-ipmap-identity-strict", policy);
	}

	/**
	 * useServicesAlternate, on_miss pass_through and a custom translator that maps nothing: every
	 * alternate address is used unchanged.
	 */
	@Test
	public void alternateEmptyCustomPassThroughJoins() throws Exception {
		requireAlternates();

		ClientPolicy policy = alternatePolicy();
		policy.addressTranslator = new EmptyTranslator();
		policy.translationOnMiss = TranslationMissMode.PASS_THROUGH;
		assertAlternateJoins("alternate-custom-empty-pass-through", policy);
	}

	/**
	 * useServicesAlternate, on_miss strict and an empty ipMap: the peer with an alternate address
	 * misses on the alternate list on every tend and only the seed is a {@link Node}.
	 */
	@Test
	public void alternateEmptyIpMapStrictDropsPeer() throws Exception {
		requireAlternates();

		ClientPolicy policy = alternatePolicy();
		policy.ipMap = new HashMap<>();
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertAlternateMisses("alternate-ipmap-empty-strict", policy, "static-map");
	}

	/**
	 * useServicesAlternate, on_miss strict and a custom translator that maps nothing: as with the
	 * empty ipMap, named by the translator class.
	 */
	@Test
	public void alternateEmptyCustomStrictDropsPeer() throws Exception {
		requireAlternates();

		ClientPolicy policy = alternatePolicy();
		policy.addressTranslator = new EmptyTranslator();
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertAlternateMisses("alternate-custom-empty-strict", policy, EmptyTranslator.class.getName());
	}

	/**
	 * useServicesAlternate seeded at the node without an alternate address: initialization fails
	 * with an error that names the node and the flag.
	 */
	@Test
	public void alternateSeedWithoutAlternateFailsInit() {
		requireAlternates();

		L2Node reachable = reachable(L2Topology.nodes());
		AerospikeException.AlternateAddressMissing e = assertThrows(AerospikeException.AlternateAddressMissing.class,
			() -> new AerospikeClient(alternatePolicy(), new Host(L2Topology.host, reachable.port)).close());

		System.out.println("L2-ALT-SEED-MISSING " + e.getClass().getName() + ": " + e.getMessage());
		assertTrue(e.getMessage(), e.getMessage().contains(reachable.name));
		assertTrue(e.getMessage(), e.getMessage().contains("useServicesAlternate"));
	}

	private static void assertAlternateJoins(String row, ClientPolicy policy) throws Exception {
		List<L2Node> nodes = L2Topology.nodes();
		L2Node reachable = reachable(nodes);
		List<L2Node> withAlternate = withAlternate(nodes);
		List<String> names = names(withAlternate);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.host, withAlternate.get(0).port))) {
			L2Topology.waitNodes(client, names);
			L2Topology.waitFor(() -> {
				assertEquals(names, L2Topology.nodeNames(client));
				return alternateMissing(reachable).size() >= MISS_RETRIES;
			});

			List<String> missing = alternateMissing(reachable);
			System.out.println("L2-ALT-JOIN " + row + " nodes=" + L2Topology.nodeNames(client) + " withoutAlternate=" +
				reachable.name + " missingErrors=" + missing.size() + " translationMisses=" + translationMisses().size());

			for (L2Node expected : withAlternate) {
				Host h = client.getCluster().getNode(expected.name).getHost();
				System.out.println("L2-ALT-NODE " + row + ' ' + expected.name + " host=" + h.name + ':' + h.port +
					" alternate=" + alternates(expected));
				assertAddress(alternates(expected).get(0), h);
			}

			assertEquals(names, L2Topology.nodeNames(client));
			assertTrue("Node " + reachable.name + " without alternate not reported every tend: " + TestL2Topology.errors,
				missing.size() >= MISS_RETRIES);
			System.out.println("L2-ALT-LOG " + row + ' ' + missing.get(0));
			assertEquals(List.of(), translationMisses());
		}
	}

	private static void assertAlternateMisses(String row, ClientPolicy policy, String translator) throws Exception {
		List<L2Node> withAlternate = withAlternate(L2Topology.nodes());
		L2Node seed = withAlternate.get(0);
		L2Node peer = withAlternate.get(1);
		String endpoint = alternates(peer).get(0);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.host, seed.port))) {
			L2Topology.waitFor(() -> {
				assertEquals(List.of(seed.name), L2Topology.nodeNames(client));
				return TestL2Topology.misses(peer).size() >= MISS_RETRIES;
			});

			List<String> misses = TestL2Topology.misses(peer);
			System.out.println("L2-ALT-MISS " + row + " peer=" + peer.name + " nodes=" + L2Topology.nodeNames(client) +
				" missWarnings=" + misses.size());
			assertTrue("Missed peer " + peer.name + " not retried every tend: " + TestL2Topology.warnings,
				misses.size() >= MISS_RETRIES);

			String message = misses.get(0);
			System.out.println("L2-ALT-MISS-LOG " + row + ' ' + message);

			for (String part : new String[] {peer.name, endpoint, "alternate", translator, "pass_through"}) {
				assertTrue(part + " not in: " + message, message.contains(part));
			}
			assertEquals(List.of(seed.name), L2Topology.nodeNames(client));
			TestL2Topology.assertSeedKeysWork(row, client, seed);
		}
	}

	private static void requireAlternates() {
		TestL2Topology.skipUnless(! L2Topology.reachableNode.isEmpty(), "Alternate list cases" + BLOCKER);
	}

	private static ClientPolicy alternatePolicy() {
		ClientPolicy policy = L2Topology.policy();
		policy.useServicesAlternate = true;
		return policy;
	}

	private static L2Node reachable(List<L2Node> nodes) {
		for (L2Node node : nodes) {
			if (node.proxy.equals(L2Topology.reachableNode)) {
				assertTrue("Reachable node " + node + " must not be the first listener", node != nodes.get(0));
				return node;
			}
		}
		throw new IllegalStateException("DISCOVERY_REACHABLE_NODE " + L2Topology.reachableNode + " has no listener: " + nodes);
	}

	private static List<L2Node> withAlternate(List<L2Node> nodes) {
		List<L2Node> list = new ArrayList<>();

		for (L2Node node : nodes) {
			if (! alternates(node).isEmpty()) {
				list.add(node);
			}
		}
		assertTrue("Fewer than two nodes with an alternate address: " + nodes, list.size() >= 2);
		return list;
	}

	private static List<String> alternates(L2Node node) {
		String reply = Info.request(L2Topology.host, node.port, "service-clear-alt");
		List<String> list = new ArrayList<>();

		if (reply != null && ! reply.isEmpty()) {
			for (Host h : Host.parseServiceHosts(reply)) {
				list.add(h.name + ':' + h.port);
			}
		}
		return list;
	}

	private static List<String> names(List<L2Node> nodes) {
		List<String> names = new ArrayList<>();

		for (L2Node node : nodes) {
			names.add(node.name);
		}
		names.sort(null);
		return names;
	}

	private static String hostOf(String endpoint) {
		return endpoint.substring(0, endpoint.lastIndexOf(':'));
	}

	/**
	 * Assert the node's host is the advertised {@code host:port}, by name or by one of the
	 * addresses the name resolves to.
	 */
	private static void assertAddress(String advertised, Host h) throws Exception {
		String name = hostOf(advertised);
		int port = Integer.parseInt(advertised.substring(advertised.lastIndexOf(':') + 1));
		List<String> addresses = new ArrayList<>();

		for (InetAddress address : InetAddress.getAllByName(name)) {
			addresses.add(address.getHostAddress());
		}
		assertTrue(h.name + " is not " + name + addresses, h.name.equals(name) || addresses.contains(h.name));
		assertEquals(port, h.port);
	}

	private static List<String> alternateMissing(L2Node node) {
		List<String> list = new ArrayList<>();

		for (String error : TestL2Topology.errors) {
			if (error.contains("Node " + node.name) && error.contains("useServicesAlternate")) {
				list.add(error);
			}
		}
		return list;
	}

	private static List<String> translationMisses() {
		List<String> list = new ArrayList<>();

		for (String message : TestL2Topology.warnings) {
			if (message.contains("has no mapping")) {
				list.add(message);
			}
		}

		for (String message : TestL2Topology.errors) {
			if (message.contains("has no mapping")) {
				list.add(message);
			}
		}
		return list;
	}
}
