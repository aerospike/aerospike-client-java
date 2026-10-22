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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.Joined;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.discovery.L2Topology.Peer;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3 rows N-15, N-16 and N-18 against the pass-through profiles of
 * test/discovery/run_l2.sh. In pass-through-names (Branch A) each node advertises its own
 * alternate-access-address hostname with its own listener port; in pass-through-ports (Branch B)
 * every node advertises one hostname with its own listener port. Every hostname resolves to the
 * one Toxiproxy container in the client container, so every listener shares one IP address and the
 * nodes differ only by port. DISCOVERY_PASS_THROUGH_BRANCH names the branch; without it every case
 * is skipped.
 */
public class TestL2PassThrough extends TestBase {
	private static final String SET = "l2-pass-through";
	private static final int KEY_COUNT = 300;
	private static final int COLLISION_RETRIES = 3;
	private static final String BRANCH_A = " needs DISCOVERY_PASS_THROUGH_BRANCH=A, each node advertising its own " +
		"alternate-access-address hostname (run_l2.sh L2_PROFILE=pass-through-names)";
	private static final String BRANCH_B = " needs DISCOVERY_PASS_THROUGH_BRANCH=B and DISCOVERY_CONTROL_DIR, every " +
		"node advertising one alternate-access-address hostname with its own port (run_l2.sh L2_PROFILE=pass-through-ports)";

	private final List<String> addedProxies = new ArrayList<>();
	private final List<String> addedNodes = new ArrayList<>();

	public static final class SingleAddressTranslator implements AddressTranslator {
		private final Host target;

		public SingleAddressTranslator(Host target) {
			this.target = target;
		}

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.of(target);
		}
	}

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		for (String proxy : addedProxies) {
			L2Topology.removeListener(proxy);
		}
		addedProxies.clear();

		for (String node : addedNodes) {
			L2Topology.control("stop " + node);
		}

		if (! addedNodes.isEmpty()) {
			addedNodes.clear();
			L2Topology.waitClusterStable(L2Topology.nodes().size());
		}
	}

	/**
	 * N-15: pass-through, Branch A. Each node advertises its own alternate-access-address hostname
	 * and its own listener port, and the client reads the alternate list through
	 * {@link PassThroughAddressTranslator} with no map. Every partition owner is a distinct
	 * {@link Node} at the address its node advertises, and writes and reads are served by the owner
	 * without a server proxy hop. One Toxiproxy container serves every listener, so every hostname
	 * resolves to the same IP address and the nodes differ only by port.
	 */
	@Test
	public void n15DistinctNamesRouteToEveryOwner() throws Exception {
		TestL2Topology.skipUnless("A".equals(L2Topology.passThroughBranch), "N-15" + BRANCH_A);

		List<L2Node> nodes = L2Topology.nodes();
		Set<String> hostnames = new HashSet<>();

		for (L2Node node : nodes) {
			String alternate = alternate(node);
			System.out.println("L2-N15-ADVERTISED " + node.name + " standard=" + node.advertised + " alternate=" + alternate);
			hostnames.add(hostOf(alternate));
			assertEquals(node.port, portOf(alternate));
		}
		assertEquals("Hostnames " + hostnames, nodes.size(), hostnames.size());
		assertRoutesToOwners("N-15", passThroughPolicy(), nodes);
	}

	/**
	 * N-16: pass-through, Branch B. Every node advertises the same alternate-access-address
	 * hostname and its own alternate-access-port, the client reads the alternate list through
	 * {@link PassThroughAddressTranslator} with no map, and the partition map is complete with
	 * every owner a distinct {@link Node} served without a server proxy hop. The server resolves the
	 * hostname at startup, so run_l2.sh also makes it resolve in the server containers.
	 */
	@Test
	public void n16SharedNameRoutesToEveryOwner() throws Exception {
		requireBranchB("N-16");

		List<L2Node> nodes = L2Topology.nodes();
		Set<String> hostnames = new HashSet<>();

		for (L2Node node : nodes) {
			String alternate = alternate(node);
			System.out.println("L2-N16-ADVERTISED " + node.name + " standard=" + node.advertised + " alternate=" + alternate);
			hostnames.add(hostOf(alternate));
			assertEquals(node.port, portOf(alternate));
		}
		assertEquals("Hostnames " + hostnames, 1, hostnames.size());
		assertRoutesToOwners("N-16", passThroughPolicy(), nodes);
	}

	/**
	 * N-16 with on_miss strict: {@link PassThroughAddressTranslator} never misses, so routing is
	 * the same as with the default.
	 */
	@Test
	public void n16SharedNameStrictRoutesToEveryOwner() throws Exception {
		requireBranchB("N-16 strict");

		ClientPolicy policy = passThroughPolicy();
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertRoutesToOwners("N-16-strict", policy, L2Topology.nodes());
	}

	/**
	 * N-16 scale-out: a node started mid-test that advertises the same hostname and its own port is
	 * an active {@link Node} within 2 x tend_interval of the peers-generation bump that announces
	 * it, with no client change. The test gives it a listener on that port before it joins.
	 */
	@Test
	public void n16NewNodeOnSharedNameActiveWithinTwoTendIntervals() throws Exception {
		requireBranchB("N-16 scale-out");

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		List<String> names = L2Topology.nodeNames();
		String hostname = hostOf(alternate(first));
		ClientPolicy policy = passThroughPolicy();

		try (AerospikeClient client = new AerospikeClient(policy, seed(first))) {
			L2Topology.waitNodes(client, names);
			L2Topology.assertAllPartitionsOwned("N-16-before", client);

			long generation = L2Topology.peersGeneration(first);
			int port = L2Topology.nextListenerPort();
			String container = start("alternate-port=" + port);
			L2Topology.addListener(container, port, container + ':' + L2Topology.upstreamPort(first.proxy));
			addedProxies.add(container);
			assertEquals("peers-generation bumped before " + container + " had a listener", generation,
				L2Topology.peersGeneration(first));

			Joined joined = L2Topology.waitJoined(client, first, generation, names);
			Node node = joined.node;
			long budget = 2L * policy.tendInterval;
			Map<String,String> info = Info.request(L2Topology.host, port, "node", "service-clear-alt");
			System.out.println("L2-N16-TIMING node=" + node.getName() + " host=" + node.getHost() + " container=" + container +
				" advertised=" + info.get("service-clear-alt") + " generation=" + generation + "->" + joined.generation +
				" bumpToActiveMs=" + joined.bumpToActiveMs + " tendInterval=" + policy.tendInterval + " budgetMs=" + budget);
			assertEquals(info.get("node"), node.getName());
			assertTrue(hostname + ':' + port + " not in " + info, info.get("service-clear-alt").contains(hostname + ':' + port));
			assertEquals(port, node.getHost().port);
			assertTrue(node.getName() + " active " + joined.bumpToActiveMs + "ms after the bump, budget " + budget + "ms",
				joined.bumpToActiveMs <= budget);

			boolean owns = L2Topology.waitFor(() -> L2Topology.owners(client).containsKey(node.getName()));
			System.out.println("L2-N16-OWNERS " + L2Topology.owners(client));
			assertTrue(node.getName() + " owns no partitions", owns);
		}
	}

	/**
	 * N-18 on pass-through: two nodes advertise the same alternate-access-address and
	 * alternate-access-port. Seeded through a node that does not collide, neither colliding node
	 * becomes a {@link Node}, the {@link AerospikeException.TranslationCollision} message names both
	 * node ids, the {@code host:port} and the translator on every tend, and the other nodes keep
	 * serving their partitions. The shared port gets a listener to one of the two only once both
	 * are in the cluster, so neither was ever a {@link Node}.
	 */
	@Test
	public void n18PassThroughCollidingNodesCreateNoNode() throws Exception {
		requireBranchB("N-18 pass-through");
		assertCollidingNodesCreateNoNode("N-18-pass-through", passThroughPolicy(), "pass-through");
	}

	/**
	 * N-18 with the default translator: as on pass-through, named static-map.
	 */
	@Test
	public void n18StaticMapCollidingNodesCreateNoNode() throws Exception {
		requireBranchB("N-18 static-map");

		ClientPolicy policy = L2Topology.policy();
		policy.useServicesAlternate = true;
		assertCollidingNodesCreateNoNode("N-18-static-map", policy, "static-map");
	}

	/**
	 * N-18 with a custom translator that maps every endpoint to the seed's address: no peer becomes
	 * a {@link Node}, the {@link AerospikeException.TranslationCollision} message names the peer,
	 * the seed, the address and the translator class on every tend, and the seed keeps serving its
	 * partitions.
	 */
	@Test
	public void n18CustomTranslatorCollidingNodesCreateNoNode() throws Exception {
		requireBranchB("N-18 custom");

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		Host seed = seed(first);
		Host target = new Host(InetAddress.getByName(seed.name).getHostAddress(), seed.port);
		ClientPolicy policy = L2Topology.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = new SingleAddressTranslator(target);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, seed)) {
			L2Topology.waitFor(() -> {
				assertEquals(List.of(first.name), L2Topology.nodeNames(client));

				for (L2Node peer : nodes.subList(1, nodes.size())) {
					if (collisions(peer.name).size() < COLLISION_RETRIES) {
						return false;
					}
				}
				return true;
			});

			for (L2Node peer : nodes.subList(1, nodes.size())) {
				List<String> collisions = collisions(peer.name);
				System.out.println("L2-N18-CUSTOM-RESULT peer=" + peer.name + " nodes=" + L2Topology.nodeNames(client) +
					" target=" + target + " collisionWarnings=" + collisions.size());
				assertTrue("Colliding peer " + peer.name + " not retried every tend: " + TestL2Topology.warnings,
					collisions.size() >= COLLISION_RETRIES);
				System.out.println("L2-N18-CUSTOM-LOG " + collisions.get(0));
				assertCollision(collisions.get(0), peer.name, first.name, target, SingleAddressTranslator.class.getName());
			}
			assertEquals(List.of(first.name), L2Topology.nodeNames(client));
			TestL2Topology.assertSeedKeysWork("N-18-custom", client, first);
		}
	}

	/**
	 * N-18 on port-map: two nodes are mapped to the same {@code host:port}. Seeded through the node
	 * that does not collide, neither colliding node becomes a {@link Node}, the
	 * {@link AerospikeException.TranslationCollision} message names both node ids, the
	 * {@code host:port} and the translator on every tend, and the seed keeps serving its partitions.
	 */
	@Test
	public void n18PortMapCollidingNodesCreateNoNode() throws Exception {
		requireBranchB("N-18 port-map");

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		L2Node a = nodes.get(1);
		L2Node b = nodes.get(2);
		Map<String,String> portMap = new LinkedHashMap<>(L2Topology.portMapByEndpoint(nodes));

		for (String advertised : b.advertised) {
			portMap.put(advertised, L2Topology.listener(a.port));
		}
		System.out.println("L2-N18-PORT-MAP " + portMap);

		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(portMap);
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitFor(() -> {
				assertEquals(List.of(first.name), L2Topology.nodeNames(client));
				return collisions(a.name).size() >= COLLISION_RETRIES && collisions(b.name).size() >= COLLISION_RETRIES;
			});

			String message = collisions(a.name).get(0);
			System.out.println("L2-N18-PORT-MAP-RESULT nodes=" + L2Topology.nodeNames(client) + " colliding=[" + a.name +
				", " + b.name + "] collisionWarnings=" + collisions(a.name).size() + '/' + collisions(b.name).size());
			System.out.println("L2-N18-PORT-MAP-LOG " + message);
			assertEquals(List.of(first.name), L2Topology.nodeNames(client));
			assertCollision(message, a.name, b.name, new Host(L2Topology.host, a.port), "port-map");
			TestL2Topology.assertSeedKeysWork("N-18-port-map", client, first);
		}
	}

	/**
	 * N-18 newcomer: a node started mid-test advertises the alternate-access-address and port of a
	 * node that is already a {@link Node}. No {@link Node} is created for the newcomer's endpoint,
	 * under the newcomer's name or under the name of the node that answers on it, and the collision
	 * is logged on every tend. FR-46 does not say what happens to the existing {@link Node}; the
	 * test reports it and does not assert it. The wait stops at the first extra {@link Node}, since
	 * each holds connections to the existing node.
	 */
	@Test
	public void n18PassThroughNewcomerCollidingWithNodeIsNotCreated() throws Exception {
		requireBranchB("N-18 newcomer");

		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		L2Node existing = nodes.get(nodes.size() - 1);
		List<String> names = L2Topology.nodeNames();
		String endpoint = alternate(existing);

		try (AerospikeClient client = new AerospikeClient(passThroughPolicy(), seed(first))) {
			L2Topology.waitNodes(client, names);
			L2Topology.assertAllPartitionsOwned("N-18-newcomer-before", client);
			Node before = client.getCluster().getNode(existing.name);
			TestL2Topology.captureWarnings();

			start("alternate-port=" + portOf(endpoint));
			L2Topology.waitClusterStable(nodes.size() + 1);

			List<String> advertising = peerNames(first, endpoint);
			advertising.remove(existing.name);
			System.out.println("L2-N18-NEWCOMER-ADVERTISED endpoint=" + endpoint + " existing=" + existing.name +
				" newcomer=" + advertising + " peers-clear-alt=" + Info.request(L2Topology.host, first.port, "peers-clear-alt"));
			assertEquals("Newcomers advertising " + endpoint, 1, advertising.size());

			String newcomer = advertising.get(0);
			L2Topology.waitFor(() -> collisions(newcomer).size() >= COLLISION_RETRIES ||
				client.getNodes().length > names.size());

			List<String> collisions = collisions(newcomer);
			List<String> nodeNames = L2Topology.nodeNames(client);
			Node after = client.getCluster().getNode(existing.name);
			int instances = Collections.frequency(nodeNames, existing.name);

			System.out.println("L2-N18-NEWCOMER-RESULT newcomer=" + newcomer + " nodes=" + nodeNames +
				" collisionWarnings=" + collisions.size());
			System.out.println("L2-N18-EXISTING node=" + existing.name + " kept=" + nodeNames.contains(existing.name) +
				" sameInstance=" + (before == after) + " instances=" + instances + " active=" + (after != null && after.isActive()) +
				" host=" + (after == null ? null : after.getHost()) + " owners=" + L2Topology.owners(client));
			assertFalse(newcomer + " is a Node: " + nodeNames, nodeNames.contains(newcomer));
			assertEquals("Node created for " + newcomer + "'s endpoint " + endpoint + ", answered by " + existing.name,
				names, nodeNames);
			assertTrue("No collision logged for " + newcomer + ": " + TestL2Topology.warnings, collisions.size() >= COLLISION_RETRIES);

			String message = collisions.get(0);
			System.out.println("L2-N18-NEWCOMER-LOG " + message);
			assertCollision(message, newcomer, existing.name, new Host(hostOf(endpoint), portOf(endpoint)), "pass-through");
		}
	}

	private void assertCollidingNodesCreateNoNode(String row, ClientPolicy policy, String translator) throws Exception {
		List<L2Node> nodes = L2Topology.nodes();
		L2Node first = nodes.get(0);
		List<String> names = L2Topology.nodeNames();
		String hostname = hostOf(alternate(first));
		int port = L2Topology.nextListenerPort();
		String endpoint = hostname + ':' + port;
		String container = start("alternate-port=" + port);
		start("alternate-port=" + port);
		L2Topology.waitClusterStable(nodes.size() + 2);
		L2Topology.addListener(container, port, container + ':' + L2Topology.upstreamPort(first.proxy));
		addedProxies.add(container);

		List<String> colliding = peerNames(first, endpoint);
		System.out.println("L2-" + row + "-ADVERTISED endpoint=" + endpoint + " nodes=" + colliding +
			" peers-clear-alt=" + Info.request(L2Topology.host, first.port, "peers-clear-alt"));
		assertEquals("Nodes advertising " + endpoint, 2, colliding.size());

		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, seed(first))) {
			L2Topology.waitNodes(client, names);
			L2Topology.waitFor(() -> {
				assertEquals(names, L2Topology.nodeNames(client));
				return collisions(colliding.get(0)).size() >= COLLISION_RETRIES &&
					collisions(colliding.get(1)).size() >= COLLISION_RETRIES;
			});

			String message = collisions(colliding.get(0)).get(0);
			System.out.println("L2-" + row + "-RESULT nodes=" + L2Topology.nodeNames(client) + " colliding=" + colliding +
				" collisionWarnings=" + collisions(colliding.get(0)).size() + '/' + collisions(colliding.get(1)).size());
			System.out.println("L2-" + row + "-LOG " + message);
			assertEquals(names, L2Topology.nodeNames(client));
			assertTrue("Colliding node " + colliding.get(1) + " not retried every tend: " + TestL2Topology.warnings,
				collisions(colliding.get(1)).size() >= COLLISION_RETRIES);
			assertCollision(message, colliding.get(0), colliding.get(1), new Host(hostname, port), translator);
			assertOthersServe(row, client, names);
		}
	}

	private void requireBranchB(String row) {
		TestL2Topology.skipUnless("B".equals(L2Topology.passThroughBranch) && ! L2Topology.controlDir.isEmpty(), row + BRANCH_B);
	}

	private String start(String options) throws Exception {
		String container = L2Topology.control("start " + options).get("container");
		assertNotNull(container);
		addedNodes.add(container);
		return container;
	}

	private static void assertRoutesToOwners(String row, ClientPolicy policy, List<L2Node> nodes) throws Exception {
		TestL2Topology.captureWarnings();

		try (AerospikeClient client = new AerospikeClient(policy, seed(nodes.get(0)))) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			Node[] masters = L2Topology.assertAllPartitionsOwned(row, client);
			Set<Node> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
			Collections.addAll(distinct, masters);
			Map<String,Integer> owners = L2Topology.owners(masters);
			System.out.println("L2-" + row + "-OWNERS nodes=" + distinct.size() + " partitionsPerNode=" + owners);
			assertEquals(nodes.size(), distinct.size());
			assertEquals(L2Topology.nodeNames(), new ArrayList<>(owners.keySet()));

			for (Node node : client.getNodes()) {
				Host h = node.getHost();
				String alternate = alternate(nodeOf(nodes, node.getName()));
				System.out.println("L2-" + row + "-NODE " + node.getName() + " host=" + h.name + ':' + h.port +
					" alternate=" + alternate);
				assertSame(node.getName(), node, client.getCluster().getNode(node.getName()));
				assertTrue(node.getName(), distinct.contains(node));
				assertAddress(alternate, h);
			}
			assertServedByOwners(row, client, masters, nodes);
			assertEquals(List.of(), TestL2Topology.warnings);
		}
	}

	/**
	 * Write and read keys spread across partitions with replica MASTER and assert, from each
	 * node's namespace statistics, that each node served exactly the keys it owns with no server
	 * proxy hop.
	 */
	private static void assertServedByOwners(String row, AerospikeClient client, Node[] masters, List<L2Node> nodes) {
		String[] stats = {"client_write_success", "client_read_success", "client_proxy_complete",
			"client_proxy_error", "client_proxy_timeout"};
		Map<String,Map<String,Long>> before = new TreeMap<>();

		for (L2Node node : nodes) {
			before.put(node.name, L2Topology.namespaceStats(node, stats));
		}

		Cluster cluster = client.getCluster();
		WritePolicy wp = client.copyWritePolicyDefault();
		wp.replica = Replica.MASTER;
		Policy rp = client.copyReadPolicyDefault();
		rp.replica = Replica.MASTER;
		Map<String,Long> served = new TreeMap<>();

		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(L2Topology.namespace, SET, row + '-' + i);
			Node owner = masters[Partition.getPartitionId(key.digest)];
			assertSame(owner, Partition.write(cluster, wp, key).getNodeWrite(cluster));
			assertSame(owner, Partition.read(cluster, rp, key).getNodeRead(cluster));
			client.put(wp, key, new Bin("v", i));
			assertEquals(i, client.get(rp, key).getInt("v"));
			served.merge(owner.getName(), 1L, Long::sum);
		}

		for (L2Node node : nodes) {
			Map<String,Long> after = L2Topology.namespaceStats(node, stats);
			Map<String,Long> start = before.get(node.name);
			long writes = after.get("client_write_success") - start.get("client_write_success");
			long reads = after.get("client_read_success") - start.get("client_read_success");
			long proxied = after.get("client_proxy_complete") - start.get("client_proxy_complete") +
				after.get("client_proxy_error") - start.get("client_proxy_error") +
				after.get("client_proxy_timeout") - start.get("client_proxy_timeout");
			long expected = served.getOrDefault(node.name, 0L);
			System.out.println("L2-" + row + "-SERVED node=" + node.name + " keys=" + expected + " writes=" + writes +
				" reads=" + reads + " proxied=" + proxied);
			assertTrue(node.name + " served no keys", expected > 0);
			assertEquals(node.name + " writes", expected, writes);
			assertEquals(node.name + " reads", expected, reads);
			assertEquals(node.name + " proxied", 0, proxied);
		}
	}

	/**
	 * Assert that every partition with a master in the client's map is owned by one of
	 * {@code names} and serves writes and reads, and that the partitions of the colliding nodes
	 * have no master.
	 */
	private static void assertOthersServe(String row, AerospikeClient client, List<String> names) {
		AtomicReferenceArray<Node> masters = client.getCluster().partitionMap.get(L2Topology.namespace).replicas[0];
		WritePolicy wp = client.copyWritePolicyDefault();
		wp.replica = Replica.MASTER;
		Policy rp = client.copyReadPolicyDefault();
		rp.replica = Replica.MASTER;
		int served = 0;
		int noMaster = 0;

		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(L2Topology.namespace, SET, row + '-' + i);
			Node node = masters.get(Partition.getPartitionId(key.digest));

			if (node == null) {
				noMaster++;
				continue;
			}
			assertTrue(node.getName(), names.contains(node.getName()));
			client.put(wp, key, new Bin("v", i));
			assertEquals(i, client.get(rp, key).getInt("v"));
			served++;
		}
		System.out.println("L2-" + row + "-KEYS served=" + served + " keysWithoutMaster=" + noMaster);
		assertTrue(served > 0);
		assertTrue(noMaster > 0);
	}

	private static void assertCollision(String message, String nodeA, String nodeB, Host host, String translator) {
		List<String> expected = List.of(
			new AerospikeException.TranslationCollision(nodeA, nodeB, host, translator).getMessage(),
			new AerospikeException.TranslationCollision(nodeB, nodeA, host, translator).getMessage());
		assertTrue(message + " not in " + expected, expected.contains(message));

		for (String part : new String[] {nodeA, nodeB, host.name + ':' + host.port, translator}) {
			assertTrue(part + " not in: " + message, message.contains(part));
		}
	}

	private static List<String> collisions(String nodeName) {
		List<String> list = new ArrayList<>();

		for (String warning : TestL2Topology.warnings) {
			if (warning.contains("to the same address") && warning.contains(nodeName)) {
				list.add(warning);
			}
		}
		return list;
	}

	private static List<String> peerNames(L2Node observer, String endpoint) {
		List<String> names = new ArrayList<>();

		for (Peer peer : L2Topology.parsePeers(Info.request(L2Topology.host, observer.port, "peers-clear-alt"))) {
			if (peer.endpoints.contains(endpoint)) {
				names.add(peer.name);
			}
		}
		names.sort(null);
		return names;
	}

	private static ClientPolicy passThroughPolicy() {
		ClientPolicy policy = L2Topology.policy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		return policy;
	}

	private static Host seed(L2Node node) {
		String alternate = alternate(node);
		return new Host(hostOf(alternate), portOf(alternate));
	}

	private static String alternate(L2Node node) {
		List<String> list = L2Topology.alternates(node);
		assertEquals(node.name + " alternate list " + list, 1, list.size());
		return list.get(0);
	}

	private static L2Node nodeOf(List<L2Node> nodes, String name) {
		for (L2Node node : nodes) {
			if (node.name.equals(name)) {
				return node;
			}
		}
		throw new IllegalStateException("Unknown node " + name);
	}

	private static String hostOf(String endpoint) {
		return endpoint.substring(0, endpoint.lastIndexOf(':'));
	}

	private static int portOf(String endpoint) {
		return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
	}

	/**
	 * Assert the node's host is the advertised {@code host:port}, by name or by one of the
	 * addresses the name resolves to.
	 */
	private static void assertAddress(String advertised, Host h) throws Exception {
		String name = hostOf(advertised);
		List<String> addresses = new ArrayList<>();

		for (InetAddress address : InetAddress.getAllByName(name)) {
			addresses.add(address.getHostAddress());
		}
		assertTrue(h.name + " is not " + name + addresses, h.name.equals(name) || addresses.contains(h.name));
		assertEquals(portOf(advertised), h.port);
	}
}
