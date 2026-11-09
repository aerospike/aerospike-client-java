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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.util.TestBase;

/**
 * Client-side translation against the unresolved-access profile of test/discovery/run_l2.sh, whose
 * nodes configure no access address. Each node advertises only its private service address, and the
 * client maps that address to the node's Toxiproxy listener with {@link PortMapAddressTranslator}.
 * Nothing on the server depends on resolving a hostname, so this holds on servers that resolve
 * access addresses at startup and on servers that advertise them verbatim.
 */
public class TestL2PortMapNoAccessAddress extends TestBase {
	private static final String BLOCKER = "Port-map without an access address needs DISCOVERY_UNRESOLVED_HOSTNAME, " +
		"nodes configuring no access address (run_l2.sh L2_PROFILE=unresolved-access)";
	private static final String SET = "l2-no-access";
	private static final int KEY_COUNT = 300;

	/**
	 * Every node advertises no alternate address and a standard address the client cannot reach.
	 * With a port-map from each standard address to its listener, the client holds one distinct
	 * {@link Node} per server node at that listener, every partition has an active master, and
	 * writes and reads with replica MASTER are served by each partition's master without a server
	 * proxy hop.
	 */
	@Test
	public void portMapToListenersRoutesEveryPartition() throws Exception {
		TestL2Topology.skipUnless(! L2Topology.unresolvedHostname.isEmpty(), BLOCKER);

		List<L2Node> nodes = L2Topology.nodes();
		Map<String,String> portMap = L2Topology.portMapByEndpoint(nodes);
		System.out.println("L2-NO-ACCESS-PORT-MAP " + portMap);

		for (L2Node node : nodes) {
			List<String> alternates = L2Topology.alternates(node);
			System.out.println("L2-NO-ACCESS-NODE " + node + " alternate=" + alternates);
			assertEquals(node.name + " alternate list", Collections.emptyList(), alternates);
			L2Topology.assertUnreachable(node.advertised.get(0));
		}

		List<String> addresses = new ArrayList<>();

		for (InetAddress address : InetAddress.getAllByName(L2Topology.host)) {
			addresses.add(address.getHostAddress());
		}

		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(portMap);

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			Node[] masters = L2Topology.assertAllPartitionsOwned("no-access-port-map", client);
			Set<Node> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
			Collections.addAll(distinct, masters);
			Map<String,Integer> owners = L2Topology.owners(masters);
			System.out.println("L2-NO-ACCESS-OWNERS nodes=" + distinct.size() + " partitionsPerNode=" + owners);
			assertEquals(nodes.size(), distinct.size());
			assertEquals(L2Topology.nodeNames(), new ArrayList<>(owners.keySet()));

			for (Node node : client.getNodes()) {
				L2Node l2 = nodeOf(nodes, node.getName());
				Host h = node.getHost();
				System.out.println("L2-NO-ACCESS-HOST " + node.getName() + " host=" + h.name + ':' + h.port);
				assertSame(node.getName(), node, client.getCluster().getNode(node.getName()));
				assertTrue(h.name + " is not " + L2Topology.host + addresses,
					h.name.equals(L2Topology.host) || addresses.contains(h.name));
				assertEquals(l2.port, h.port);
			}
			assertServedByOwners(client, masters, nodes);
		}
	}

	private static void assertServedByOwners(AerospikeClient client, Node[] masters, List<L2Node> nodes) {
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
			Key key = new Key(L2Topology.namespace, SET, "no-access-" + i);
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
			System.out.println("L2-NO-ACCESS-SERVED node=" + node.name + " keys=" + expected + " writes=" + writes +
				" reads=" + reads + " proxied=" + proxied);
			assertTrue(node.name + " served no keys", expected > 0);
			assertEquals(node.name + " writes", expected, writes);
			assertEquals(node.name + " reads", expected, reads);
			assertEquals(node.name + " proxied", 0, proxied);
		}
	}

	private static L2Node nodeOf(List<L2Node> nodes, String name) {
		for (L2Node node : nodes) {
			if (node.name.equals(name)) {
				return node;
			}
		}
		throw new IllegalStateException("Unknown node " + name);
	}
}
