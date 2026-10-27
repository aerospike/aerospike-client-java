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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.K8sCluster.Pod;
import com.aerospike.test.util.TestBase;

/**
 * Pass-through translation against an Aerospike cluster whose alternate access addresses the
 * Aerospike Kubernetes Operator renders. The client seeds through the endpoint in {@code -Dargs},
 * reads the alternate list with {@link PassThroughAddressTranslator}, and has no translation map,
 * no port arithmetic and no seed provider: every peer comes from what the server advertises.
 */
public class TestK8sPassThrough extends TestBase {
	private static final String SET = "l3-pass-through";
	private static final int MIN_PODS = 2;
	private static final long ROUTING_WAIT_MS = 120000;
	private static final int KEY_SEARCH_LIMIT = 100000;
	private static final String SHARED_ENDPOINT_PROFILE = " needs multiPodPerHost: true with every node labelled with " +
		"one endpoint hostname, the cluster setup-kind-ako builds (run_l3.sh default profile, or the discovery-k8s workflow)";
	private static final String PER_NODE_PROFILE = " needs multiPodPerHost: false with each kind worker labelled with " +
		"its own endpoint hostname (run_l3.sh L3_PROFILE=per-node)";

	private K8sCluster scaled;
	private int scaledFrom;

	@Before
	public void requireCluster() {
		skipUnless(! K8sCluster.kubeconfig.isEmpty(), "Kubernetes pass-through " + K8sCluster.SKIP);
	}

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		if (scaled != null) {
			K8sCluster cluster = scaled;
			scaled = null;
			cluster.scale(scaledFrom);
		}
	}

	/**
	 * Every node carries the same endpoint hostname as its configured-alternate-access-address
	 * label, so the operator advertises that hostname with each pod's own Service NodePort as
	 * alternate-access-port. Scaled to at least two pods, the client seeded through the ingress
	 * port holds one node per pod, each but the seeded one at the hostname and NodePort its pod
	 * advertises, and every partition's master is one of those nodes and serves a write and a read.
	 * The seeded node is reached a second time at its own hostname and NodePort.
	 * Each advertised port is compared with the NodePort of the pod's Service read from the API
	 * server, so the test fails if the operator stops advertising the NodePort.
	 */
	@Test
	public void sharedEndpointAdvertisesEachPodNodePortAndRoutesEveryPartition() throws Exception {
		K8sCluster cluster = K8sCluster.find();
		skipUnless(cluster.multiPodPerHost(), "Shared endpoint pass-through" + SHARED_ENDPOINT_PROFILE);
		assertFalse("DISCOVERY_ENDPOINT_HOSTNAME is not set", K8sCluster.endpointHostname.isEmpty());
		assertEquals("configuredIP", cluster.get(".spec.aerospikeNetworkPolicy.alternateAccess"));

		int size = cluster.size();

		if (size < MIN_PODS) {
			scaled = cluster;
			scaledFrom = size;
			cluster.scale(MIN_PODS);
		}

		int port = cluster.servicePort();
		List<Pod> pods = cluster.pods();
		Map<String,String> expected = new LinkedHashMap<>();
		Set<Integer> nodePorts = new HashSet<>();

		for (Pod pod : pods) {
			String label = K8sCluster.nodeLabel(pod.node);
			int nodePort = cluster.nodePort(pod.name, port);
			System.out.println("L3-POD " + pod + " label=" + label + " serviceNodePort=" + nodePort);
			assertEquals(pod.node + " label", K8sCluster.endpointHostname, label);
			assertTrue(pod.name + " has no Service NodePort for " + port, nodePort > 0);
			nodePorts.add(nodePort);
			expected.put(pod.name, K8sCluster.endpointHostname + ':' + nodePort);
		}
		assertEquals("Distinct NodePorts " + nodePorts, pods.size(), nodePorts.size());
		assertTrue("DISCOVERY_SERVICE_NODE_PORTS " + K8sCluster.serviceNodePorts + " not in " + nodePorts,
			nodePorts.containsAll(K8sCluster.serviceNodePorts));
		assertRoutesEveryPartition("shared-endpoint", pods, expected);
	}

	/**
	 * Each kind worker carries its own hostname as its configured-alternate-access-address label
	 * and runs one pod, so the operator advertises the worker's hostname with the pod's hostPort.
	 * The client holds one node per pod, each but the seeded one at its worker's hostname and port,
	 * and every partition's master is one of those nodes and serves a write and a read.
	 */
	@Test
	public void perNodeEndpointsAdvertiseEachWorkerHostnameAndRouteEveryPartition() throws Exception {
		K8sCluster cluster = K8sCluster.find();
		skipUnless(! cluster.multiPodPerHost(), "Per-node pass-through" + PER_NODE_PROFILE);
		assertEquals("configuredIP", cluster.get(".spec.aerospikeNetworkPolicy.alternateAccess"));

		int port = cluster.servicePort();
		List<Pod> pods = cluster.pods();
		assertTrue("Pods " + pods, pods.size() >= MIN_PODS);
		Map<String,String> expected = new LinkedHashMap<>();
		Set<String> workers = new HashSet<>();
		Set<String> labels = new HashSet<>();

		for (Pod pod : pods) {
			String label = K8sCluster.nodeLabel(pod.node);
			int hostPort = cluster.hostPort(pod.name, port);
			System.out.println("L3-POD " + pod + " label=" + label + " nodeAddress=" + K8sCluster.nodeAddress(pod.node) +
				" hostPort=" + hostPort + " serviceNodePort=" + cluster.nodePort(pod.name, port));
			assertFalse(pod.node + " has no " + K8sCluster.ALTERNATE_ACCESS_LABEL + " label", label.isEmpty());
			assertTrue(pod.name + " has no hostPort for " + port, hostPort > 0);
			workers.add(pod.node);
			labels.add(label);
			expected.put(pod.name, label + ':' + hostPort);
		}
		assertEquals("Workers " + workers, pods.size(), workers.size());
		assertEquals("Labels " + labels, pods.size(), labels.size());
		assertRoutesEveryPartition("per-node", pods, expected);
	}

	/**
	 * Connect with pass-through translation and assert that the client holds one node per pod, at
	 * the alternate address the pod advertises or, for the node it seeded, at the seed, that the
	 * advertised address is {@code expected}, that every partition's master is a node the client
	 * holds and serves a write and a read, and that a node held at the seed is also reachable at
	 * the address it advertises.
	 * Each node is matched to its pod by the pod IP it advertises as its access address.
	 */
	private static void assertRoutesEveryPartition(String shape, List<Pod> pods, Map<String,String> expected)
		throws Exception {
		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		Host[] seeds = Host.parseHosts(args.host, args.port);
		Map<String,String> seededAt = new LinkedHashMap<>();
		captureLog();

		try (AerospikeClient client = new AerospikeClient(policy, seeds)) {
			Node[] masters = new Node[Node.PARTITIONS];
			boolean routed = K8sCluster.waitFor(() -> client.getNodes().length == pods.size() &&
				unowned(client, masters) == 0 && owners(masters).size() == pods.size(), ROUTING_WAIT_MS, 500);
			int unowned = unowned(client, masters);
			Map<String,Integer> owners = owners(masters);
			System.out.println("L3-ROUTING shape=" + shape + " seeds=" + List.of(seeds) + " pods=" + pods.size() +
				" nodes=" + client.getNodes().length + " partitions=" + Node.PARTITIONS + " unowned=" + unowned +
				" owners=" + owners);
			assertTrue("Client did not hold " + pods.size() + " nodes owning every partition within " +
				ROUTING_WAIT_MS + "ms", routed);

			Map<String,Pod> podsByIp = new TreeMap<>();

			for (Pod pod : pods) {
				podsByIp.put(pod.ip, pod);
			}

			Set<String> matched = new HashSet<>();

			for (Node node : client.getNodes()) {
				Map<String,String> info = Info.request(null, node, "service-clear-std", "service-clear-alt");
				Pod pod = podOf(podsByIp, info.get("service-clear-std"));
				String advertised = info.get("service-clear-alt");
				Host h = node.getHost();
				System.out.println("L3-NODE shape=" + shape + " node=" + node.getName() + " pod=" +
					(pod == null ? null : pod.name) + " worker=" + (pod == null ? null : pod.node) + " access=" +
					info.get("service-clear-std") + " alternate=" + advertised + " expected=" +
					(pod == null ? null : expected.get(pod.name)) + " clientHost=" + h.name + ':' + h.port);
				assertNotNull(node.getName() + " access " + info.get("service-clear-std") + " is no pod IP in " +
					podsByIp.keySet(), pod);
				assertTrue(pod.name + " matched twice", matched.add(pod.name));
				assertEquals(node.getName() + " (" + pod.name + ") advertised alternate address",
					expected.get(pod.name), advertised);

				if (isAddress(seeds, h)) {
					seededAt.put(node.getName(), advertised);
				}
				else {
					assertAddress(advertised, h);
				}
				assertSame(node, client.getCluster().getNode(node.getName()));
			}
			assertEquals(expected.keySet(), matched);
			assertServedByEveryNode(shape, client, masters);
		}

		for (Map.Entry<String,String> entry : seededAt.entrySet()) {
			assertReachableAtAdvertised(shape, policy, entry.getKey(), entry.getValue());
		}
	}

	/**
	 * The client holds the node it seeded at the seed, so connect a second client seeded at the
	 * node's advertised address and assert that it reaches the same node there.
	 */
	private static void assertReachableAtAdvertised(String shape, ClientPolicy policy, String name, String advertised)
		throws Exception {
		int index = advertised.lastIndexOf(':');
		Host seed = new Host(advertised.substring(0, index), Integer.parseInt(advertised.substring(index + 1)));

		try (AerospikeClient client = new AerospikeClient(policy, seed)) {
			Node node = client.getCluster().getNode(name);
			Host h = node.getHost();
			System.out.println("L3-ADVERTISED shape=" + shape + " node=" + name + " seed=" + advertised +
				" clientHost=" + h.name + ':' + h.port);
			assertAddress(advertised, h);
			assertTrue(name + " at " + advertised + " is not active", node.isActive());
		}
	}

	/**
	 * Write and read one key per node with replica MASTER, each key in a partition the node is
	 * master of, and assert the client sent both commands to that node.
	 */
	private static void assertServedByEveryNode(String shape, AerospikeClient client, Node[] masters) {
		Cluster cluster = client.getCluster();
		WritePolicy wp = client.copyWritePolicyDefault();
		wp.replica = Replica.MASTER;
		Policy rp = client.copyReadPolicyDefault();
		rp.replica = Replica.MASTER;

		for (Node node : client.getNodes()) {
			Key key = null;

			for (int i = 0; i < KEY_SEARCH_LIMIT && key == null; i++) {
				Key candidate = new Key(args.namespace, SET, shape + '-' + i);

				if (masters[Partition.getPartitionId(candidate.digest)] == node) {
					key = candidate;
				}
			}
			assertNotNull(node.getName() + " is master of no partition", key);
			assertSame(node, Partition.write(cluster, wp, key).getNodeWrite(cluster));
			assertSame(node, Partition.read(cluster, rp, key).getNodeRead(cluster));
			client.put(wp, key, new Bin("v", node.getName()));
			assertEquals(node.getName(), client.get(rp, key).getString("v"));
			System.out.println("L3-SERVED shape=" + shape + " node=" + node.getName() + " key=" + key.userKey +
				" partition=" + Partition.getPartitionId(key.digest) + " write=ok read=ok");
		}
	}

	private static int unowned(AerospikeClient client, Node[] masters) {
		Partitions partitions = client.getCluster().partitionMap.get(args.namespace);
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = null;

			if (partitions != null) {
				AtomicReferenceArray<Node> replicas = partitions.replicas[0];
				node = replicas.get(i);
			}

			if (node == null || ! node.isActive() || client.getCluster().getNode(node.getName()) != node) {
				masters[i] = null;
				unowned++;
			}
			else {
				masters[i] = node;
			}
		}
		return unowned;
	}

	private static Map<String,Integer> owners(Node[] masters) {
		Map<String,Integer> owners = new TreeMap<>();

		for (Node node : masters) {
			if (node != null) {
				owners.merge(node.getName(), 1, Integer::sum);
			}
		}
		return owners;
	}

	private static Pod podOf(Map<String,Pod> podsByIp, String access) {
		if (access == null) {
			return null;
		}

		for (Host h : Host.parseServiceHosts(access)) {
			Pod pod = podsByIp.get(h.name);

			if (pod != null) {
				return pod;
			}
		}
		return null;
	}

	/**
	 * Assert the node's host is the advertised {@code host:port}, by name or by one of the
	 * addresses the name resolves to.
	 */
	private static void assertAddress(String advertised, Host h) throws Exception {
		int index = advertised.lastIndexOf(':');
		Host host = new Host(advertised.substring(0, index), Integer.parseInt(advertised.substring(index + 1)));
		assertTrue(h.name + ':' + h.port + " is not " + host.name + addresses(host.name) + ':' + host.port,
			isAddress(new Host[] {host}, h));
	}

	/**
	 * Return whether the node's host is one of {@code hosts}, by name or by one of the addresses
	 * the name resolves to. A node the client seeded is held at its seed.
	 */
	private static boolean isAddress(Host[] hosts, Host h) throws Exception {
		for (Host host : hosts) {
			if (host.port == h.port && (host.name.equals(h.name) || addresses(host.name).contains(h.name))) {
				return true;
			}
		}
		return false;
	}

	private static List<String> addresses(String name) throws Exception {
		List<String> addresses = new ArrayList<>();

		for (InetAddress address : InetAddress.getAllByName(name)) {
			addresses.add(address.getHostAddress());
		}
		return addresses;
	}

	private static void captureLog() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> System.out.println("CLIENT-LOG " + level + ' ' + message));
	}

	private static void skipUnless(boolean condition, String reason) {
		if (! condition) {
			System.out.println("L3-SKIP " + reason);
		}
		Assume.assumeTrue(reason, condition);
	}
}
