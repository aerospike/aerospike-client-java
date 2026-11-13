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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
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
 * Scaling the AerospikeCluster under a running pass-through client, and the per-pod Service
 * NodePorts across pod rescheduling and recreation, on the shared endpoint the operator renders:
 * every pod advertises the endpoint hostname with its own Service NodePort as alternate address.
 * Under networkLoadBalancer the same holds with one pod per node, and the advertised address
 * survives a rescheduled pod and a recreated Node.
 */
public class TestK8sScale extends TestBase {
	private static final String SET = "l3-scale";
	private static final int MIN_PODS = 2;
	private static final long ROUTING_WAIT_MS = 120000;
	private static final int LOAD_THREADS = 4;
	private static final int LOAD_KEYS = 1000;
	private static final int QUIET_TEND_INTERVALS = 5;
	private static final int KEY_SEARCH_LIMIT = 100000;
	private static final long NODE_REGISTER_TIMEOUT_MS = 300000;
	private static final String NODE_MARK = "l3-discovery-mark";
	private static final String SHARED_ENDPOINT_PROFILE = " needs every pod advertising one endpoint hostname: " +
		"configuredIP with multiPodPerHost: true and every node labelled with it, or networkLoadBalancer, the clusters " +
		"setup-kind-ako builds (run_l3.sh default, nlb or nlb-per-pod profile, or the discovery-k8s workflow)";
	private static final String NLB_PROFILE = " needs alternateAccess networkLoadBalancer (run_l3.sh nlb or nlb-per-pod " +
		"profile, or the l3-nlb jobs of the discovery-k8s workflow)";

	private K8sCluster cluster;
	private int foundSize;
	private String pinnedService;
	private String heldService;

	@Before
	public void requireCluster() {
		assertTrue("Kubernetes scale " + K8sCluster.SKIP, ! K8sCluster.kubeconfig.isEmpty());
		K8sCluster found = K8sCluster.find();
		String alternateAccess = found.alternateAccess();
		boolean multiPodPerHost = found.multiPodPerHost();
		assertTrue("Kubernetes scale" + SHARED_ENDPOINT_PROFILE + "; found alternateAccess " + alternateAccess +
			", multiPodPerHost " + multiPodPerHost,
			found.networkLoadBalancer() || (K8sCluster.CONFIGURED_IP.equals(alternateAccess) && multiPodPerHost));
		assertFalse("DISCOVERY_ENDPOINT_HOSTNAME is not set", K8sCluster.endpointHostname.isEmpty());

		if (found.networkLoadBalancer()) {
			assertEquals("loadBalancerAlternateAccessAddress", K8sCluster.endpointHostname, found.loadBalancerAddress());
		}
		cluster = found;
		foundSize = found.size();
	}

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		if (cluster == null) {
			return;
		}

		K8sCluster c = cluster;
		cluster = null;

		try {
			if (heldService != null) {
				c.deleteService(heldService);
				heldService = null;
			}

			if (pinnedService != null) {
				c.deleteService(pinnedService);
				System.out.println("L3-PINNED-DELETED service=" + pinnedService);
				pinnedService = null;
			}
		}
		finally {
			if (c.size() != foundSize || c.pods().size() != foundSize) {
				c.scale(foundSize);
			}
		}
	}

	/**
	 * One client seeded once holds the pod the operator adds within 2 x tend interval of the
	 * later of the peers-generation bump that announces it and the pod turning Ready (its Service
	 * NodePort only routes to a Ready pod), at the endpoint hostname and the new pod's own Service
	 * NodePort, without being rebuilt or reseeded, and every partition is owned afterwards.
	 */
	@Test
	public void scaleUpNewNodeActiveWithinTwoTendIntervals() throws Exception {
		int size = foundSize;
		int port = cluster.servicePort();
		Set<String> before = podNames(cluster.pods());
		ClientPolicy policy = policy();
		Host[] seeds = Host.parseHosts(args.host, args.port);
		boolean spread = ! cluster.multiPodPerHost();

		assertNull(policy.seedCandidateProvider);

		try (AerospikeClient client = new AerospikeClient(policy, seeds)) {
			awaitRouting("scale-up-before", client, size);

			Map<String,Node> existing = new HashMap<>();

			for (Node node : client.getNodes()) {
				existing.put(node.getName(), node);
			}

			Node observer = client.getNodes()[0];
			long generation = peersGeneration(observer);
			long[] bump = {0};
			long[] bumpAt = {0};
			long[] ready = {0};
			long[] active = {0};
			Node[] added = {null};

			cluster.resize(size + 1);
			boolean seen = K8sCluster.waitFor(() -> {
				if (spread && active[0] == 0) {
					cluster.serveNodePortsOnEveryNode();
				}

				long now = System.nanoTime();

				if (bump[0] == 0 && peersGeneration(observer) != generation) {
					bump[0] = now;
					bumpAt[0] = System.currentTimeMillis();
				}

				if (ready[0] == 0 && ! before.containsAll(cluster.readyPods())) {
					ready[0] = now;
				}

				if (active[0] == 0) {
					for (Node node : client.getNodes()) {
						if (node.isActive() && ! existing.containsKey(node.getName())) {
							active[0] = now;
							added[0] = node;
						}
					}
				}
				return bump[0] != 0 && ready[0] != 0 && active[0] != 0;
			}, K8sCluster.SCALE_TIMEOUT_MS, 5);
			assertTrue("No peers-generation bump on " + observer.getName() + ", no new Ready pod or no new active node",
				seen);

			long bumpToReady = TimeUnit.NANOSECONDS.toMillis(ready[0] - bump[0]);
			long elapsed = Math.max(0, TimeUnit.NANOSECONDS.toMillis(active[0] - Math.max(bump[0], ready[0])));
			long budget = 2L * policy.tendInterval;
			cluster.scale(size + 1);

			Pod pod = null;

			for (Pod p : cluster.pods()) {
				if (! before.contains(p.name)) {
					pod = p;
				}
			}
			assertNotNull("No new pod in " + cluster.pods(), pod);

			int nodePort = cluster.nodePort(pod.name, port);
			Node node = added[0];
			Map<String,String> info = Info.request(null, node, "service-clear-std", "service-clear-alt");
			Host h = node.getHost();
			System.out.println("L3-SCALE-UP node=" + node.getName() + " pod=" + pod.name + " access=" +
				info.get("service-clear-std") + " alternate=" + info.get("service-clear-alt") + " serviceNodePort=" +
				nodePort + " clientHost=" + h.name + ':' + h.port + " generation=" + generation + "->" +
				peersGeneration(observer) + " bumpToReadyMs=" + bumpToReady + " readyToActiveMs=" + elapsed +
				" bumpToActiveMs=" + TimeUnit.NANOSECONDS.toMillis(active[0] - bump[0]) + " tendInterval=" +
				policy.tendInterval + " budgetMs=" + budget + " bumpAt=" + Instant.ofEpochMilli(bumpAt[0]) + " podReadyAt=" +
				cluster.podReadyAt(pod.name) + " readinessProbe=" + cluster.readinessProbe(pod.name) +
				" publishNotReadyAddresses=" + cluster.publishNotReadyAddresses(pod.name));
			assertTrue(pod.name + " has no Service NodePort for " + port, nodePort > 0);
			assertTrue(node.getName() + " access " + info.get("service-clear-std") + " is not " + pod,
				hasAddress(info.get("service-clear-std"), pod.ip));
			assertEquals(node.getName() + " (" + pod.name + ") advertised alternate address",
				K8sCluster.endpointHostname + ':' + nodePort, info.get("service-clear-alt"));
			assertEquals(nodePort, h.port);
			assertTrue(node.getName() + " active " + elapsed + "ms after the bump and " + pod.name + " Ready, budget " +
				budget + "ms",
				elapsed <= budget);

			awaitRouting("scale-up-after", client, size + 1);

			for (Map.Entry<String,Node> entry : existing.entrySet()) {
				assertSame(entry.getKey() + " replaced", entry.getValue(), client.getCluster().getNode(entry.getKey()));
			}
			assertServedByEveryNode("scale-up", client);
		}
	}

	/**
	 * With reads and writes running, the pod the operator removes leaves the client's node list and
	 * partition map, every partition is owned by a remaining node, and once it is gone commands
	 * succeed while the error and retry counts stay flat over several tend intervals.
	 */
	@Test
	public void scaleDownRemovesNodeWithoutRetryStorm() throws Exception {
		if (foundSize < MIN_PODS) {
			cluster.scale(MIN_PODS);
		}

		int size = cluster.size();
		List<Pod> pods = cluster.pods();
		Pod removed = highestOrdinal(pods);
		ClientPolicy policy = policy();

		try (AerospikeClient client = new AerospikeClient(policy, Host.parseHosts(args.host, args.port))) {
			awaitRouting("scale-down-before", client, size);

			Node victim = nodeOf(client, removed);
			assertNotNull("No node for " + removed, victim);
			String victimName = victim.getName();
			System.out.println("L3-SCALE-DOWN-VICTIM node=" + victimName + " pod=" + removed);

			Load load = new Load(client);

			try {
				assertTrue("Load did not start", K8sCluster.waitFor(() -> load.succeeded.get() > 0, ROUTING_WAIT_MS, 10));

				long failedBefore = load.failed.get();
				long start = System.nanoTime();
				cluster.scale(size - 1);

				Node[] masters = new Node[Node.PARTITIONS];
				boolean gone = K8sCluster.waitFor(() -> ! names(client).contains(victimName) &&
					! inPartitionMap(client, victimName) && client.getNodes().length == size - 1 &&
					unowned(client, masters) == 0, ROUTING_WAIT_MS, 50);
				long transitionMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
				long transitionFailed = load.failed.get() - failedBefore;
				System.out.println("L3-SCALE-DOWN-TRANSITION node=" + victimName + " pod=" + removed.name +
					" transitionMs=" + transitionMs + " failedCommands=" + transitionFailed + " succeededCommands=" +
					load.succeeded.get() + " lastError=" + load.lastError.get() + " nodes=" +
					names(client) + " owners=" + owners(masters));
				assertTrue(victimName + " still in the node list or partition map, or a partition unowned, " +
					ROUTING_WAIT_MS + "ms after the scale down: nodes=" + names(client), gone);

				Set<String> remaining = podNames(cluster.pods());

				for (Node node : client.getNodes()) {
					assertTrue(node.getName() + " is not active", node.isActive());
					Pod pod = podOf(cluster.pods(), node);
					assertNotNull(node.getName() + " is no remaining pod", pod);
					assertTrue(pod.name + " not in " + remaining, remaining.contains(pod.name));
				}
				assertFalse(owners(masters).containsKey(victimName));

				long errors = errors(client);
				long retries = client.getCluster().getRetryCount();
				long failed = load.failed.get();
				long succeeded = load.succeeded.get();
				long window = (long)QUIET_TEND_INTERVALS * policy.tendInterval;
				boolean grew = K8sCluster.waitFor(() -> errors(client) != errors ||
					client.getCluster().getRetryCount() != retries || load.failed.get() != failed, window, 50);
				System.out.println("L3-SCALE-DOWN-QUIET windowMs=" + window + " errors=" + errors + "->" + errors(client) +
					" retries=" + retries + "->" + client.getCluster().getRetryCount() + " failed=" + failed + "->" +
					load.failed.get() + " succeeded=" + succeeded + "->" + load.succeeded.get() + " lastError=" +
					load.lastError.get());
				assertFalse("Errors, retries or failed commands grew after " + victimName + " left", grew);
				assertTrue("No command succeeded after " + victimName + " left", load.succeeded.get() > succeeded);
			}
			finally {
				load.close();
			}
			assertServedByEveryNode("scale-down", client);
		}
	}

	/**
	 * A pod's Service keeps its NodePort when the pod is rescheduled and is recreated when the pod
	 * is removed and added again. A separate NodePort Service pinned to pod 0 keeps its NodePort
	 * through both, no pod advertises it, and a client seeded only through it reaches every node.
	 */
	@Test
	public void nodePortsStableAcrossPodRescheduleAndRecreate() throws Exception {
		if (foundSize < MIN_PODS) {
			cluster.scale(MIN_PODS);
		}

		int size = cluster.size();
		int port = cluster.servicePort();
		List<Pod> pods = cluster.pods();
		Pod first = pods.get(0);

		for (Pod pod : pods) {
			if (K8sCluster.ordinal(pod.name) < K8sCluster.ordinal(first.name)) {
				first = pod;
			}
		}
		assertEquals(first.name + " ordinal", 0, K8sCluster.ordinal(first.name));

		int[] range = cluster.nodePortRange();
		Set<Integer> allocated = new HashSet<>(K8sCluster.allocatedNodePorts());
		int pinned = 0;

		for (int p = range[1]; p >= range[0] && pinned == 0; p--) {
			if (! allocated.contains(p)) {
				pinned = p;
			}
		}
		assertTrue("No free NodePort in " + range[0] + '-' + range[1], pinned > 0);

		String service = cluster.name + "-pinned-seed";
		pinnedService = service;
		cluster.createPodNodePortService(service, first.name, port, pinned);
		System.out.println("L3-PINNED service=" + service + " pod=" + first.name + " nodePort=" + pinned +
			" range=" + range[0] + '-' + range[1] + " allocated=" + allocated.size());
		assertEquals(pinned, cluster.nodePort(service, port));
		assertPinnedSeed("created", service, port, pinned, size);

		String podUid = cluster.uid("pod", first.name);
		String serviceUid = cluster.uid("service", first.name);
		int nodePort = cluster.nodePort(first.name, port);
		cluster.deletePod(first.name);
		String name = first.name;
		boolean rescheduled = K8sCluster.waitFor(() -> {
			String uid = cluster.uid("pod", name);

			if (uid.isEmpty() || uid.equals(podUid)) {
				return false;
			}

			for (Pod pod : cluster.pods()) {
				if (pod.name.equals(name)) {
					return "Running".equals(pod.phase);
				}
			}
			return false;
		}, K8sCluster.SCALE_TIMEOUT_MS, 2000);
		assertTrue(name + " not Running again within " + K8sCluster.SCALE_TIMEOUT_MS + "ms", rescheduled);
		cluster.scale(size);
		System.out.println("L3-POD-RESCHEDULED pod=" + name + " podUid=" + podUid + "->" + cluster.uid("pod", name) +
			" serviceUid=" + serviceUid + "->" + cluster.uid("service", name) + " nodePort=" + nodePort + "->" +
			cluster.nodePort(name, port));
		assertEquals(name + " Service UID", serviceUid, cluster.uid("service", name));
		assertEquals(name + " Service NodePort", nodePort, cluster.nodePort(name, port));
		assertEquals(pinned, cluster.nodePort(service, port));
		assertPinnedSeed("rescheduled", service, port, pinned, size);

		Pod last = highestOrdinal(cluster.pods());
		String lastUid = cluster.uid("service", last.name);
		int lastNodePort = cluster.nodePort(last.name, port);
		cluster.scale(size - 1);
		String lastName = last.name;
		assertTrue(lastName + " Service not deleted within " + K8sCluster.SCALE_TIMEOUT_MS + "ms",
			K8sCluster.waitFor(() -> cluster.uid("service", lastName).isEmpty(), K8sCluster.SCALE_TIMEOUT_MS, 1000));

		// Hold the old NodePort so the allocator cannot hand it back to the recreated Service by chance.
		String held = cluster.name + "-held-node-port";
		heldService = held;
		cluster.createPodNodePortService(held, last.name, port, lastNodePort);
		cluster.scale(size);
		String recreatedUid = cluster.uid("service", last.name);
		int recreatedNodePort = cluster.nodePort(last.name, port);
		cluster.deleteService(held);
		heldService = null;
		System.out.println("L3-POD-RECREATED pod=" + last.name + " serviceUid=" + lastUid + "->(deleted)->" +
			recreatedUid + " oldNodePort=" + lastNodePort + " newNodePort=" + recreatedNodePort);
		assertTrue(last.name + " has no Service after it was recreated", ! recreatedUid.isEmpty());
		assertNotEquals(last.name + " Service not recreated", lastUid, recreatedUid);
		assertTrue(last.name + " has no Service NodePort for " + port, recreatedNodePort > 0);
		assertNotEquals(last.name + " recreated Service kept NodePort " + lastNodePort, lastNodePort, recreatedNodePort);
		assertEquals(pinned, cluster.nodePort(service, port));
		assertPinnedSeed("recreated", service, port, pinned, size);
	}

	/**
	 * Under networkLoadBalancer, a pod deleted and recreated by its StatefulSet turns Ready again
	 * advertising loadBalancerAlternateAccessAddress with the same Service NodePort, and the
	 * client seeded once routes every partition again without being rebuilt or reseeded.
	 */
	@Test
	public void advertisedAddressSurvivesPodReschedule() throws Exception {
		skipUnless(cluster.networkLoadBalancer(), "Pod reschedule" + NLB_PROFILE);

		if (foundSize < MIN_PODS) {
			cluster.scale(MIN_PODS);
		}

		int size = cluster.size();
		int port = cluster.servicePort();
		String address = cluster.loadBalancerAddress();
		Pod victim = highestOrdinal(cluster.pods());
		String podUid = cluster.uid("pod", victim.name);
		int nodePort = cluster.nodePort(victim.name, port);
		assertTrue(victim.name + " has no Service NodePort for " + port, nodePort > 0);
		Host[] seeds = Host.parseHosts(args.host, args.port);

		try (AerospikeClient client = new AerospikeClient(policy(), seeds)) {
			awaitRouting("reschedule-before", client, size);
			Node before = nodeOf(client, victim);
			assertNotNull("No node for " + victim, before);
			assertEquals(victim.name + " advertised alternate address", address + ':' + nodePort,
				Info.request(null, before, "service-clear-alt"));

			cluster.deletePod(victim.name);
			Pod pod = awaitReady(victim.name, podUid);
			Node node = awaitNodeOf(client, pod);
			awaitRouting("reschedule-after", client, size);
			String advertised = Info.request(null, node, "service-clear-alt");
			Host h = node.getHost();
			System.out.println("L3-RESCHEDULED pod=" + pod.name + " podUid=" + podUid + "->" +
				cluster.uid("pod", pod.name) + " worker=" + victim.node + "->" + pod.node + " ip=" + victim.ip + "->" +
				pod.ip + " node=" + before.getName() + "->" + node.getName() + " alternate=" + advertised +
				" serviceNodePort=" + nodePort + "->" + cluster.nodePort(pod.name, port) + " clientHost=" + h.name + ':' +
				h.port + " seeds=" + List.of(seeds));
			assertEquals(pod.name + " Service NodePort", nodePort, cluster.nodePort(pod.name, port));
			assertEquals(node.getName() + " (" + pod.name + ") advertised alternate address", address + ':' + nodePort,
				advertised);
			assertEquals(node.getName() + " client port", nodePort, h.port);
			assertServedByEveryNode("reschedule", client);
		}
	}

	/**
	 * Under networkLoadBalancer, a pod on a Kubernetes Node that is deleted and registered again,
	 * and so carries none of the labels set through the API, turns Ready again advertising
	 * loadBalancerAlternateAccessAddress with its Service NodePort, and the client seeded once
	 * routes every partition. kind's kubelet does not register a deleted Node again on its own;
	 * test/discovery/run_l3.sh restarts it. The pod must run on a node the endpoint hostname does
	 * not resolve to: deleting the one node of a single-node cluster deletes every pod with its
	 * work directory, and the operator does not recover the cluster from that.
	 */
	@Test
	public void podOnRecreatedNodeAdvertisesClusterAddress() throws Exception {
		skipUnless(cluster.networkLoadBalancer(), "Recreated Node" + NLB_PROFILE);

		if (foundSize < MIN_PODS) {
			cluster.scale(MIN_PODS);
		}

		int size = cluster.size();
		int port = cluster.servicePort();
		String address = cluster.loadBalancerAddress();
		Pod victim = awayFromEndpoint(cluster.pods());
		skipUnless(victim != null, "Recreated Node needs a pod on a node " + K8sCluster.endpointHostname +
			" does not resolve to (run_l3.sh nlb-per-pod profile, or the l3-nlb-per-pod job of the discovery-k8s workflow)");
		String worker = victim.node;
		String podUid = cluster.uid("pod", victim.name);
		String nodeUid = K8sCluster.nodeUid(worker);
		K8sCluster.labelNode(worker, NODE_MARK, "set-through-api");
		assertEquals(worker + " " + NODE_MARK, "set-through-api", K8sCluster.nodeLabel(worker, NODE_MARK));
		Host[] seeds = Host.parseHosts(args.host, args.port);

		List<String> cordoned = new ArrayList<>();

		try (AerospikeClient client = new AerospikeClient(policy(), seeds)) {
			awaitRouting("node-before", client, size);

			// Otherwise the replacement pod schedules onto another node before this one registers again.
			for (String other : K8sCluster.nodeNames()) {
				if (! other.equals(worker)) {
					K8sCluster.cordon(other, true);
					cordoned.add(other);
				}
			}

			long deleted = System.nanoTime();
			K8sCluster.deleteNode(worker);
			// Pod garbage collection races the kubelet restart that registers the Node again.
			cluster.forceDeletePod(victim.name);
			String[] uid = {""};
			boolean registered = K8sCluster.waitFor(() -> {
				uid[0] = K8sCluster.nodeUid(worker);
				return ! uid[0].isEmpty() && ! uid[0].equals(nodeUid);
			}, NODE_REGISTER_TIMEOUT_MS, 1000);
			long registeredMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - deleted);
			assertTrue(worker + " not registered again within " + NODE_REGISTER_TIMEOUT_MS + "ms", registered);

			Pod pod = awaitReady(victim.name, podUid);
			assertEquals(pod.name + " node", worker, pod.node);
			assertTrue("Not every pod Ready within " + K8sCluster.SCALE_TIMEOUT_MS + "ms: " + cluster.pods(),
				K8sCluster.waitFor(() -> cluster.readyPods().size() == size, K8sCluster.SCALE_TIMEOUT_MS, 2000));

			Node node = awaitNodeOf(client, pod);
			awaitRouting("node-after", client, size);

			int nodePort = cluster.nodePort(pod.name, port);
			String advertised = Info.request(null, node, "service-clear-alt");
			System.out.println("L3-NODE-RECREATED worker=" + worker + " nodeUid=" + nodeUid + "->" + uid[0] +
				" registeredAfterMs=" + registeredMs + " mark=" + K8sCluster.nodeLabel(worker, NODE_MARK) + " label=" +
				K8sCluster.nodeLabel(worker) + " pod=" + pod.name + " podUid=" + podUid + "->" +
				cluster.uid("pod", pod.name) + " podWorker=" + pod.node + " node=" + node.getName() + " alternate=" +
				advertised + " serviceNodePort=" + nodePort);
			assertEquals(worker + " " + NODE_MARK + " after it registered again", "",
				K8sCluster.nodeLabel(worker, NODE_MARK));
			assertEquals(worker + " " + K8sCluster.ALTERNATE_ACCESS_LABEL, "", K8sCluster.nodeLabel(worker));
			assertTrue(pod.name + " has no Service NodePort for " + port, nodePort > 0);
			assertEquals(node.getName() + " (" + pod.name + ") advertised alternate address", address + ':' + nodePort,
				advertised);
			assertServedByEveryNode("node-recreated", client);
		}
		finally {
			for (String other : cordoned) {
				K8sCluster.cordon(other, false);
			}
		}
	}

	/**
	 * Return the highest-ordinal pod on a worker the endpoint hostname does not resolve to, so
	 * deleting that node leaves the seed and the other pods running, or null when there is none.
	 */
	private static Pod awayFromEndpoint(List<Pod> pods) throws Exception {
		Set<String> endpoint = new HashSet<>();

		for (InetAddress a : InetAddress.getAllByName(K8sCluster.endpointHostname)) {
			endpoint.add(a.getHostAddress());
		}

		List<Pod> away = new ArrayList<>();

		for (Pod pod : pods) {
			if (! endpoint.contains(K8sCluster.nodeAddress(pod.node)) && ! K8sCluster.controlPlane(pod.node)) {
				away.add(pod);
			}
		}
		return away.isEmpty() ? null : highestOrdinal(away);
	}

	/**
	 * Wait until one of the client's nodes answers from the pod's IP, then return it. A node the
	 * client held before the pod was replaced can stay active for a few tends while its
	 * connections fail.
	 */
	private static Node awaitNodeOf(AerospikeClient client, Pod pod) throws Exception {
		Node[] node = {null};
		boolean found = K8sCluster.waitFor(() -> {
			for (Node n : client.getNodes()) {
				try {
					if (hasAddress(Info.request(null, n, "service-clear-std"), pod.ip)) {
						node[0] = n;
						return true;
					}
				}
				catch (AerospikeException e) {
					// The node the client held before the pod was replaced.
				}
			}
			return false;
		}, ROUTING_WAIT_MS, 500);
		assertTrue("No node answers from " + pod + " within " + ROUTING_WAIT_MS + "ms: " + names(client), found);
		return node[0];
	}

	/**
	 * Wait until the pod exists with a UID other than {@code oldUid} and is Ready, then return it.
	 */
	private Pod awaitReady(String name, String oldUid) throws Exception {
		boolean ready = K8sCluster.waitFor(() -> {
			String uid = cluster.uid("pod", name);
			return ! uid.isEmpty() && ! uid.equals(oldUid) && cluster.readyPods().contains(name);
		}, K8sCluster.SCALE_TIMEOUT_MS, 2000);
		assertTrue(name + " not Ready again within " + K8sCluster.SCALE_TIMEOUT_MS + "ms: " + cluster.pods(), ready);

		for (Pod pod : cluster.pods()) {
			if (pod.name.equals(name)) {
				return pod;
			}
		}
		throw new AssertionError(name + " not in " + cluster.pods());
	}

	/**
	 * Assert that no pod advertises the pinned NodePort, every pod advertises its own Service
	 * NodePort, and a client seeded only at the endpoint hostname and the pinned NodePort holds one
	 * node per pod, owning every partition, and serves a write and a read on each.
	 */
	private void assertPinnedSeed(String step, String service, int port, int pinned, int size) throws Exception {
		Host seed = new Host(K8sCluster.endpointHostname, pinned);

		assertTrue(service + " has no ready endpoint within " + ROUTING_WAIT_MS + "ms",
			K8sCluster.waitFor(() -> cluster.readyEndpoints(service) > 0, ROUTING_WAIT_MS, 500));

		try (AerospikeClient client = connect(seed)) {
			awaitRouting("pinned-" + step, client, size);
			List<Pod> pods = cluster.pods();

			for (Node node : client.getNodes()) {
				Pod pod = podOf(pods, node);
				assertNotNull(node.getName() + " is no pod in " + pods, pod);
				String advertised = Info.request(null, node, "service-clear-alt");
				int nodePort = cluster.nodePort(pod.name, port);
				Host h = node.getHost();
				System.out.println("L3-PINNED-NODE step=" + step + " node=" + node.getName() + " pod=" + pod.name +
					" alternate=" + advertised + " serviceNodePort=" + nodePort + " pinned=" + pinned + " clientHost=" +
					h.name + ':' + h.port);
				assertEquals(node.getName() + " (" + pod.name + ") advertised alternate address",
					K8sCluster.endpointHostname + ':' + nodePort, advertised);
				assertNotEquals(pod.name + " advertises the pinned NodePort of " + service, pinned, nodePort);
			}
			assertServedByEveryNode("pinned-" + step, client);
		}
	}

	/**
	 * Connect a client to {@code seed}, retrying until the seed accepts connections or
	 * {@link #ROUTING_WAIT_MS} has passed.
	 */
	private static AerospikeClient connect(Host seed) throws Exception {
		AerospikeClient[] client = {null};
		AerospikeException[] error = {null};
		K8sCluster.waitFor(() -> {
			try {
				client[0] = new AerospikeClient(policy(), seed);
				return true;
			}
			catch (AerospikeException.Connection e) {
				error[0] = e;
				return false;
			}
		}, ROUTING_WAIT_MS, 1000);

		if (client[0] == null) {
			throw error[0];
		}
		return client[0];
	}

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		args.setClientPolicy(policy);
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		captureLog();
		return policy;
	}

	/**
	 * Wait until the client holds {@code size} active nodes and every partition's master is one
	 * of them, then log the owners.
	 */
	private static void awaitRouting(String step, AerospikeClient client, int size) throws Exception {
		Node[] masters = new Node[Node.PARTITIONS];
		boolean routed = K8sCluster.waitFor(() -> client.getNodes().length == size && unowned(client, masters) == 0 &&
			owners(masters).size() == size, ROUTING_WAIT_MS, 50);
		System.out.println("L3-ROUTING step=" + step + " nodes=" + names(client) + " unowned=" +
			unowned(client, masters) + " owners=" + owners(masters));
		assertTrue("Client did not hold " + size + " nodes owning every partition within " + ROUTING_WAIT_MS +
			"ms: " + names(client), routed);
	}

	/**
	 * Write and read one key per node with replica MASTER, each key in a partition the node is
	 * master of, and assert the client sent both commands to that node.
	 */
	private static void assertServedByEveryNode(String step, AerospikeClient client) {
		Node[] masters = new Node[Node.PARTITIONS];
		assertEquals(0, unowned(client, masters));
		WritePolicy wp = client.copyWritePolicyDefault();
		wp.replica = Replica.MASTER;
		Policy rp = client.copyReadPolicyDefault();
		rp.replica = Replica.MASTER;

		for (Node node : client.getNodes()) {
			Key key = null;

			for (int i = 0; i < KEY_SEARCH_LIMIT && key == null; i++) {
				Key candidate = new Key(args.namespace, SET, step + '-' + i);

				if (masters[Partition.getPartitionId(candidate.digest)] == node) {
					key = candidate;
				}
			}
			assertNotNull(node.getName() + " is master of no partition", key);
			assertSame(node, Partition.write(client.getCluster(), wp, key).getNodeWrite(client.getCluster()));
			client.put(wp, key, new Bin("v", node.getName()));
			assertEquals(node.getName(), client.get(rp, key).getString("v"));
			System.out.println("L3-SERVED step=" + step + " node=" + node.getName() + " key=" + key.userKey +
				" partition=" + Partition.getPartitionId(key.digest) + " write=ok read=ok");
		}
	}

	/**
	 * Writes and reads on {@link #LOAD_THREADS} threads until closed, counting the commands that
	 * succeed and fail.
	 */
	private static final class Load implements AutoCloseable {
		final AtomicLong succeeded = new AtomicLong();
		final AtomicLong failed = new AtomicLong();
		final AtomicReference<String> lastError = new AtomicReference<>();
		private final AtomicBoolean stop = new AtomicBoolean();
		private final ExecutorService executor = Executors.newFixedThreadPool(LOAD_THREADS);
		private final List<Future<?>> futures = new ArrayList<>();

		Load(AerospikeClient client) {
			for (int t = 0; t < LOAD_THREADS; t++) {
				int thread = t;
				futures.add(executor.submit(() -> {
					for (int i = 0; ! stop.get(); i++) {
						Key key = new Key(args.namespace, SET, "load-" + ((thread + i * LOAD_THREADS) % LOAD_KEYS));

						try {
							if (i % 2 == 0) {
								client.put(null, key, new Bin("v", i));
							}
							else {
								client.get(null, key);
							}
							succeeded.incrementAndGet();
						}
						catch (RuntimeException e) {
							failed.incrementAndGet();
							lastError.set(e.getClass().getSimpleName() + ": " + e.getMessage());
						}
					}
				}));
			}
		}

		@Override
		public void close() throws Exception {
			stop.set(true);
			executor.shutdown();

			for (Future<?> future : futures) {
				future.get(ROUTING_WAIT_MS, TimeUnit.MILLISECONDS);
			}
		}
	}

	private static long errors(AerospikeClient client) {
		long errors = 0;

		for (Node node : client.getNodes()) {
			errors += node.getErrorCount() + node.getTimeoutCount();
		}
		return errors;
	}

	private static boolean inPartitionMap(AerospikeClient client, String name) {
		Partitions partitions = client.getCluster().partitionMap.get(args.namespace);

		if (partitions == null) {
			return false;
		}

		for (AtomicReferenceArray<Node> replicas : partitions.replicas) {
			for (int i = 0; i < replicas.length(); i++) {
				Node node = replicas.get(i);

				if (node != null && node.getName().equals(name)) {
					return true;
				}
			}
		}
		return false;
	}

	private static int unowned(AerospikeClient client, Node[] masters) {
		Partitions partitions = client.getCluster().partitionMap.get(args.namespace);
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = partitions == null ? null : partitions.replicas[0].get(i);

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

	private static List<String> names(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		return names;
	}

	private static Set<String> podNames(List<Pod> pods) {
		Set<String> names = new HashSet<>();

		for (Pod pod : pods) {
			names.add(pod.name);
		}
		return names;
	}

	private static Pod highestOrdinal(List<Pod> pods) {
		Pod last = pods.get(0);

		for (Pod pod : pods) {
			if (K8sCluster.ordinal(pod.name) > K8sCluster.ordinal(last.name)) {
				last = pod;
			}
		}
		return last;
	}

	/**
	 * Return the pod whose IP the node advertises as its access address, or null.
	 */
	private static Pod podOf(List<Pod> pods, Node node) {
		String access = Info.request(null, node, "service-clear-std");

		for (Pod pod : pods) {
			if (hasAddress(access, pod.ip)) {
				return pod;
			}
		}
		return null;
	}

	private static Node nodeOf(AerospikeClient client, Pod pod) {
		for (Node node : client.getNodes()) {
			if (hasAddress(Info.request(null, node, "service-clear-std"), pod.ip)) {
				return node;
			}
		}
		return null;
	}

	private static boolean hasAddress(String access, String ip) {
		if (access == null) {
			return false;
		}

		for (Host h : Host.parseServiceHosts(access)) {
			if (h.name.equals(ip)) {
				return true;
			}
		}
		return false;
	}

	private static long peersGeneration(Node node) {
		return Long.parseLong(Info.request(null, node, "peers-generation"));
	}

	private static void skipUnless(boolean condition, String reason) {
		if (! condition) {
			System.out.println("L3-SKIP " + reason);
		}
		Assume.assumeTrue(reason, condition);
	}

	private static void captureLog() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> System.out.println("CLIENT-LOG " + level + ' ' + message));
	}
}
