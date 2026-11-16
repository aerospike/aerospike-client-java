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
package com.aerospike.client.cluster;

import static com.aerospike.client.cluster.TendDriver.stopTendThread;
import static com.aerospike.client.cluster.TendDriver.tend;
import static com.aerospike.client.cluster.TendDriver.timedTend;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.async.StubNodeServer;
import com.aerospike.client.discovery.GuardedResolverProvider;
import com.aerospike.client.policy.ClientPolicy;

/**
 * A node on a hostname with several addresses moves to another address of the hostname once it has
 * failed three tends in a row, trying one address per tend.
 */
public class HostnameAddressMoveTest {
	private static final int TEND_INTERVAL = 250;
	private static final long MOVE_BOUND = 20L * TEND_INTERVAL;
	private static final long MARGIN = 150;
	private static final String A = "BB9A000000000001";
	private static final String B = "BB9B000000000001";

	private static InetAddress first;
	private static InetAddress second;

	private final List<StubNodeServer> servers = new ArrayList<>();
	private final List<AutoCloseable> closeables = new ArrayList<>();
	private final List<String> hostnames = new ArrayList<>();
	private final List<String> moves = new ArrayList<>();
	private final List<String> replaces = new ArrayList<>();
	private final List<String> warnings = new ArrayList<>();
	private AerospikeClient client;
	private Sampler sampler;

	private enum Other {
		DIFFERENT_NAME, IP_LITERAL, SINGLE_ADDRESS, NO_FAILURES
	}

	private interface Bind {
		void bind(int port) throws IOException;
	}

	@Before
	public void setUp() throws Exception {
		first = InetAddress.getByName("127.0.0.1");
		second = InetAddress.getByName("127.0.0.2");

		if (! canBind(second)) {
			second = InetAddress.getByName("::1");
		}
		Assume.assumeTrue("Neither 127.0.0.2 nor ::1 can be bound locally", canBind(second));

		Log.setLevel(Log.Level.INFO);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.INFO && message.startsWith("Move node ")) {
				synchronized (moves) {
					moves.add(message);
				}
			}
			else if (level == Log.Level.INFO && message.startsWith("Replace node")) {
				synchronized (replaces) {
					replaces.add(message);
				}
			}
			else if (level == Log.Level.WARN) {
				synchronized (warnings) {
					warnings.add(message);
				}
			}
		});
	}

	@After
	public void tearDown() throws Exception {
		if (sampler != null) {
			sampler.stop();
		}

		if (client != null) {
			client.close();
		}

		for (StubNodeServer server : servers) {
			server.close();
		}

		for (AutoCloseable closeable : closeables) {
			closeable.close();
		}

		for (String hostname : hostnames) {
			GuardedResolverProvider.unregister(hostname);
		}
		Log.setCallback(null);
	}

	@Test
	public void failingNodeMovesToOtherAddress() throws Exception {
		String hostname = hostname("move", first, second);
		StubNodeServer live = pair(A, A)[0];
		int port = live.port();

		client = new AerospikeClient(policy(), new Host(hostname, port));
		Cluster cluster = client.getCluster();
		Node node = node(cluster, A);
		assertEquals(first, node.getAddress().getAddress());
		sampler = new Sampler(cluster);

		long begin = System.nanoTime();
		live.close();
		await(() -> onAddress(cluster, A, second));
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue("move took " + elapsed + "ms", elapsed < MOVE_BOUND);
		await(() -> ownsPartitions(cluster, A, second));
		assertNotNull(client.get(null, new Key(StubNodeServer.NAMESPACE, "set", 1)));
		assertEquals(1, cluster.getNodes().length);
		assertFalse(node.isActive());
		assertEquals(0, sampler.stop());
		assertEquals(List.of("Move node " + A + ' ' + hostname + " from 127.0.0.1 " + port + " to " +
			node(cluster, A).getHost()), moves());
	}

	@Test
	public void everyNodeOnDeadAddressMoves() throws Exception {
		String hostname = hostname("zone", first, second);
		String[] names = {A, B, "BB9C000000000001"};
		int[] ports = new int[names.length];
		List<StubNodeServer> dead = new ArrayList<>();

		for (int i = 0; i < names.length; i++) {
			StubNodeServer[] pair = pair(names[i], names[i]);
			ports[i] = pair[0].port();
			dead.add(pair[0]);

			// Only the node closed last owns partitions, so a tend between the closes keeps every node.
			if (i < names.length - 1) {
				pair[0].setOwnsPartitions(false);
				pair[1].setOwnsPartitions(false);
			}
		}

		for (int i = 0; i < names.length; i++) {
			StringBuilder sb = new StringBuilder("1,3000,[");

			for (int j = 0; j < names.length; j++) {
				if (j != i) {
					if (sb.charAt(sb.length() - 1) == ']') {
						sb.append(',');
					}
					sb.append('[').append(names[j]).append(",,[").append(hostname).append(':').append(ports[j]).append("]]");
				}
			}
			sb.append(']');

			for (StubNodeServer server : servers) {
				if (server.port() == ports[i]) {
					server.setPeers(sb.toString());
				}
			}
		}

		client = new AerospikeClient(policy(), new Host(hostname, ports[0]));
		Cluster cluster = client.getCluster();
		await(() -> cluster.getNodes().length == names.length);

		for (String name : names) {
			assertTrue(onAddress(cluster, name, first));
		}
		sampler = new Sampler(cluster);

		long begin = System.nanoTime();

		for (StubNodeServer server : dead) {
			server.close();
		}

		for (String name : names) {
			await(() -> onAddress(cluster, name, second));
		}
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue("move took " + elapsed + "ms", elapsed < MOVE_BOUND);
		await(() -> cluster.getNodes().length == names.length);
		await(() -> ownsPartitions(cluster, names[names.length - 1], second));
		assertNotNull(client.get(null, new Key(StubNodeServer.NAMESPACE, "set", 1)));
		assertEquals(0, sampler.stop());
		assertEquals(names.length, moves().size());
	}

	@Test
	public void noMoveWhenOtherAddressIsDifferentNode() throws Exception {
		otherNodeStays(Other.DIFFERENT_NAME);
	}

	@Test
	public void noMoveForIpLiteralHost() throws Exception {
		otherNodeStays(Other.IP_LITERAL);
	}

	@Test
	public void noMoveForSingleAddressHostname() throws Exception {
		otherNodeStays(Other.SINGLE_ADDRESS);
	}

	@Test
	public void noLookupForNodeWithoutFailures() throws Exception {
		otherNodeStays(Other.NO_FAILURES);
	}

	/**
	 * Node A is reached through a two-address hostname and moves when its first address dies.
	 * Node B is set up per case so that it must not move.
	 */
	private void otherNodeStays(Other other) throws Exception {
		String hostA = hostname("a", first, second);
		String hostB = (other == Other.SINGLE_ADDRESS) ? hostname("b", first) : hostname("b", first, second);
		String peerB = (other == Other.IP_LITERAL) ? "127.0.0.1" : hostB;

		StubNodeServer[] pairA = pair(A, A);
		StubNodeServer a1 = pairA[0];
		StubNodeServer a2 = pairA[1];
		StubNodeServer b1 = pair(B, other == Other.DIFFERENT_NAME ? "BB9C000000000001" : B)[0];
		int portA = a1.port();
		int portB = b1.port();

		String peersA = "1,3000,[[" + B + ",,[" + peerB + ':' + portB + "]]]";
		a1.setPeers(peersA);
		a2.setPeers(other == Other.NO_FAILURES ? peersA : "1,3000,[]");
		b1.setPeers("1,3000,[[" + A + ",,[" + hostA + ':' + portA + "]]]");

		if (other == Other.NO_FAILURES) {
			// B owns every partition, so it is kept while the failing A does not list it.
			a1.setOwnsPartitions(false);
			a2.setOwnsPartitions(false);
		}

		client = new AerospikeClient(policy(), new Host(hostA, portA));
		Cluster cluster = client.getCluster();
		await(() -> cluster.getNodes().length == 2);
		Node nodeB = node(cluster, B);
		assertEquals(first, nodeB.getAddress().getAddress());
		sampler = new Sampler(cluster);

		int lookupsB = GuardedResolverProvider.lookups(hostB);
		int lookupsOther = GuardedResolverProvider.lookups() - GuardedResolverProvider.lookups(hostA);

		a1.close();

		if (other != Other.NO_FAILURES) {
			b1.close();
		}
		await(() -> onAddress(cluster, A, second));

		if (other == Other.NO_FAILURES) {
			Thread.sleep(10L * TEND_INTERVAL);
			assertTrue(nodeB.isActive());
			assertEquals(nodeB, node(cluster, B));
			assertEquals(lookupsB, GuardedResolverProvider.lookups(hostB));
		}
		else {
			await(() -> node(cluster, B) == null);
		}

		if (other == Other.IP_LITERAL) {
			assertEquals(lookupsOther, GuardedResolverProvider.lookups() - GuardedResolverProvider.lookups(hostA));
		}
		assertNotNull(client.get(null, new Key(StubNodeServer.NAMESPACE, "set", 1)));
		assertEquals(0, sampler.stop());
		assertFalse(sampler.movedB);
		assertEquals(1, moves().size());
		assertTrue(moves().get(0).startsWith("Move node " + A + ' '));
	}

	@Test
	public void noMoveBeforeThirdFailedTend() throws Exception {
		String hostname = hostname("third", first, second);
		StubNodeServer dead = pair(A, A)[0];
		int port = dead.port();

		Cluster cluster = drivenCluster(policy(), hostname, port);
		Node node = node(cluster, A);
		int lookups = GuardedResolverProvider.lookups(hostname);

		dead.close();

		for (int i = 1; i <= 2; i++) {
			tend(cluster);
			assertSame("tend " + i, node, node(cluster, A));
		}
		assertEquals(lookups, GuardedResolverProvider.lookups(hostname));
		assertEquals(List.of(), moves());

		tend(cluster);
		assertTrue(onAddress(cluster, A, second));
		assertFalse(node.isActive());
		assertEquals(1, moves().size());
	}

	@Test
	public void nodeRecoveringOnSecondTendDoesNotMove() throws Exception {
		String hostname = hostname("recover", first, second);
		StubNodeServer flapping = pair(A, A)[0];
		int port = flapping.port();

		Cluster cluster = drivenCluster(policy(), hostname, port);
		Node node = node(cluster, A);

		flapping.close();
		tend(cluster);
		flapping = server(first, port, A);
		tend(cluster);
		assertSame(node, node(cluster, A));
		flapping.close();

		for (int i = 1; i <= 2; i++) {
			tend(cluster);
			assertSame("failed tend " + i + " after recovery", node, node(cluster, A));
		}
		assertEquals(List.of(), moves());

		tend(cluster);
		assertTrue(onAddress(cluster, A, second));
		assertEquals(1, moves().size());
	}

	@Test
	public void oneCandidatePerTend() throws Exception {
		StubNodeServer[] pair = pair(A, A);
		StubNodeServer dead = pair[0];
		StubNodeServer live = pair[1];
		int port = dead.port();
		InetAddress hung = StaggeredConnectTest.hung(closeables, InetAddress.getByName("127.0.0.3"), "192.0.2.1", port);
		String hostname = hostname("one", first, hung, second);

		ClientPolicy policy = policy();
		policy.timeout = 1000;
		policy.tendInterval = 500;
		Cluster cluster = drivenCluster(policy, hostname, port);
		Node node = node(cluster, A);
		assertTrue(ownsPartitions(cluster, A, first));

		dead.close();
		tend(cluster);
		tend(cluster);

		int connections = live.connections();
		long elapsed = timedTend(cluster);

		assertSame(node, node(cluster, A));
		assertEquals("live address tried in the tend that tried the hung address", connections, live.connections());
		assertTrue("tend took " + elapsed + "ms", elapsed >= 250 - 10 && elapsed < 250 + MARGIN);

		tend(cluster);

		Node moved = node(cluster, A);
		assertTrue(onAddress(cluster, A, second));
		assertEquals(A, moved.getName());
		assertFalse(node.isActive());
		tend(cluster);
		assertTrue(ownsPartitions(cluster, A, second));
		assertEquals(List.of("Move node " + A + ' ' + hostname + " from " + node.getHost() + " to " + moved.getHost()),
			moves());
	}

	@Test
	public void silentCandidateEndsWithinBound() throws Exception {
		StubNodeServer dead = server(A, port -> closeables.add(new ServerSocket(port, 50, second)));
		int port = dead.port();
		String hostname = hostname("silent", first, second);

		ClientPolicy policy = policy();
		policy.timeout = 1000;
		policy.tendInterval = 400;
		Cluster cluster = drivenCluster(policy, hostname, port);
		Node node = node(cluster, A);

		dead.close();
		tend(cluster);
		tend(cluster);

		// The candidate accepts the connect and never answers: min(1000, 400 / 2) = 200ms.
		long elapsed = timedTend(cluster);

		assertTrue("tend took " + elapsed + "ms", elapsed >= 200 - 10 && elapsed < 200 + MARGIN);
		assertSame(node, node(cluster, A));
		assertEquals(List.of(), moves());
	}

	@Test
	public void candidateWithOtherNameIsSkipped() throws Exception {
		String hostname = hostname("other", first, second);
		StubNodeServer[] pair = pair(A, B);
		StubNodeServer dead = pair[0];
		StubNodeServer other = pair[1];
		int port = dead.port();

		Cluster cluster = drivenCluster(policy(), hostname, port);
		Node node = node(cluster, A);

		dead.close();

		for (int i = 1; i <= 4; i++) {
			int connections = other.connections();
			tend(cluster);
			assertSame("tend " + i, node, node(cluster, A));
			assertEquals("tend " + i, i >= 3, other.connections() > connections);
		}
		assertNull(node(cluster, B));
		assertEquals(List.of(), moves());
	}

	@Test
	public void candidateRejectedByCollisionCheckIsSkipped() throws Exception {
		String hostname = hostname("collide", first, second);
		StubNodeServer[] pair = pair(A, B);
		StubNodeServer dead = pair[0];
		StubNodeServer candidate = pair[1];
		int port = dead.port();

		Cluster cluster = drivenCluster(policy(), hostname, port);
		Node node = node(cluster, A);

		// Node B holds the candidate address, which answers as node A.
		NodeValidator nv = new NodeValidator();
		nv.validateAlias(cluster, new Host(second.getHostAddress(), port), second, cluster.connectTimeout);
		Node holder = cluster.createNode(nv);
		cluster.nodesMap.put(B, holder);
		candidate.setNodeName(A);

		try {
			dead.close();

			for (int i = 1; i <= 4; i++) {
				tend(cluster);
				assertSame("tend " + i, node, node(cluster, A));
			}
		}
		finally {
			cluster.nodesMap.remove(B);
			holder.close();
		}
		assertEquals(List.of(), moves());
		assertTrue(warnings().toString(), warnings().stream().anyMatch(w -> w.contains(A) && w.contains(B) &&
			w.contains(second.getHostAddress())));
	}

	@Test
	public void ipLiteralNodeDoesNotMove() throws Exception {
		StubNodeServer dead = pair(A, A)[0];
		int port = dead.port();

		Cluster cluster = drivenCluster(policy(), "127.0.0.1", port);
		Node node = node(cluster, A);
		int lookups = GuardedResolverProvider.lookups();

		dead.close();

		for (int i = 1; i <= 4; i++) {
			tend(cluster);
			assertSame("tend " + i, node, node(cluster, A));
		}
		assertEquals(lookups, GuardedResolverProvider.lookups());
		assertEquals(List.of(), moves());
	}

	@Test
	public void singleAddressNodeDoesNotMove() throws Exception {
		String hostname = hostname("single", first);
		StubNodeServer[] pair = pair(A, A);
		StubNodeServer dead = pair[0];
		StubNodeServer other = pair[1];
		int port = dead.port();

		Cluster cluster = drivenCluster(policy(), hostname, port);
		Node node = node(cluster, A);

		dead.close();

		for (int i = 1; i <= 4; i++) {
			tend(cluster);
			assertSame("tend " + i, node, node(cluster, A));
		}
		assertEquals(0, other.connections());
		assertEquals(List.of(), moves());
	}

	@Test
	public void peerPathDoesNotMoveSooner() throws Exception {
		String hostA = hostname("peer-a", first, second);
		StubNodeServer[] pair = pair(A, A);
		StubNodeServer a1 = pair[0];
		StubNodeServer a2 = pair[1];
		int portA = a1.port();
		StubNodeServer b = server(first, 0, B);

		String peersA = "1,3000,[[" + B + ",,[127.0.0.1:" + b.port() + "]]]";
		a1.setPeers(peersA);
		a2.setPeers(peersA);
		b.setPeers("1,3000,[[" + A + ",,[" + hostA + ':' + portA + "]]]");

		Cluster cluster = drivenCluster(policy(), hostA, portA);
		assertEquals(2, cluster.getNodes().length);
		Node node = node(cluster, A);

		a1.close();

		for (int i = 1; i <= 2; i++) {
			b.setPeersGeneration(1 + i);
			int connections = a2.connections();
			tend(cluster);
			assertSame("tend " + i, node, node(cluster, A));
			assertEquals("tend " + i, connections, a2.connections());
		}
		assertEquals(List.of(), moves());

		b.setPeersGeneration(4);
		tend(cluster);
		assertTrue(onAddress(cluster, A, second));
		assertEquals(1, moves().size());
		assertEquals(List.of(), replaces());
	}

	@Test
	public void slowLookupDelaysMoveOnTendThread() throws Exception {
		String hostname = hostname("slow", first, second);
		StubNodeServer dead = pair(A, A)[0];
		int port = dead.port();

		Cluster cluster = drivenCluster(policy(), hostname, port);
		Node node = node(cluster, A);

		dead.close();
		tend(cluster);
		tend(cluster);

		long delay = 2L * TEND_INTERVAL;
		GuardedResolverProvider.delay(hostname, delay);
		Set<String> before = threadNames();
		long elapsed = timedTend(cluster);

		assertTrue("tend took " + elapsed + "ms", elapsed >= delay);
		assertTrue(onAddress(cluster, A, second));
		assertFalse(node.isActive());

		Set<String> during = GuardedResolverProvider.delayThreads(hostname);
		assertNotNull(during);
		assertEquals(Thread.currentThread().getName(), during.iterator().next());
		Set<String> started = new HashSet<>(during);
		started.removeAll(before);
		assertEquals(Set.of(), started);
	}

	/**
	 * Client whose tend thread is stopped, so the test drives every tend.
	 */
	private Cluster drivenCluster(ClientPolicy policy, String host, int port) throws Exception {
		client = new AerospikeClient(policy, new Host(host, port));
		Cluster cluster = client.getCluster();
		stopTendThread(cluster);
		return cluster;
	}

	private ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.timeout = 500;
		policy.tendInterval = TEND_INTERVAL;
		return policy;
	}

	private String hostname(String prefix, InetAddress... addresses) throws Exception {
		String hostname = prefix + '-' + System.nanoTime() + ".move.test";
		GuardedResolverProvider.register(hostname, addresses);
		hostnames.add(hostname);
		return hostname;
	}

	private StubNodeServer server(InetAddress address, int port, String name) throws IOException {
		StubNodeServer server = new StubNodeServer(address, port, name);
		servers.add(server);
		return server;
	}

	private List<String> moves() {
		synchronized (moves) {
			return new ArrayList<>(moves);
		}
	}

	private List<String> replaces() {
		synchronized (replaces) {
			return new ArrayList<>(replaces);
		}
	}

	private List<String> warnings() {
		synchronized (warnings) {
			return new ArrayList<>(warnings);
		}
	}

	private static Set<String> threadNames() {
		Set<String> names = new HashSet<>();

		for (Thread thread : Thread.getAllStackTraces().keySet()) {
			names.add(thread.getName());
		}
		return names;
	}

	private static Node node(Cluster cluster, String name) {
		for (Node node : cluster.getNodes()) {
			if (node.getName().equals(name)) {
				return node;
			}
		}
		return null;
	}

	private static boolean onAddress(Cluster cluster, String name, InetAddress address) {
		Node node = node(cluster, name);
		return node != null && node.isActive() && node.getAddress().getAddress().equals(address);
	}

	private static boolean ownsPartitions(Cluster cluster, String name, InetAddress address) {
		Partitions partitions = cluster.partitionMap.get(StubNodeServer.NAMESPACE);

		if (partitions == null) {
			return false;
		}

		AtomicReferenceArray<Node> master = partitions.replicas[0];

		for (int i = 0; i < master.length(); i++) {
			Node node = master.get(i);

			if (node == null || ! node.getName().equals(name) || ! node.getAddress().getAddress().equals(address)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Servers with the given names on one port of the first and the second address.
	 */
	private StubNodeServer[] pair(String firstName, String secondName) throws IOException {
		StubNodeServer[] pair = new StubNodeServer[2];
		pair[0] = server(firstName, port -> pair[1] = server(second, port, secondName));
		return pair;
	}

	/**
	 * Server with the given name on a free port of the first address, once the bind has bound the
	 * same port of the second address. A port the bind finds taken is replaced by another.
	 */
	private StubNodeServer server(String name, Bind bind) throws IOException {
		for (int i = 0; i < 100; i++) {
			StubNodeServer server = server(first, 0, name);

			try {
				bind.bind(server.port());
				return server;
			}
			catch (BindException be) {
				servers.remove(server);
				server.close();
			}
		}
		throw new IOException("No port free on both " + first + " and " + second);
	}

	private static boolean canBind(InetAddress address) {
		try (ServerSocket socket = new ServerSocket(0, 1, address)) {
			return true;
		}
		catch (IOException ioe) {
			return false;
		}
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}

	/**
	 * Watch the cluster node array for an empty array, and for node B on the second address.
	 */
	private static final class Sampler implements Runnable {
		private final Cluster cluster;
		private final Thread thread;
		private volatile boolean running = true;
		private volatile int empty;
		private volatile boolean movedB;

		Sampler(Cluster cluster) {
			this.cluster = cluster;
			this.thread = new Thread(this, "node-sampler");
			this.thread.setDaemon(true);
			this.thread.start();
		}

		@Override
		public void run() {
			while (running) {
				Node[] nodes = cluster.getNodes();

				if (nodes.length == 0) {
					empty++;
				}

				for (Node node : nodes) {
					if (node.getName().equals(B) && node.getAddress().getAddress().equals(second)) {
						movedB = true;
					}
				}
				Thread.onSpinWait();
			}
		}

		int stop() throws InterruptedException {
			running = false;
			thread.join();
			return empty;
		}
	}
}
