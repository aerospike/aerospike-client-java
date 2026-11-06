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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
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

public class HostnameAddressMoveTest {
	private static final int TEND_INTERVAL = 250;
	private static final long MOVE_BOUND = 20L * TEND_INTERVAL;
	private static final String A = "BB9A000000000001";
	private static final String B = "BB9B000000000001";

	private static InetAddress first;
	private static InetAddress second;

	private final List<StubNodeServer> servers = new ArrayList<>();
	private final List<String> hostnames = new ArrayList<>();
	private final List<String> moves = new ArrayList<>();
	private AerospikeClient client;
	private Sampler sampler;

	private enum Other {
		DIFFERENT_NAME, IP_LITERAL, SINGLE_ADDRESS, NO_FAILURES
	}

	@Before
	public void setUp() throws Exception {
		first = InetAddress.getByName("127.0.0.1");
		second = InetAddress.getByName("127.0.0.2");
		Assume.assumeTrue("Address 127.0.0.2 cannot be bound locally", canBind(second));

		Log.setLevel(Log.Level.INFO);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.INFO && message.startsWith("Move node ")) {
				synchronized (moves) {
					moves.add(message);
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

		for (String hostname : hostnames) {
			GuardedResolverProvider.unregister(hostname);
		}
		Log.setCallback(null);
	}

	@Test
	public void failingNodeMovesToOtherAddress() throws Exception {
		String hostname = hostname("move", first, second);
		int port = freePort();
		StubNodeServer live = server(first, port, A);
		server(second, port, A);

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
		assertEquals(List.of("Move node " + A + ' ' + hostname + " from 127.0.0.1 " + port + " to 127.0.0.2 " + port),
			moves());
	}

	@Test
	public void everyNodeOnDeadAddressMoves() throws Exception {
		String hostname = hostname("zone", first, second);
		String[] names = {A, B, "BB9C000000000001"};
		int[] ports = new int[names.length];
		List<StubNodeServer> dead = new ArrayList<>();

		for (int i = 0; i < names.length; i++) {
			ports[i] = freePort();
			dead.add(server(first, ports[i], names[i]));
			server(second, ports[i], names[i]);
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
		int portA = freePort();
		int portB = freePort();
		String peerB = (other == Other.IP_LITERAL) ? "127.0.0.1" : hostB;

		StubNodeServer a1 = server(first, portA, A);
		StubNodeServer a2 = server(second, portA, A);
		StubNodeServer b1 = server(first, portB, B);
		server(second, portB, other == Other.DIFFERENT_NAME ? "BB9C000000000001" : B);

		String peersA = "1,3000,[[" + B + ",,[" + peerB + ':' + portB + "]]]";
		a1.setPeers(peersA);
		a2.setPeers(other == Other.NO_FAILURES ? peersA : "1,3000,[]");
		b1.setPeers("1,3000,[[" + A + ",,[" + hostA + ':' + portA + "]]]");

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
	 * Free port on both loopback addresses.
	 */
	private static int freePort() throws IOException {
		for (int i = 0; i < 100; i++) {
			int port;

			try (ServerSocket socket = new ServerSocket(0, 1, second)) {
				port = socket.getLocalPort();
			}

			try (ServerSocket socket = new ServerSocket(port, 1, first)) {
				return port;
			}
			catch (BindException be) {
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
