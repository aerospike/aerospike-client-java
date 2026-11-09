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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.async.StubNodeServer;
import com.aerospike.client.discovery.GuardedResolverProvider;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Tends driven one at a time from the test thread never leave two nodes with one name. The tend
 * thread is stopped once the client is created.
 */
public class DuplicateNodeTest {
	private static final String X = "BB9E000000000001";
	private static final String Y = "BB9F000000000001";

	private InetAddress first;
	private InetAddress second;
	private InetAddress local;
	private InetAddress unreachable;

	private final List<StubNodeServer> servers = new ArrayList<>();
	private final List<String> hostnames = new ArrayList<>();
	private final List<String> moves = new ArrayList<>();
	private AerospikeClient client;

	@Before
	public void setUp() throws Exception {
		first = InetAddress.getByName("127.0.0.1");
		second = InetAddress.getByName("127.0.0.2");
		local = localAddress();
		unreachable = InetAddress.getByName("192.0.2.1");

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
		});
	}

	@After
	public void tearDown() throws Exception {
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
	public void oneNodePerNameAfterSecondPhaseReplace() throws Exception {
		Cluster cluster = secondPhaseReplace();

		for (int i = 0; i < 3; i++) {
			tend(cluster);
			assertOneNodePerName(cluster);
		}
		assertEquals(2, cluster.getNodes().length);
	}

	@Test
	public void secondPhaseReplaceRemovesNodeInSameTend() throws Exception {
		Cluster cluster = secondPhaseReplace();
		Node oldX = node(cluster, X);
		Node oldY = node(cluster, Y);

		tend(cluster);

		assertOneNodePerName(cluster);
		assertEquals(2, cluster.getNodes().length);
		assertFalse(oldX.isActive());
		assertFalse(oldY.isActive());

		Node newX = node(cluster, X);
		assertNotSame(oldX, newX);
		assertTrue(newX.isActive());
		assertEquals(local, newX.getAddress().getAddress());
		assertEquals(second, node(cluster, Y).getAddress().getAddress());
	}

	@Test
	public void nodeNotInMapIsRemovedWithoutMove() throws Exception {
		String hostname = hostname("stale", first, second);
		int port = freePort();
		server(first, port, X);
		StubNodeServer dead = server(second, port, X);

		client = new AerospikeClient(policy(), new Host(hostname, port));
		Cluster cluster = client.getCluster();
		stopTendThread(cluster);

		Node current = node(cluster, X);
		Node stale = createNode(cluster, hostname, second, port);
		setNodes(cluster, current, stale);

		dead.close();
		tend(cluster);

		assertOneNodePerName(cluster);
		assertEquals(1, cluster.getNodes().length);
		assertSame(current, cluster.getNodes()[0]);
		assertFalse(stale.isActive());
		assertTrue(current.isActive());
		assertEquals(List.of(), moves());
	}

	@Test
	public void sameNameRemovalsBothApplied() throws Exception {
		String hostname = hostname("twice", first, second);
		int port = freePort();
		StubNodeServer dead = server(first, port, X);
		server(second, port, X);

		client = new AerospikeClient(policy(), new Host(hostname, port));
		Cluster cluster = client.getCluster();
		stopTendThread(cluster);

		Node current = node(cluster, X);
		Node stale = createNode(cluster, hostname, first, port);
		setNodes(cluster, current, stale);

		dead.close();
		tend(cluster);

		assertOneNodePerName(cluster);
		assertEquals(1, cluster.getNodes().length);
		assertFalse(current.isActive());
		assertFalse(stale.isActive());

		Node moved = node(cluster, X);
		assertTrue(moved.isActive());
		assertEquals(second, moved.getAddress().getAddress());
		assertEquals(1, moves().size());
	}

	/**
	 * Node Y on a two-address loopback hostname lists node X under a hostname that resolves to a
	 * local non-loopback address that reaches X, and an address that reaches nothing. Y's first address
	 * dies and X drops its connections, so in the next tend Y moves to its second address, X's
	 * move fails, and the moved Y's peers replace X on its own address.
	 */
	private Cluster secondPhaseReplace() throws Exception {
		Assume.assumeTrue("No non-loopback IPv4 address can be bound locally", local != null);

		String hostX = hostname("x", local, unreachable);
		String hostY = hostname("y", first, second);
		StubNodeServer x = server(local, 0, X);
		int portX = x.port();
		int portY = freePort();
		StubNodeServer y1 = server(first, portY, Y);
		StubNodeServer y2 = server(second, portY, Y);

		String peersY = "1,3000,[[" + X + ",,[" + hostX + ':' + portX + "]]]";
		y1.setPeers(peersY);
		y2.setPeers(peersY);
		x.setPeers("1,3000,[[" + Y + ",,[" + hostY + ':' + portY + "]]]");

		client = new AerospikeClient(policy(), new Host(hostY, portY));
		Cluster cluster = client.getCluster();
		stopTendThread(cluster);

		assertEquals(2, cluster.getNodes().length);
		assertEquals(local, node(cluster, X).getAddress().getAddress());
		assertEquals(first, node(cluster, Y).getAddress().getAddress());

		y1.close();
		x.closeConnections();
		return cluster;
	}

	private static void assertOneNodePerName(Cluster cluster) {
		Node[] nodes = cluster.getNodes();
		Set<String> names = new HashSet<>();

		for (Node node : nodes) {
			assertTrue("Two nodes named " + node.getName() + " in " + Arrays.toString(nodes), names.add(node.getName()));
		}

		for (Node node : nodes) {
			assertSame(node, cluster.nodesMap.get(node.getName()));
		}
		assertEquals(nodes.length, cluster.nodesMap.size());
	}

	private ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.timeout = 500;
		return policy;
	}

	private String hostname(String prefix, InetAddress... addresses) throws Exception {
		String hostname = prefix + '-' + System.nanoTime() + ".duplicate.test";
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
		Node found = null;

		for (Node node : cluster.getNodes()) {
			if (node.getName().equals(name)) {
				assertTrue("Two nodes named " + name, found == null);
				found = node;
			}
		}
		return found;
	}

	private static Node createNode(Cluster cluster, String hostname, InetAddress address, int port) throws Exception {
		NodeValidator nv = new NodeValidator();
		nv.validateAlias(cluster, new Host(hostname, port), address);
		return cluster.createNode(nv);
	}

	private static void setNodes(Cluster cluster, Node... nodes) throws Exception {
		Field field = Cluster.class.getDeclaredField("nodes");
		field.setAccessible(true);
		field.set(cluster, nodes);
	}

	/**
	 * Stop the tend thread so the test drives every later tend.
	 */
	private static void stopTendThread(Cluster cluster) throws Exception {
		Field field = Cluster.class.getDeclaredField("tendThread");
		field.setAccessible(true);
		Thread thread = (Thread)field.get(cluster);

		cluster.tendValid = false;
		thread.interrupt();
		thread.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse("tend thread did not stop", thread.isAlive());
	}

	private static void tend(Cluster cluster) throws Exception {
		Method method = Cluster.class.getDeclaredMethod("tend", boolean.class, boolean.class);
		method.setAccessible(true);

		try {
			method.invoke(cluster, false, false);
		}
		catch (InvocationTargetException ite) {
			throw (Exception)ite.getCause();
		}
	}

	/**
	 * Free port on both loopback addresses.
	 */
	private int freePort() throws IOException {
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

	private static InetAddress localAddress() throws SocketException {
		for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
			if (! ni.isUp()) {
				continue;
			}

			for (InetAddress address : Collections.list(ni.getInetAddresses())) {
				if (address instanceof Inet4Address && ! address.isLoopbackAddress() && ! address.isLinkLocalAddress() &&
					canBind(address)) {
					return address;
				}
			}
		}
		return null;
	}

	private static boolean canBind(InetAddress address) {
		try (ServerSocket socket = new ServerSocket(0, 1, address)) {
			return true;
		}
		catch (IOException ioe) {
			return false;
		}
	}
}
