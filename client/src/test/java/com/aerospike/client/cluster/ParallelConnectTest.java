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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.async.StubNodeServer;
import com.aerospike.client.discovery.GuardedResolverProvider;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Peer validation of a host name that resolves to several addresses connects to all of them at once.
 */
public class ParallelConnectTest {
	private static final String NODE = "BB9D000000000001";
	private static final long MARGIN = 250;

	private InetAddress first;
	private InetAddress second;
	private final List<AutoCloseable> closeables = new ArrayList<>();
	private final List<String> hostnames = new ArrayList<>();
	private AerospikeClient client;

	@Before
	public void setUp() throws Exception {
		first = InetAddress.getByName("127.0.0.1");
		second = InetAddress.getByName("127.0.0.2");

		if (! canBind(second)) {
			second = InetAddress.getByName("::1");
		}
		Assume.assumeTrue("Neither 127.0.0.2 nor ::1 can be bound locally", canBind(second));
	}

	@After
	public void tearDown() throws Exception {
		if (client != null) {
			client.close();
		}

		for (AutoCloseable closeable : closeables) {
			closeable.close();
		}

		for (String hostname : hostnames) {
			GuardedResolverProvider.unregister(hostname);
		}
	}

	@Test
	public void hungAddressDoesNotDelayServingAddress() throws Throwable {
		Cluster cluster = cluster(3000, 250);
		int port = freePort();
		InetAddress hung = hung(second, "192.0.2.1", port);
		server(first, port, NODE);
		String hostname = hostname("hung", hung, first);

		NodeValidator nv = new NodeValidator();
		long elapsed = validate(nv, cluster, new Host(hostname, port));

		assertTrue("validateNode took " + elapsed + "ms", elapsed < 250 + MARGIN);
		assertEquals(NODE, nv.name);
		assertEquals(first, nv.primaryAddress.getAddress());
	}

	@Test
	public void everyAddressHungFailsWithinBound() throws Throwable {
		Cluster cluster = cluster(3000, 250);
		int port = freePort();
		String hostname = hostname("dead", hung(first, "192.0.2.1", port), hung(second, "192.0.2.2", port));

		long begin = System.nanoTime();

		try {
			new NodeValidator().validateNode(cluster, new Host(hostname, port));
			fail("validateNode succeeded");
		}
		catch (AerospikeException.Connection e) {
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			assertTrue("validateNode took " + elapsed + "ms", elapsed < 250 + MARGIN);
			assertTrue(e.getMessage(), e.getMessage().contains(hostname + ' ' + port));
			assertTrue(e.getMessage(), e.getMessage().contains("2 addresses"));
			assertTrue(e.getMessage(), e.getMessage().contains("250ms"));
		}
	}

	@Test
	public void lastValidAddressIsPreferred() throws Throwable {
		Cluster cluster = cluster(1000, 250);
		int port = freePort();
		server(first, port, NODE);
		server(second, port, NODE);
		String hostname = hostname("prefer", first, second);

		NodeValidator nv = new NodeValidator();
		validate(nv, cluster, new Host(hostname, port));
		InetAddress chosen = nv.primaryAddress.getAddress();
		InetAddress other = chosen.equals(first) ? second : first;

		GuardedResolverProvider.register(hostname, other, chosen);

		for (int i = 0; i < 10; i++) {
			nv = new NodeValidator();
			validate(nv, cluster, new Host(hostname, port));
			assertEquals("call " + i, chosen, nv.primaryAddress.getAddress());
		}
		assertEquals(10, GuardedResolverProvider.lookups(hostname));
	}

	@Test
	public void singleAddressConnectsAsBefore() throws Throwable {
		Cluster cluster = cluster(600, 250);
		int port = freePort();
		server(first, port, NODE);
		String hostname = hostname("single", first);

		NodeValidator nv = new NodeValidator();
		validate(nv, cluster, new Host(hostname, port));
		assertEquals(first, nv.primaryAddress.getAddress());
		assertEquals(hostname, nv.hostname);
		assertEquals(1, GuardedResolverProvider.lookups(hostname));

		int hungPort = freePort();
		String hung = hostname("single-hung", hung(first, "192.0.2.1", hungPort));
		long elapsed = failedValidate(cluster, new Host(hung, hungPort));

		assertTrue("validateNode took " + elapsed + "ms, not the connect timeout", elapsed >= 600 - 50);
		assertEquals(1, GuardedResolverProvider.lookups(hung));
	}

	@Test
	public void ipLiteralConnectsAsBefore() throws Throwable {
		Cluster cluster = cluster(600, 250);
		int port = freePort();
		server(first, port, NODE);
		int lookups = GuardedResolverProvider.lookups();

		NodeValidator nv = new NodeValidator();
		GuardedResolverProvider.arm();

		try {
			validate(nv, cluster, new Host("127.0.0.1", port));
		}
		finally {
			GuardedResolverProvider.disarm();
		}
		assertEquals(first, nv.primaryAddress.getAddress());
		assertEquals(null, nv.hostname);

		int hungPort = freePort();
		String literal = hung(first, "192.0.2.1", hungPort).getHostAddress();
		long elapsed = failedValidate(cluster, new Host(literal, hungPort));

		assertTrue("validateNode took " + elapsed + "ms, not the connect timeout", elapsed >= 600 - 50);
		assertEquals(lookups, GuardedResolverProvider.lookups());
	}

	@Test
	public void timeoutBelowTendIntervalBoundsWait() throws Throwable {
		Cluster cluster = cluster(400, 1000);
		int port = freePort();
		String hostname = hostname("timeout", hung(first, "192.0.2.1", port), hung(second, "192.0.2.2", port));

		long begin = System.nanoTime();

		try {
			new NodeValidator().validateNode(cluster, new Host(hostname, port));
			fail("validateNode succeeded");
		}
		catch (AerospikeException.Connection e) {
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			assertTrue("validateNode took " + elapsed + "ms", elapsed >= 400 - 50 && elapsed < 400 + MARGIN);
			assertTrue(e.getMessage(), e.getMessage().contains("400ms"));
		}
	}

	private Cluster cluster(int timeout, int tendInterval) throws Exception {
		StubNodeServer seed = new StubNodeServer();
		closeables.add(seed);

		ClientPolicy policy = new ClientPolicy();
		policy.timeout = timeout;
		policy.tendInterval = tendInterval;
		client = new AerospikeClient(policy, new Host("127.0.0.1", seed.port()));
		return client.getCluster();
	}

	private static long validate(NodeValidator nv, Cluster cluster, Host host) throws Throwable {
		long begin = System.nanoTime();
		nv.validateNode(cluster, host);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		nv.primaryConn.close();
		return elapsed;
	}

	private static long failedValidate(Cluster cluster, Host host) throws Throwable {
		long begin = System.nanoTime();

		try {
			new NodeValidator().validateNode(cluster, host);
			fail("validateNode succeeded");
		}
		catch (AerospikeException.Connection e) {
		}
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
	}

	private String hostname(String prefix, InetAddress... addresses) throws Exception {
		String hostname = prefix + '-' + System.nanoTime() + ".connect.test";
		GuardedResolverProvider.register(hostname, addresses);
		hostnames.add(hostname);
		return hostname;
	}

	private void server(InetAddress address, int port, String name) throws IOException {
		closeables.add(new StubNodeServer(address, port, name));
	}

	/**
	 * Address whose connect to the port never completes: the loopback address with a listener whose
	 * accept backlog is full, where the kernel drops further SYNs (Linux), or else the given
	 * TEST-NET-1 address (RFC 5737), which is routed nowhere (macOS resets connects to a full backlog).
	 */
	private InetAddress hung(InetAddress loopback, String testNet, int port) throws IOException {
		ServerSocket listener = new ServerSocket(port, 1, loopback);
		closeables.add(listener);

		try {
			for (int i = 0; i < 64; i++) {
				Socket socket = new Socket();
				closeables.add(socket);
				socket.connect(new InetSocketAddress(loopback, port), 200);
			}
		}
		catch (SocketTimeoutException ste) {
			return loopback;
		}
		catch (IOException ioe) {
		}
		listener.close();

		InetAddress address = InetAddress.getByName(testNet);

		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(address, port), 200);
		}
		catch (SocketTimeoutException ste) {
			return address;
		}
		catch (IOException ioe) {
		}
		Assume.assumeTrue("No connect hangs: the accept backlog of " + loopback + ' ' + port +
			" did not fill and " + testNet + " is not routed nowhere", false);
		return null;
	}

	/**
	 * Free port on both test addresses.
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

	private static boolean canBind(InetAddress address) {
		try (ServerSocket socket = new ServerSocket(0, 1, address)) {
			return true;
		}
		catch (IOException ioe) {
			return false;
		}
	}
}
