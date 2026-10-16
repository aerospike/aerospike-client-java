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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.Endpoint.SourceList;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Version;

public class PassThroughTranslationTest {
	private final List<String> warnings = new ArrayList<>();
	private final List<FakeNode> servers = new ArrayList<>();
	private final List<Node> nodes = new ArrayList<>();
	private AerospikeClient client;
	private Thread testThread;

	@Before
	public void setUp() {
		testThread = Thread.currentThread();
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN && Thread.currentThread() == testThread) {
				synchronized (warnings) {
					warnings.add(message);
				}
			}
		});
	}

	@After
	public void tearDown() throws Exception {
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);

		for (Node node : nodes) {
			node.close();
		}

		if (client != null) {
			client.close();
		}

		for (FakeNode server : servers) {
			server.close();
		}
	}

	@Test
	public void strictNeverMisses() {
		for (SourceList sourceList : SourceList.values()) {
			for (String host : new String[] {"10.0.0.2", "2001:db8::10", "node2.priv.example.com"}) {
				Host result = Cluster.translate(PassThroughAddressTranslator.INSTANCE, TranslationMissMode.STRICT,
					new Endpoint("BB9", "db2.example.com", host, 3000, sourceList));

				assertEquals(host, result.name);
				assertEquals(3000, result.port);
				assertEquals("db2.example.com", result.tlsName);
			}
		}

		Host result = Cluster.translate(PassThroughAddressTranslator.INSTANCE, TranslationMissMode.STRICT,
			new Endpoint("BB9", null, "10.0.0.2", 3000, SourceList.ALTERNATE));
		assertNull(result.tlsName);
	}

	@Test
	public void clusterTranslatesUnchangedUnderStrict() {
		ClientPolicy policy = policy();
		policy.ipMap = Map.of("10.0.0.1", "192.168.1.1");
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		policy.translationOnMiss = TranslationMissMode.STRICT;
		policy.useServicesAlternate = true;

		try (AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1))) {
			Cluster cluster = client.getCluster();
			Host host = cluster.translateAddress("BB9", "tls1", "10.0.0.1", 31002);

			assertEquals("10.0.0.1", host.name);
			assertEquals(31002, host.port);
			assertEquals("tls1", host.tlsName);
			assertNull(cluster.translateAddress("BB9", null, "10.0.0.1", 3000).tlsName);
			assertEquals("pass-through", cluster.translatorName());
		}
	}

	@Test
	public void sameHostAndPortCollisionNamesPassThrough() throws Exception {
		FakeNode b = server("BB9B", "peers-clear-std", "[]");
		FakeNode a = server("BB9A", "peers-clear-std",
			"[[BB9B,,[127.0.0.1:" + b.port() + "]],[BB9C,,[127.0.0.1:" + b.port() + "]]]");
		Node seed = node(policy(), a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);

		assertTrue(peers.nodes.isEmpty());
		assertEquals(-1, seed.getPeersGeneration());

		List<String> collisions = warnings();
		assertEquals(2, collisions.size());

		for (String message : collisions) {
			for (String part : new String[] {"BB9B", "BB9C", "127.0.0.1:" + b.port(), "pass-through", "same address"}) {
				assertTrue(message, message.contains(part));
			}
		}
	}

	@Test
	public void alternateSharedHostPortPerNodeCreatesEveryPeer() throws Exception {
		FakeNode b = server("BB9B", "peers-clear-alt", "[]");
		FakeNode c = server("BB9C", "peers-clear-alt", "[]");
		FakeNode a = server("BB9A", "peers-clear-alt",
			"[[BB9B,,[127.0.0.1:" + b.port() + "]],[BB9C,,[127.0.0.1:" + c.port() + "]]]");
		ClientPolicy policy = policy();
		policy.useServicesAlternate = true;
		policy.translationOnMiss = TranslationMissMode.STRICT;
		Node seed = node(policy, a);

		Peers peers = new Peers(16);
		seed.refreshPeers(peers);
		nodes.addAll(peers.nodes.values());

		assertEquals(2, peers.nodes.size());
		assertEquals(new Host("127.0.0.1", b.port()), peers.nodes.get("BB9B").getHost());
		assertEquals(new Host("127.0.0.1", c.port()), peers.nodes.get("BB9C").getHost());
		assertEquals(5, seed.getPeersGeneration());
		assertEquals(List.of(), warnings());
	}

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		policy.failIfNotConnected = false;
		policy.timeout = 1000;
		policy.tendInterval = 5000;
		return policy;
	}

	private FakeNode server(String name, String peersCommand, String peers) throws IOException {
		FakeNode server = new FakeNode(name, peersCommand, "5,3000," + peers);
		servers.add(server);
		return server;
	}

	private Node node(ClientPolicy policy, FakeNode server) throws Exception {
		client = new AerospikeClient(policy, new Host("127.0.0.1", 1));

		synchronized (warnings) {
			warnings.clear();
		}

		InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port());
		NodeValidator nv = new NodeValidator();
		nv.name = server.name;
		nv.primaryHost = new Host("127.0.0.1", server.port());
		nv.primaryAddress = address;
		nv.primaryConn = new Connection(address, 1000);
		nv.serverVersion = new Version("8.1.0.0");
		Node node = new Node(client.getCluster(), nv);
		nodes.add(node);
		return node;
	}

	private List<String> warnings() {
		synchronized (warnings) {
			return new ArrayList<>(warnings);
		}
	}

	private static final class FakeNode implements AutoCloseable {
		final String name;
		private final String peersCommand;
		private final String peers;
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();

		FakeNode(String name, String peersCommand, String peers) throws IOException {
			this.name = name;
			this.peersCommand = peersCommand;
			this.peers = peers;
			this.server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		private void accept() {
			while (true) {
				Socket socket;

				try {
					socket = server.accept();
				}
				catch (IOException ioe) {
					return;
				}

				synchronized (sockets) {
					sockets.add(socket);
				}
				Thread thread = new Thread(() -> serve(socket));
				thread.setDaemon(true);
				thread.start();
			}
		}

		private void serve(Socket socket) {
			try {
				DataInputStream in = new DataInputStream(socket.getInputStream());
				OutputStream out = socket.getOutputStream();
				byte[] header = new byte[8];

				while (true) {
					in.readFully(header);
					byte[] request = new byte[(int)(Buffer.bytesToLong(header, 0) & 0xFFFFFFFFFFFFL)];
					in.readFully(request);

					StringBuilder sb = new StringBuilder();

					for (String command : new String(request, StandardCharsets.UTF_8).split("\n")) {
						if (! command.isEmpty()) {
							sb.append(command).append('\t').append(value(command)).append('\n');
						}
					}

					byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
					byte[] response = new byte[8 + body.length];
					Buffer.longToBytes(body.length | (2L << 56) | (1L << 48), response, 0);
					System.arraycopy(body, 0, response, 8, body.length);
					out.write(response);
					out.flush();
				}
			}
			catch (IOException ioe) {
			}
		}

		private String value(String command) {
			if (command.equals(peersCommand)) {
				return peers;
			}

			switch (command) {
			case "node":
				return name;
			case "partition-generation":
			case "peers-generation":
				return "1";
			case "build":
				return "8.1.0.0";
			default:
				return "ok";
			}
		}

		@Override
		public void close() throws IOException {
			server.close();

			synchronized (sockets) {
				for (Socket socket : sockets) {
					socket.close();
				}
			}
		}
	}
}
