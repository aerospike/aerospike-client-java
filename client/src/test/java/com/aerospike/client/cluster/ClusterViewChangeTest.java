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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.policy.ClientPolicy;

public class ClusterViewChangeTest {
	private final Map<String,String> view = new ConcurrentHashMap<>();
	private final Map<String,FakeNode> servers = new ConcurrentHashMap<>();
	private final AtomicInteger generation = new AtomicInteger(1);
	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private AerospikeClient client;

	final class NodeNameTranslator implements AddressTranslator {
		final List<Integer> viewChanges = new CopyOnWriteArrayList<>();
		volatile boolean fail;

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			FakeNode server = servers.get(advertised.nodeName);
			return (server != null)? Optional.of(new Host("127.0.0.1", server.port())) : Optional.empty();
		}

		@Override
		public void onClusterViewChanged(int viewChange) {
			viewChanges.add(viewChange);

			if (fail) {
				throw new IllegalStateException("cache flush failed");
			}
		}
	}

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
		});
	}

	@After
	public void tearDown() throws Exception {
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);

		if (client != null) {
			client.close();
		}

		for (FakeNode server : servers.values()) {
			server.close();
		}
	}

	@Test
	public void viewChangeCountsNodeAddRemoveAndAddressChange() throws Exception {
		NodeNameTranslator translator = new NodeNameTranslator();
		start(translator);

		assertEquals(2, client.getNodes().length);
		awaitTends(3);
		assertEquals(List.of(), translator.viewChanges);

		addNode("BB9C", "10.0.0.3:3000");
		await(() -> client.getNodes().length == 3);
		await(() -> translator.viewChanges.size() == 1);
		assertEquals(List.of(1), translator.viewChanges);

		int peersRequests = servers.get("BB9A").requests("peers-clear-std");
		generation.incrementAndGet();
		await(() -> servers.get("BB9A").requests("peers-clear-std") > peersRequests);
		awaitTends(3);
		assertEquals(List.of(1), translator.viewChanges);

		view.remove("BB9C");
		generation.incrementAndGet();
		await(() -> client.getNodes().length == 2);
		await(() -> translator.viewChanges.size() == 2);
		assertEquals(List.of(1, 2), translator.viewChanges);

		view.put("BB9B", "10.0.0.22:3000");
		generation.incrementAndGet();
		await(() -> translator.viewChanges.size() == 3);
		assertEquals(List.of(1, 2, 3), translator.viewChanges);
		assertEquals(2, client.getNodes().length);

		awaitTends(3);
		assertEquals(List.of(1, 2, 3), translator.viewChanges);
		assertEquals(List.of(), viewWarnings());
	}

	@Test
	public void viewChangeExceptionDoesNotStopTend() throws Exception {
		NodeNameTranslator translator = new NodeNameTranslator();
		translator.fail = true;
		start(translator);

		addNode("BB9C", "10.0.0.3:3000");
		await(() -> client.getNodes().length == 3);
		await(() -> translator.viewChanges.size() == 1);

		view.remove("BB9C");
		generation.incrementAndGet();
		await(() -> client.getNodes().length == 2);
		await(() -> translator.viewChanges.size() == 2);
		assertEquals(List.of(1, 2), translator.viewChanges);

		List<String> logged = viewWarnings();
		assertEquals(logged.toString(), 2, logged.size());

		for (String message : logged) {
			assertTrue(message, message.contains(NodeNameTranslator.class.getName()));
			assertTrue(message, message.contains("cache flush failed"));
		}
	}

	private void start(NodeNameTranslator translator) throws Exception {
		server("BB9A", "10.0.0.1:3000");
		server("BB9B", "10.0.0.2:3000");

		ClientPolicy policy = new ClientPolicy();
		policy.timeout = 1000;
		policy.tendInterval = 250;
		policy.addressTranslator = translator;
		client = new AerospikeClient(policy, new Host("127.0.0.1", servers.get("BB9A").port()));
	}

	private void addNode(String name, String address) throws IOException {
		server(name, address);
		generation.incrementAndGet();
	}

	private void server(String name, String address) throws IOException {
		servers.put(name, new FakeNode(name));
		view.put(name, address);
	}

	private void awaitTends(int count) throws InterruptedException {
		int target = servers.get("BB9A").requests("peers-generation") + count;
		await(() -> servers.get("BB9A").requests("peers-generation") >= target);
	}

	private List<String> viewWarnings() {
		List<String> list = new ArrayList<>();

		for (String message : warnings) {
			if (message.contains("onClusterViewChanged")) {
				list.add(message);
			}
		}
		return list;
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}

	private final class FakeNode implements AutoCloseable {
		final String name;
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();
		private final Map<String,AtomicInteger> requests = new ConcurrentHashMap<>();

		FakeNode(String name) throws IOException {
			this.name = name;
			this.server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		int requests(String command) {
			AtomicInteger count = requests.get(command);
			return (count != null)? count.get() : 0;
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
							requests.computeIfAbsent(command, k -> new AtomicInteger()).incrementAndGet();
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
			catch (EOFException eof) {
			}
			catch (IOException ioe) {
			}
		}

		private String value(String command) {
			switch (command) {
			case "node":
				return name;
			case "partition-generation":
				return "1";
			case "peers-generation":
				return String.valueOf(generation.get());
			case "build":
				return "8.1.0.0";
			case "replicas":
				return "";
			case "peers-clear-std":
				return peers();
			default:
				return "ok";
			}
		}

		private String peers() {
			StringBuilder sb = new StringBuilder();
			sb.append(generation.get()).append(",3000,[");
			boolean comma = false;

			for (Map.Entry<String,String> entry : new TreeMap<>(view).entrySet()) {
				if (entry.getKey().equals(name)) {
					continue;
				}

				if (comma) {
					sb.append(',');
				}
				comma = true;
				sb.append('[').append(entry.getKey()).append(",,[").append(entry.getValue()).append("]]");
			}
			return sb.append(']').toString();
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
