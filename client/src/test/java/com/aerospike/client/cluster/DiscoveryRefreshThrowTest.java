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
import static org.junit.Assert.assertSame;
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
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;

public class DiscoveryRefreshThrowTest {
	private static final String REFRESH_FAILED = "Seed candidate refresh failed: ";

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private FakeNode server;
	private AerospikeClient client;

	static final class ThrowingProvider implements SeedCandidateProvider {
		final AtomicInteger calls = new AtomicInteger();
		final Throwable[] errors = new Throwable[] {
			new IllegalStateException("refresh runtime"),
			new IOException("refresh checked"),
			new AssertionError("refresh error")
		};
		private final Host seed;

		ThrowingProvider(Host seed) {
			this.seed = seed;
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			int index = calls.getAndIncrement() - 1;

			if (index >= 0 && index < errors.length) {
				throw ThrowingProvider.<RuntimeException>sneaky(errors[index]);
			}
			return List.of(seed);
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return true;
		}

		@Override
		public boolean supportsDeadline() {
			return true;
		}

		@SuppressWarnings("unchecked")
		private static <T extends Throwable> T sneaky(Throwable t) throws T {
			throw (T)t;
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

		if (server != null) {
			server.close();
		}
	}

	@Test
	public void m3ProviderThrowDoesNotStopTend() throws Exception {
		server = new FakeNode("BB9A");
		Host seed = new Host("127.0.0.1", server.port());
		ThrowingProvider provider = new ThrowingProvider(seed);

		ClientPolicy policy = new ClientPolicy();
		policy.timeout = 1000;
		policy.tendInterval = 250;
		policy.discoveryRefreshInterval = 1000;
		policy.seedCandidateProvider = provider;
		client = new AerospikeClient(policy, seed);

		Cluster cluster = client.getCluster();
		Host[] seeds = cluster.getSeeds();
		assertEquals(1, client.getNodes().length);
		assertEquals(1, provider.calls.get());

		for (int i = 0; i < provider.errors.length; i++) {
			int calls = i + 2;
			await(() -> provider.calls.get() >= calls);
			awaitTends(2);

			assertEquals(calls, provider.calls.get());
			assertSame(seeds, cluster.getSeeds());
			assertEquals(1, client.getNodes().length);

			List<String> logged = refreshWarnings();
			assertEquals(logged.toString(), i + 1, logged.size());
			assertTrue(logged.get(i), logged.get(i).contains(provider.errors[i].getMessage()));
		}
		assertEquals(warnings.toString(), provider.errors.length, warnings.size());
	}

	private void awaitTends(int count) throws InterruptedException {
		int target = server.requests("peers-generation") + count;
		await(() -> server.requests("peers-generation") >= target);
	}

	private List<String> refreshWarnings() {
		List<String> list = new ArrayList<>();

		for (String message : warnings) {
			if (message.startsWith(REFRESH_FAILED)) {
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

	private static final class FakeNode implements AutoCloseable {
		final String name;
		private final ServerSocket server;
		private final List<Socket> sockets = new CopyOnWriteArrayList<>();
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

				sockets.add(socket);
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
			case "peers-generation":
				return "1";
			case "build":
				return "8.1.0.0";
			case "replicas":
				return "";
			case "peers-clear-std":
				return "1,3000,[]";
			default:
				return "ok";
			}
		}

		@Override
		public void close() throws IOException {
			server.close();

			for (Socket socket : sockets) {
				socket.close();
			}
		}
	}
}
