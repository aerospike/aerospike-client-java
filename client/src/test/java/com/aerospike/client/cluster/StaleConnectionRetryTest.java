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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.command.SyncCommand;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Version;

/**
 * A sync command whose pooled socket was closed or reset by the peer while idle runs the same
 * attempt once more on a new connection, without consuming a retry or counting an error. A write
 * whose request was sent is never sent again.
 */
public class StaleConnectionRetryTest {
	private static final String NAMESPACE = "test";
	private static final int TIMEOUT = 2000;
	private static final int RST_WAIT_MS = 200;

	private Peer peer;
	private AerospikeClient client;
	private Node node;

	@Before
	public void setUp() throws Exception {
		peer = new Peer();

		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		client = new AerospikeClient(policy, new Host("127.0.0.1", 1));

		InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), peer.port());
		NodeValidator nv = new NodeValidator();
		nv.name = "BB9000000000001";
		nv.primaryHost = new Host("127.0.0.1", peer.port());
		nv.primaryAddress = address;
		nv.primaryConn = new Connection(address, 1000);
		nv.serverVersion = new Version("8.1.0.0");
		node = new Node(client.getCluster(), nv);
		peer.waitAccepted(1);
	}

	@After
	public void tearDown() throws Exception {
		if (node != null) {
			node.close();
		}
		if (client != null) {
			client.close();
		}
		if (peer != null) {
			peer.close();
		}
	}

	@Test
	public void healthyPooledSocketIsReused() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();

		new TestCommand(false).execute();

		assertEquals(accepted, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(0);
	}

	@Test
	public void pooledSocketClosedByPeerIsRetriedOnNewConnection() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();
		peer.last().close();
		Thread.sleep(RST_WAIT_MS);

		new TestCommand(false).execute();

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(1);
	}

	@Test
	public void pooledSocketResetByPeerIsRetriedOnNewConnection() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();
		reset(peer.last());

		new TestCommand(false).execute();

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(1);
	}

	@Test
	public void writeOnPooledSocketResetByPeerIsRetriedOnNewConnection() throws Exception {
		new TestCommand(true).execute();
		int accepted = peer.accepted.get();
		reset(peer.last());

		new TestCommand(true).execute();

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertNoErrorOrRetry(1);
	}

	@Test
	public void writeFailedAfterSendIsNotRetried() throws Exception {
		new TestCommand(true).execute();
		int accepted = peer.accepted.get();
		peer.resetAfterRequest = true;

		try {
			new TestCommand(true).execute();
			fail("Write succeeded after the peer reset the connection");
		}
		catch (AerospikeException.Connection ae) {
			assertTrue(ae.getInDoubt());
		}

		assertEquals(accepted, peer.accepted.get());
		assertEquals(2, peer.requests.get());
		assertEquals(1, node.getErrorCount());
	}

	@Test
	public void staleRetryHappensOncePerAttempt() throws Exception {
		new TestCommand(false).execute();
		int accepted = peer.accepted.get();
		peer.resetAfterRequest = true;

		try {
			new TestCommand(false).execute();
			fail("Read succeeded after the peer reset every connection");
		}
		catch (AerospikeException.Connection ae) {
			assertEquals(1, ae.getIteration());
		}

		assertEquals(accepted + 1, peer.accepted.get());
		assertEquals(3, peer.requests.get());
		assertEquals(1, node.getErrorCount());
	}

	private void assertNoErrorOrRetry(int closed) throws Exception {
		Field field = Node.class.getDeclaredField("errorRateCount");
		field.setAccessible(true);

		assertEquals(0, ((AtomicInteger)field.get(node)).get());
		assertEquals(0, node.getErrorCount());
		assertEquals(0, client.getCluster().getRetryCount());
		assertEquals(closed, node.getConnectionStats().closed);
	}

	private static void reset(Socket socket) throws Exception {
		socket.setSoLinger(true, 0);
		socket.close();
		Thread.sleep(RST_WAIT_MS);
	}

	private final class TestCommand extends SyncCommand {
		private final boolean write;

		TestCommand(boolean write) {
			super(client.getCluster(), policy(), NAMESPACE);
			this.write = write;
		}

		@Override
		protected boolean isWrite() {
			return write;
		}

		@Override
		protected Node getNode() {
			return node;
		}

		@Override
		protected LatencyType getLatencyType() {
			return LatencyType.NONE;
		}

		@Override
		protected void writeBuffer() {
			dataOffset = 8;
			sizeBuffer();
		}

		@Override
		protected void parseResult(Node node, Connection conn) throws IOException {
			conn.readFully(new byte[8], 8);
		}

		@Override
		protected boolean prepareRetry(boolean timeout) {
			return true;
		}
	}

	private static Policy policy() {
		Policy policy = new Policy();
		policy.socketTimeout = TIMEOUT;
		policy.totalTimeout = TIMEOUT;
		policy.maxRetries = 0;
		policy.sleepBetweenRetries = 0;
		return policy;
	}

	/**
	 * Answers every 8-byte request with an 8-byte response, or resets the connection after reading
	 * the request when resetAfterRequest is set.
	 */
	private static final class Peer implements AutoCloseable {
		final AtomicInteger accepted = new AtomicInteger();
		final AtomicInteger requests = new AtomicInteger();
		volatile boolean resetAfterRequest;
		private final ServerSocket server;
		private final List<Socket> sockets = new ArrayList<>();

		Peer() throws IOException {
			server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());

			Thread thread = new Thread(this::accept);
			thread.setDaemon(true);
			thread.start();
		}

		int port() {
			return server.getLocalPort();
		}

		Socket last() {
			synchronized (sockets) {
				return sockets.get(sockets.size() - 1);
			}
		}

		void waitAccepted(int count) throws InterruptedException {
			long end = System.currentTimeMillis() + TIMEOUT;

			while (accepted.get() < count && System.currentTimeMillis() < end) {
				Thread.sleep(10);
			}
			assertEquals(count, accepted.get());
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
				accepted.incrementAndGet();

				Thread thread = new Thread(() -> serve(socket));
				thread.setDaemon(true);
				thread.start();
			}
		}

		private void serve(Socket socket) {
			try {
				DataInputStream in = new DataInputStream(socket.getInputStream());
				OutputStream out = socket.getOutputStream();
				byte[] request = new byte[8];

				while (true) {
					in.readFully(request);
					requests.incrementAndGet();

					if (resetAfterRequest) {
						socket.setSoLinger(true, 0);
						socket.close();
						return;
					}
					out.write(new byte[8]);
					out.flush();
				}
			}
			catch (IOException ioe) {
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
