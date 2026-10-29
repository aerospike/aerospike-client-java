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
package com.aerospike.client.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.StubNodeServer.Close;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueIoHandler;
import io.netty.channel.nio.NioIoHandler;

/**
 * Netty async commands on channels the peer closed or reset: while idle in the pool, mid-command
 * and during the send. Runs on every Netty transport the host can load.
 */
@RunWith(Parameterized.class)
public class NettyStaleChannelTest {
	private static final int TIMEOUT = 2000;
	private static final long FAST_MS = 1000;
	private static final int LARGE_BIN = 16 * 1024 * 1024;

	@Parameters(name = "{0}")
	public static List<Object[]> transports() {
		List<Object[]> list = new ArrayList<>();
		list.add(new Object[] {EventLoopType.NETTY_NIO});

		if (Epoll.isAvailable()) {
			list.add(new Object[] {EventLoopType.NETTY_EPOLL});
		}

		if (KQueue.isAvailable()) {
			list.add(new Object[] {EventLoopType.NETTY_KQUEUE});
		}
		return list;
	}

	private final EventLoopType type;
	private final AtomicInteger key = new AtomicInteger();
	private StubNodeServer server;
	private EventLoopGroup group;
	private AerospikeClient client;
	private EventLoop eventLoop;
	private Node node;

	public NettyStaleChannelTest(EventLoopType type) {
		this.type = type;
	}

	@Before
	public void setUp() throws Exception {
		server = new StubNodeServer();

		switch (type) {
		case NETTY_EPOLL:
			group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
			break;

		case NETTY_KQUEUE:
			group = new MultiThreadIoEventLoopGroup(1, KQueueIoHandler.newFactory());
			break;

		default:
			group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
			break;
		}

		NettyEventLoops eventLoops = new NettyEventLoops(new EventPolicy(), group, type);
		ClientPolicy policy = new ClientPolicy();
		policy.eventLoops = eventLoops;
		client = new AerospikeClient(policy, new Host("127.0.0.1", server.port()));
		eventLoop = eventLoops.get(0);
		node = client.getNodes()[0];
	}

	@After
	public void tearDown() throws Exception {
		if (client != null) {
			client.close();
		}

		if (group != null) {
			group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await();
		}

		if (server != null) {
			server.close();
		}
	}

	@Test
	public void failedSendIsConnectionErrorNotInDoubt() throws Exception {
		server.resetOnNextHeader();
		Outcome outcome = put(new Bin("v", new byte[LARGE_BIN]));

		assertTrue("Expected connection error: " + outcome.error, outcome.error instanceof AerospikeException.Connection);
		assertFalse("Failed send must not be in doubt: " + outcome.error, outcome.error.getInDoubt());
		assertFast(outcome);
	}

	@Test
	public void peerCloseMidCommandFailsFast() throws Exception {
		server.closeOnNextRequest(Close.FIN);
		Outcome outcome = get();

		assertTrue("Expected connection error: " + outcome.error, outcome.error instanceof AerospikeException.Connection);
		assertFast(outcome);
	}

	@Test
	public void staleClosedChannelRetriedOnNewConnection() throws Exception {
		warm();
		server.closeDataConnections(Close.FIN);
		Thread.sleep(200);

		Counts before = new Counts();
		Outcome outcome = get();

		assertNull("Read on closed pooled channel failed: " + outcome.error, outcome.error);
		before.assertStaleRetry();
	}

	@Test
	public void staleResetChannelRetriedOnNewConnection() throws Exception {
		warm();
		server.closeDataConnections(Close.RST);
		Thread.sleep(200);

		Counts before = new Counts();
		Outcome outcome = put(new Bin("v", 1));

		assertNull("Write on reset pooled channel failed: " + outcome.error, outcome.error);
		before.assertStaleRetry();

		server.closeDataConnections(Close.RST);
		Thread.sleep(200);

		before = new Counts();
		outcome = get();

		assertNull("Read on reset pooled channel failed: " + outcome.error, outcome.error);
		before.assertStaleRetry();
	}

	@Test
	public void staleRetryAtMostOncePerAttempt() throws Exception {
		warm();
		server.closeDataConnections(Close.RST);
		Thread.sleep(200);
		server.resetOnNextHeader();

		Counts before = new Counts();
		Outcome outcome = put(new Bin("v", new byte[LARGE_BIN]));

		assertTrue("Expected connection error: " + outcome.error, outcome.error instanceof AerospikeException.Connection);
		assertFalse("Failed send must not be in doubt: " + outcome.error, outcome.error.getInDoubt());
		assertEquals(1, outcome.error.getIteration());
		assertEquals("New connections", 1, server.dataConnections() - before.connections);
		assertFast(outcome);
	}

	@Test
	public void sentWriteNotResent() throws Exception {
		for (Close close : Close.values()) {
			warm();
			server.closeOnNextRequest(close);

			Counts before = new Counts();
			Outcome outcome = put(new Bin("v", 1));

			assertNotNull(close + ": sent write succeeded", outcome.error);
			assertTrue(close + ": sent write must be in doubt: " + outcome.error, outcome.error.getInDoubt());
			assertEquals(close + ": requests", 1, server.dataRequests() - before.requests);
			assertEquals(close + ": new connections", 0, server.dataConnections() - before.connections);
			assertFast(outcome);

			if (close == Close.FIN) {
				assertTrue("Expected connection error: " + outcome.error, outcome.error instanceof AerospikeException.Connection);
			}
		}
	}

	@Test
	public void healthyPooledChannelReused() throws Exception {
		warm();

		Counts before = new Counts();
		assertNull(put(new Bin("v", 1)).error);
		assertNull(get().error);

		assertEquals("New connections", 0, server.dataConnections() - before.connections);
		assertEquals("Requests", 2, server.dataRequests() - before.requests);
		assertEquals("Retries", before.retries, client.getClusterStats().retryCount);
		assertEquals("Node errors", before.errors, node.getErrorCount());
		assertEquals(1, pooled());
	}

	private void warm() throws Exception {
		Outcome outcome = put(new Bin("v", 0));
		assertNull("Warm-up failed: " + outcome.error, outcome.error);
		assertEquals(1, pooled());
	}

	private void assertFast(Outcome outcome) {
		assertTrue("Took " + outcome.ms + "ms: " + outcome.error, outcome.ms < FAST_MS);
	}

	private int pooled() throws Exception {
		CompletableFuture<Integer> size = new CompletableFuture<>();
		eventLoop.execute(() -> size.complete(node.getAsyncPool(0).queue.size()));
		return size.get(5, TimeUnit.SECONDS);
	}

	private int errorRate() throws Exception {
		Field field = Node.class.getDeclaredField("errorRateCount");
		field.setAccessible(true);
		return ((AtomicInteger)field.get(node)).get();
	}

	private Outcome put(Bin bin) throws Exception {
		WritePolicy policy = new WritePolicy();
		policy.maxRetries = 0;
		policy.socketTimeout = 0;
		policy.totalTimeout = TIMEOUT;

		CompletableFuture<Object> future = new CompletableFuture<>();
		long begin = System.nanoTime();
		client.put(eventLoop, new WriteListener() {
			@Override
			public void onSuccess(Key key) {
				future.complete(key);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		}, policy, nextKey(), bin);
		return new Outcome(future, begin);
	}

	private Outcome get() throws Exception {
		Policy policy = new Policy();
		policy.maxRetries = 0;
		policy.socketTimeout = 0;
		policy.totalTimeout = TIMEOUT;

		CompletableFuture<Object> future = new CompletableFuture<>();
		long begin = System.nanoTime();
		client.get(eventLoop, new RecordListener() {
			@Override
			public void onSuccess(Key key, Record record) {
				future.complete(key);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		}, policy, nextKey());
		return new Outcome(future, begin);
	}

	private Key nextKey() {
		return new Key(StubNodeServer.NAMESPACE, "s", key.incrementAndGet());
	}

	private static final class Outcome {
		final AerospikeException error;
		final long ms;

		Outcome(CompletableFuture<Object> future, long begin) throws Exception {
			AerospikeException error = null;

			try {
				future.get(TIMEOUT * 3, TimeUnit.MILLISECONDS);
			}
			catch (ExecutionException ee) {
				error = (AerospikeException)ee.getCause();
			}
			this.error = error;
			this.ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		}
	}

	private final class Counts {
		final int connections = server.dataConnections();
		final int requests = server.dataRequests();
		final long retries = client.getClusterStats().retryCount;
		final long errors = node.getErrorCount();
		final int errorRate;

		Counts() throws Exception {
			this.errorRate = errorRate();
		}

		void assertStaleRetry() throws Exception {
			assertEquals("New connections", 1, server.dataConnections() - connections);
			assertEquals("Requests", 1, server.dataRequests() - requests);
			assertEquals("Retries", retries, client.getClusterStats().retryCount);
			assertEquals("Node errors", errors, node.getErrorCount());
			assertEquals("Error rate", errorRate, errorRate());
			assertEquals(1, pooled());
		}
	}
}
