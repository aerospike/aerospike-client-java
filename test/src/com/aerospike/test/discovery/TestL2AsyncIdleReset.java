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
import static org.junit.Assert.assertTrue;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoopType;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.NodeStats;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.util.TestBase;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.nio.NioIoHandler;

/**
 * Async counterpart of {@link TestL2IdleReset} on Netty event loops, against the default
 * {@link L2Topology} in the idle-reset profile of run_l2.sh. Every toxic the case adds is removed
 * in {@link #restore()}.
 */
public class TestL2AsyncIdleReset extends TestBase {
	private static final String BLOCKER = "Async idle reset needs DISCOVERY_IDLE_RESET (run_l2.sh L2_PROFILE=idle-reset)";
	private static final String SET = "l2fa";
	private static final int EVENT_LOOPS = 2;
	private static final int WARM_CONCURRENCY = 8;
	private static final int WARM_ROUNDS = 25;
	private static final int KEY_COUNT = 100;
	private static final int WAIT_COMMAND_MS = 30000;

	private final List<String[]> toxics = new ArrayList<>();

	@After
	public void restore() throws Exception {
		for (String[] toxic : toxics) {
			L2Topology.removeToxic(toxic[0], toxic[1]);
		}
		toxics.clear();
	}

	@Test
	public void idleResetAbsorbedByPoolNettyNio() throws Exception {
		idleResetAbsorbedByPool(EventLoopType.NETTY_NIO);
	}

	@Test
	public void idleResetAbsorbedByPoolNettyEpoll() throws Exception {
		TestL2Topology.skipUnless(Epoll.isAvailable(), "NETTY_EPOLL unavailable on " + System.getProperty("os.name") +
			' ' + System.getProperty("os.arch") + ": " + Epoll.unavailabilityCause());
		idleResetAbsorbedByPool(EventLoopType.NETTY_EPOLL);
	}

	/**
	 * Idle flows torn down with RST, as an NLB tears down an idle flow. Every listener's connections
	 * are made to close with RST rather than FIN, and the listeners are then cycled while the
	 * client's async connections sit idle in the pool. With the default {@link ClientPolicy} the
	 * next async commands succeed and no command fails to the caller. A raw probe connection on
	 * every listener and the client's async connection counters prove the resets happened.
	 */
	private void idleResetAbsorbedByPool(EventLoopType type) throws Exception {
		TestL2Topology.skipUnless(! L2Topology.idleReset.isEmpty(), BLOCKER);
		List<L2Node> nodes = L2Topology.nodes();
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));
		assertEquals(new ClientPolicy().maxSocketIdle, policy.maxSocketIdle);

		EventLoopGroup group = new MultiThreadIoEventLoopGroup(EVENT_LOOPS, (type == EventLoopType.NETTY_EPOLL) ?
			EpollIoHandler.newFactory() : NioIoHandler.newFactory());
		EventLoops eventLoops = new NettyEventLoops(new EventPolicy(), group, type);
		policy.eventLoops = eventLoops;

		for (L2Node node : nodes) {
			addToxic(node.proxy, "fa-rst", "reset_peer", 0.0, "{\"timeout\":0}");
		}

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			L2Topology.assertAllPartitionsOwned("async idle reset", client);

			WritePolicy wp = client.copyWritePolicyDefault();
			Policy rp = client.copyReadPolicyDefault();
			List<String> warmErrors = new ArrayList<>();

			for (int round = 0; round < WARM_ROUNDS; round++) {
				List<CompletableFuture<Object>> futures = new ArrayList<>();

				for (int i = 0; i < WARM_CONCURRENCY; i++) {
					Key key = new Key(L2Topology.namespace, SET, "fa-" + ((round * WARM_CONCURRENCY + i) % KEY_COUNT));
					futures.add(put(client, eventLoops, wp, key, new Bin("v", round)));
					futures.add(get(client, eventLoops, rp, key));
				}

				for (CompletableFuture<Object> future : futures) {
					AerospikeException ae = await(future);

					if (ae != null) {
						warmErrors.add(ae.getClass().getSimpleName() + ' ' + TestL2Topology.messages(ae));
					}
				}
			}
			assertEquals("Warm-up errors: " + warmErrors, 0, warmErrors.size());

			List<Socket> probes = new ArrayList<>();
			long pooled = 0;

			try {
				for (L2Node node : nodes) {
					probes.add(TestL2IdleReset.probe(node));
				}

				Map<String,long[]> idle = asyncStats(client);

				for (long[] v : idle.values()) {
					pooled += v[L2Topology.IN_POOL];
				}
				System.out.println("L2-ASYNC-IDLE transport=" + type + " maxSocketIdle=" + policy.maxSocketIdle +
					" pooled=" + pooled + ' ' + L2Topology.format(idle));
				assertTrue("No idle pooled async connection to reset", pooled >= nodes.size());

				for (L2Node node : nodes) {
					L2Topology.setListener(node.proxy, false);
					L2Topology.setListener(node.proxy, true);
				}

				for (int i = 0; i < nodes.size(); i++) {
					String result = TestL2IdleReset.probeResult(probes.get(i));
					System.out.println("L2-ASYNC-PROBE transport=" + type + " listener=" + nodes.get(i).port + " result=" + result);
					assertTrue("Listener " + nodes.get(i).port + " did not reset an idle flow with RST: " + result,
						result.startsWith("RST"));
				}
			}
			finally {
				for (Socket probe : probes) {
					probe.close();
				}
			}

			ClusterStats before = client.getClusterStats();
			Map<String,long[]> start = asyncStats(client);
			List<String> failures = new ArrayList<>();
			int commands = 0;

			// The async pools are LIFO. Issue each phase at once so every idle pooled channel is checked out.
			for (boolean write : new boolean[] {true, false}) {
				List<CompletableFuture<Object>> futures = new ArrayList<>();

				for (int i = 0; i < KEY_COUNT; i++) {
					Key key = new Key(L2Topology.namespace, SET, "fa-" + i);
					futures.add(write ? put(client, eventLoops, wp, key, new Bin("v", i)) : get(client, eventLoops, rp, key));
				}

				for (int i = 0; i < KEY_COUNT; i++) {
					commands++;
					AerospikeException ae = await(futures.get(i));

					if (ae != null) {
						failures.add((write ? "put " : "get ") + i + ": " + ae.getClass().getSimpleName() + ' ' +
							TestL2Topology.messages(ae));
					}
				}
			}

			Map<String,long[]> end = asyncStats(client);
			List<String> undetected = new ArrayList<>();
			long closed = 0;
			long errors = 0;

			for (Map.Entry<String,long[]> entry : end.entrySet()) {
				long[] s = start.get(entry.getKey());
				long nodeClosed = entry.getValue()[L2Topology.CLOSED] - s[L2Topology.CLOSED];

				if (nodeClosed == 0) {
					undetected.add(entry.getKey());
				}
				closed += nodeClosed;
				errors += entry.getValue()[L2Topology.ERRORS] - s[L2Topology.ERRORS];
			}

			long retries = client.getClusterStats().retryCount - before.retryCount;
			System.out.println("L2-ASYNC-RESULT transport=" + type + " commands=" + commands + " failedToCaller=" +
				failures.size() + " pooledReset=" + pooled + " closedDelta=" + closed + " errorDelta=" + errors +
				" retryDelta=" + retries + " writeMaxRetries=" + wp.maxRetries + " readMaxRetries=" + rp.maxRetries);
			System.out.println("L2-ASYNC-STATS transport=" + type + ' ' + L2Topology.format(end));

			for (String failure : failures) {
				System.out.println("L2-ASYNC-FAILED transport=" + type + ' ' + failure);
			}
			assertTrue(type + ": no reset pooled async connection was detected on " + undetected, undetected.isEmpty());
			assertEquals(type + ": commands failed to the caller after idle RST: " + failures, 0, failures.size());
		}
		finally {
			group.shutdownGracefully(0, 0, TimeUnit.SECONDS).await();
		}
	}

	private void addToxic(String proxy, String name, String type, double toxicity, String attributes) throws Exception {
		L2Topology.addToxic(proxy, name, type, "upstream", toxicity, attributes);
		toxics.add(new String[] {proxy, name});
	}

	/**
	 * Return the async connection and error counters of every node, by node name, in the layout of
	 * {@link L2Topology#connectionStats}.
	 */
	private static Map<String,long[]> asyncStats(AerospikeClient client) {
		ClusterStats stats = client.getClusterStats();
		Map<String,long[]> map = new TreeMap<>();

		for (NodeStats node : stats.nodes) {
			map.put(node.node.getName(), new long[] {node.async.inUse, node.async.inPool, node.async.opened,
				node.async.closed, node.errorCount, node.timeoutCount});
		}
		return map;
	}

	private static CompletableFuture<Object> put(AerospikeClient client, EventLoops eventLoops, WritePolicy policy,
		Key key, Bin bin) {
		CompletableFuture<Object> future = new CompletableFuture<>();
		client.put(eventLoops.next(), new WriteListener() {
			@Override
			public void onSuccess(Key key) {
				future.complete(key);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		}, policy, key, bin);
		return future;
	}

	private static CompletableFuture<Object> get(AerospikeClient client, EventLoops eventLoops, Policy policy, Key key) {
		CompletableFuture<Object> future = new CompletableFuture<>();
		client.get(eventLoops.next(), new RecordListener() {
			@Override
			public void onSuccess(Key key, Record record) {
				future.complete(key);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				future.completeExceptionally(ae);
			}
		}, policy, key);
		return future;
	}

	private static AerospikeException await(CompletableFuture<Object> future) throws Exception {
		try {
			future.get(WAIT_COMMAND_MS, TimeUnit.MILLISECONDS);
			return null;
		}
		catch (ExecutionException ee) {
			return (AerospikeException)ee.getCause();
		}
	}
}
